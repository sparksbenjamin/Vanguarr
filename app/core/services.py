from __future__ import annotations

import json
import logging
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from sqlalchemy import desc, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.seer import SeerClient
from app.core.models import DecisionLog, RequestedMedia, TaskRun
from app.core.prompts import (
    build_decision_messages,
    build_profile_enrichment_messages,
)
from app.core.settings import Settings


logger = logging.getLogger("vanguarr.service")


class ProfileStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, username: str) -> Path:
        safe_name = quote(username, safe="-_.")
        return self.root / f"{safe_name}.txt"

    def list_profiles(self) -> list[str]:
        return sorted(unquote(path.stem) for path in self.root.glob("*.txt"))

    def read(self, username: str) -> str:
        path = self.path_for(username)
        if not path.exists():
            return self.default_block(username)
        return path.read_text(encoding="utf-8")

    def write(self, username: str, content: str) -> Path:
        path = self.path_for(username)
        body = content.strip()
        if body:
            body += "\n"
        path.write_text(body, encoding="utf-8")
        return path

    @staticmethod
    def default_block(username: str) -> str:
        return f"""[VANGUARR_PROFILE_V3]
User: {username}
Core Interests:
- Insufficient viewing history.
Recent Momentum:
- No recent signals captured yet.
Taste Signals:
- Code-driven profile will strengthen as more viewing data arrives.
Avoidance Signals:
- No reliable user-specific avoidance signal yet.
Request Bias:
- Stay conservative until more evidence is available.
"""


class VanguarrService:
    def __init__(
        self,
        *,
        settings: Settings,
        jellyfin: JellyfinClient,
        seer: SeerClient,
        llm: LLMClient,
        session_factory: sessionmaker[Session],
    ) -> None:
        self.settings = settings
        self.jellyfin = jellyfin
        self.seer = seer
        self.llm = llm
        self.session_factory = session_factory
        self.profile_store = ProfileStore(settings.profiles_dir)

    @contextmanager
    def session_scope(self) -> Session:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def list_profiles(self) -> list[str]:
        return self.profile_store.list_profiles()

    def read_profile(self, username: str) -> str:
        return self.profile_store.read(username)

    def save_profile(self, username: str, content: str) -> Path:
        return self.profile_store.write(username, content)

    def get_logs(self, *, search: str | None = None, limit: int | None = None) -> list[DecisionLog]:
        with self.session_scope() as session:
            stmt = select(DecisionLog).order_by(desc(DecisionLog.created_at))
            if search:
                like = f"%{search}%"
                stmt = stmt.where(
                    or_(
                        DecisionLog.username.ilike(like),
                        DecisionLog.media_title.ilike(like),
                        DecisionLog.reasoning.ilike(like),
                    )
                )
            stmt = stmt.limit(limit or self.settings.decision_page_size)
            return list(session.scalars(stmt))

    def get_task_runs(self, limit: int = 10) -> list[TaskRun]:
        with self.session_scope() as session:
            stmt = select(TaskRun).order_by(desc(TaskRun.started_at)).limit(limit)
            return list(session.scalars(stmt))

    async def run_profile_architect(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Profile Architect started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "profile_architect")

        updated_users: list[str] = []
        errors: list[str] = []

        try:
            users = await self.jellyfin.list_users()
            if username:
                users = [user for user in users if user.get("Name") == username]

            for user in users:
                current_username = user.get("Name", "unknown")
                try:
                    history = await self.jellyfin.get_playback_history(user["Id"], self.settings.profile_history_limit)
                    compact_history = self._build_profile_history_context(
                        history,
                        top_limit=self.settings.profile_architect_top_titles_limit,
                        recent_limit=self.settings.profile_architect_recent_momentum_limit,
                    )
                    enrichment = await self._suggest_profile_enrichment(
                        current_username,
                        compact_history,
                    )
                    new_profile = self._render_profile_block(
                        current_username,
                        compact_history,
                        enrichment=enrichment,
                    )
                    bounded_profile = self._limit_words(new_profile, max_words=500)
                    self.profile_store.write(current_username, bounded_profile)
                    updated_users.append(current_username)
                    logger.info("Profile Architect updated profile for user=%s", current_username)
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")
                    logger.exception("Profile Architect failed for user=%s", current_username)

            if not users:
                status = "error"
                summary = "No Jellyfin users matched the requested target."
            elif errors:
                status = "partial"
                summary = f"Updated {len(updated_users)} profile(s) with {len(errors)} error(s)."
            else:
                status = "success"
                summary = f"Updated {len(updated_users)} profile(s)."
        except Exception as exc:
            status = "error"
            summary = f"Profile Architect failed: {exc}"
            errors.append(str(exc))

        with self.session_scope() as session:
            self._finish_task(session, task.id, status=status, summary=summary)

        logger.info("Profile Architect finished status=%s summary=%s", status, summary)

        return {
            "engine": "profile_architect",
            "status": status,
            "summary": summary,
            "updated_users": updated_users,
            "errors": errors,
        }

    async def run_decision_engine(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Decision Engine started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "decision_engine")

        evaluated = 0
        requested = 0
        skipped = 0
        errors: list[str] = []
        exclusions = self._parse_global_exclusions()

        try:
            users = await self.jellyfin.list_users()
            if username:
                users = [user for user in users if user.get("Name") == username]

            for user in users:
                current_username = user.get("Name", "unknown")
                try:
                    history = await self.jellyfin.get_playback_history(user["Id"], self.settings.profile_history_limit)
                    profile_summary = self._build_profile_history_context(
                        history,
                        top_limit=self.settings.profile_architect_top_titles_limit,
                        recent_limit=self.settings.profile_architect_recent_momentum_limit,
                    )
                    profile_block = self.profile_store.read(current_username)
                    if profile_block.strip() == self.profile_store.default_block(current_username).strip():
                        profile_block = self._render_profile_block(current_username, profile_summary)
                    recommendation_seeds = self._select_recommendation_seeds(
                        history,
                        limit=self.settings.recommendation_seed_limit,
                    )
                    viewing_history = self._build_viewing_history_context(
                        history,
                        recommendation_seeds=recommendation_seeds,
                        profile_summary=profile_summary,
                    )
                    candidates = await self.seer.discover_candidates(
                        recommendation_seeds,
                        limit=self.settings.candidate_limit,
                    )

                    for candidate in candidates:
                        if self._is_managed_candidate(candidate):
                            skipped += 1
                            continue

                        with self.session_scope() as session:
                            if self._already_requested(session, current_username, candidate):
                                skipped += 1
                                continue

                        try:
                            llm_payload = await self.llm.generate_json(
                                messages=build_decision_messages(
                                    username=current_username,
                                    profile_block=profile_block,
                                    viewing_history=viewing_history,
                                    candidate=candidate,
                                    global_exclusions=exclusions,
                                ),
                                max_tokens=350,
                                temperature=0,
                            )

                            decision = str(llm_payload.get("decision", "IGNORE")).upper()
                            if decision not in {"REQUEST", "IGNORE"}:
                                decision = "IGNORE"

                            confidence = self._coerce_float(llm_payload.get("confidence"))
                            reasoning = str(llm_payload.get("reasoning", "No reasoning provided.")).strip()
                            should_request = decision == "REQUEST" and confidence >= self.settings.request_threshold

                            request_id: int | None = None
                            error: str | None = None
                            if should_request:
                                try:
                                    response = await self.seer.request_media(
                                        candidate["media_type"],
                                        candidate["media_id"],
                                    )
                                    request_id = response.get("id")
                                    logger.info(
                                        "Decision Engine requested media user=%s title=%s type=%s request_id=%s",
                                        current_username,
                                        candidate["title"],
                                        candidate["media_type"],
                                        request_id,
                                    )
                                except Exception as exc:
                                    error = str(exc)
                                    errors.append(f"{current_username}::{candidate['title']}: {exc}")
                                    logger.exception(
                                        "Decision Engine request failed user=%s title=%s",
                                        current_username,
                                        candidate["title"],
                                    )

                            with self.session_scope() as session:
                                if should_request and error is None:
                                    requested += 1
                                    session.add(
                                        RequestedMedia(
                                            username=current_username,
                                            media_type=candidate["media_type"],
                                            media_id=candidate["media_id"],
                                            media_title=candidate["title"],
                                            source=", ".join(candidate["sources"]),
                                            seer_request_id=request_id,
                                        )
                                    )

                                session.add(
                                    DecisionLog(
                                        username=current_username,
                                        media_type=candidate["media_type"],
                                        media_id=candidate["media_id"],
                                        media_title=candidate["title"],
                                        source=", ".join(candidate["sources"]),
                                        decision=decision,
                                        confidence=confidence,
                                        threshold=self.settings.request_threshold,
                                        requested=should_request and error is None,
                                        request_id=request_id,
                                        reasoning=reasoning,
                                        payload_json=json.dumps(candidate, ensure_ascii=True),
                                        error=error,
                                    )
                                )

                            evaluated += 1
                        except Exception as exc:
                            errors.append(f"{current_username}::{candidate.get('title', 'unknown')}: {exc}")
                            logger.exception(
                                "Decision Engine evaluation failed user=%s title=%s",
                                current_username,
                                candidate.get("title", "unknown"),
                            )
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")
                    logger.exception("Decision Engine failed while preparing user=%s", current_username)

            if not users:
                status = "error"
                summary = "No Jellyfin users matched the requested target."
            elif errors:
                status = "partial"
                summary = (
                    f"Evaluated {evaluated} candidates, requested {requested}, "
                    f"skipped {skipped}, errors {len(errors)}."
                )
            else:
                status = "success"
                summary = f"Evaluated {evaluated} candidates, requested {requested}, skipped {skipped}."
        except Exception as exc:
            status = "error"
            summary = f"Decision Engine failed: {exc}"
            errors.append(str(exc))

        with self.session_scope() as session:
            self._finish_task(session, task.id, status=status, summary=summary)

        logger.info("Decision Engine finished status=%s summary=%s", status, summary)

        return {
            "engine": "decision_engine",
            "status": status,
            "summary": summary,
            "evaluated": evaluated,
            "requested": requested,
            "skipped": skipped,
            "errors": errors,
        }

    def _start_task(self, session: Session, engine_name: str) -> TaskRun:
        task = TaskRun(engine=engine_name, status="running", summary="Task started.")
        session.add(task)
        session.commit()
        session.refresh(task)
        return task

    def _finish_task(self, session: Session, task_id: int, *, status: str, summary: str) -> None:
        task = session.get(TaskRun, task_id)
        if task is None:
            return
        task.status = status
        task.finished_at = datetime.utcnow()
        task.summary = summary
        session.add(task)

    def _already_requested(self, session: Session, username: str, candidate: dict[str, Any]) -> bool:
        stmt = select(RequestedMedia).where(
            RequestedMedia.username == username,
            RequestedMedia.media_type == candidate["media_type"],
            RequestedMedia.media_id == candidate["media_id"],
        )
        return session.scalar(stmt) is not None

    @classmethod
    def _build_profile_history_context(
        cls,
        history: list[dict[str, Any]],
        *,
        top_limit: int = 8,
        recent_limit: int = 5,
        recent_window: int = 12,
    ) -> dict[str, Any]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        genre_counts: Counter[str] = Counter()
        recent_genre_counts: Counter[str] = Counter()
        media_type_counts: Counter[str] = Counter()
        genre_pairs: Counter[tuple[str, str]] = Counter()
        recent_grouped: dict[tuple[str, str], dict[str, Any]] = {}

        for item in history:
            media_type = cls._map_history_media_type(item.get("Type")) or "other"
            title = cls._seed_title(item, media_type)
            genres = cls._normalize_genres(item.get("Genres", []), limit=6)
            key = (title, media_type)
            grouped_entry = grouped.setdefault(
                key,
                {
                    "title": title,
                    "media_type": media_type,
                    "play_count": 0,
                    "genres": [],
                    "community_rating": item.get("CommunityRating"),
                    "last_played": None,
                    "_last_played_score": 0.0,
                },
            )

            grouped_entry["play_count"] += 1
            grouped_entry["genres"] = cls._merge_unique_strings(grouped_entry["genres"], genres[:4])

            if grouped_entry.get("community_rating") is None and item.get("CommunityRating") is not None:
                grouped_entry["community_rating"] = item.get("CommunityRating")

            last_played = item.get("UserData", {}).get("LastPlayedDate")
            last_played_score = cls._to_timestamp(last_played)
            if last_played_score >= grouped_entry["_last_played_score"]:
                grouped_entry["last_played"] = last_played
                grouped_entry["_last_played_score"] = last_played_score

            if media_type in {"movie", "tv"}:
                media_type_counts[media_type] += 1

            for genre in genres:
                genre_counts[genre] += 1

            for source_genre in genres:
                for target_genre in genres:
                    if source_genre != target_genre:
                        genre_pairs[(source_genre, target_genre)] += 1

        for item in history[:recent_window]:
            media_type = cls._map_history_media_type(item.get("Type")) or "other"
            title = cls._seed_title(item, media_type)
            genres = cls._normalize_genres(item.get("Genres", []), limit=5)
            key = (title, media_type)
            recent_entry = recent_grouped.setdefault(
                key,
                {
                    "title": title,
                    "media_type": media_type,
                    "play_count": 0,
                    "genres": [],
                    "community_rating": item.get("CommunityRating"),
                    "last_played": None,
                    "_last_played_score": 0.0,
                },
            )
            recent_entry["play_count"] += 1
            recent_entry["genres"] = cls._merge_unique_strings(recent_entry["genres"], genres[:3])

            if recent_entry.get("community_rating") is None and item.get("CommunityRating") is not None:
                recent_entry["community_rating"] = item.get("CommunityRating")

            last_played = item.get("UserData", {}).get("LastPlayedDate")
            last_played_score = cls._to_timestamp(last_played)
            if last_played_score >= recent_entry["_last_played_score"]:
                recent_entry["last_played"] = last_played
                recent_entry["_last_played_score"] = last_played_score
            for genre in genres:
                recent_genre_counts[genre] += 1

        top_titles = cls._sort_profile_entries(list(grouped.values()))
        recent_momentum = cls._sort_profile_entries(list(recent_grouped.values()))

        normalized_top_titles = [cls._clean_profile_entry(item) for item in top_titles[:top_limit]]
        normalized_recent_momentum = [cls._clean_profile_entry(item) for item in recent_momentum[:recent_limit]]
        repeat_titles = [cls._clean_profile_entry(item) for item in top_titles if int(item.get("play_count") or 0) > 1][:5]
        ranked_genres = cls._rank_genres(genre_counts, recent_genre_counts)
        primary_genres = [genre for genre, _score in ranked_genres[:4]]
        secondary_genres = [genre for genre, _score in ranked_genres[4:8]]
        recent_genres = [genre for genre, _count in recent_genre_counts.most_common(4)]

        total_genre_events = sum(genre_counts.values())
        focus_share = 0.0
        if total_genre_events and primary_genres:
            focus_share = sum(genre_counts[genre] for genre in primary_genres[:3]) / total_genre_events

        return {
            "history_count": len(history),
            "unique_titles": len(grouped),
            "top_titles": normalized_top_titles,
            "top_genres": [genre for genre, _count in genre_counts.most_common(8)],
            "primary_genres": primary_genres,
            "secondary_genres": secondary_genres,
            "recent_genres": recent_genres,
            "recent_momentum": normalized_recent_momentum,
            "repeat_titles": repeat_titles,
            "format_preference": cls._determine_format_preference(media_type_counts),
            "average_top_rating": cls._average_rating(normalized_top_titles),
            "genre_focus_share": round(focus_share, 3),
            "discovery_lanes": cls._build_discovery_lanes(
                primary_genres=primary_genres,
                secondary_genres=secondary_genres,
                recent_genres=recent_genres,
                genre_pairs=genre_pairs,
            ),
        }

    @classmethod
    def _select_recommendation_seeds(
        cls,
        history: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, int], dict[str, Any]] = {}

        for item in history:
            media_type = cls._map_history_media_type(item.get("Type"))
            tmdb_id = cls._extract_tmdb_id(item)
            if media_type not in {"movie", "tv"} or tmdb_id is None:
                continue

            key = (media_type, tmdb_id)
            seed = grouped.setdefault(
                key,
                {
                    "media_type": media_type,
                    "media_id": tmdb_id,
                    "title": cls._seed_title(item, media_type),
                    "genres": [],
                    "overview": (item.get("Overview") or "")[:280],
                    "community_rating": item.get("CommunityRating"),
                    "play_count": 0,
                    "last_played": None,
                    "_last_played_score": 0.0,
                },
            )

            seed["play_count"] += 1

            genres = item.get("Genres", [])
            if genres:
                seed["genres"] = cls._merge_unique_strings(seed["genres"], genres)

            if not seed.get("overview") and item.get("Overview"):
                seed["overview"] = str(item.get("Overview"))[:280]

            if seed.get("community_rating") is None and item.get("CommunityRating") is not None:
                seed["community_rating"] = item.get("CommunityRating")

            last_played = item.get("UserData", {}).get("LastPlayedDate")
            last_played_score = cls._to_timestamp(last_played)
            if last_played_score >= seed["_last_played_score"]:
                seed["last_played"] = last_played
                seed["_last_played_score"] = last_played_score

        seeds = list(grouped.values())
        seeds.sort(
            key=lambda item: (
                -int(item.get("play_count") or 0),
                -float(item.get("_last_played_score") or 0.0),
                -float(item.get("community_rating") or 0.0),
                str(item.get("title") or "").lower(),
            )
        )

        trimmed: list[dict[str, Any]] = []
        for seed in seeds[:limit]:
            cleaned = dict(seed)
            cleaned.pop("_last_played_score", None)
            trimmed.append(cleaned)
        return trimmed

    @classmethod
    def _build_viewing_history_context(
        cls,
        history: list[dict[str, Any]],
        *,
        recommendation_seeds: list[dict[str, Any]],
        profile_summary: dict[str, Any] | None = None,
        recent_limit: int = 12,
    ) -> dict[str, Any]:
        recent_plays: list[dict[str, Any]] = []
        summary = profile_summary or cls._build_profile_history_context(history)

        for item in history[:recent_limit]:
            recent_plays.append(
                {
                    "name": item.get("SeriesName") or item.get("Name"),
                    "type": item.get("Type"),
                    "genres": item.get("Genres", [])[:4],
                    "community_rating": item.get("CommunityRating"),
                    "last_played": item.get("UserData", {}).get("LastPlayedDate"),
                }
            )

        return {
            "history_count": summary.get("history_count", len(history)),
            "top_content": recommendation_seeds,
            "top_titles": summary.get("top_titles", [])[:5],
            "top_genres": summary.get("top_genres", []),
            "primary_genres": summary.get("primary_genres", []),
            "repeat_titles": summary.get("repeat_titles", [])[:3],
            "recent_momentum": summary.get("recent_momentum", [])[:5],
            "format_preference": summary.get("format_preference", {}),
            "discovery_lanes": summary.get("discovery_lanes", []),
            "recent_plays": recent_plays,
        }

    async def _suggest_profile_enrichment(
        self,
        username: str,
        history_summary: dict[str, Any],
    ) -> dict[str, list[str]]:
        if not self.settings.profile_llm_enrichment_enabled:
            return {}
        if int(history_summary.get("history_count") or 0) == 0:
            return {}

        try:
            payload = await self.llm.generate_json(
                messages=build_profile_enrichment_messages(username, history_summary),
                max_tokens=min(
                    self.settings.profile_llm_enrichment_max_output_tokens,
                    self.settings.profile_architect_max_output_tokens,
                ),
                temperature=0.1,
            )
        except Exception as exc:
            logger.warning("Profile enrichment skipped for user=%s reason=%s", username, exc)
            return {}

        primary_genres = {
            genre.lower()
            for genre in history_summary.get("primary_genres", [])
            if isinstance(genre, str) and genre.strip()
        }
        adjacent_genres: list[str] = []
        for raw in payload.get("adjacent_genres", []):
            value = str(raw).strip()
            if value and value.lower() not in primary_genres:
                adjacent_genres.append(value)

        adjacent_themes = [str(raw).strip() for raw in payload.get("adjacent_themes", []) if str(raw).strip()]
        return {
            "adjacent_genres": self._merge_unique_strings([], adjacent_genres)[:3],
            "adjacent_themes": self._merge_unique_strings([], adjacent_themes)[:2],
        }

    @classmethod
    def _render_profile_block(
        cls,
        username: str,
        history_summary: dict[str, Any],
        *,
        enrichment: dict[str, list[str]] | None = None,
    ) -> str:
        if int(history_summary.get("history_count") or 0) == 0:
            return ProfileStore.default_block(username)

        adjacent_genres = cls._merge_unique_strings(
            list(history_summary.get("discovery_lanes", [])),
            (enrichment or {}).get("adjacent_genres", []),
        )[:3]
        adjacent_themes = cls._merge_unique_strings([], (enrichment or {}).get("adjacent_themes", []))[:2]

        lines = [
            "[VANGUARR_PROFILE_V3]",
            f"User: {username}",
            "Core Interests:",
        ]
        lines.extend(f"- {line}" for line in cls._build_core_interest_lines(history_summary))
        lines.append("Recent Momentum:")
        lines.extend(f"- {line}" for line in cls._build_recent_momentum_lines(history_summary))
        lines.append("Taste Signals:")
        lines.extend(
            f"- {line}"
            for line in cls._build_taste_signal_lines(
                history_summary,
                adjacent_genres=adjacent_genres,
                adjacent_themes=adjacent_themes,
            )
        )
        lines.append("Avoidance Signals:")
        lines.extend(f"- {line}" for line in cls._build_avoidance_lines(history_summary))
        lines.append("Request Bias:")
        lines.extend(
            f"- {line}"
            for line in cls._build_request_bias_lines(
                history_summary,
                adjacent_genres=adjacent_genres,
                adjacent_themes=adjacent_themes,
            )
        )
        return "\n".join(lines)

    @classmethod
    def _build_core_interest_lines(cls, history_summary: dict[str, Any]) -> list[str]:
        primary_genres = history_summary.get("primary_genres") or history_summary.get("top_genres", [])[:4]
        format_preference = history_summary.get("format_preference", {})
        top_titles = history_summary.get("top_titles", [])
        history_count = int(history_summary.get("history_count") or 0)
        unique_titles = int(history_summary.get("unique_titles") or 0)
        preferred = str(format_preference.get("preferred") or "balanced")
        movie_plays = int(format_preference.get("movie_plays") or 0)
        tv_plays = int(format_preference.get("tv_plays") or 0)

        lines: list[str] = []
        if primary_genres:
            lines.append(f"Primary genres: {cls._human_join(primary_genres[:4])}.")

        if preferred == "tv":
            lines.append(
                f"Format bias: series-forward, built from {history_count} plays across {unique_titles} grouped titles "
                f"({tv_plays} TV vs {movie_plays} movie plays)."
            )
        elif preferred == "movie":
            lines.append(
                f"Format bias: movie-forward, built from {history_count} plays across {unique_titles} grouped titles "
                f"({movie_plays} movie vs {tv_plays} TV plays)."
            )
        else:
            lines.append(
                f"Format bias: balanced across movies and series, built from {history_count} plays across "
                f"{unique_titles} grouped titles."
            )

        if top_titles:
            lines.append(f"Anchor titles: {cls._format_title_entries(top_titles[:3])}.")

        return lines

    @classmethod
    def _build_recent_momentum_lines(cls, history_summary: dict[str, Any]) -> list[str]:
        recent_momentum = history_summary.get("recent_momentum", [])
        recent_genres = history_summary.get("recent_genres", [])
        lines: list[str] = []

        if recent_momentum:
            lines.append(f"Active titles now: {cls._format_title_entries(recent_momentum[:3])}.")
        else:
            lines.append("No strong short-term title surge has formed yet.")

        if recent_genres:
            lines.append(f"Current genre push: {cls._human_join(recent_genres[:3])}.")
        else:
            lines.append("Recent activity is not concentrated enough to define a new lane yet.")

        return lines

    @classmethod
    def _build_taste_signal_lines(
        cls,
        history_summary: dict[str, Any],
        *,
        adjacent_genres: list[str],
        adjacent_themes: list[str],
    ) -> list[str]:
        lines = [cls._describe_engagement_style(history_summary)]

        average_top_rating = history_summary.get("average_top_rating")
        if average_top_rating is not None:
            lines.append(
                f"Top watched titles average community rating {average_top_rating}, so stronger-reviewed catalog is a positive signal."
            )

        focus_share = float(history_summary.get("genre_focus_share") or 0.0)
        if focus_share >= 0.72:
            lines.append("Genre profile is focused, so strong overlap on a few dependable lanes matters more than broad popularity.")
        elif focus_share >= 0.5:
            lines.append("Genre profile is balanced between a stable core and a smaller amount of exploration.")
        else:
            lines.append("Genre profile is broad enough to support adjacent discovery when tone and format still line up.")

        if adjacent_genres:
            lane_line = f"Add-on lanes worth testing: {cls._human_join(adjacent_genres)}."
            if adjacent_themes:
                lane_line += f" Theme hooks: {cls._human_join(adjacent_themes)}."
            lines.append(lane_line)
        elif adjacent_themes:
            lines.append(f"Adjacent theme hooks worth testing: {cls._human_join(adjacent_themes)}.")

        return lines

    @classmethod
    def _build_avoidance_lines(cls, history_summary: dict[str, Any]) -> list[str]:
        format_preference = history_summary.get("format_preference", {})
        preferred = str(format_preference.get("preferred") or "balanced")

        if preferred == "tv":
            return [
                "Lower evidence for standalone movies than for serialized TV, so films need stronger genre or franchise overlap.",
                "Treat non-engagement as unknown, not dislike, unless stronger evidence shows up in the watch history.",
            ]
        if preferred == "movie":
            return [
                "Lower evidence for long-running series than for movies, so TV picks need stronger momentum or genre overlap.",
                "Treat non-engagement as unknown, not dislike, unless stronger evidence shows up in the watch history.",
            ]
        return [
            "No strong user-specific format aversion is visible from watch history alone.",
            "Treat non-engagement as unknown, not dislike, unless stronger evidence shows up in the watch history.",
        ]

    @classmethod
    def _build_request_bias_lines(
        cls,
        history_summary: dict[str, Any],
        *,
        adjacent_genres: list[str],
        adjacent_themes: list[str],
    ) -> list[str]:
        primary_genres = history_summary.get("primary_genres") or history_summary.get("top_genres", [])[:3]
        format_preference = history_summary.get("format_preference", {})
        preferred = str(format_preference.get("preferred") or "balanced")
        lines: list[str] = []

        if primary_genres:
            lines.append(
                f"Favor candidates that match {cls._human_join(primary_genres[:3])} and connect to anchor titles or repeat-watch neighborhoods."
            )

        if preferred == "tv":
            lines.append("Give extra weight to serialized TV that matches the core genres and recent momentum.")
        elif preferred == "movie":
            lines.append("Give extra weight to movies that line up with the core genres and anchor-title neighborhoods.")
        else:
            lines.append("Use genre overlap first, then let recent momentum break ties between movies and series.")

        if adjacent_genres or adjacent_themes:
            extension = cls._human_join(adjacent_genres) if adjacent_genres else cls._human_join(adjacent_themes)
            lines.append(f"When the core match is already strong, allow controlled exploration into {extension}.")

        return lines

    @classmethod
    def _describe_engagement_style(cls, history_summary: dict[str, Any]) -> str:
        repeat_titles = history_summary.get("repeat_titles", [])
        top_titles = history_summary.get("top_titles", [])
        history_count = int(history_summary.get("history_count") or 0)
        unique_titles = int(history_summary.get("unique_titles") or 0)

        if repeat_titles:
            top_repeat_count = int(repeat_titles[0].get("play_count") or 0)
            if history_count and top_repeat_count >= max(2, history_count // 4):
                return f"Loyalty-heavy; returns to favorites like {cls._format_title_entries(repeat_titles[:2])}."
            return f"Repeat-friendly; revisits titles like {cls._format_title_entries(repeat_titles[:3])}."

        if unique_titles >= max(6, int(history_count * 0.7)):
            return f"Exploratory; sampled {unique_titles} distinct titles across {history_count} recent plays."

        if top_titles:
            return f"Balanced; keeps a stable core anchored by {cls._format_title_entries(top_titles[:2])} while still exploring."

        return "Balanced; keeps a stable core while still exploring."

    @staticmethod
    def _sort_profile_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        entries.sort(
            key=lambda item: (
                -int(item.get("play_count") or 0),
                -float(item.get("_last_played_score") or 0.0),
                str(item.get("title") or "").lower(),
            )
        )
        return entries

    @staticmethod
    def _clean_profile_entry(item: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(item)
        cleaned.pop("_last_played_score", None)
        return cleaned

    @staticmethod
    def _rank_genres(
        genre_counts: Counter[str],
        recent_genre_counts: Counter[str],
    ) -> list[tuple[str, float]]:
        ranked: list[tuple[str, float, int]] = []
        for genre in set(genre_counts) | set(recent_genre_counts):
            score = float(genre_counts.get(genre, 0)) + (float(recent_genre_counts.get(genre, 0)) * 0.75)
            ranked.append((genre, score, int(genre_counts.get(genre, 0))))

        ranked.sort(key=lambda item: (-item[1], -item[2], item[0].lower()))
        return [(genre, score) for genre, score, _count in ranked]

    @classmethod
    def _build_discovery_lanes(
        cls,
        *,
        primary_genres: list[str],
        secondary_genres: list[str],
        recent_genres: list[str],
        genre_pairs: Counter[tuple[str, str]],
        limit: int = 3,
    ) -> list[str]:
        primary_set = {genre.lower() for genre in primary_genres}
        scores: Counter[str] = Counter()

        for primary_genre in primary_genres[:3]:
            for (source_genre, target_genre), count in genre_pairs.items():
                if source_genre == primary_genre and target_genre.lower() not in primary_set:
                    scores[target_genre] += count

        for genre in recent_genres:
            if genre.lower() not in primary_set:
                scores[genre] += 2

        for genre in secondary_genres:
            if genre.lower() not in primary_set:
                scores[genre] += 1

        ranked = [genre for genre, _count in scores.most_common()]
        return cls._merge_unique_strings([], ranked)[:limit]

    @staticmethod
    def _determine_format_preference(media_type_counts: Counter[str]) -> dict[str, Any]:
        movie_plays = int(media_type_counts.get("movie", 0))
        tv_plays = int(media_type_counts.get("tv", 0))
        total_plays = movie_plays + tv_plays

        if tv_plays > movie_plays and tv_plays >= max(2, int(total_plays * 0.6)):
            preferred = "tv"
        elif movie_plays > tv_plays and movie_plays >= max(2, int(total_plays * 0.6)):
            preferred = "movie"
        else:
            preferred = "balanced"

        return {
            "preferred": preferred,
            "movie_plays": movie_plays,
            "tv_plays": tv_plays,
        }

    @staticmethod
    def _average_rating(entries: list[dict[str, Any]]) -> float | None:
        ratings = [float(item["community_rating"]) for item in entries if item.get("community_rating") is not None]
        if not ratings:
            return None
        return round(sum(ratings) / len(ratings), 1)

    @classmethod
    def _format_title_entries(cls, entries: list[dict[str, Any]]) -> str:
        parts: list[str] = []
        for entry in entries:
            title = str(entry.get("title") or entry.get("name") or "Unknown")
            play_count = int(entry.get("play_count") or 0)
            if play_count > 0:
                suffix = "play" if play_count == 1 else "plays"
                parts.append(f"{title} ({play_count} {suffix})")
            else:
                parts.append(title)
        return cls._human_join(parts)

    @staticmethod
    def _human_join(values: list[str]) -> str:
        filtered = [value.strip() for value in values if value and value.strip()]
        if not filtered:
            return ""
        if len(filtered) == 1:
            return filtered[0]
        if len(filtered) == 2:
            return f"{filtered[0]} and {filtered[1]}"
        return f"{', '.join(filtered[:-1])}, and {filtered[-1]}"

    @staticmethod
    def _limit_words(text: str, *, max_words: int) -> str:
        words = text.split()
        if len(words) <= max_words:
            return text.strip()
        return " ".join(words[:max_words]).strip()

    def _parse_global_exclusions(self) -> list[str]:
        raw = self.settings.global_exclusions.replace("\n", ",")
        return [item.strip() for item in raw.split(",") if item.strip()]

    @staticmethod
    def _is_managed_candidate(candidate: dict[str, Any]) -> bool:
        media_info = candidate.get("media_info") or {}
        status = str(media_info.get("status", "")).lower()
        return status in {"available", "partial", "processing", "pending"}

    @staticmethod
    def _coerce_float(value: Any) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _extract_tmdb_id(item: dict[str, Any]) -> int | None:
        provider_ids = item.get("ProviderIds", {})
        raw_tmdb = provider_ids.get("Tmdb") or provider_ids.get("TMDB") or provider_ids.get("tmdb")
        if raw_tmdb is None:
            return None

        try:
            return int(raw_tmdb)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _map_history_media_type(item_type: str | None) -> str | None:
        if item_type == "Movie":
            return "movie"
        if item_type in {"Series", "Episode"}:
            return "tv"
        return None

    @staticmethod
    def _seed_title(item: dict[str, Any], media_type: str) -> str:
        if media_type == "tv":
            return str(item.get("SeriesName") or item.get("Name") or "Unknown TV")
        return str(item.get("Name") or "Unknown Movie")

    @staticmethod
    def _normalize_genres(raw_genres: list[Any], *, limit: int | None = None) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()

        for raw in raw_genres:
            value = str(raw).strip()
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            normalized.append(value)
            seen.add(lowered)
            if limit is not None and len(normalized) >= limit:
                break

        return normalized

    @staticmethod
    def _merge_unique_strings(current: list[str], extra: list[Any]) -> list[str]:
        merged = list(current)
        seen = {value.lower() for value in current}
        for raw in extra:
            value = str(raw).strip()
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            merged.append(value)
            seen.add(lowered)
        return merged

    @staticmethod
    def _to_timestamp(value: Any) -> float:
        if not value:
            return 0.0

        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
