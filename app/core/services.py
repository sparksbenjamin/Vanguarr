from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.base import ClientConfigError
from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.media_server import MediaServerClientProtocol
from app.api.seer import SeerClient
from app.api.tmdb import TMDbClient
from app.core.models import (
    DecisionLog,
    LibraryMedia,
    RequestedMedia,
    SeerWebhookEvent,
    SuggestedMedia,
    TaskRun,
)
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

    def json_path_for(self, username: str) -> Path:
        safe_name = quote(username, safe="-_.")
        return self.root / f"{safe_name}.json"

    def summary_path_for(self, username: str) -> Path:
        safe_name = quote(username, safe="-_.")
        return self.root / f"{safe_name}.txt"

    def path_for(self, username: str) -> Path:
        return self.json_path_for(username)

    def list_profiles(self) -> list[str]:
        json_profiles = {unquote(path.stem) for path in self.root.glob("*.json")}
        text_profiles = {unquote(path.stem) for path in self.root.glob("*.txt")}
        return sorted(json_profiles | text_profiles)

    def read_payload(self, username: str) -> dict[str, Any]:
        json_path = self.json_path_for(username)
        if json_path.exists():
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    return self._normalize_payload(username, payload)
            except json.JSONDecodeError:
                logger.warning("Profile JSON is invalid for user=%s path=%s", username, json_path)

        summary_path = self.summary_path_for(username)
        if summary_path.exists():
            return self.legacy_payload(username, summary_path.read_text(encoding="utf-8"))

        return self.default_payload(username)

    def read_payload_text(self, username: str) -> str:
        return json.dumps(self.read_payload(username), indent=2, ensure_ascii=True)

    def read_summary(self, username: str) -> str:
        payload = self.read_payload(username)
        summary = str(payload.get("summary_block") or "").strip()
        return summary or self.default_block(username)

    def write_payload(self, username: str, payload: dict[str, Any]) -> tuple[Path, Path]:
        normalized = self._normalize_payload(username, payload)
        json_path = self.json_path_for(username)
        summary_path = self.summary_path_for(username)

        json_body = json.dumps(normalized, indent=2, ensure_ascii=True).strip()
        if json_body:
            json_body += "\n"
        json_path.write_text(json_body, encoding="utf-8")

        summary_body = str(normalized.get("summary_block") or "").strip()
        if summary_body:
            summary_body += "\n"
        summary_path.write_text(summary_body, encoding="utf-8")

        return json_path, summary_path

    def _normalize_payload(self, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self.default_payload(username)
        normalized.update(payload)
        normalized["profile_version"] = str(normalized.get("profile_version") or "v5")
        normalized["username"] = username
        summary = str(normalized.get("summary_block") or "").strip()
        normalized["summary_block"] = summary or self.default_block(username)
        return normalized

    @classmethod
    def default_payload(cls, username: str) -> dict[str, Any]:
        return {
            "profile_version": "v5",
            "profile_state": "default",
            "username": username,
            "generated_at": None,
            "history_count": 0,
            "unique_titles": 0,
            "top_titles": [],
            "top_genres": [],
            "ranked_genres": [],
            "primary_genres": [],
            "secondary_genres": [],
            "recent_genres": [],
            "recent_momentum": [],
            "repeat_titles": [],
            "format_preference": {"preferred": "balanced", "movie_plays": 0, "tv_plays": 0},
            "release_year_preference": {"bias": "balanced", "average_year": None},
            "average_top_rating": None,
            "genre_focus_share": 0.0,
            "discovery_lanes": [],
            "adjacent_genres": [],
            "adjacent_themes": [],
            "seed_lanes": [],
            "explicit_feedback": {
                "liked_titles": [],
                "disliked_titles": [],
                "liked_genres": [],
                "disliked_genres": [],
            },
            "profile_exclusions": [],
            "operator_notes": "",
            "top_keywords": [],
            "favorite_people": [],
            "preferred_brands": [],
            "favorite_collections": [],
            "summary_block": cls.default_block(username),
        }

    @classmethod
    def legacy_payload(cls, username: str, summary_text: str) -> dict[str, Any]:
        payload = cls.default_payload(username)
        payload["profile_state"] = "legacy_text"
        summary = summary_text.strip()
        if summary:
            payload["summary_block"] = summary
            payload["legacy_summary_text"] = summary
        return payload

    @staticmethod
    def is_structured_payload(payload: dict[str, Any]) -> bool:
        if str(payload.get("profile_state") or "") in {"default", "legacy_text"}:
            return False
        return int(payload.get("history_count") or 0) > 0

    @staticmethod
    def default_block(username: str) -> str:
        return f"""[VANGUARR_PROFILE_SUMMARY_V1]
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
        media_server: MediaServerClientProtocol,
        seer: SeerClient,
        tmdb: TMDbClient,
        llm: LLMClient,
        session_factory: sessionmaker[Session],
    ) -> None:
        self.settings = settings
        self.media_server = media_server
        self.seer = seer
        self.tmdb = tmdb
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
        return self.profile_store.read_payload_text(username)

    def read_profile_summary(self, username: str) -> str:
        return self.profile_store.read_summary(username)

    def save_profile(self, username: str, content: str) -> Path:
        payload = json.loads(content)
        if not isinstance(payload, dict):
            raise ValueError("Profile manifest must be a JSON object.")
        normalized = self._normalize_saved_profile_payload(username, payload)
        json_path, _summary_path = self.profile_store.write_payload(username, normalized)
        return json_path

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

    def recover_interrupted_tasks(self) -> int:
        with self.session_scope() as session:
            running_tasks = list(
                session.scalars(
                    select(TaskRun).where(
                        TaskRun.status == "running",
                        TaskRun.finished_at.is_(None),
                    )
                )
            )
            recovered_at = datetime.utcnow()

            for task in running_tasks:
                previous_summary = str(task.summary or "").strip()
                task.status = "interrupted"
                task.finished_at = recovered_at
                task.summary = (
                    f"{previous_summary} Recovered as interrupted after a restart before completion."
                    if previous_summary
                    else "Recovered as interrupted after a restart before completion."
                )
                session.add(task)

        if running_tasks:
            logger.warning(
                "Recovered %s interrupted task run(s) left in running state from a previous process.",
                len(running_tasks),
            )
        return len(running_tasks)

    def get_recent_requests(self, limit: int = 8) -> list[RequestedMedia]:
        with self.session_scope() as session:
            stmt = select(RequestedMedia).order_by(desc(RequestedMedia.created_at)).limit(limit)
            return list(session.scalars(stmt))

    def get_suggestions(
        self,
        *,
        username: str | None = None,
        jellyfin_user_id: str | None = None,
        limit: int | None = None,
    ) -> list[SuggestedMedia]:
        with self.session_scope() as session:
            stmt = select(SuggestedMedia).order_by(SuggestedMedia.rank.asc(), SuggestedMedia.score.desc())
            if jellyfin_user_id:
                stmt = stmt.where(SuggestedMedia.jellyfin_user_id == jellyfin_user_id)
            elif username:
                stmt = stmt.where(SuggestedMedia.username == username)
            else:
                return []
            if limit is not None:
                stmt = stmt.limit(limit)
            return list(session.scalars(stmt))

    def get_library_sync_snapshot(self) -> dict[str, Any]:
        with self.session_scope() as session:
            total_items = int(session.scalar(select(func.count(LibraryMedia.id))) or 0)
            available_items = int(
                session.scalar(
                    select(func.count(LibraryMedia.id)).where(LibraryMedia.state == "available")
                )
                or 0
            )
            removed_items = int(
                session.scalar(
                    select(func.count(LibraryMedia.id)).where(LibraryMedia.state == "removed")
                )
                or 0
            )
            movies = int(
                session.scalar(
                    select(func.count(LibraryMedia.id)).where(
                        LibraryMedia.state == "available",
                        LibraryMedia.media_type == "movie",
                    )
                )
                or 0
            )
            series = int(
                session.scalar(
                    select(func.count(LibraryMedia.id)).where(
                        LibraryMedia.state == "available",
                        LibraryMedia.media_type == "tv",
                    )
                )
                or 0
            )
            last_seen_at = session.scalar(select(func.max(LibraryMedia.last_seen_at)))
            last_task = session.scalar(
                select(TaskRun)
                .where(TaskRun.engine == "library_sync")
                .order_by(desc(TaskRun.started_at))
                .limit(1)
            )

        return {
            "total_items": total_items,
            "available_items": available_items,
            "removed_items": removed_items,
            "movies": movies,
            "series": series,
            "last_seen_at": last_seen_at,
            "last_task": last_task,
            "task_status": self.get_task_snapshot("library_sync"),
        }

    def get_task_snapshot(self, engine_name: str) -> dict[str, Any]:
        with self.session_scope() as session:
            task = session.scalar(
                select(TaskRun)
                .where(TaskRun.engine == engine_name)
                .order_by(desc(TaskRun.started_at))
                .limit(1)
            )
        return self._serialize_task_run(task)

    async def install_jellyfin_plugin(self) -> dict[str, Any]:
        if self.settings.normalized_media_server_provider != "jellyfin":
            raise ClientConfigError(
                "Jellyfin plugin install is only available when Jellyfin is the active media server."
            )

        client = self._jellyfin_client()
        return await client.install_vanguarr_plugin()

    def get_profile_cards(self, limit: int = 6) -> list[dict[str, Any]]:
        cards: list[dict[str, Any]] = []
        for username in self.list_profiles()[:limit]:
            payload = self.profile_store.read_payload(username)
            top_titles = [
                str(item.get("title") or "").strip()
                for item in payload.get("top_titles", [])[:2]
                if isinstance(item, dict) and str(item.get("title") or "").strip()
            ]
            recent_titles = [
                str(item.get("title") or "").strip()
                for item in payload.get("recent_momentum", [])[:2]
                if isinstance(item, dict) and str(item.get("title") or "").strip()
            ]
            cards.append(
                {
                    "username": username,
                    "profile_state": str(payload.get("profile_state") or "default"),
                    "history_count": int(payload.get("history_count") or 0),
                    "primary_genres": self._normalize_string_list(payload.get("primary_genres", []), limit=3),
                    "adjacent_genres": self._normalize_string_list(payload.get("adjacent_genres", []), limit=2),
                    "favorite_people": self._normalize_string_list(payload.get("favorite_people", []), limit=2),
                    "top_titles": top_titles,
                    "recent_titles": recent_titles,
                    "format_preference": str((payload.get("format_preference") or {}).get("preferred") or "balanced"),
                }
            )
        return cards

    def get_dashboard_snapshot(self) -> dict[str, Any]:
        now = datetime.utcnow()
        week_ago = now - timedelta(days=7)
        with self.session_scope() as session:
            total_requests = int(session.scalar(select(func.count(RequestedMedia.id))) or 0)
            total_decisions = int(session.scalar(select(func.count(DecisionLog.id))) or 0)
            request_users = int(session.scalar(select(func.count(func.distinct(RequestedMedia.username)))) or 0)
            requests_last_7d = int(
                session.scalar(
                    select(func.count(RequestedMedia.id)).where(RequestedMedia.created_at >= week_ago)
                )
                or 0
            )
            last_request_at = session.scalar(select(func.max(RequestedMedia.created_at)))
            last_decision_at = session.scalar(select(func.max(DecisionLog.created_at)))
            request_failures = int(
                session.scalar(select(func.count(DecisionLog.id)).where(DecisionLog.error.is_not(None))) or 0
            )

        profiles = self.list_profiles()
        request_rate = round((total_requests / total_decisions) * 100, 1) if total_decisions else 0.0

        return {
            "tracked_profiles": len(profiles),
            "total_requests": total_requests,
            "total_decisions": total_decisions,
            "request_rate": request_rate,
            "request_users": request_users,
            "requests_last_7d": requests_last_7d,
            "request_failures": request_failures,
            "last_request_at": last_request_at,
            "last_decision_at": last_decision_at,
            "recent_requests": self.get_recent_requests(limit=6),
            "profile_cards": self.get_profile_cards(limit=6),
        }

    async def run_profile_architect(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Profile Architect started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "profile_architect")

        updated_users: list[str] = []
        suggestion_refreshes = 0
        suggestion_targets: list[dict[str, Any]] = []
        errors: list[str] = []

        try:
            users = await self.media_server.list_users()
            if username:
                users = [user for user in users if user.get("Name") == username]

            for user in users:
                current_username = user.get("Name", "unknown")
                try:
                    history = await self.media_server.get_playback_history(
                        user["Id"],
                        self.settings.profile_history_limit,
                    )
                    stored_payload = self.profile_store.read_payload(current_username)
                    compact_history = self._build_profile_history_context(
                        history,
                        top_limit=self.settings.profile_architect_top_titles_limit,
                        recent_limit=self.settings.profile_architect_recent_momentum_limit,
                    )
                    recommendation_seeds = self._build_recommendation_seed_pool(
                        history,
                        profile_summary=compact_history,
                        limit=self.settings.recommendation_seed_limit,
                    )
                    compact_history = await self._enrich_profile_summary_with_tmdb(
                        compact_history,
                        recommendation_seeds=recommendation_seeds,
                    )
                    enrichment = await self._suggest_profile_enrichment(
                        current_username,
                        compact_history,
                    )
                    profile_payload = self._build_profile_payload(
                        current_username,
                        compact_history,
                        enrichment=enrichment,
                        existing_payload=stored_payload,
                    )
                    self.profile_store.write_payload(current_username, profile_payload)
                    updated_users.append(current_username)
                    suggestion_targets.append(user)
                    logger.info("Profile Architect updated profile for user=%s", current_username)
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")
                    logger.exception("Profile Architect failed for user=%s", current_username)

            if self.settings.suggestions_enabled:
                for user in suggestion_targets:
                    current_username = str(user.get("Name") or "unknown")
                    try:
                        await self._refresh_user_suggestions(user)
                        suggestion_refreshes += 1
                    except Exception as exc:
                        errors.append(f"{current_username} suggestions: {exc}")
                        logger.exception(
                            "Profile Architect follow-up suggestion refresh failed for user=%s",
                            current_username,
                        )

            if not users:
                status = "error"
                summary = f"No {self.settings.media_server_label} users matched the requested target."
            elif errors:
                status = "partial"
                summary = (
                    f"Updated {len(updated_users)} profile(s), refreshed {suggestion_refreshes} "
                    f"suggestion snapshot(s), with {len(errors)} error(s)."
                )
            else:
                status = "success"
                summary = (
                    f"Updated {len(updated_users)} profile(s) and refreshed "
                    f"{suggestion_refreshes} suggestion snapshot(s)."
                )
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
            "suggestion_refreshes": suggestion_refreshes,
            "errors": errors,
        }

    async def run_decision_engine(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Decision Engine started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "decision_engine")

        scored = 0
        shortlisted = 0
        evaluated = 0
        requested = 0
        skipped = 0
        errors: list[str] = []
        exclusions = self._parse_global_exclusions()

        try:
            users = await self.media_server.list_users()
            if username:
                users = [user for user in users if user.get("Name") == username]

            for user in users:
                current_username = user.get("Name", "unknown")
                try:
                    history = await self.media_server.get_playback_history(
                        user["Id"],
                        self.settings.profile_history_limit,
                    )
                    history_summary = self._build_profile_history_context(
                        history,
                        top_limit=self.settings.profile_architect_top_titles_limit,
                        recent_limit=self.settings.profile_architect_recent_momentum_limit,
                    )
                    stored_profile = self.profile_store.read_payload(current_username)
                    recommendation_seeds = self._build_recommendation_seed_pool(
                        history,
                        profile_summary=history_summary,
                        limit=self.settings.recommendation_seed_limit,
                    )
                    history_summary = await self._enrich_profile_summary_with_tmdb(
                        history_summary,
                        recommendation_seeds=recommendation_seeds,
                    )
                    profile_payload = self._build_profile_payload(
                        current_username,
                        history_summary,
                        enrichment={
                            "adjacent_genres": stored_profile.get("adjacent_genres", []),
                            "adjacent_themes": stored_profile.get("adjacent_themes", []),
                        },
                        existing_payload=stored_profile,
                    )
                    if not ProfileStore.is_structured_payload(stored_profile) and int(
                        profile_payload.get("history_count") or 0
                    ) > 0:
                        self.profile_store.write_payload(current_username, profile_payload)

                    viewing_history = self._build_viewing_history_context(
                        history,
                        recommendation_seeds=recommendation_seeds,
                        profile_summary=profile_payload,
                    )
                    genre_seeds = self._build_genre_discovery_seeds(profile_payload)
                    candidate_pool = await self.seer.discover_candidates(
                        recommendation_seeds,
                        genre_seeds=genre_seeds,
                        limit=self.settings.candidate_limit,
                        genre_limit=self.settings.genre_candidate_limit,
                        trending_limit=self.settings.trending_candidate_limit,
                    )
                    watched_media_keys = self._build_watched_media_keys(history)
                    ranked_candidates = self._rank_candidate_pool(
                        candidate_pool,
                        profile_summary=profile_payload,
                    )

                    with self.session_scope() as session:
                        requested_media_keys = self._requested_media_keys(session, current_username)

                    filtered_candidates: list[dict[str, Any]] = []
                    for candidate in ranked_candidates:
                        scored += 1
                        if self._is_managed_candidate(candidate):
                            skipped += 1
                            continue
                        if self._candidate_key(candidate) in watched_media_keys:
                            skipped += 1
                            continue
                        if self._candidate_key(candidate) in requested_media_keys:
                            skipped += 1
                            continue

                        deterministic_score = float(
                            candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0
                        )
                        if deterministic_score < self._decision_prefilter_threshold():
                            skipped += 1
                            continue

                        filtered_candidates.append(candidate)

                    filtered_candidates = await self._enrich_candidate_pool_with_tmdb(
                        filtered_candidates,
                        limit=self.settings.tmdb_candidate_enrichment_limit,
                    )
                    filtered_candidates = self._rank_candidate_pool(
                        filtered_candidates,
                        profile_summary=profile_payload,
                    )

                    candidates = self._diversify_candidates(
                        filtered_candidates,
                        limit=self.settings.decision_shortlist_limit,
                    )
                    shortlisted += len(candidates)

                    for candidate in candidates:
                        try:
                            deterministic_score = float(
                                candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0
                            )
                            llm_vote = "UNAVAILABLE"
                            llm_confidence: float | None = None
                            llm_reasoning = ""

                            try:
                                llm_payload = await self.llm.generate_json(
                                    messages=build_decision_messages(
                                        username=current_username,
                                        profile_payload=profile_payload,
                                        viewing_history=viewing_history,
                                        candidate=candidate,
                                        global_exclusions=exclusions,
                                    ),
                                    temperature=0,
                                    purpose="decision",
                                )
                                llm_vote = str(llm_payload.get("decision", "IGNORE")).upper()
                                if llm_vote not in {"REQUEST", "IGNORE"}:
                                    llm_vote = "IGNORE"
                                llm_confidence = self._coerce_float(llm_payload.get("confidence"))
                                llm_reasoning = str(llm_payload.get("reasoning", "No reasoning provided.")).strip()
                            except Exception as exc:
                                logger.warning(
                                    "Decision Engine LLM fallback triggered user=%s title=%s reason=%s",
                                    current_username,
                                    candidate.get("title", "unknown"),
                                    exc,
                                )

                            confidence = self._blend_confidences(
                                deterministic_score=deterministic_score,
                                llm_confidence=llm_confidence,
                                llm_vote=llm_vote,
                                llm_weight_percent=self.settings.decision_ai_weight_percent,
                            )
                            should_request = confidence >= self.settings.request_threshold
                            decision = "REQUEST" if should_request else "IGNORE"
                            reasoning = self._compose_decision_reasoning(
                                candidate,
                                deterministic_score=deterministic_score,
                                hybrid_confidence=confidence,
                                decision=decision,
                                request_threshold=self.settings.request_threshold,
                                llm_vote=llm_vote,
                                llm_reasoning=llm_reasoning,
                            )

                            request_id: int | None = None
                            error: str | None = None
                            if should_request:
                                try:
                                    response = await self.seer.request_media(
                                        candidate["media_type"],
                                        candidate["media_id"],
                                    )
                                    request_id = response.get("id")
                                    requested_media_keys.add(self._candidate_key(candidate))
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
                summary = f"No {self.settings.media_server_label} users matched the requested target."
            elif errors:
                status = "partial"
                summary = (
                    f"Scored {scored} candidates, shortlisted {shortlisted}, evaluated {evaluated}, requested {requested}, "
                    f"skipped {skipped}, errors {len(errors)}."
                )
            else:
                status = "success"
                summary = (
                    f"Scored {scored} candidates, shortlisted {shortlisted}, "
                    f"evaluated {evaluated}, requested {requested}, skipped {skipped}."
                )
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
            "scored": scored,
            "shortlisted": shortlisted,
            "evaluated": evaluated,
            "requested": requested,
            "skipped": skipped,
            "errors": errors,
        }

    async def run_suggested_for_you(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Suggested For You refresh started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "suggested_for_you")

        refreshed_users: list[str] = []
        stored = 0
        scored = 0
        errors: list[str] = []

        try:
            if not self.settings.suggestions_enabled:
                status = "success"
                summary = "Suggested For You is disabled in runtime settings."
            else:
                users = await self.media_server.list_users()
                if username:
                    users = [user for user in users if user.get("Name") == username]

                for user in users:
                    current_username = str(user.get("Name") or "unknown")
                    try:
                        result = await self._refresh_user_suggestions(user)
                        refreshed_users.append(current_username)
                        stored += int(result.get("stored") or 0)
                        scored += int(result.get("scored") or 0)
                    except Exception as exc:
                        errors.append(f"{current_username}: {exc}")
                        logger.exception("Suggested For You refresh failed for user=%s", current_username)

                if not users:
                    status = "error"
                    summary = f"No {self.settings.media_server_label} users matched the requested target."
                elif errors:
                    status = "partial"
                    summary = (
                        f"Refreshed {len(refreshed_users)} user suggestion snapshot(s), "
                        f"stored {stored} suggestion(s), scored {scored} available item(s), errors {len(errors)}."
                    )
                else:
                    status = "success"
                    summary = (
                        f"Refreshed {len(refreshed_users)} user suggestion snapshot(s), "
                        f"stored {stored} suggestion(s), scored {scored} available item(s)."
                    )
        except Exception as exc:
            status = "error"
            summary = f"Suggested For You refresh failed: {exc}"
            errors.append(str(exc))

        with self.session_scope() as session:
            self._finish_task(session, task.id, status=status, summary=summary)

        logger.info("Suggested For You refresh finished status=%s summary=%s", status, summary)

        return {
            "engine": "suggested_for_you",
            "status": status,
            "summary": summary,
            "refreshed_users": refreshed_users,
            "stored": stored,
            "scored": scored,
            "errors": errors,
        }

    async def run_library_sync(self) -> dict[str, Any]:
        logger.info("Library Sync started.")
        with self.session_scope() as session:
            task = self._start_task(session, "library_sync")

        indexed = 0
        added = 0
        updated = 0
        removed = 0
        skipped = 0
        refreshed_users: list[str] = []
        errors: list[str] = []
        sync_libraries: list[dict[str, Any]] = []
        suggestion_refresh: dict[str, Any] = {
            "state": "pending",
            "completed_users": 0,
            "total_users": 0,
        }

        try:
            client = self._jellyfin_client()
            allow_removals = True
            now = datetime.utcnow()
            now = datetime.utcnow()
            seen_ids: set[str] = set()
            normalized_payloads: list[dict[str, Any]] = []
            raw_folders: list[dict[str, Any]] = []

            try:
                raw_folders = await client.get_library_folders()
            except Exception as exc:
                allow_removals = False
                errors.append(f"Could not enumerate Jellyfin libraries: {exc}")
                logger.exception("Library Sync could not enumerate Jellyfin libraries.")

            normalized_folders = [
                folder
                for folder in (self._normalize_library_folder(item) for item in raw_folders)
                if folder is not None
            ]

            if normalized_folders:
                sync_libraries = [
                    {
                        **folder,
                        "state": "pending",
                        "items_discovered": 0,
                        "indexed": 0,
                        "skipped": 0,
                        "error": "",
                    }
                    for folder in normalized_folders
                ]
            else:
                sync_libraries = [
                    {
                        "id": "all-libraries",
                        "item_id": None,
                        "name": "All Libraries",
                        "collection_type": "mixed",
                        "state": "pending",
                        "items_discovered": 0,
                        "indexed": 0,
                        "skipped": 0,
                        "error": "",
                    }
                ]

            total_progress_steps = len(sync_libraries) + (1 if self.settings.suggestions_enabled else 0)
            self._update_task(
                task.id,
                status="running",
                summary=f"Starting Jellyfin library sync across {len(sync_libraries)} librar{'y' if len(sync_libraries) == 1 else 'ies'}.",
                progress_current=0,
                progress_total=total_progress_steps,
                current_label="Preparing library sync",
                detail_payload={
                    "phase": "indexing",
                    "libraries": sync_libraries,
                    "suggestion_refresh": suggestion_refresh,
                },
            )

            for index, library in enumerate(sync_libraries):
                library["state"] = "running"
                self._update_task(
                    task.id,
                    status="running",
                    summary=f"Indexing {library['name']} ({index + 1}/{len(sync_libraries)}).",
                    progress_current=index,
                    progress_total=total_progress_steps,
                    current_label=str(library["name"]),
                    detail_payload={
                        "phase": "indexing",
                        "libraries": sync_libraries,
                        "suggestion_refresh": suggestion_refresh,
                    },
                )

                try:
                    items = await client.get_library_items(parent_id=library.get("item_id"))
                    library["items_discovered"] = len(items)
                    library_indexed = 0
                    library_skipped = 0

                    for item in items:
                        payload = self._library_item_to_sync_payload(item)
                        if payload is None:
                            skipped += 1
                            library_skipped += 1
                            continue
                        seen_ids.add(str(payload["media_server_id"]))
                        normalized_payloads.append(payload)
                        library_indexed += 1

                    library["indexed"] = library_indexed
                    library["skipped"] = library_skipped
                    library["state"] = "success"
                except Exception as exc:
                    library["state"] = "error"
                    library["error"] = str(exc)
                    allow_removals = False
                    errors.append(f"{library['name']}: {exc}")
                    logger.exception("Library Sync failed while indexing library=%s", library["name"])

                completed_libraries = sum(
                    1 for current in sync_libraries if current.get("state") in {"success", "error"}
                )
                self._update_task(
                    task.id,
                    status="running",
                    summary=f"Finished {library['name']}. {completed_libraries}/{len(sync_libraries)} libraries processed.",
                    progress_current=completed_libraries,
                    progress_total=total_progress_steps,
                    current_label=str(library["name"]),
                    detail_payload={
                        "phase": "indexing",
                        "libraries": sync_libraries,
                        "suggestion_refresh": suggestion_refresh,
                    },
                )

            with self.session_scope() as session:
                existing_rows = {
                    row.media_server_id: row
                    for row in session.scalars(select(LibraryMedia).where(LibraryMedia.source_provider == "jellyfin"))
                }

                for payload in normalized_payloads:
                    media_server_id = str(payload["media_server_id"])
                    row = existing_rows.get(media_server_id)
                    if row is None:
                        row = LibraryMedia(
                            source_provider="jellyfin",
                            media_server_id=media_server_id,
                        )
                        session.add(row)
                        added += 1
                    else:
                        updated += 1

                    row.media_type = str(payload["media_type"])
                    row.title = str(payload["title"])
                    row.sort_title = str(payload["sort_title"])
                    row.overview = str(payload["overview"])
                    row.production_year = payload["production_year"]
                    row.release_date = payload["release_date"]
                    row.community_rating = payload["community_rating"]
                    row.genres_json = json.dumps(payload["genres"], ensure_ascii=True)
                    row.state = "available"
                    row.tmdb_id = payload["tmdb_id"]
                    row.tvdb_id = payload["tvdb_id"]
                    row.imdb_id = payload["imdb_id"]
                    row.last_seen_at = now
                    row.payload_json = str(payload["payload_json"])
                    indexed += 1

                if allow_removals:
                    for media_server_id, row in existing_rows.items():
                        if media_server_id in seen_ids or row.state == "removed":
                            continue
                        row.state = "removed"
                        removed += 1

            if self.settings.suggestions_enabled:
                users = await self.media_server.list_users()
                suggestion_refresh["state"] = "running"
                suggestion_refresh["total_users"] = len(users)
                processed_users = 0
                self._update_task(
                    task.id,
                    status="running",
                    summary="Refreshing per-user suggestion snapshots.",
                    progress_current=len(sync_libraries),
                    progress_total=total_progress_steps,
                    current_label="Refreshing suggestions",
                    detail_payload={
                        "phase": "refreshing_suggestions",
                        "libraries": sync_libraries,
                        "suggestion_refresh": suggestion_refresh,
                    },
                )
                for user in users:
                    current_username = str(user.get("Name") or "unknown")
                    try:
                        await self._refresh_user_suggestions(user)
                        refreshed_users.append(current_username)
                    except Exception as exc:
                        errors.append(f"{current_username}: {exc}")
                        logger.exception(
                            "Library Sync suggestion refresh failed for user=%s",
                            current_username,
                        )
                    finally:
                        processed_users += 1
                        suggestion_refresh["completed_users"] = processed_users
                        self._update_task(
                            task.id,
                            status="running",
                            summary=(
                                f"Refreshing suggestions for {processed_users}/"
                                f"{suggestion_refresh['total_users']} users."
                            ),
                            progress_current=len(sync_libraries),
                            progress_total=total_progress_steps,
                            current_label="Refreshing suggestions",
                            detail_payload={
                                "phase": "refreshing_suggestions",
                                "libraries": sync_libraries,
                                "suggestion_refresh": suggestion_refresh,
                            },
                        )
                suggestion_refresh["state"] = "success" if len(refreshed_users) == len(users) else "partial"
            else:
                suggestion_refresh["state"] = "disabled"

            if errors:
                status = "partial"
                summary = (
                    f"Indexed {indexed} Jellyfin item(s), added {added}, updated {updated}, "
                    f"removed {removed}, skipped {skipped}, refreshed {len(refreshed_users)} suggestion snapshot(s), "
                    f"errors {len(errors)}."
                )
            else:
                status = "success"
                summary = (
                    f"Indexed {indexed} Jellyfin item(s), added {added}, updated {updated}, "
                    f"removed {removed}, skipped {skipped}, refreshed {len(refreshed_users)} suggestion snapshot(s)."
                )
        except Exception as exc:
            status = "error"
            summary = f"Library Sync failed: {exc}"
            errors.append(str(exc))

        self._update_task(
            task.id,
            status=status,
            summary=summary,
            progress_current=total_progress_steps if 'total_progress_steps' in locals() else 0,
            progress_total=total_progress_steps if 'total_progress_steps' in locals() else 0,
            current_label="Complete" if status in {"success", "partial"} else "Failed",
            detail_payload={
                "phase": "complete" if status in {"success", "partial"} else "error",
                "libraries": sync_libraries,
                "suggestion_refresh": suggestion_refresh,
            },
            finished=True,
        )

        logger.info("Library Sync finished status=%s summary=%s", status, summary)

        return {
            "engine": "library_sync",
            "status": status,
            "summary": summary,
            "indexed": indexed,
            "added": added,
            "updated": updated,
            "removed": removed,
            "skipped": skipped,
            "refreshed_users": refreshed_users,
            "errors": errors,
        }

    async def ingest_seer_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        notification_type = str(
            payload.get("notification_type")
            or payload.get("notificationType")
            or ""
        ).strip()
        event_name = str(payload.get("event") or "").strip()
        request_id = self._coerce_int(payload.get("request_id") or payload.get("requestId"))
        requested_by_username = str(
            payload.get("requested_by")
            or payload.get("requestedBy_username")
            or ""
        ).strip() or None
        media_type = str(payload.get("media_type") or "").strip().lower() or None
        media_status = str(payload.get("media_status") or "").strip().upper() or None
        tmdb_id = self._coerce_int(payload.get("media_tmdbid"))
        tvdb_id = self._coerce_int(payload.get("media_tvdbid"))
        subject = str(payload.get("subject") or "").strip()
        delivery_key = "|".join(
            [
                notification_type or "unknown",
                str(request_id or 0),
                requested_by_username or "unknown",
                media_type or "unknown",
                str(tmdb_id or 0),
                str(tvdb_id or 0),
                media_status or "unknown",
            ]
        )

        created = False
        with self.session_scope() as session:
            existing = session.scalar(
                select(SeerWebhookEvent).where(SeerWebhookEvent.delivery_key == delivery_key)
            )
            if existing is None:
                session.add(
                    SeerWebhookEvent(
                        delivery_key=delivery_key,
                        notification_type=notification_type or "unknown",
                        event_name=event_name,
                        request_id=request_id,
                        requested_by_username=requested_by_username,
                        media_type=media_type,
                        media_status=media_status,
                        tmdb_id=tmdb_id,
                        tvdb_id=tvdb_id,
                        subject=subject,
                        payload_json=json.dumps(payload, ensure_ascii=True),
                    )
                )
                created = True

        if not created:
            return {
                "status": "duplicate",
                "delivery_key": delivery_key,
                "notification_type": notification_type or "unknown",
            }

        refreshed = False
        if (
            self.settings.suggestions_enabled
            and requested_by_username
            and media_status in {"AVAILABLE", "PARTIALLY_AVAILABLE"}
        ):
            users = await self.media_server.list_users()
            target_user = next(
                (user for user in users if user.get("Name") == requested_by_username),
                None,
            )
            if target_user is not None:
                await self._refresh_user_suggestions(target_user)
                refreshed = True

        return {
            "status": "accepted",
            "delivery_key": delivery_key,
            "notification_type": notification_type or "unknown",
            "requested_by_username": requested_by_username,
            "refreshed_suggestions": refreshed,
        }

    def _jellyfin_client(self) -> JellyfinClient:
        if isinstance(self.media_server, JellyfinClient):
            return self.media_server
        client = getattr(self.media_server, "jellyfin", None)
        if isinstance(client, JellyfinClient):
            return client
        raise RuntimeError("Suggested For You requires a Jellyfin media server client.")

    async def _refresh_user_suggestions(self, user: dict[str, Any]) -> dict[str, Any]:
        current_username = str(user.get("Name") or "unknown")
        jellyfin_user_id = str(user.get("Id") or "").strip()
        if not jellyfin_user_id:
            raise ValueError("Jellyfin user id is required for suggestion refresh.")

        history = await self.media_server.get_playback_history(
            jellyfin_user_id,
            self.settings.profile_history_limit,
        )
        stored_profile = self.profile_store.read_payload(current_username)
        history_summary = self._build_profile_history_context(
            history,
            top_limit=self.settings.profile_architect_top_titles_limit,
            recent_limit=self.settings.profile_architect_recent_momentum_limit,
        )
        recommendation_seeds = self._build_recommendation_seed_pool(
            history,
            profile_summary=history_summary,
            limit=self.settings.recommendation_seed_limit,
        )
        history_summary = await self._enrich_profile_summary_with_tmdb(
            history_summary,
            recommendation_seeds=recommendation_seeds,
        )
        profile_payload = self._build_profile_payload(
            current_username,
            history_summary,
            enrichment={
                "adjacent_genres": stored_profile.get("adjacent_genres", []),
                "adjacent_themes": stored_profile.get("adjacent_themes", []),
            },
            existing_payload=stored_profile,
        )
        if not ProfileStore.is_structured_payload(stored_profile) and int(
            profile_payload.get("history_count") or 0
        ) > 0:
            self.profile_store.write_payload(current_username, profile_payload)

        available_candidates = await self._build_available_library_candidates(jellyfin_user_id)
        watched_external_keys = self._build_watched_external_keys(history)
        filtered_candidates = [
            candidate
            for candidate in available_candidates
            if not self._candidate_matches_external_keys(candidate, watched_external_keys)
        ]
        ranked_candidates = self._rank_candidate_pool(
            filtered_candidates,
            profile_summary=profile_payload,
        )
        selected_candidates = self._diversify_candidates(
            ranked_candidates,
            limit=max(1, int(self.settings.suggestions_limit)),
        )

        with self.session_scope() as session:
            for existing in session.scalars(
                select(SuggestedMedia).where(SuggestedMedia.jellyfin_user_id == jellyfin_user_id)
            ):
                session.delete(existing)

            for index, candidate in enumerate(selected_candidates, start=1):
                features = candidate.get("recommendation_features", {})
                reasoning = (
                    f"Score {float(features.get('deterministic_score') or 0.0):.2f}. "
                    f"{str(features.get('analysis_summary') or 'Limited alignment signals.').strip()}"
                ).strip()
                external_ids = candidate.get("external_ids", {}) if isinstance(candidate.get("external_ids"), dict) else {}
                session.add(
                    SuggestedMedia(
                        jellyfin_user_id=jellyfin_user_id,
                        username=current_username,
                        rank=index,
                        media_type=str(candidate.get("media_type") or "unknown"),
                        title=str(candidate.get("title") or "Unknown"),
                        overview=str(candidate.get("overview") or ""),
                        production_year=self._parse_release_year(candidate.get("release_date")),
                        score=float(features.get("deterministic_score") or 0.0),
                        reasoning=reasoning,
                        state="available",
                        tmdb_id=self._coerce_int(external_ids.get("tmdb")),
                        tvdb_id=self._coerce_int(external_ids.get("tvdb")),
                        imdb_id=str(external_ids.get("imdb") or "").strip() or None,
                        payload_json=json.dumps(candidate, ensure_ascii=True),
                    )
                )

        return {
            "username": current_username,
            "stored": len(selected_candidates),
            "scored": len(filtered_candidates),
        }

    async def _build_available_library_candidates(self, jellyfin_user_id: str) -> list[dict[str, Any]]:
        with self.session_scope() as session:
            indexed_rows = list(
                session.scalars(
                    select(LibraryMedia)
                    .where(LibraryMedia.state == "available")
                    .order_by(LibraryMedia.sort_title.asc(), LibraryMedia.title.asc())
                )
            )

        if indexed_rows:
            candidates = [
                candidate
                for candidate in (self._library_media_to_candidate(row) for row in indexed_rows)
                if candidate is not None
            ]
            if candidates:
                return candidates

        items = await self._jellyfin_client().get_library_items(user_id=jellyfin_user_id)
        candidates: list[dict[str, Any]] = []
        for item in items:
            candidate = self._library_item_to_candidate(item)
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _start_task(self, session: Session, engine_name: str) -> TaskRun:
        task = TaskRun(
            engine=engine_name,
            status="running",
            summary="Task started.",
            progress_current=0,
            progress_total=0,
            current_label="",
            detail_json="{}",
        )
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

    def _update_task(
        self,
        task_id: int,
        *,
        status: str | None = None,
        summary: str | None = None,
        progress_current: int | None = None,
        progress_total: int | None = None,
        current_label: str | None = None,
        detail_payload: dict[str, Any] | None = None,
        finished: bool = False,
    ) -> None:
        with self.session_scope() as session:
            task = session.get(TaskRun, task_id)
            if task is None:
                return
            if status is not None:
                task.status = status
            if summary is not None:
                task.summary = summary
            if progress_current is not None:
                task.progress_current = max(0, int(progress_current))
            if progress_total is not None:
                task.progress_total = max(0, int(progress_total))
            if current_label is not None:
                task.current_label = str(current_label)
            if detail_payload is not None:
                task.detail_json = json.dumps(detail_payload, ensure_ascii=True)
            if finished:
                task.finished_at = datetime.utcnow()
            session.add(task)

    @staticmethod
    def _serialize_task_run(task: TaskRun | None) -> dict[str, Any]:
        if task is None:
            return {
                "id": None,
                "engine": "",
                "status": "idle",
                "summary": "No runs yet.",
                "started_at": None,
                "finished_at": None,
                "progress_current": 0,
                "progress_total": 0,
                "percent": 0.0,
                "current_label": "",
                "detail": {},
            }

        detail: dict[str, Any] = {}
        try:
            parsed = json.loads(task.detail_json or "{}")
            if isinstance(parsed, dict):
                detail = parsed
        except json.JSONDecodeError:
            detail = {}

        progress_current = int(task.progress_current or 0)
        progress_total = int(task.progress_total or 0)
        percent = 0.0
        if progress_total > 0:
            percent = round(min(100.0, max(0.0, (progress_current / progress_total) * 100.0)), 1)

        return {
            "id": task.id,
            "engine": task.engine,
            "status": task.status,
            "summary": task.summary,
            "started_at": task.started_at.isoformat() if task.started_at else None,
            "finished_at": task.finished_at.isoformat() if task.finished_at else None,
            "progress_current": progress_current,
            "progress_total": progress_total,
            "percent": percent,
            "current_label": str(task.current_label or ""),
            "detail": detail,
        }

    def _already_requested(self, session: Session, username: str, candidate: dict[str, Any]) -> bool:
        stmt = select(RequestedMedia).where(
            RequestedMedia.username == username,
            RequestedMedia.media_type == candidate["media_type"],
            RequestedMedia.media_id == candidate["media_id"],
        )
        return session.scalar(stmt) is not None

    @staticmethod
    def _candidate_key(candidate: dict[str, Any]) -> tuple[str, int]:
        return str(candidate.get("media_type") or "unknown"), int(candidate.get("media_id") or 0)

    @staticmethod
    def _requested_media_keys(session: Session, username: str) -> set[tuple[str, int]]:
        stmt = select(RequestedMedia).where(RequestedMedia.username == username)
        return {
            (item.media_type, item.media_id)
            for item in session.scalars(stmt)
        }

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
        release_years: list[int] = []
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

            release_year = cls._extract_history_release_year(item)
            if release_year is not None:
                release_years.append(release_year)

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
        ranked_genre_details = [
            {
                "genre": genre,
                "raw_count": int(genre_counts.get(genre, 0)),
                "recent_count": int(recent_genre_counts.get(genre, 0)),
                "weighted_score": round(score, 3),
            }
            for genre, score in ranked_genres[:8]
        ]

        total_genre_events = sum(genre_counts.values())
        focus_share = 0.0
        if total_genre_events and primary_genres:
            focus_share = sum(genre_counts[genre] for genre in primary_genres[:3]) / total_genre_events

        return {
            "history_count": len(history),
            "unique_titles": len(grouped),
            "top_titles": normalized_top_titles,
            "top_genres": [genre for genre, _count in genre_counts.most_common(8)],
            "ranked_genres": ranked_genre_details,
            "primary_genres": primary_genres,
            "secondary_genres": secondary_genres,
            "recent_genres": recent_genres,
            "recent_momentum": normalized_recent_momentum,
            "repeat_titles": repeat_titles,
            "format_preference": cls._determine_format_preference(media_type_counts),
            "release_year_preference": cls._build_release_year_preference(release_years),
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
    def _collect_recommendation_seed_candidates(cls, history: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        for seed in seeds:
            cleaned = dict(seed)
            cleaned.pop("_last_played_score", None)
            trimmed.append(cleaned)
        return trimmed

    @classmethod
    def _select_recommendation_seeds(
        cls,
        history: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        return cls._collect_recommendation_seed_candidates(history)[:limit]

    @classmethod
    def _build_recommendation_seed_pool(
        cls,
        history: list[dict[str, Any]],
        *,
        profile_summary: dict[str, Any],
        limit: int,
    ) -> list[dict[str, Any]]:
        seeds = cls._collect_recommendation_seed_candidates(history)
        if not seeds or limit <= 0:
            return []

        primary_genres = cls._normalize_genres(profile_summary.get("primary_genres", []), limit=4)
        recent_genres = cls._normalize_genres(profile_summary.get("recent_genres", []), limit=3)

        recent_order = sorted(
            seeds,
            key=lambda item: (
                -float(cls._to_timestamp(item.get("last_played"))),
                -int(item.get("play_count") or 0),
                -float(item.get("community_rating") or 0.0),
                str(item.get("title") or "").lower(),
            ),
        )

        top_lookup = {
            (str(item.get("media_type") or ""), int(item.get("media_id") or 0))
            for item in seeds[: max(2, min(limit, 4))]
        }
        recent_lookup = {
            (str(item.get("media_type") or ""), int(item.get("media_id") or 0))
            for item in recent_order[: max(2, min(limit, 4))]
        }

        pool: list[dict[str, Any]] = []
        for seed in seeds:
            annotated = dict(seed)
            annotated["seed_lanes"] = cls._build_seed_lanes(
                seed,
                top_lookup=top_lookup,
                recent_lookup=recent_lookup,
                primary_genres=primary_genres,
                recent_genres=recent_genres,
            )
            if annotated["seed_lanes"]:
                pool.append(annotated)

        pool.sort(
            key=lambda item: (
                -len(item.get("seed_lanes", [])),
                -int(item.get("play_count") or 0),
                -float(cls._to_timestamp(item.get("last_played"))),
                -float(item.get("community_rating") or 0.0),
                str(item.get("title") or "").lower(),
            )
        )
        return pool[:limit]

    @classmethod
    def _build_seed_lanes(
        cls,
        seed: dict[str, Any],
        *,
        top_lookup: set[tuple[str, int]],
        recent_lookup: set[tuple[str, int]],
        primary_genres: list[str],
        recent_genres: list[str],
    ) -> list[str]:
        lanes: list[str] = []
        key = (str(seed.get("media_type") or ""), int(seed.get("media_id") or 0))
        seed_genres = cls._normalize_genres(seed.get("genres", []), limit=5)

        if key in top_lookup:
            lanes.append("top_seed")
        if int(seed.get("play_count") or 0) > 1:
            lanes.append("repeat_watch_seed")
        if key in recent_lookup:
            lanes.append("recent_seed")
        if cls._intersect_strings(seed_genres, primary_genres) or cls._intersect_strings(seed_genres, recent_genres):
            lanes.append("genre_anchor_seed")

        return lanes

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
            "ranked_genres": summary.get("ranked_genres", [])[:5],
            "primary_genres": summary.get("primary_genres", []),
            "secondary_genres": summary.get("secondary_genres", []),
            "repeat_titles": summary.get("repeat_titles", [])[:3],
            "recent_momentum": summary.get("recent_momentum", [])[:5],
            "format_preference": summary.get("format_preference", {}),
            "release_year_preference": summary.get("release_year_preference", {}),
            "discovery_lanes": summary.get("discovery_lanes", []),
            "adjacent_genres": summary.get("adjacent_genres", []),
            "adjacent_themes": summary.get("adjacent_themes", []),
            "seed_lanes": summary.get("seed_lanes", []),
            "top_keywords": summary.get("top_keywords", [])[:8],
            "favorite_people": summary.get("favorite_people", [])[:6],
            "preferred_brands": summary.get("preferred_brands", [])[:6],
            "favorite_collections": summary.get("favorite_collections", [])[:4],
            "recent_plays": recent_plays,
        }

    @classmethod
    def _build_genre_discovery_seeds(cls, profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
        primary_genres = cls._normalize_genres(
            profile_summary.get("primary_genres") or profile_summary.get("top_genres", []),
            limit=3,
        )
        recent_genres = cls._normalize_genres(profile_summary.get("recent_genres", []), limit=2)
        adjacent_genres = cls._normalize_genres(profile_summary.get("adjacent_genres", []), limit=2)
        preferred_media_type = str((profile_summary.get("format_preference") or {}).get("preferred") or "balanced")
        media_types = ["tv", "movie"] if preferred_media_type == "tv" else ["movie", "tv"]
        if preferred_media_type == "balanced":
            media_types = ["movie", "tv"]

        seeds: list[dict[str, Any]] = []
        seen: set[str] = set()
        for genres, lane in (
            (primary_genres, "primary_genre_seed"),
            (recent_genres, "recent_genre_seed"),
            (adjacent_genres, "adjacent_genre_seed"),
        ):
            for genre in genres:
                lowered = genre.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                seeds.append(
                    {
                        "genre_name": genre,
                        "source": f"genre:{genre}",
                        "source_lanes": [lane],
                        "media_types": media_types,
                    }
                )

        return seeds

    @classmethod
    def _build_watched_media_keys(cls, history: list[dict[str, Any]]) -> set[tuple[str, int]]:
        watched: set[tuple[str, int]] = set()
        for item in history:
            media_type = cls._map_history_media_type(item.get("Type"))
            tmdb_id = cls._extract_tmdb_id(item)
            if media_type not in {"movie", "tv"} or tmdb_id is None:
                continue
            watched.add((media_type, tmdb_id))
        return watched

    @classmethod
    def _rank_candidate_pool(
        cls,
        candidates: list[dict[str, Any]],
        *,
        profile_summary: dict[str, Any],
    ) -> list[dict[str, Any]]:
        enriched = [cls._annotate_candidate(candidate, profile_summary=profile_summary) for candidate in candidates]
        enriched.sort(
            key=lambda item: (
                -float(item.get("recommendation_features", {}).get("deterministic_score") or 0.0),
                -float(item.get("rating") or 0.0),
                -float(item.get("vote_count") or 0.0),
                str(item.get("title") or "").lower(),
            )
        )
        return enriched

    @classmethod
    def _annotate_candidate(
        cls,
        candidate: dict[str, Any],
        *,
        profile_summary: dict[str, Any],
    ) -> dict[str, Any]:
        annotated = dict(candidate)
        annotated["recommendation_features"] = cls._build_candidate_features(
            candidate,
            profile_summary=profile_summary,
        )
        return annotated

    @classmethod
    def _build_candidate_features(
        cls,
        candidate: dict[str, Any],
        *,
        profile_summary: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_genres = cls._normalize_genres(candidate.get("genres", []), limit=6)
        primary_genres = cls._normalize_genres(profile_summary.get("primary_genres", []), limit=6)
        secondary_genres = cls._normalize_genres(profile_summary.get("secondary_genres", []), limit=6)
        recent_genres = cls._normalize_genres(profile_summary.get("recent_genres", []), limit=6)
        discovery_lanes = cls._merge_unique_strings(
            cls._normalize_genres(profile_summary.get("discovery_lanes", []), limit=6),
            profile_summary.get("adjacent_genres", []),
        )[:6]
        source_lanes = cls._normalize_string_list(candidate.get("source_lanes", []), limit=6)
        tmdb_details = candidate.get("tmdb_details", {}) if isinstance(candidate.get("tmdb_details"), dict) else {}
        candidate_keywords = cls._normalize_string_list(tmdb_details.get("keywords", []), limit=10)
        candidate_people = cls._normalize_string_list(tmdb_details.get("featured_people", []), limit=8)
        candidate_brands = cls._normalize_string_list(tmdb_details.get("brands", []), limit=6)
        candidate_collection = str(tmdb_details.get("collection_name") or "").strip()
        profile_keywords = cls._normalize_string_list(profile_summary.get("top_keywords", []), limit=8)
        profile_people = cls._normalize_string_list(profile_summary.get("favorite_people", []), limit=6)
        profile_brands = cls._normalize_string_list(profile_summary.get("preferred_brands", []), limit=6)
        profile_collections = cls._normalize_string_list(profile_summary.get("favorite_collections", []), limit=4)
        profile_theme_hints = cls._normalize_string_list(profile_summary.get("adjacent_themes", []), limit=4)

        matched_primary = cls._intersect_strings(candidate_genres, primary_genres)
        matched_secondary = cls._intersect_strings(candidate_genres, secondary_genres)
        matched_recent = cls._intersect_strings(candidate_genres, recent_genres)
        matched_discovery = cls._intersect_strings(candidate_genres, discovery_lanes)
        matched_keywords = cls._intersect_strings(candidate_keywords, profile_keywords)
        matched_people = cls._intersect_strings(candidate_people, profile_people)
        matched_brands = cls._intersect_strings(candidate_brands, profile_brands)
        theme_matches = cls._match_theme_hints(candidate_keywords, profile_theme_hints)
        collection_match = candidate_collection if candidate_collection and candidate_collection.lower() in {
            value.lower() for value in profile_collections
        } else None

        source_titles = cls._extract_source_titles(candidate.get("sources", []))
        top_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("top_titles", [])}
        repeat_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("repeat_titles", [])}
        recent_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("recent_momentum", [])}
        recommended_source_titles = [title for title in source_titles if title.lower() in top_titles or title.lower() in repeat_titles]

        score_breakdown: dict[str, float] = {}
        score_breakdown["source_affinity"] = cls._score_source_affinity(
            sources=candidate.get("sources", []),
            source_lanes=source_lanes,
            source_titles=source_titles,
            top_titles=top_titles,
            repeat_titles=repeat_titles,
            recent_titles=recent_titles,
        )
        score_breakdown["genre_affinity"] = cls._score_genre_affinity(
            candidate_genres=candidate_genres,
            matched_primary=matched_primary,
            matched_secondary=matched_secondary,
            matched_recent=matched_recent,
            matched_discovery=matched_discovery,
            ranked_genres=profile_summary.get("ranked_genres", []),
        )
        score_breakdown["format_fit"] = cls._score_format_fit(
            candidate_media_type=str(candidate.get("media_type") or ""),
            format_preference=profile_summary.get("format_preference", {}),
        )
        freshness_score, freshness_fit = cls._score_release_year_fit(
            candidate_release_date=candidate.get("release_date"),
            release_year_preference=profile_summary.get("release_year_preference", {}),
        )
        score_breakdown["freshness_fit"] = freshness_score
        score_breakdown["quality"] = cls._score_quality(
            rating=candidate.get("rating"),
            vote_count=candidate.get("vote_count"),
        )
        score_breakdown["exploration"] = cls._score_exploration(
            matched_discovery=matched_discovery,
            source_titles=source_titles,
            recommended_source_titles=recommended_source_titles,
        )
        score_breakdown["popularity"] = cls._score_popularity(candidate.get("popularity"))
        score_breakdown["feedback_fit"] = cls._score_feedback_fit(
            candidate_title=str(candidate.get("title") or ""),
            candidate_genres=candidate_genres,
            explicit_feedback=profile_summary.get("explicit_feedback", {}),
        )
        score_breakdown["tmdb_themes"] = cls._score_tmdb_theme_affinity(
            matched_keywords=matched_keywords,
            theme_matches=theme_matches,
        )
        score_breakdown["tmdb_people"] = cls._score_tmdb_people_affinity(matched_people)
        score_breakdown["tmdb_brands"] = cls._score_tmdb_brand_affinity(
            matched_brands=matched_brands,
            collection_match=collection_match,
        )
        score_breakdown["tmdb_guardrails"] = cls._score_tmdb_guardrails(tmdb_details)

        deterministic_score = round(max(0.0, min(1.0, sum(score_breakdown.values()))), 3)
        lane_tags = cls._derive_lane_tags(
            sources=candidate.get("sources", []),
            source_lanes=source_lanes,
            matched_primary=matched_primary,
            matched_recent=matched_recent,
            matched_discovery=matched_discovery,
        )

        return {
            "deterministic_score": deterministic_score,
            "lane_tags": lane_tags,
            "matched_primary_genres": matched_primary,
            "matched_secondary_genres": matched_secondary,
            "matched_recent_genres": matched_recent,
            "matched_discovery_lanes": matched_discovery,
            "matched_keywords": matched_keywords,
            "theme_matches": theme_matches,
            "matched_people": matched_people,
            "matched_brands": matched_brands,
            "collection_match": collection_match,
            "source_titles": source_titles,
            "source_lanes": source_lanes,
            "recommended_source_titles": recommended_source_titles,
            "dominant_genre": cls._choose_dominant_genre(
                candidate_genres,
                matched_primary=matched_primary,
                matched_recent=matched_recent,
            ),
            "freshness_fit": freshness_fit,
            "score_breakdown": {key: round(value, 3) for key, value in score_breakdown.items()},
            "analysis_summary": cls._build_analysis_summary(
                matched_primary=matched_primary,
                matched_recent=matched_recent,
                matched_discovery=matched_discovery,
                matched_keywords=matched_keywords,
                theme_matches=theme_matches,
                matched_people=matched_people,
                matched_brands=matched_brands,
                collection_match=collection_match,
                source_titles=source_titles,
                lane_tags=lane_tags,
                freshness_fit=freshness_fit,
            ),
        }

    @classmethod
    def _diversify_candidates(
        cls,
        candidates: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        lane_counts: Counter[str] = Counter()
        genre_counts: Counter[str] = Counter()

        for candidate in candidates:
            if len(selected) >= limit:
                break

            features = candidate.get("recommendation_features", {})
            lane_key = (features.get("lane_tags") or ["generic"])[0]
            dominant_genre = str(features.get("dominant_genre") or "unknown")

            if lane_counts[lane_key] >= 3:
                continue
            if dominant_genre != "unknown" and genre_counts[dominant_genre] >= 3:
                continue

            selected.append(candidate)
            lane_counts[lane_key] += 1
            if dominant_genre != "unknown":
                genre_counts[dominant_genre] += 1

        if len(selected) < limit:
            selected_keys = {cls._candidate_key(candidate) for candidate in selected}
            for candidate in candidates:
                if len(selected) >= limit:
                    break
                if cls._candidate_key(candidate) in selected_keys:
                    continue
                selected.append(candidate)
                selected_keys.add(cls._candidate_key(candidate))

        return selected

    def _decision_prefilter_threshold(self) -> float:
        return max(0.28, min(0.42, self.settings.request_threshold * 0.55))

    @classmethod
    def _blend_confidences(
        cls,
        *,
        deterministic_score: float,
        llm_confidence: float | None,
        llm_vote: str,
        llm_weight_percent: int,
    ) -> float:
        normalized_code_score = max(0.0, min(1.0, deterministic_score))
        if llm_confidence is None or llm_vote == "UNAVAILABLE":
            return normalized_code_score

        llm_weight = max(0.0, min(1.0, llm_weight_percent / 100))
        llm_score = cls._llm_request_score(llm_confidence=llm_confidence, llm_vote=llm_vote)
        blended = (normalized_code_score * (1.0 - llm_weight)) + (llm_score * llm_weight)
        return max(0.0, min(1.0, round(blended, 3)))

    @staticmethod
    def _llm_request_score(*, llm_confidence: float, llm_vote: str) -> float:
        normalized_confidence = max(0.0, min(1.0, llm_confidence))
        if llm_vote == "REQUEST":
            return 0.5 + (normalized_confidence * 0.5)
        if llm_vote == "IGNORE":
            return 0.5 - (normalized_confidence * 0.5)
        return 0.5

    @classmethod
    def _compose_decision_reasoning(
        cls,
        candidate: dict[str, Any],
        *,
        deterministic_score: float,
        hybrid_confidence: float,
        decision: str,
        request_threshold: float,
        llm_vote: str,
        llm_reasoning: str,
    ) -> str:
        features = candidate.get("recommendation_features", {})
        breakdown = features.get("score_breakdown", {})
        summary = str(features.get("analysis_summary") or "Limited alignment signals.")
        reasoning = (
            f"Final score {hybrid_confidence:.2f}. Code score {deterministic_score:.2f}. {summary} "
            f"Breakdown: source {float(breakdown.get('source_affinity', 0.0)):.2f}, "
            f"genres {float(breakdown.get('genre_affinity', 0.0)):.2f}, "
            f"format {float(breakdown.get('format_fit', 0.0)):.2f}, "
            f"freshness {float(breakdown.get('freshness_fit', 0.0)):.2f}, "
            f"quality {float(breakdown.get('quality', 0.0)):.2f}, "
            f"themes {float(breakdown.get('tmdb_themes', 0.0)):.2f}, "
            f"people {float(breakdown.get('tmdb_people', 0.0)):.2f}, "
            f"brands {float(breakdown.get('tmdb_brands', 0.0)):.2f}."
        )
        if llm_vote == "UNAVAILABLE":
            return reasoning + " LLM unavailable, so the final score used the code-driven score only."
        if decision == "IGNORE" and llm_vote == "REQUEST":
            reasoning += f" The LLM leaned REQUEST, but the final score stayed below the request threshold of {request_threshold:.2f}."
        elif decision == "REQUEST" and llm_vote == "IGNORE":
            reasoning += f" The code-driven score still cleared the request threshold of {request_threshold:.2f}."
        if llm_reasoning:
            return reasoning + f" LLM vote: {llm_vote}. {llm_reasoning}"
        return reasoning + f" LLM vote: {llm_vote}."

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
                temperature=0.1,
                purpose="profile_enrichment",
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

    async def _enrich_profile_summary_with_tmdb(
        self,
        history_summary: dict[str, Any],
        *,
        recommendation_seeds: list[dict[str, Any]],
    ) -> dict[str, Any]:
        summary = dict(history_summary)
        summary.setdefault("top_keywords", [])
        summary.setdefault("favorite_people", [])
        summary.setdefault("preferred_brands", [])
        summary.setdefault("favorite_collections", [])

        if not self.tmdb.enabled or not recommendation_seeds:
            return summary

        enriched_seeds = await self._enrich_items_with_tmdb(
            recommendation_seeds,
            limit=min(self.settings.tmdb_seed_enrichment_limit, len(recommendation_seeds)),
        )

        keyword_scores: Counter[str] = Counter()
        people_scores: Counter[str] = Counter()
        brand_scores: Counter[str] = Counter()
        collection_scores: Counter[str] = Counter()

        for seed in enriched_seeds:
            details = seed.get("tmdb_details", {})
            if not isinstance(details, dict) or not details:
                continue

            weight = 1.0 + min(2.5, float(seed.get("play_count") or 0) * 0.45)
            seed_lanes = {str(lane).strip().lower() for lane in seed.get("seed_lanes", [])}
            if "repeat_watch_seed" in seed_lanes:
                weight += 0.5
            if "recent_seed" in seed_lanes:
                weight += 0.25
            if "genre_anchor_seed" in seed_lanes:
                weight += 0.2

            for keyword in self._normalize_string_list(details.get("keywords", []), limit=10):
                keyword_scores[keyword] += weight
            for person in self._normalize_string_list(details.get("featured_people", []), limit=8):
                people_scores[person] += weight
            for brand in self._normalize_string_list(details.get("brands", []), limit=6):
                brand_scores[brand] += weight * 0.7

            collection_name = str(details.get("collection_name") or "").strip()
            if collection_name:
                collection_scores[collection_name] += weight

        summary["top_keywords"] = self._rank_counter(keyword_scores, limit=8)
        summary["favorite_people"] = self._rank_counter(people_scores, limit=6)
        summary["preferred_brands"] = self._rank_counter(brand_scores, limit=6)
        summary["favorite_collections"] = self._rank_counter(collection_scores, limit=4)
        return summary

    async def _enrich_candidate_pool_with_tmdb(
        self,
        candidates: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        if not self.tmdb.enabled or limit <= 0 or not candidates:
            return candidates
        return await self._enrich_items_with_tmdb(candidates, limit=min(limit, len(candidates)))

    async def _enrich_items_with_tmdb(
        self,
        items: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        if not self.tmdb.enabled or limit <= 0 or not items:
            return items

        enriched = [dict(item) for item in items]
        targets: list[tuple[int, str, int]] = []
        for index, item in enumerate(enriched[:limit]):
            media_type = str(item.get("media_type") or "").strip()
            media_id = item.get("media_id")
            if media_type not in {"movie", "tv"} or media_id is None:
                continue
            if isinstance(item.get("tmdb_details"), dict) and item.get("tmdb_details"):
                continue
            targets.append((index, media_type, int(media_id)))

        if not targets:
            return enriched

        semaphore = asyncio.Semaphore(6)

        async def fetch_details(media_type: str, media_id: int) -> dict[str, Any]:
            async with semaphore:
                return await self.tmdb.get_details(media_type, media_id)

        results = await asyncio.gather(
            *(fetch_details(media_type, media_id) for _index, media_type, media_id in targets),
            return_exceptions=True,
        )

        for (index, media_type, media_id), result in zip(targets, results):
            if isinstance(result, Exception):
                logger.warning(
                    "TMDb enrichment skipped media_type=%s media_id=%s reason=%s",
                    media_type,
                    media_id,
                    result,
                )
                continue
            if result:
                enriched[index]["tmdb_details"] = result

        return enriched

    @staticmethod
    def _rank_counter(counter: Counter[str], *, limit: int) -> list[str]:
        ranked = sorted(counter.items(), key=lambda item: (-float(item[1]), item[0].lower()))
        return [name for name, _score in ranked[:limit]]

    @classmethod
    def _build_profile_payload(
        cls,
        username: str,
        history_summary: dict[str, Any],
        *,
        enrichment: dict[str, list[str]] | None = None,
        existing_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = ProfileStore.default_payload(username)
        payload.update(history_summary)

        existing = existing_payload if isinstance(existing_payload, dict) else {}
        payload["profile_version"] = "v5"
        payload["profile_state"] = "ready" if cls._has_profile_signal(payload) else "default"
        payload["username"] = username
        payload["generated_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        payload["top_titles"] = cls._normalize_profile_entries(payload.get("top_titles", []), limit=8)
        payload["repeat_titles"] = cls._normalize_profile_entries(payload.get("repeat_titles", []), limit=5)
        payload["recent_momentum"] = cls._normalize_profile_entries(payload.get("recent_momentum", []), limit=5)
        payload["top_genres"] = cls._normalize_string_list(payload.get("top_genres", []), limit=8)
        payload["primary_genres"] = cls._normalize_string_list(payload.get("primary_genres", []), limit=4)
        payload["secondary_genres"] = cls._normalize_string_list(payload.get("secondary_genres", []), limit=4)
        payload["recent_genres"] = cls._normalize_string_list(payload.get("recent_genres", []), limit=4)
        payload["ranked_genres"] = cls._normalize_ranked_genres(payload.get("ranked_genres", []), limit=8)
        payload["discovery_lanes"] = cls._normalize_string_list(payload.get("discovery_lanes", []), limit=4)
        payload["top_keywords"] = cls._normalize_string_list(payload.get("top_keywords", []), limit=8)
        payload["favorite_people"] = cls._normalize_string_list(payload.get("favorite_people", []), limit=6)
        payload["preferred_brands"] = cls._normalize_string_list(payload.get("preferred_brands", []), limit=6)
        payload["favorite_collections"] = cls._normalize_string_list(payload.get("favorite_collections", []), limit=4)
        payload["adjacent_genres"] = cls._merge_unique_strings(
            cls._normalize_string_list(existing.get("adjacent_genres", []), limit=4),
            (enrichment or {}).get("adjacent_genres", []),
        )[:4]
        payload["adjacent_themes"] = cls._merge_unique_strings(
            cls._normalize_string_list(existing.get("adjacent_themes", []), limit=3),
            (enrichment or {}).get("adjacent_themes", []),
        )[:3]
        payload["seed_lanes"] = cls._build_profile_seed_lanes(payload)
        payload["format_preference"] = cls._normalize_format_preference(payload.get("format_preference", {}))
        payload["release_year_preference"] = cls._normalize_release_year_preference(
            payload.get("release_year_preference", {})
        )
        payload["explicit_feedback"] = cls._normalize_explicit_feedback(existing.get("explicit_feedback", {}))
        payload["profile_exclusions"] = cls._normalize_string_list(existing.get("profile_exclusions", []), limit=8)
        payload["operator_notes"] = str(existing.get("operator_notes") or "").strip()
        payload["summary_block"] = (
            cls._limit_words(cls._render_profile_block(username, payload), max_words=500)
            if cls._has_profile_signal(payload)
            else ProfileStore.default_block(username)
        )
        return payload

    @classmethod
    def _normalize_saved_profile_payload(cls, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = ProfileStore.default_payload(username)
        normalized.update(payload)
        normalized["profile_version"] = "v5"
        normalized["username"] = username
        normalized["generated_at"] = str(normalized.get("generated_at") or "").strip() or (
            datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        )
        normalized["history_count"] = max(0, int(normalized.get("history_count") or 0))
        normalized["unique_titles"] = max(0, int(normalized.get("unique_titles") or 0))
        normalized["average_top_rating"] = cls._coerce_optional_number(normalized.get("average_top_rating"))
        normalized["genre_focus_share"] = max(0.0, min(1.0, float(normalized.get("genre_focus_share") or 0.0)))
        normalized["top_titles"] = cls._normalize_profile_entries(normalized.get("top_titles", []), limit=8)
        normalized["repeat_titles"] = cls._normalize_profile_entries(normalized.get("repeat_titles", []), limit=5)
        normalized["recent_momentum"] = cls._normalize_profile_entries(normalized.get("recent_momentum", []), limit=5)
        normalized["top_genres"] = cls._normalize_string_list(normalized.get("top_genres", []), limit=8)
        normalized["primary_genres"] = cls._normalize_string_list(normalized.get("primary_genres", []), limit=4)
        normalized["secondary_genres"] = cls._normalize_string_list(normalized.get("secondary_genres", []), limit=4)
        normalized["recent_genres"] = cls._normalize_string_list(normalized.get("recent_genres", []), limit=4)
        normalized["ranked_genres"] = cls._normalize_ranked_genres(normalized.get("ranked_genres", []), limit=8)
        normalized["discovery_lanes"] = cls._normalize_string_list(normalized.get("discovery_lanes", []), limit=4)
        normalized["top_keywords"] = cls._normalize_string_list(normalized.get("top_keywords", []), limit=8)
        normalized["favorite_people"] = cls._normalize_string_list(normalized.get("favorite_people", []), limit=6)
        normalized["preferred_brands"] = cls._normalize_string_list(normalized.get("preferred_brands", []), limit=6)
        normalized["favorite_collections"] = cls._normalize_string_list(
            normalized.get("favorite_collections", []),
            limit=4,
        )
        normalized["adjacent_genres"] = cls._normalize_string_list(normalized.get("adjacent_genres", []), limit=4)
        normalized["adjacent_themes"] = cls._normalize_string_list(normalized.get("adjacent_themes", []), limit=3)
        normalized["seed_lanes"] = cls._normalize_string_list(normalized.get("seed_lanes", []), limit=8)
        if not normalized["seed_lanes"]:
            normalized["seed_lanes"] = cls._build_profile_seed_lanes(normalized)
        normalized["format_preference"] = cls._normalize_format_preference(normalized.get("format_preference", {}))
        normalized["release_year_preference"] = cls._normalize_release_year_preference(
            normalized.get("release_year_preference", {})
        )
        normalized["explicit_feedback"] = cls._normalize_explicit_feedback(normalized.get("explicit_feedback", {}))
        normalized["profile_exclusions"] = cls._normalize_string_list(normalized.get("profile_exclusions", []), limit=8)
        normalized["operator_notes"] = str(normalized.get("operator_notes") or "").strip()
        normalized["profile_state"] = "ready" if cls._has_profile_signal(normalized) else "default"
        normalized["summary_block"] = (
            cls._limit_words(cls._render_profile_block(username, normalized), max_words=500)
            if cls._has_profile_signal(normalized)
            else ProfileStore.default_block(username)
        )
        return normalized

    @classmethod
    def _normalize_profile_entries(cls, raw_items: Any, *, limit: int) -> list[dict[str, Any]]:
        if not isinstance(raw_items, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in raw_items:
            if isinstance(item, dict):
                title = str(item.get("title") or item.get("name") or "").strip()
                if not title:
                    continue
                entry = {
                    "title": title,
                    "media_type": str(item.get("media_type") or "").strip() or None,
                    "play_count": max(0, int(item.get("play_count") or 0)),
                    "genres": cls._normalize_string_list(item.get("genres", []), limit=5),
                    "community_rating": cls._coerce_optional_number(item.get("community_rating")),
                    "last_played": str(item.get("last_played") or "").strip() or None,
                }
            else:
                title = str(item).strip()
                if not title:
                    continue
                entry = {
                    "title": title,
                    "media_type": None,
                    "play_count": 0,
                    "genres": [],
                    "community_rating": None,
                    "last_played": None,
                }
            normalized.append(entry)
            if len(normalized) >= limit:
                break
        return normalized

    @classmethod
    def _normalize_ranked_genres(cls, raw_items: Any, *, limit: int) -> list[dict[str, Any]]:
        if not isinstance(raw_items, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in raw_items:
            if isinstance(item, dict):
                genre = str(item.get("genre") or "").strip()
                if not genre:
                    continue
                normalized.append(
                    {
                        "genre": genre,
                        "raw_count": max(0, int(item.get("raw_count") or 0)),
                        "recent_count": max(0, int(item.get("recent_count") or 0)),
                        "weighted_score": round(float(item.get("weighted_score") or 0.0), 3),
                    }
                )
            else:
                genre = str(item).strip()
                if not genre:
                    continue
                normalized.append(
                    {
                        "genre": genre,
                        "raw_count": 0,
                        "recent_count": 0,
                        "weighted_score": 0.0,
                    }
                )
            if len(normalized) >= limit:
                break
        return normalized

    @classmethod
    def _build_profile_seed_lanes(cls, profile_summary: dict[str, Any]) -> list[str]:
        lanes: list[str] = []
        if profile_summary.get("top_titles"):
            lanes.append("top_seed")
        if profile_summary.get("repeat_titles"):
            lanes.append("repeat_watch_seed")
        if profile_summary.get("recent_momentum"):
            lanes.append("recent_seed")
        if profile_summary.get("primary_genres") or profile_summary.get("recent_genres"):
            lanes.append("genre_anchor_seed")
        return lanes

    @staticmethod
    def _normalize_string_list(raw_items: Any, *, limit: int | None = None) -> list[str]:
        if not isinstance(raw_items, list):
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in raw_items:
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
    def _normalize_format_preference(raw_value: Any) -> dict[str, Any]:
        value = raw_value if isinstance(raw_value, dict) else {}
        preferred = str(value.get("preferred") or "balanced").strip().lower()
        if preferred not in {"balanced", "movie", "tv"}:
            preferred = "balanced"
        return {
            "preferred": preferred,
            "movie_plays": max(0, int(value.get("movie_plays") or 0)),
            "tv_plays": max(0, int(value.get("tv_plays") or 0)),
        }

    @staticmethod
    def _normalize_release_year_preference(raw_value: Any) -> dict[str, Any]:
        value = raw_value if isinstance(raw_value, dict) else {}
        bias = str(value.get("bias") or "balanced").strip().lower()
        if bias not in {"balanced", "recent", "catalog"}:
            bias = "balanced"
        average_year = value.get("average_year")
        try:
            normalized_year = int(average_year) if average_year not in (None, "") else None
        except (TypeError, ValueError):
            normalized_year = None
        return {"bias": bias, "average_year": normalized_year}

    @classmethod
    def _normalize_explicit_feedback(cls, raw_value: Any) -> dict[str, list[str]]:
        value = raw_value if isinstance(raw_value, dict) else {}
        return {
            "liked_titles": cls._normalize_string_list(value.get("liked_titles", []), limit=12),
            "disliked_titles": cls._normalize_string_list(value.get("disliked_titles", []), limit=12),
            "liked_genres": cls._normalize_string_list(value.get("liked_genres", []), limit=8),
            "disliked_genres": cls._normalize_string_list(value.get("disliked_genres", []), limit=8),
        }

    @staticmethod
    def _coerce_optional_number(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return round(float(value), 3)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _has_profile_signal(payload: dict[str, Any]) -> bool:
        return any(
            (
                int(payload.get("history_count") or 0) > 0,
                bool(payload.get("top_titles")),
                bool(payload.get("primary_genres")),
                bool(payload.get("ranked_genres")),
            )
        )

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
            cls._merge_unique_strings(
                list(history_summary.get("discovery_lanes", [])),
                list(history_summary.get("adjacent_genres", [])),
            ),
            (enrichment or {}).get("adjacent_genres", []),
        )[:3]
        adjacent_themes = cls._merge_unique_strings(
            list(history_summary.get("adjacent_themes", [])),
            (enrichment or {}).get("adjacent_themes", []),
        )[:2]

        lines = [
            "[VANGUARR_PROFILE_SUMMARY_V1]",
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
        ranked_genres = history_summary.get("ranked_genres", [])
        format_preference = history_summary.get("format_preference", {})
        release_year_preference = history_summary.get("release_year_preference", {})
        top_titles = history_summary.get("top_titles", [])
        favorite_collections = history_summary.get("favorite_collections", [])
        history_count = int(history_summary.get("history_count") or 0)
        unique_titles = int(history_summary.get("unique_titles") or 0)
        preferred = str(format_preference.get("preferred") or "balanced")
        movie_plays = int(format_preference.get("movie_plays") or 0)
        tv_plays = int(format_preference.get("tv_plays") or 0)

        lines: list[str] = []
        if primary_genres:
            lines.append(f"Primary genres: {cls._human_join(primary_genres[:4])}.")
        if ranked_genres:
            ranked_preview = [f"{item['genre']} ({item['raw_count']})" for item in ranked_genres[:4]]
            lines.append(f"Ranked genre stack: {cls._human_join(ranked_preview)}.")

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

        release_bias = str(release_year_preference.get("bias") or "balanced")
        average_year = release_year_preference.get("average_year")
        if average_year is not None:
            if release_bias == "recent":
                lines.append(f"Release window leans recent, centered around {average_year}.")
            elif release_bias == "catalog":
                lines.append(f"Release window leans catalog, centered around {average_year}.")
            else:
                lines.append(f"Release window is balanced, centered around {average_year}.")

        if top_titles:
            lines.append(f"Anchor titles: {cls._format_title_entries(top_titles[:3])}.")
        if favorite_collections:
            lines.append(f"Recurring franchise pull: {cls._human_join(favorite_collections[:2])}.")

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
        explicit_feedback = history_summary.get("explicit_feedback", {})
        liked_titles = cls._normalize_string_list(explicit_feedback.get("liked_titles", []), limit=2)
        liked_genres = cls._normalize_string_list(explicit_feedback.get("liked_genres", []), limit=3)
        top_keywords = cls._normalize_string_list(history_summary.get("top_keywords", []), limit=4)
        favorite_people = cls._normalize_string_list(history_summary.get("favorite_people", []), limit=3)
        preferred_brands = cls._normalize_string_list(history_summary.get("preferred_brands", []), limit=3)

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

        if top_keywords:
            lines.append(f"TMDb theme signals repeat around {cls._human_join(top_keywords)}.")
        if favorite_people:
            lines.append(f"Recurring talent signals show up around {cls._human_join(favorite_people)}.")
        if preferred_brands:
            lines.append(f"Brand or network gravity leans toward {cls._human_join(preferred_brands)}.")

        if liked_titles or liked_genres:
            feedback_parts: list[str] = []
            if liked_titles:
                feedback_parts.append(f"favor titles adjacent to {cls._human_join(liked_titles)}")
            if liked_genres:
                feedback_parts.append(f"keep leaning into {cls._human_join(liked_genres)}")
            lines.append(f"Explicit positive feedback says to {cls._human_join(feedback_parts)}.")

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
        explicit_feedback = history_summary.get("explicit_feedback", {})
        disliked_titles = cls._normalize_string_list(explicit_feedback.get("disliked_titles", []), limit=2)
        disliked_genres = cls._normalize_string_list(explicit_feedback.get("disliked_genres", []), limit=3)
        profile_exclusions = cls._normalize_string_list(history_summary.get("profile_exclusions", []), limit=3)

        feedback_line = ""
        if disliked_titles or disliked_genres or profile_exclusions:
            parts: list[str] = []
            if disliked_titles:
                parts.append(f"avoid titles adjacent to {cls._human_join(disliked_titles)}")
            if disliked_genres:
                parts.append(f"be careful with {cls._human_join(disliked_genres)}")
            if profile_exclusions:
                parts.append(f"honor manual exclusions like {cls._human_join(profile_exclusions)}")
            feedback_line = f"Explicit avoidance guidance: {cls._human_join(parts)}."

        if preferred == "tv":
            lines = [
                "Lower evidence for standalone movies than for serialized TV, so films need stronger genre or franchise overlap.",
                "Treat non-engagement as unknown, not dislike, unless stronger evidence shows up in the watch history.",
            ]
            if feedback_line:
                lines.insert(0, feedback_line)
            return lines
        if preferred == "movie":
            lines = [
                "Lower evidence for long-running series than for movies, so TV picks need stronger momentum or genre overlap.",
                "Treat non-engagement as unknown, not dislike, unless stronger evidence shows up in the watch history.",
            ]
            if feedback_line:
                lines.insert(0, feedback_line)
            return lines
        lines = [
            "No strong user-specific format aversion is visible from watch history alone.",
            "Treat non-engagement as unknown, not dislike, unless stronger evidence shows up in the watch history.",
        ]
        if feedback_line:
            lines.insert(0, feedback_line)
        return lines

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
        release_year_preference = history_summary.get("release_year_preference", {})
        operator_notes = str(history_summary.get("operator_notes") or "").strip()
        top_keywords = cls._normalize_string_list(history_summary.get("top_keywords", []), limit=3)
        favorite_people = cls._normalize_string_list(history_summary.get("favorite_people", []), limit=2)
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

        release_bias = str(release_year_preference.get("bias") or "balanced")
        if release_bias == "recent":
            lines.append("Prefer newer releases when the genre and source affinity are already there.")
        elif release_bias == "catalog":
            lines.append("Do not underrate older catalog titles if they fit the core genre stack.")

        if top_keywords or favorite_people:
            detail = cls._human_join(top_keywords) if top_keywords else cls._human_join(favorite_people)
            lines.append(f"Use TMDb metadata to break ties when candidates line up on {detail}.")

        if adjacent_genres or adjacent_themes:
            extension = cls._human_join(adjacent_genres) if adjacent_genres else cls._human_join(adjacent_themes)
            lines.append(f"When the core match is already strong, allow controlled exploration into {extension}.")

        if operator_notes:
            lines.append(f"Operator note: {operator_notes}.")

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
    def _intersect_strings(left: list[str], right: list[str]) -> list[str]:
        right_lookup = {value.lower() for value in right}
        return [value for value in left if value.lower() in right_lookup]

    @staticmethod
    def _extract_source_titles(sources: list[Any]) -> list[str]:
        titles: list[str] = []
        for raw in sources:
            source = str(raw).strip()
            if source.lower().startswith("recommended:"):
                title = source.split(":", 1)[1].strip()
                if title:
                    titles.append(title)
        return titles

    @classmethod
    def _score_source_affinity(
        cls,
        *,
        sources: list[Any],
        source_lanes: list[str],
        source_titles: list[str],
        top_titles: set[str],
        repeat_titles: set[str],
        recent_titles: set[str],
    ) -> float:
        score = 0.0
        lane_lookup = {lane.lower() for lane in source_lanes}
        if "repeat_watch_seed" in lane_lookup:
            score += 0.08
        if "top_seed" in lane_lookup:
            score += 0.06
        if "recent_seed" in lane_lookup:
            score += 0.06
        if "genre_anchor_seed" in lane_lookup:
            score += 0.04
        if "primary_genre_seed" in lane_lookup:
            score += 0.05
        if "recent_genre_seed" in lane_lookup:
            score += 0.04
        if "adjacent_genre_seed" in lane_lookup:
            score += 0.02
        if any(str(source).strip().lower().startswith("trending") for source in sources):
            score += 0.04

        for title in source_titles:
            lowered = title.lower()
            if lowered in repeat_titles:
                score += 0.14
            elif lowered in top_titles:
                score += 0.12
            elif lowered in recent_titles:
                score += 0.1
            else:
                score += 0.08

        return min(0.32, score)

    @staticmethod
    def _score_genre_affinity(
        *,
        candidate_genres: list[str],
        matched_primary: list[str],
        matched_secondary: list[str],
        matched_recent: list[str],
        matched_discovery: list[str],
        ranked_genres: list[dict[str, Any]],
    ) -> float:
        score = 0.0
        score += min(0.22, 0.11 * len(matched_primary))
        score += min(0.12, 0.06 * len(matched_secondary))
        score += min(0.1, 0.05 * len(matched_recent))
        score += min(0.08, 0.04 * len(matched_discovery))
        ranked_lookup = {
            str(item.get("genre") or "").lower(): float(item.get("weighted_score") or 0.0)
            for item in ranked_genres
            if isinstance(item, dict) and str(item.get("genre") or "").strip()
        }
        rank_bonus = 0.0
        for genre in candidate_genres:
            weighted_score = ranked_lookup.get(genre.lower())
            if weighted_score:
                rank_bonus += min(0.04, weighted_score / 40.0)
        score += min(0.08, rank_bonus)
        return min(0.38, score)

    @staticmethod
    def _score_format_fit(candidate_media_type: str, format_preference: dict[str, Any]) -> float:
        preferred = str(format_preference.get("preferred") or "balanced")
        if preferred == "balanced":
            return 0.04
        if candidate_media_type == preferred:
            return 0.08
        return 0.0

    @staticmethod
    def _score_quality(rating: Any, vote_count: Any) -> float:
        try:
            rating_value = float(rating or 0.0)
        except (TypeError, ValueError):
            rating_value = 0.0

        try:
            vote_value = float(vote_count or 0.0)
        except (TypeError, ValueError):
            vote_value = 0.0

        rating_norm = max(0.0, min(1.0, (rating_value - 6.0) / 3.0))
        vote_norm = max(0.0, min(1.0, vote_value / 500.0))
        return 0.1 * ((rating_norm * 0.8) + (vote_norm * 0.2))

    @classmethod
    def _score_release_year_fit(
        cls,
        *,
        candidate_release_date: Any,
        release_year_preference: dict[str, Any],
    ) -> tuple[float, str]:
        candidate_year = cls._parse_release_year(candidate_release_date)
        if candidate_year is None:
            return 0.0, "unknown"

        current_year = datetime.utcnow().year
        bias = str(release_year_preference.get("bias") or "balanced")
        average_year = release_year_preference.get("average_year")

        if bias == "recent":
            if candidate_year >= current_year - 4:
                return 0.07, "recent"
            if candidate_year >= current_year - 8:
                return 0.04, "slightly-recent"
            return 0.0, "older-than-usual"

        if bias == "catalog":
            if candidate_year <= current_year - 8:
                return 0.07, "catalog"
            if candidate_year <= current_year - 4:
                return 0.04, "mid-catalog"
            return 0.0, "newer-than-usual"

        if average_year is None:
            return 0.03, "balanced"

        year_delta = abs(int(average_year) - candidate_year)
        if year_delta <= 5:
            return 0.07, "close-to-core-window"
        if year_delta <= 10:
            return 0.04, "near-core-window"
        return 0.02, "outside-core-window"

    @staticmethod
    def _score_exploration(
        *,
        matched_discovery: list[str],
        source_titles: list[str],
        recommended_source_titles: list[str],
    ) -> float:
        if not matched_discovery:
            return 0.0
        if source_titles and recommended_source_titles:
            return 0.02
        return 0.05

    @staticmethod
    def _score_popularity(popularity: Any) -> float:
        try:
            popularity_value = float(popularity or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return min(0.03, max(0.0, popularity_value / 2000.0))

    @classmethod
    def _score_feedback_fit(
        cls,
        *,
        candidate_title: str,
        candidate_genres: list[str],
        explicit_feedback: dict[str, Any],
    ) -> float:
        if not isinstance(explicit_feedback, dict):
            return 0.0

        liked_titles = {value.lower() for value in cls._normalize_string_list(explicit_feedback.get("liked_titles", []))}
        disliked_titles = {
            value.lower() for value in cls._normalize_string_list(explicit_feedback.get("disliked_titles", []))
        }
        liked_genres = {value.lower() for value in cls._normalize_string_list(explicit_feedback.get("liked_genres", []))}
        disliked_genres = {
            value.lower() for value in cls._normalize_string_list(explicit_feedback.get("disliked_genres", []))
        }

        score = 0.0
        lowered_title = candidate_title.strip().lower()
        if lowered_title and lowered_title in liked_titles:
            score += 0.08
        if lowered_title and lowered_title in disliked_titles:
            score -= 0.08

        for genre in candidate_genres:
            lowered = genre.lower()
            if lowered in liked_genres:
                score += 0.03
            if lowered in disliked_genres:
                score -= 0.03

        return max(-0.12, min(0.12, score))

    @staticmethod
    def _match_theme_hints(candidate_keywords: list[str], theme_hints: list[str]) -> list[str]:
        matches: list[str] = []
        for hint in theme_hints:
            normalized_hint = hint.strip().lower()
            if not normalized_hint:
                continue
            for keyword in candidate_keywords:
                lowered_keyword = keyword.lower()
                if normalized_hint in lowered_keyword or lowered_keyword in normalized_hint:
                    matches.append(hint)
                    break
        deduped: list[str] = []
        seen: set[str] = set()
        for value in matches:
            lowered = value.lower()
            if lowered in seen:
                continue
            deduped.append(value)
            seen.add(lowered)
        return deduped

    @staticmethod
    def _score_tmdb_theme_affinity(
        *,
        matched_keywords: list[str],
        theme_matches: list[str],
    ) -> float:
        score = 0.0
        score += min(0.08, 0.03 * len(matched_keywords))
        score += min(0.04, 0.02 * len(theme_matches))
        return min(0.1, score)

    @staticmethod
    def _score_tmdb_people_affinity(matched_people: list[str]) -> float:
        return min(0.08, 0.03 * len(matched_people))

    @staticmethod
    def _score_tmdb_brand_affinity(
        *,
        matched_brands: list[str],
        collection_match: str | None,
    ) -> float:
        score = min(0.05, 0.025 * len(matched_brands))
        if collection_match:
            score += 0.04
        return min(0.08, score)

    @staticmethod
    def _score_tmdb_guardrails(tmdb_details: dict[str, Any]) -> float:
        if bool(tmdb_details.get("adult")):
            return -0.12
        return 0.0

    @staticmethod
    def _derive_lane_tags(
        *,
        sources: list[Any],
        source_lanes: list[str],
        matched_primary: list[str],
        matched_recent: list[str],
        matched_discovery: list[str],
    ) -> list[str]:
        tags: list[str] = []
        source_strings = [str(source).strip().lower() for source in sources]
        lane_lookup = {lane.lower() for lane in source_lanes}
        if any(source.startswith("recommended:") for source in source_strings):
            tags.append("because_you_watched")
        if "repeat_watch_seed" in lane_lookup:
            tags.append("repeat_watch_lane")
        if "genre_anchor_seed" in lane_lookup:
            tags.append("genre_match_lane")
        if {"primary_genre_seed", "recent_genre_seed"} & lane_lookup:
            tags.append("genre_discovery_lane")
        if "adjacent_genre_seed" in lane_lookup:
            tags.append("adjacent_seed_lane")
        if matched_primary:
            tags.append("top_genre_lane")
        if "recent_seed" in lane_lookup:
            tags.append("recent_seed_lane")
        if matched_recent:
            tags.append("recent_momentum_lane")
        if matched_discovery:
            tags.append("adjacent_explore_lane")
        if any(source.startswith("trending") for source in source_strings):
            tags.append("trending_lane")
        return tags or ["generic_lane"]

    @staticmethod
    def _choose_dominant_genre(
        candidate_genres: list[str],
        *,
        matched_primary: list[str],
        matched_recent: list[str],
    ) -> str:
        if matched_primary:
            return matched_primary[0]
        if matched_recent:
            return matched_recent[0]
        if candidate_genres:
            return candidate_genres[0]
        return "unknown"

    @classmethod
    def _build_analysis_summary(
        cls,
        *,
        matched_primary: list[str],
        matched_recent: list[str],
        matched_discovery: list[str],
        matched_keywords: list[str],
        theme_matches: list[str],
        matched_people: list[str],
        matched_brands: list[str],
        collection_match: str | None,
        source_titles: list[str],
        lane_tags: list[str],
        freshness_fit: str,
    ) -> str:
        parts: list[str] = []
        if source_titles:
            parts.append(f"Because-you-watched seeds: {cls._human_join(source_titles[:2])}.")
        if matched_primary:
            parts.append(f"Matches top genres {cls._human_join(matched_primary[:3])}.")
        if matched_recent:
            parts.append(f"Aligns with recent momentum in {cls._human_join(matched_recent[:2])}.")
        if matched_discovery:
            parts.append(f"Supports controlled exploration into {cls._human_join(matched_discovery[:2])}.")
        if matched_keywords or theme_matches:
            keyword_parts = matched_keywords[:2] or theme_matches[:2]
            parts.append(f"TMDb theme overlap: {cls._human_join(keyword_parts)}.")
        if matched_people:
            parts.append(f"Recurring talent match: {cls._human_join(matched_people[:2])}.")
        if matched_brands:
            parts.append(f"Brand or network overlap: {cls._human_join(matched_brands[:2])}.")
        if collection_match:
            parts.append(f"Franchise overlap via {collection_match}.")
        if freshness_fit not in {"unknown", "balanced"}:
            parts.append(f"Release fit: {freshness_fit}.")
        if not parts:
            parts.append(f"Primary lane: {lane_tags[0].replace('_', ' ')}.")
        return " ".join(parts)

    @classmethod
    def _build_release_year_preference(cls, release_years: list[int]) -> dict[str, Any]:
        if not release_years:
            return {"bias": "balanced", "average_year": None}

        average_year = round(sum(release_years) / len(release_years))
        current_year = datetime.utcnow().year
        if average_year >= current_year - 6:
            bias = "recent"
        elif average_year <= current_year - 15:
            bias = "catalog"
        else:
            bias = "balanced"

        return {"bias": bias, "average_year": average_year}

    @classmethod
    def _extract_history_release_year(cls, item: dict[str, Any]) -> int | None:
        if item.get("ProductionYear") is not None:
            try:
                return int(item.get("ProductionYear"))
            except (TypeError, ValueError):
                return None
        return cls._parse_release_year(item.get("PremiereDate"))

    @staticmethod
    def _parse_release_year(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, int):
            return value
        try:
            text = str(value).strip()
            if len(text) >= 4:
                return int(text[:4])
        except (TypeError, ValueError):
            return None
        return None

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
    def _coerce_int(value: Any) -> int | None:
        if value in ("", None):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _library_item_to_candidate(cls, item: dict[str, Any]) -> dict[str, Any] | None:
        media_type = cls._map_history_media_type(item.get("Type"))
        if media_type not in {"movie", "tv"}:
            return None

        title = cls._seed_title(item, media_type).strip()
        if not title:
            return None

        external_ids = cls._extract_external_ids(item)
        if not external_ids:
            return None

        release_date = item.get("PremiereDate")
        if not release_date and item.get("ProductionYear") is not None:
            release_date = str(item.get("ProductionYear"))

        return {
            "media_type": media_type,
            "media_id": cls._stable_candidate_media_id(media_type, title, external_ids, release_date),
            "title": title,
            "overview": item.get("Overview", ""),
            "genres": cls._normalize_genres(item.get("Genres", []), limit=6),
            "rating": item.get("CommunityRating"),
            "vote_count": 0,
            "popularity": 0,
            "release_date": release_date,
            "sources": ["library:available"],
            "source_lanes": ["available_library"],
            "media_info": {"status": "available"},
            "external_ids": external_ids,
        }

    @classmethod
    def _library_item_to_sync_payload(cls, item: dict[str, Any]) -> dict[str, Any] | None:
        media_server_id = str(item.get("Id") or "").strip()
        if not media_server_id:
            return None

        candidate = cls._library_item_to_candidate(item)
        if candidate is None:
            return None

        external_ids = candidate.get("external_ids", {}) if isinstance(candidate.get("external_ids"), dict) else {}
        return {
            "media_server_id": media_server_id,
            "media_type": str(candidate.get("media_type") or "unknown"),
            "title": str(candidate.get("title") or "Unknown"),
            "sort_title": str(item.get("SortName") or candidate.get("title") or "").strip(),
            "overview": str(candidate.get("overview") or ""),
            "production_year": cls._parse_release_year(candidate.get("release_date")),
            "release_date": str(candidate.get("release_date") or "").strip() or None,
            "community_rating": candidate.get("rating"),
            "genres": cls._normalize_genres(candidate.get("genres", []), limit=6),
            "tmdb_id": cls._coerce_int(external_ids.get("tmdb")),
            "tvdb_id": cls._coerce_int(external_ids.get("tvdb")),
            "imdb_id": str(external_ids.get("imdb") or "").strip() or None,
            "payload_json": json.dumps(item, ensure_ascii=True),
        }

    @staticmethod
    def _normalize_library_folder(item: dict[str, Any]) -> dict[str, Any] | None:
        item_id = str(item.get("ItemId") or item.get("itemId") or item.get("Id") or "").strip() or None
        name = str(item.get("Name") or item.get("name") or "").strip()
        if not name:
            return None
        return {
            "id": str(item.get("Guid") or item.get("guid") or item_id or name).strip(),
            "item_id": item_id,
            "name": name,
            "collection_type": str(item.get("CollectionType") or item.get("collectionType") or "mixed").strip()
            or "mixed",
        }

    @classmethod
    def _library_media_to_candidate(cls, row: LibraryMedia) -> dict[str, Any] | None:
        external_ids = {
            key: value
            for key, value in {
                "tmdb": row.tmdb_id,
                "tvdb": row.tvdb_id,
                "imdb": row.imdb_id,
            }.items()
            if value not in (None, "")
        }
        if not external_ids:
            return None

        genres: list[str] = []
        try:
            parsed = json.loads(row.genres_json or "[]")
            if isinstance(parsed, list):
                genres = cls._normalize_genres(parsed, limit=6)
        except json.JSONDecodeError:
            genres = []

        release_date = row.release_date or (str(row.production_year) if row.production_year is not None else None)
        return {
            "media_type": row.media_type,
            "media_id": cls._stable_candidate_media_id(row.media_type, row.title, external_ids, release_date),
            "title": row.title,
            "overview": row.overview,
            "genres": genres,
            "rating": row.community_rating,
            "vote_count": 0,
            "popularity": 0,
            "release_date": release_date,
            "sources": ["library:indexed"],
            "source_lanes": ["available_library"],
            "media_info": {"status": row.state},
            "external_ids": external_ids,
        }

    @classmethod
    def _build_watched_external_keys(cls, history: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
        watched: set[tuple[str, str, str]] = set()
        for item in history:
            media_type = cls._map_history_media_type(item.get("Type"))
            if media_type not in {"movie", "tv"}:
                continue
            external_ids = cls._extract_external_ids(item)
            for provider_key, provider_id in external_ids.items():
                watched.add((media_type, provider_key, provider_id))
        return watched

    @staticmethod
    def _candidate_matches_external_keys(
        candidate: dict[str, Any],
        watched_external_keys: set[tuple[str, str, str]],
    ) -> bool:
        media_type = str(candidate.get("media_type") or "")
        external_ids = candidate.get("external_ids", {}) if isinstance(candidate.get("external_ids"), dict) else {}
        for provider_key, provider_id in external_ids.items():
            if (media_type, str(provider_key), str(provider_id)) in watched_external_keys:
                return True
        return False

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
    def _extract_tvdb_id(item: dict[str, Any]) -> int | None:
        provider_ids = item.get("ProviderIds", {})
        raw_tvdb = provider_ids.get("Tvdb") or provider_ids.get("TVDB") or provider_ids.get("tvdb")
        if raw_tvdb is None:
            return None

        try:
            return int(raw_tvdb)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_imdb_id(item: dict[str, Any]) -> str | None:
        provider_ids = item.get("ProviderIds", {})
        raw_imdb = provider_ids.get("Imdb") or provider_ids.get("IMDB") or provider_ids.get("imdb")
        value = str(raw_imdb or "").strip()
        return value or None

    @classmethod
    def _extract_external_ids(cls, item: dict[str, Any]) -> dict[str, str]:
        external_ids: dict[str, str] = {}
        tmdb_id = cls._extract_tmdb_id(item)
        tvdb_id = cls._extract_tvdb_id(item)
        imdb_id = cls._extract_imdb_id(item)
        if tmdb_id is not None:
            external_ids["tmdb"] = str(tmdb_id)
        if tvdb_id is not None:
            external_ids["tvdb"] = str(tvdb_id)
        if imdb_id:
            external_ids["imdb"] = imdb_id
        return external_ids

    @staticmethod
    def _stable_candidate_media_id(
        media_type: str,
        title: str,
        external_ids: dict[str, str],
        release_date: Any,
    ) -> int:
        if "tmdb" in external_ids:
            try:
                return int(external_ids["tmdb"])
            except (TypeError, ValueError):
                pass
        if "tvdb" in external_ids:
            try:
                return 1_000_000_000 + int(external_ids["tvdb"])
            except (TypeError, ValueError):
                pass

        seed = "|".join(
            [
                media_type,
                title.lower(),
                str(release_date or ""),
                external_ids.get("imdb", ""),
            ]
        )
        return int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)

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
