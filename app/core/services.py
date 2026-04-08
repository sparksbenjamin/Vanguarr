from __future__ import annotations

import json
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
    PROFILE_ARCHITECT_SYSTEM_PROMPT,
    build_decision_messages,
    build_profile_architect_user_prompt,
)
from app.core.settings import Settings


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
- Awaiting Profile Architect refresh.
Avoidance Signals:
- Respect global exclusions until stronger user-specific signals exist.
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
                    compact_history = self._compact_history(history)
                    prompt = build_profile_architect_user_prompt(
                        current_username,
                        compact_history,
                        self.profile_store.read(current_username),
                    )
                    new_profile = await self.llm.generate_text(
                        system_prompt=PROFILE_ARCHITECT_SYSTEM_PROMPT,
                        user_prompt=prompt,
                        max_tokens=self.settings.llm_max_output_tokens,
                        temperature=0.1,
                    )
                    bounded_profile = self._limit_words(new_profile, max_words=500)
                    self.profile_store.write(current_username, bounded_profile)
                    updated_users.append(current_username)
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")

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

        return {
            "engine": "profile_architect",
            "status": status,
            "summary": summary,
            "updated_users": updated_users,
            "errors": errors,
        }

    async def run_decision_engine(self, username: str | None = None) -> dict[str, Any]:
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
                    profile_block = self.profile_store.read(current_username)
                    history = await self.jellyfin.get_playback_history(user["Id"], self.settings.profile_history_limit)
                    candidates = await self.seer.discover_candidates(
                        history,
                        limit=self.settings.candidate_limit,
                        seed_limit=self.settings.recommendation_seed_limit,
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
                                except Exception as exc:
                                    error = str(exc)
                                    errors.append(f"{current_username}::{candidate['title']}: {exc}")

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
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")

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

    @staticmethod
    def _compact_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
        compacted: list[dict[str, Any]] = []
        for item in history[:30]:
            compacted.append(
                {
                    "name": item.get("Name"),
                    "type": item.get("Type"),
                    "overview": item.get("Overview", "")[:500],
                    "genres": item.get("Genres", []),
                    "community_rating": item.get("CommunityRating"),
                    "date_played": item.get("UserData", {}).get("LastPlayedDate"),
                    "provider_ids": item.get("ProviderIds", {}),
                }
            )
        return compacted

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
