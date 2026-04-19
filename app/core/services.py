from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections import Counter
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.base import ClientConfigError
from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.media_server import MediaServerClientProtocol
from app.api.seer import SeerClient, SeerRequestResult
from app.api.tmdb import TMDbClient
from app.core.models import (
    DecisionLog,
    LibraryMedia,
    RequestOutcomeEvent,
    RequestedMedia,
    RequestedMediaSupporter,
    SeerWebhookEvent,
    SuggestedMedia,
    TaskRun,
)
from app.core.prompts import (
    build_decision_messages,
    build_profile_enrichment_messages,
    build_suggestion_messages,
)
from app.core.settings import Settings


logger = logging.getLogger("vanguarr.service")


def normalize_jellyfin_user_id(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return uuid.UUID(raw).hex
    except (ValueError, AttributeError):
        compact = raw.replace("-", "").strip().lower()
        return compact or None


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
            "enabled": True,
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
            "favorite_titles": [],
            "favorite_genres": [],
            "favorite_signal_count": 0,
            "format_preference": {"preferred": "balanced", "movie_plays": 0, "tv_plays": 0},
            "release_year_preference": {"bias": "balanced", "average_year": None},
            "average_top_rating": None,
            "genre_focus_share": 0.0,
            "discovery_lanes": [],
            "adjacent_genres": [],
            "adjacent_themes": [],
            "seer_adjacent_titles": [],
            "seer_adjacent_genres": [],
            "similar_users": [],
            "similar_user_genres": [],
            "similar_user_titles": [],
            "seed_lanes": [],
            "explicit_feedback": {
                "liked_titles": [],
                "disliked_titles": [],
                "liked_genres": [],
                "disliked_genres": [],
            },
            "blocked_titles": [],
            "profile_exclusions": [],
            "operator_notes": "",
            "request_outcome_insights": {
                "counts": {},
                "positive_titles": [],
                "negative_titles": [],
                "positive_genres": [],
                "negative_genres": [],
                "recent_outcomes": [],
            },
            "profile_review": {
                "health_score": 0,
                "health_status": "unknown",
                "confidence": "low",
                "freshness": "unknown",
                "warnings": [],
                "strengths": [],
                "changed_fields": [],
                "diff_summary": [],
                "evidence": {},
                "summary": "",
                "last_reviewed_at": None,
            },
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
        return int(payload.get("history_count") or 0) > 0 or bool(payload.get("favorite_titles"))

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

    def _playback_history_limit(self) -> int | None:
        if bool(getattr(self.settings, "profile_use_full_history", False)):
            return None
        return max(1, int(self.settings.profile_history_limit))

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

    def is_profile_enabled(self, username: str) -> bool:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            return True
        payload = self.profile_store.read_payload(cleaned_username)
        return self._normalize_profile_enabled(payload.get("enabled", True))

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
        feed = self.get_log_feed(
            search=search,
            limit=limit or self.settings.decision_page_size,
        )
        return feed["raw_rows"]

    def get_log_feed(
        self,
        *,
        search: str | None = None,
        view: str = "all",
        sort_by: str = "created_at",
        sort_direction: str = "desc",
        page: int = 1,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_view = self._normalize_log_view(view)
        normalized_sort = self._normalize_log_sort(sort_by)
        normalized_direction = "asc" if str(sort_direction).lower() == "asc" else "desc"
        page_size = max(1, int(limit or self.settings.decision_page_size))
        search_value = str(search or "").strip()

        with self.session_scope() as session:
            base_conditions = self._build_log_conditions(search_value)
            counts = {
                "all": self._count_logs(session, base_conditions),
                "requests": self._count_logs(
                    session,
                    [*base_conditions, DecisionLog.engine == "decision_engine"],
                ),
                "suggestions": self._count_logs(
                    session,
                    [*base_conditions, DecisionLog.engine == "suggested_for_you"],
                ),
            }

            filtered_conditions = [*base_conditions, *self._view_log_conditions(normalized_view)]
            total_rows = self._count_logs(session, filtered_conditions)
            total_pages = max(1, (total_rows + page_size - 1) // page_size) if total_rows else 1
            current_page = min(max(1, int(page)), total_pages)
            offset = (current_page - 1) * page_size

            stmt = select(DecisionLog)
            if filtered_conditions:
                stmt = stmt.where(*filtered_conditions)
            stmt = stmt.order_by(*self._log_ordering(normalized_sort, normalized_direction))
            stmt = stmt.offset(offset).limit(page_size)
            rows = list(session.scalars(stmt))
            error_rows = self._count_logs(session, [*filtered_conditions, DecisionLog.error.is_not(None)])

        return {
            "rows": [self._serialize_log_row(row) for row in rows],
            "raw_rows": rows,
            "query": search_value,
            "view": normalized_view,
            "sort_by": normalized_sort,
            "sort_direction": normalized_direction,
            "page": current_page,
            "page_size": page_size,
            "total_rows": total_rows,
            "total_pages": total_pages,
            "has_previous": current_page > 1,
            "has_next": current_page < total_pages,
            "view_counts": counts,
            "error_rows": error_rows,
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }

    def get_task_runs(self, limit: int = 10) -> list[TaskRun]:
        with self.session_scope() as session:
            stmt = select(TaskRun).order_by(desc(TaskRun.started_at)).limit(limit)
            return list(session.scalars(stmt))

    @staticmethod
    def _normalize_log_view(value: str | None) -> str:
        raw = str(value or "all").strip().lower()
        if raw in {"requests", "suggestions"}:
            return raw
        return "all"

    @staticmethod
    def _normalize_log_sort(value: str | None) -> str:
        raw = str(value or "created_at").strip().lower()
        if raw in {"created_at", "engine", "username", "media_title", "decision", "confidence", "requested", "reasoning"}:
            return raw
        return "created_at"

    @classmethod
    def _build_log_conditions(cls, search: str) -> list[Any]:
        if not search:
            return []
        like = f"%{search}%"
        return [
            or_(
                DecisionLog.username.ilike(like),
                DecisionLog.media_title.ilike(like),
                DecisionLog.reasoning.ilike(like),
                DecisionLog.source.ilike(like),
                DecisionLog.decision.ilike(like),
                DecisionLog.engine.ilike(like),
            )
        ]

    @staticmethod
    def _view_log_conditions(view: str) -> list[Any]:
        if view == "requests":
            return [DecisionLog.engine == "decision_engine"]
        if view == "suggestions":
            return [DecisionLog.engine == "suggested_for_you"]
        return []

    @staticmethod
    def _count_logs(session: Session, conditions: list[Any]) -> int:
        stmt = select(func.count(DecisionLog.id))
        if conditions:
            stmt = stmt.where(*conditions)
        return int(session.scalar(stmt) or 0)

    @staticmethod
    def _log_ordering(sort_by: str, sort_direction: str) -> tuple[Any, ...]:
        column = {
            "created_at": DecisionLog.created_at,
            "engine": DecisionLog.engine,
            "username": DecisionLog.username,
            "media_title": DecisionLog.media_title,
            "decision": DecisionLog.decision,
            "confidence": DecisionLog.confidence,
            "requested": DecisionLog.requested,
            "reasoning": DecisionLog.reasoning,
        }.get(sort_by, DecisionLog.created_at)

        primary = column.asc() if sort_direction == "asc" else column.desc()
        if sort_by == "created_at":
            return (primary, DecisionLog.id.desc())
        return (primary, DecisionLog.created_at.desc(), DecisionLog.id.desc())

    @staticmethod
    def _format_log_timestamp(value: datetime | None) -> str:
        if value is None:
            return ""
        return value.replace(microsecond=0).isoformat(sep=" ")

    @classmethod
    def _serialize_log_row(cls, row: DecisionLog) -> dict[str, Any]:
        return {
            "id": row.id,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "created_at_display": cls._format_log_timestamp(row.created_at),
            "engine": row.engine,
            "engine_label": cls._engine_label(row.engine),
            "username": row.username,
            "media_type": row.media_type,
            "media_id": row.media_id,
            "media_title": row.media_title,
            "source": row.source,
            "decision": row.decision,
            "confidence": float(row.confidence or 0.0),
            "threshold": float(row.threshold or 0.0),
            "requested": bool(row.requested),
            "request_id": row.request_id,
            "reasoning": row.reasoning,
            "error": row.error,
        }

    @staticmethod
    def _engine_label(engine: str) -> str:
        labels = {
            "profile_architect": "Profile Architect",
            "decision_engine": "Decision Engine",
            "suggested_for_you": "Suggested For You",
            "library_sync": "Library Sync",
            "request_status_sync": "Request Status Sync",
            "profile_feedback": "Profile Feedback",
            "request_outcome": "Request Outcome",
            "decision_preview": "Decision Dry Run",
            "backtesting": "Backtesting",
        }
        normalized = str(engine or "").strip()
        return labels.get(normalized, normalized.replace("_", " ").title() or "System")

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

    def get_profile_payload_with_live_context(self, username: str) -> dict[str, Any]:
        payload = self.profile_store.read_payload(username)
        return self._with_live_profile_context(username, payload)

    def _with_live_profile_context(self, username: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_saved_profile_payload(username, payload)
        insights = self._build_request_outcome_insights(username)
        normalized["request_outcome_insights"] = insights
        stored_review = self._normalize_profile_review(payload.get("profile_review", {}))
        live_review = self._build_profile_review(normalized)
        if stored_review.get("diff_summary"):
            live_review["diff_summary"] = stored_review["diff_summary"]
            live_review["changed_fields"] = stored_review["changed_fields"]
        normalized["profile_review"] = live_review
        return normalized

    def _requested_media_payload(self, session: Session, row: RequestedMedia) -> dict[str, Any]:
        conditions = [
            DecisionLog.username == row.username,
            DecisionLog.media_type == row.media_type,
            DecisionLog.media_id == row.media_id,
            DecisionLog.requested.is_(True),
        ]
        if row.seer_request_id is not None:
            conditions = [or_(DecisionLog.request_id == row.seer_request_id, *conditions)]
        log = session.scalar(
            select(DecisionLog)
            .where(*conditions)
            .order_by(desc(DecisionLog.created_at))
            .limit(1)
        )
        if log is None:
            return {}
        try:
            payload = json.loads(log.payload_json or "{}")
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _request_supporter_lookup(
        session: Session,
        requested_media_ids: list[int] | set[int],
    ) -> dict[int, list[str]]:
        ids = sorted({int(value) for value in requested_media_ids if int(value or 0) > 0})
        if not ids:
            return {}

        supporters: dict[int, list[str]] = {}
        rows = session.scalars(
            select(RequestedMediaSupporter)
            .where(RequestedMediaSupporter.requested_media_id.in_(ids))
            .order_by(RequestedMediaSupporter.created_at.asc())
        )
        for row in rows:
            username = str(row.username or "").strip()
            if not username:
                continue
            supporters.setdefault(int(row.requested_media_id), []).append(username)
        return supporters

    @staticmethod
    def _request_audience_usernames(
        session: Session,
        requested_row: RequestedMedia,
    ) -> list[str]:
        usernames: list[str] = []
        owner = str(requested_row.username or "").strip()
        if owner:
            usernames.append(owner)

        supporters = VanguarrService._request_supporter_lookup(session, [int(requested_row.id or 0)]).get(
            int(requested_row.id or 0),
            [],
        )
        for username in supporters:
            if username.lower() not in {value.lower() for value in usernames}:
                usernames.append(username)
        return usernames

    def add_request_supporter(
        self,
        *,
        requested_media_id: int,
        username: str,
        source: str = "manual",
        reason: str = "",
    ) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            raise ValueError("Username is required to support a shared request.")

        with self.session_scope() as session:
            requested_row = session.get(RequestedMedia, requested_media_id)
            if requested_row is None:
                raise ValueError("Shared request was not found.")
            if str(requested_row.username or "").strip().lower() == cleaned_username.lower():
                return {
                    "created": False,
                    "requested_media_id": requested_row.id,
                    "media_title": requested_row.media_title,
                    "owner_username": requested_row.username,
                }

            existing = session.scalar(
                select(RequestedMediaSupporter).where(
                    RequestedMediaSupporter.requested_media_id == requested_row.id,
                    RequestedMediaSupporter.username == cleaned_username,
                )
            )
            if existing is None:
                session.add(
                    RequestedMediaSupporter(
                        requested_media_id=requested_row.id,
                        username=cleaned_username,
                        source=source,
                    )
                )
                created = True
            else:
                created = False

            media_title = requested_row.media_title
            media_type = requested_row.media_type
            owner_username = requested_row.username

        if created:
            self.record_operation_event(
                engine="request_support",
                username=cleaned_username,
                media_type=media_type,
                media_title=media_title,
                source=source,
                decision="SUPPORT",
                reasoning=reason or f"Attached {cleaned_username} as a supporter on shared request {media_title}.",
                detail_payload={
                    "requested_media_id": requested_media_id,
                    "owner_username": owner_username,
                },
            )
        live_payload = self._with_live_profile_context(cleaned_username, self.profile_store.read_payload(cleaned_username))
        self.profile_store.write_payload(cleaned_username, live_payload)
        return {
            "created": created,
            "requested_media_id": requested_media_id,
            "media_title": media_title,
            "owner_username": owner_username,
        }

    def get_request_history(self, username: str, limit: int = 10) -> list[dict[str, Any]]:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            return []

        with self.session_scope() as session:
            owned_rows = list(
                session.scalars(
                    select(RequestedMedia)
                    .where(RequestedMedia.username == cleaned_username)
                    .order_by(desc(RequestedMedia.created_at))
                    .limit(limit)
                )
            )
            supporter_rows = list(
                session.scalars(
                    select(RequestedMediaSupporter)
                    .where(RequestedMediaSupporter.username == cleaned_username)
                    .order_by(desc(RequestedMediaSupporter.created_at))
                    .limit(limit)
                )
            )
            supported_ids = {int(row.requested_media_id) for row in supporter_rows if int(row.requested_media_id or 0) > 0}
            supported_requested_rows = list(
                session.scalars(
                    select(RequestedMedia)
                    .where(RequestedMedia.id.in_(supported_ids))
                    .order_by(desc(RequestedMedia.created_at))
                )
            ) if supported_ids else []
            requested_rows = sorted(
                {row.id: row for row in [*owned_rows, *supported_requested_rows]}.values(),
                key=lambda row: row.created_at or datetime.min,
                reverse=True,
            )[:limit]
            if not requested_rows:
                return []

            requested_ids = [row.id for row in requested_rows]
            request_ids = [row.seer_request_id for row in requested_rows if row.seer_request_id is not None]
            outcome_rows = list(
                session.scalars(
                    select(RequestOutcomeEvent)
                    .where(
                        or_(
                            RequestOutcomeEvent.requested_media_id.in_(requested_ids),
                            RequestOutcomeEvent.request_id.in_(request_ids) if request_ids else False,
                            RequestOutcomeEvent.username == cleaned_username,
                        )
                    )
                    .order_by(desc(RequestOutcomeEvent.created_at))
                )
            )

            payload_cache = {
                row.id: self._requested_media_payload(session, row)
                for row in requested_rows
            }
            supporter_lookup = self._request_supporter_lookup(session, requested_ids)

        history: list[dict[str, Any]] = []
        for row in requested_rows:
            matching_events = [
                event
                for event in outcome_rows
                if (
                    event.requested_media_id == row.id
                    or (row.seer_request_id is not None and event.request_id == row.seer_request_id)
                    or (
                        event.username == row.username
                        and event.media_type == row.media_type
                        and event.media_id == row.media_id
                    )
                )
            ]
            timeline = [
                {
                    "id": event.id,
                    "outcome": self._normalize_request_outcome_label(event.outcome),
                    "detail": str(event.detail or "").strip(),
                    "source": str(event.source or "").strip() or "manual",
                    "created_at": event.created_at.isoformat() if event.created_at else None,
                }
                for event in matching_events[:6]
            ]
            latest = timeline[0] if timeline else None
            payload = payload_cache.get(row.id, {})
            supporters = supporter_lookup.get(int(row.id), [])
            audience = [row.username, *supporters]
            shared_with = [value for value in audience if value.lower() != cleaned_username.lower()]
            history.append(
                {
                    "id": row.id,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "username": row.username,
                    "requested_by": row.username,
                    "supporters": supporters,
                    "audience": audience,
                    "shared_with": shared_with,
                    "is_owner": row.username == cleaned_username,
                    "is_supporting": cleaned_username in supporters,
                    "media_type": row.media_type,
                    "media_id": row.media_id,
                    "media_title": row.media_title,
                    "source": row.source,
                    "seer_request_id": row.seer_request_id,
                    "latest_outcome": latest["outcome"] if latest else "requested",
                    "latest_outcome_source": latest["source"] if latest else "decision_engine",
                    "latest_outcome_detail": latest["detail"] if latest else "",
                    "timeline": timeline,
                    "genres": self._normalize_genres(payload.get("genres", []), limit=5),
                }
            )
        return history

    def update_profile_feedback(
        self,
        *,
        username: str,
        action: str,
        title: str,
        genres: list[str] | None = None,
        media_type: str = "unknown",
        source: str = "manual",
    ) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        cleaned_title = str(title or "").strip()
        normalized_action = str(action or "").strip().lower().replace("-", "_")
        if not cleaned_username or not cleaned_title:
            raise ValueError("Username and title are required for profile feedback.")

        payload = self.profile_store.read_payload(cleaned_username)
        feedback = self._normalize_explicit_feedback(payload.get("explicit_feedback", {}))
        blocked_titles = self._normalize_string_list(payload.get("blocked_titles", []), limit=12)
        candidate_genres = self._normalize_genres(genres or [], limit=5)

        def add_unique(items: list[str], value: str, *, limit: int) -> list[str]:
            return self._normalize_string_list(items + [value], limit=limit)

        def remove_casefold(items: list[str], value: str) -> list[str]:
            lowered = value.lower()
            return [item for item in items if item.lower() != lowered]

        if normalized_action == "like":
            feedback["liked_titles"] = add_unique(feedback["liked_titles"], cleaned_title, limit=12)
            feedback["disliked_titles"] = remove_casefold(feedback["disliked_titles"], cleaned_title)
            blocked_titles = remove_casefold(blocked_titles, cleaned_title)
        elif normalized_action == "dislike":
            feedback["disliked_titles"] = add_unique(feedback["disliked_titles"], cleaned_title, limit=12)
            feedback["liked_titles"] = remove_casefold(feedback["liked_titles"], cleaned_title)
        elif normalized_action == "more_like_this":
            feedback["liked_titles"] = add_unique(feedback["liked_titles"], cleaned_title, limit=12)
            feedback["disliked_titles"] = remove_casefold(feedback["disliked_titles"], cleaned_title)
            blocked_titles = remove_casefold(blocked_titles, cleaned_title)
            for genre in candidate_genres:
                feedback["liked_genres"] = add_unique(feedback["liked_genres"], genre, limit=8)
                feedback["disliked_genres"] = remove_casefold(feedback["disliked_genres"], genre)
        elif normalized_action == "less_like_this":
            feedback["disliked_titles"] = add_unique(feedback["disliked_titles"], cleaned_title, limit=12)
            feedback["liked_titles"] = remove_casefold(feedback["liked_titles"], cleaned_title)
            for genre in candidate_genres:
                feedback["disliked_genres"] = add_unique(feedback["disliked_genres"], genre, limit=8)
                feedback["liked_genres"] = remove_casefold(feedback["liked_genres"], genre)
        elif normalized_action == "never_again":
            blocked_titles = add_unique(blocked_titles, cleaned_title, limit=12)
            feedback["disliked_titles"] = add_unique(feedback["disliked_titles"], cleaned_title, limit=12)
            feedback["liked_titles"] = remove_casefold(feedback["liked_titles"], cleaned_title)
        else:
            raise ValueError(f"Unsupported feedback action: {action}")

        payload["explicit_feedback"] = feedback
        payload["blocked_titles"] = blocked_titles
        normalized = self._normalize_saved_profile_payload(cleaned_username, payload)
        normalized["request_outcome_insights"] = self._normalize_request_outcome_insights(
            payload.get("request_outcome_insights", {})
        )
        normalized["profile_review"] = self._build_profile_review(normalized)
        self.profile_store.write_payload(cleaned_username, normalized)

        verb_lookup = {
            "like": "liked",
            "dislike": "disliked",
            "more_like_this": "asked for more titles like",
            "less_like_this": "asked for less titles like",
            "never_again": "blocked",
        }
        self.record_operation_event(
            engine="profile_feedback",
            username=cleaned_username,
            media_type=media_type or "unknown",
            media_title=cleaned_title,
            source=source,
            decision=normalized_action.upper(),
            reasoning=(
                f"Operator {verb_lookup.get(normalized_action, normalized_action)} {cleaned_title}"
                + (f" with genre hints {self._human_join(candidate_genres)}." if candidate_genres and normalized_action in {"more_like_this", "less_like_this"} else ".")
            ),
            detail_payload={
                "action": normalized_action,
                "title": cleaned_title,
                "genres": candidate_genres,
            },
        )
        return normalized

    def update_profile_guidance(
        self,
        *,
        username: str,
        enabled: bool | None = None,
        liked_titles: list[str] | None = None,
        disliked_titles: list[str] | None = None,
        liked_genres: list[str] | None = None,
        disliked_genres: list[str] | None = None,
        blocked_titles: list[str] | None = None,
        profile_exclusions: list[str] | None = None,
        operator_notes: str = "",
        source: str = "manual",
    ) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            raise ValueError("A username is required before saving profile guidance.")

        payload = self.profile_store.read_payload(cleaned_username)
        if enabled is not None:
            payload["enabled"] = bool(enabled)
        payload["explicit_feedback"] = self._normalize_explicit_feedback(
            {
                "liked_titles": liked_titles or [],
                "disliked_titles": disliked_titles or [],
                "liked_genres": liked_genres or [],
                "disliked_genres": disliked_genres or [],
            }
        )
        payload["blocked_titles"] = self._normalize_string_list(blocked_titles or [], limit=12)
        payload["profile_exclusions"] = self._normalize_string_list(profile_exclusions or [], limit=8)
        payload["operator_notes"] = str(operator_notes or "").strip()

        normalized = self._normalize_saved_profile_payload(cleaned_username, payload)
        normalized["request_outcome_insights"] = self._normalize_request_outcome_insights(
            payload.get("request_outcome_insights", {})
        )
        normalized["profile_review"] = self._build_profile_review(normalized)
        self.profile_store.write_payload(cleaned_username, normalized)

        self.record_operation_event(
            engine="profile_feedback",
            username=cleaned_username,
            media_type="profile",
            media_title=f"Guidance updated for {cleaned_username}",
            source=source,
            decision="GUIDANCE",
            reasoning="Updated editable profile guidance, request eligibility, exclusions, and operator notes.",
            detail_payload={
                "enabled": normalized.get("enabled", True),
                "liked_titles": normalized["explicit_feedback"]["liked_titles"],
                "disliked_titles": normalized["explicit_feedback"]["disliked_titles"],
                "liked_genres": normalized["explicit_feedback"]["liked_genres"],
                "disliked_genres": normalized["explicit_feedback"]["disliked_genres"],
                "blocked_titles": normalized.get("blocked_titles", []),
                "profile_exclusions": normalized.get("profile_exclusions", []),
                "operator_notes": normalized.get("operator_notes", ""),
            },
        )
        return normalized

    def record_request_outcome(
        self,
        *,
        username: str,
        requested_media_id: int,
        outcome: str,
        source: str = "manual",
        detail: str = "",
    ) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        normalized_outcome = self._normalize_request_outcome_label(outcome)
        if normalized_outcome not in {"approved", "denied", "ignored", "unavailable", "downloaded", "watched"}:
            raise ValueError("Unsupported request outcome.")

        affected_usernames: list[str] = []
        with self.session_scope() as session:
            requested_row = session.get(RequestedMedia, requested_media_id)
            if requested_row is None:
                raise ValueError("Requested media entry was not found for that user.")

            audience = self._request_audience_usernames(session, requested_row)
            if cleaned_username.lower() not in {value.lower() for value in audience}:
                raise ValueError("Requested media entry was not found for that user.")

            payload = self._requested_media_payload(session, requested_row)
            target_usernames = [cleaned_username] if normalized_outcome == "watched" else audience
            for target_username in target_usernames:
                session.add(
                    RequestOutcomeEvent(
                        requested_media_id=requested_row.id,
                        username=target_username,
                        media_type=requested_row.media_type,
                        media_id=requested_row.media_id,
                        media_title=requested_row.media_title,
                        request_id=requested_row.seer_request_id,
                        outcome=normalized_outcome,
                        source=source,
                        detail=str(detail or "").strip(),
                        payload_json=json.dumps(payload, ensure_ascii=True),
                    )
                )
            affected_usernames = list(dict.fromkeys(target_usernames))

        for target_username in affected_usernames:
            self.record_operation_event(
                engine="request_outcome",
                username=target_username,
                media_type=requested_row.media_type,
                media_title=requested_row.media_title,
                source=source,
                decision=normalized_outcome.upper(),
                reasoning=(
                    f"Recorded request outcome {normalized_outcome} for {requested_row.media_title}."
                    + (" Shared request audience was updated." if len(affected_usernames) > 1 else "")
                ),
                detail_payload={
                    "requested_media_id": requested_media_id,
                    "request_id": requested_row.seer_request_id,
                    "outcome": normalized_outcome,
                    "affected_usernames": affected_usernames,
                },
            )
            live_payload = self._with_live_profile_context(target_username, self.profile_store.read_payload(target_username))
            self.profile_store.write_payload(target_username, live_payload)
        return {
            "requested_media_id": requested_media_id,
            "outcome": normalized_outcome,
            "media_title": requested_row.media_title,
        }

    def _build_request_outcome_insights(self, username: str) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            return self._normalize_request_outcome_insights({})

        with self.session_scope() as session:
            rows = list(
                session.scalars(
                    select(RequestOutcomeEvent)
                    .where(RequestOutcomeEvent.username == cleaned_username)
                    .order_by(desc(RequestOutcomeEvent.created_at))
                    .limit(80)
                )
            )

        if not rows:
            return self._normalize_request_outcome_insights({})

        latest_by_key: dict[tuple[Any, ...], RequestOutcomeEvent] = {}
        recent_outcomes: list[dict[str, Any]] = []
        for row in rows:
            outcome = self._normalize_request_outcome_label(row.outcome)
            key = (
                int(row.requested_media_id or 0),
                str(row.media_type or ""),
                int(row.media_id or 0),
                str(row.media_title or "").strip().lower(),
            )
            latest_by_key.setdefault(key, row)
            recent_outcomes.append(
                {
                    "title": row.media_title,
                    "outcome": outcome,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "source": row.source,
                }
            )
            if len(recent_outcomes) >= 8:
                break

        positive_outcomes = {"approved", "downloaded", "watched"}
        negative_outcomes = {"denied", "ignored", "unavailable"}
        counts: Counter[str] = Counter()
        positive_titles: Counter[str] = Counter()
        negative_titles: Counter[str] = Counter()
        positive_genres: Counter[str] = Counter()
        negative_genres: Counter[str] = Counter()

        for row in latest_by_key.values():
            outcome = self._normalize_request_outcome_label(row.outcome)
            counts[outcome] += 1
            title = str(row.media_title or "").strip()
            try:
                payload = json.loads(row.payload_json or "{}")
            except json.JSONDecodeError:
                payload = {}
            payload = payload if isinstance(payload, dict) else {}
            genres = self._normalize_genres(payload.get("genres", []), limit=5)
            if outcome in positive_outcomes:
                if title:
                    positive_titles[title] += 1
                for genre in genres:
                    positive_genres[genre] += 1
            elif outcome in negative_outcomes:
                if title:
                    negative_titles[title] += 1
                for genre in genres:
                    negative_genres[genre] += 1

        return self._normalize_request_outcome_insights(
            {
                "counts": dict(counts),
                "positive_titles": self._rank_counter(positive_titles, limit=4),
                "negative_titles": self._rank_counter(negative_titles, limit=4),
                "positive_genres": self._rank_counter(positive_genres, limit=4),
                "negative_genres": self._rank_counter(negative_genres, limit=4),
                "recent_outcomes": recent_outcomes,
            }
        )

    async def _load_user_favorite_items(
        self,
        user_id: str,
        *,
        username: str,
    ) -> list[dict[str, Any]]:
        get_favorite_items = getattr(self.media_server, "get_favorite_items", None)
        if not callable(get_favorite_items):
            return []
        try:
            favorite_items = await get_favorite_items(user_id)
        except Exception as exc:
            logger.warning("Favorite-item enrichment skipped user=%s reason=%s", username, exc)
            return []
        return favorite_items if isinstance(favorite_items, list) else []

    def sync_watched_request_outcomes_from_history(
        self,
        *,
        username: str,
        history: list[dict[str, Any]],
        source: str = "profile_architect",
    ) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        if not cleaned_username or not history:
            return {"count": 0, "titles": []}

        watch_timestamps = self._build_history_watch_timestamps(history)
        if not watch_timestamps["media_keys"] and not watch_timestamps["title_keys"]:
            return {"count": 0, "titles": []}

        updates: list[dict[str, Any]] = []
        with self.session_scope() as session:
            owned_rows = list(
                session.scalars(
                    select(RequestedMedia)
                    .where(RequestedMedia.username == cleaned_username)
                    .order_by(desc(RequestedMedia.created_at))
                )
            )
            support_rows = list(
                session.scalars(
                    select(RequestedMediaSupporter)
                    .where(RequestedMediaSupporter.username == cleaned_username)
                    .order_by(desc(RequestedMediaSupporter.created_at))
                )
            )
            supported_ids = {int(row.requested_media_id) for row in support_rows if int(row.requested_media_id or 0) > 0}
            supported_requested_rows = list(
                session.scalars(
                    select(RequestedMedia)
                    .where(RequestedMedia.id.in_(supported_ids))
                    .order_by(desc(RequestedMedia.created_at))
                )
            ) if supported_ids else []
            requested_rows = list(
                {row.id: row for row in [*owned_rows, *supported_requested_rows]}.values()
            )
            if not requested_rows:
                return {"count": 0, "titles": []}

            watched_events = list(
                session.scalars(
                    select(RequestOutcomeEvent).where(
                        RequestOutcomeEvent.username == cleaned_username,
                        RequestOutcomeEvent.outcome == "watched",
                    )
                )
            )
            watched_requested_ids = {
                int(event.requested_media_id)
                for event in watched_events
                if event.requested_media_id is not None
            }
            watched_request_ids = {
                int(event.request_id)
                for event in watched_events
                if event.request_id is not None
            }
            watched_media_keys = {
                (str(event.media_type or ""), int(event.media_id or 0))
                for event in watched_events
                if int(event.media_id or 0) > 0
            }
            watched_title_keys = {
                (str(event.media_type or ""), str(event.media_title or "").strip().lower())
                for event in watched_events
                if str(event.media_title or "").strip()
            }

            for row in requested_rows:
                if row.id in watched_requested_ids:
                    continue
                if row.seer_request_id is not None and int(row.seer_request_id) in watched_request_ids:
                    continue
                row_media_key = (str(row.media_type or ""), int(row.media_id or 0))
                row_title_key = (str(row.media_type or ""), str(row.media_title or "").strip().lower())
                if row_media_key in watched_media_keys or row_title_key in watched_title_keys:
                    continue

                playback_ts = max(
                    float(watch_timestamps["media_keys"].get(row_media_key, 0.0)),
                    float(watch_timestamps["title_keys"].get(row_title_key, 0.0)),
                )
                if playback_ts <= 0:
                    continue

                request_ts = (
                    row.created_at.replace(tzinfo=timezone.utc).timestamp()
                    if row.created_at
                    else 0.0
                )
                if request_ts and playback_ts < request_ts:
                    continue

                payload = self._requested_media_payload(session, row)
                detail = "Inferred watched from playback history during Profile Architect."
                if playback_ts > 0:
                    detail = (
                        "Inferred watched from playback history during Profile Architect after playback at "
                        f"{datetime.utcfromtimestamp(playback_ts).replace(microsecond=0).isoformat()}Z."
                    )
                session.add(
                    RequestOutcomeEvent(
                        requested_media_id=row.id,
                        username=row.username,
                        media_type=row.media_type,
                        media_id=row.media_id,
                        media_title=row.media_title,
                        request_id=row.seer_request_id,
                        outcome="watched",
                        source=source,
                        detail=detail,
                        payload_json=json.dumps(payload, ensure_ascii=True),
                    )
                )
                updates.append(
                    {
                        "requested_media_id": row.id,
                        "request_id": row.seer_request_id,
                        "media_type": row.media_type,
                        "media_id": row.media_id,
                        "media_title": row.media_title,
                        "playback_ts": playback_ts,
                    }
                )

        for update in updates:
            detail_payload = {
                "requested_media_id": update["requested_media_id"],
                "request_id": update["request_id"],
                "media_id": update["media_id"],
                "source": source,
                "playback_at": (
                    datetime.utcfromtimestamp(float(update["playback_ts"])).replace(microsecond=0).isoformat() + "Z"
                    if float(update["playback_ts"] or 0.0) > 0
                    else None
                ),
            }
            self.record_operation_event(
                engine="request_outcome",
                username=cleaned_username,
                media_type=str(update["media_type"] or "unknown"),
                media_title=str(update["media_title"] or "Unknown"),
                source=source,
                decision="WATCHED",
                reasoning=(
                    f"Inferred watched from playback history during Profile Architect for "
                    f"{update['media_title']}."
                ),
                detail_payload=detail_payload,
            )

        if updates:
            live_payload = self._with_live_profile_context(cleaned_username, self.profile_store.read_payload(cleaned_username))
            self.profile_store.write_payload(cleaned_username, live_payload)

        return {
            "count": len(updates),
            "titles": [str(item["media_title"] or "") for item in updates if str(item["media_title"] or "").strip()],
        }

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
                normalized_user_id = normalize_jellyfin_user_id(jellyfin_user_id)
                user_id_candidates = {
                    candidate
                    for candidate in {str(jellyfin_user_id).strip(), normalized_user_id}
                    if candidate
                }
                stmt = stmt.where(SuggestedMedia.jellyfin_user_id.in_(sorted(user_id_candidates)))
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

    def get_request_status_sync_snapshot(self) -> dict[str, Any]:
        with self.session_scope() as session:
            tracked_requests = int(session.scalar(select(func.count(RequestedMedia.id))) or 0)
            seer_linked_requests = int(
                session.scalar(
                    select(func.count(RequestedMedia.id)).where(RequestedMedia.seer_request_id.is_not(None))
                )
                or 0
            )
            synced_outcomes = int(
                session.scalar(
                    select(func.count(RequestOutcomeEvent.id)).where(RequestOutcomeEvent.source == "seer_sync")
                )
                or 0
            )
            last_task = session.scalar(
                select(TaskRun)
                .where(TaskRun.engine == "request_status_sync")
                .order_by(desc(TaskRun.started_at))
                .limit(1)
            )

        return {
            "tracked_requests": tracked_requests,
            "seer_linked_requests": seer_linked_requests,
            "synced_outcomes": synced_outcomes,
            "last_task": last_task,
            "task_status": self.get_task_snapshot("request_status_sync"),
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

    def get_task_snapshot_for_target(self, engine_name: str, username: str | None = None) -> dict[str, Any]:
        target_username = str(username or "").strip().casefold()
        if not target_username:
            return self.get_task_snapshot(engine_name)

        with self.session_scope() as session:
            tasks = list(
                session.scalars(
                    select(TaskRun)
                    .where(TaskRun.engine == engine_name)
                    .order_by(desc(TaskRun.started_at))
                    .limit(40)
                )
            )

        for task in tasks:
            if self._task_matches_username(task, target_username):
                return self._serialize_task_run(task)
        return self._serialize_task_run(None)

    def get_profile_task_snapshots(self, username: str | None) -> dict[str, dict[str, Any]]:
        engines = ("profile_architect", "decision_engine", "decision_preview", "suggested_for_you")
        return {
            engine: self.get_task_snapshot_for_target(engine, username)
            for engine in engines
        }

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

    @staticmethod
    def _history_item_played_timestamp(item: dict[str, Any]) -> float:
        last_played_ts = VanguarrService._to_timestamp(item.get("UserData", {}).get("LastPlayedDate"))
        if last_played_ts <= 0:
            last_played_ts = VanguarrService._to_timestamp(item.get("DatePlayed"))
        return last_played_ts

    @classmethod
    def _split_history_for_backtest(
        cls,
        history: list[dict[str, Any]],
        *,
        cutoff_ts: float,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        baseline: list[dict[str, Any]] = []
        holdout: list[dict[str, Any]] = []
        for item in history:
            played_ts = cls._history_item_played_timestamp(item)
            if played_ts <= 0:
                baseline.append(item)
                continue
            if played_ts >= cutoff_ts:
                holdout.append(item)
            else:
                baseline.append(item)
        return baseline, holdout

    @classmethod
    def _backtest_candidate_skip_reason(
        cls,
        candidate: dict[str, Any],
        *,
        profile_summary: dict[str, Any],
        prefilter_threshold: float,
        watched_media_keys: set[tuple[str, int]],
        watched_external_keys: set[tuple[str, str, str]],
        watched_title_keys: set[tuple[str, str]],
        requested_media_keys: set[tuple[str, int]],
        requested_title_keys: set[tuple[str, str]],
    ) -> str | None:
        if (
            cls._candidate_key(candidate) in watched_media_keys
            or cls._candidate_matches_external_keys(candidate, watched_external_keys)
            or cls._candidate_matches_title_keys(candidate, watched_title_keys)
        ):
            return "already_watched"
        if (
            cls._candidate_key(candidate) in requested_media_keys
            or cls._candidate_matches_title_keys(candidate, requested_title_keys)
        ):
            return "already_requested"
        block_reason = cls._candidate_feedback_block_reason(candidate, profile_summary)
        if block_reason is not None:
            return block_reason

        deterministic_score = float(candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0)
        if deterministic_score < float(prefilter_threshold):
            return "below_threshold"
        return None

    @classmethod
    def _holdout_result_for_candidate(
        cls,
        candidate: dict[str, Any],
        *,
        holdout_context: dict[str, set[tuple[str, Any]]],
        current_favorite_context: dict[str, set[tuple[str, Any]]],
    ) -> tuple[str, str]:
        if (
            cls._candidate_key(candidate) in holdout_context["media_keys"]
            or cls._candidate_matches_external_keys(candidate, holdout_context["external_keys"])
            or cls._candidate_matches_title_keys(candidate, holdout_context["title_keys"])
        ):
            return "watched_later", "Matched this title in post-cutoff playback history."
        if (
            cls._candidate_matches_external_keys(candidate, current_favorite_context["external_keys"])
            or cls._candidate_matches_title_keys(candidate, current_favorite_context["title_keys"])
        ):
            return "favorite_overlap", "Matches the user's current favorites (favorites do not expose timestamps)."
        return "miss", "No watched-later or current-favorite overlap was found in the replay window."

    async def build_backtest_report(
        self,
        *,
        username: str | None = None,
        days: int = 60,
        shortlist_limit: int = 5,
    ) -> dict[str, Any]:
        normalized_username = str(username or "").strip()
        normalized_days = max(14, min(365, int(days or 60)))
        normalized_shortlist = max(1, min(12, int(shortlist_limit or self.settings.decision_shortlist_limit or 5)))
        generated_at = datetime.utcnow().replace(microsecond=0)
        cutoff_at = generated_at - timedelta(days=normalized_days)
        cutoff_ts = cutoff_at.timestamp()
        notes = [
            "Replay profiles are rebuilt from watch history available before the cutoff date.",
            "Current favorites are reported only as overlap signals because the media server does not expose favorite timestamps.",
            "Replay scoring uses deterministic ranking only, so this page stays fast and avoids paid or variable LLM calls.",
        ]

        users = await self.media_server.list_users()
        if normalized_username:
            users = [user for user in users if str(user.get("Name") or "").strip() == normalized_username]

        disabled_profiles: list[str] = []
        selected_users: list[dict[str, Any]] = []
        for user in users:
            current_username = str(user.get("Name") or "").strip()
            if not current_username:
                continue
            if normalized_username or self.is_profile_enabled(current_username):
                selected_users.append(user)
            else:
                disabled_profiles.append(current_username)

        if normalized_username and not selected_users:
            raise ValueError(f"No {self.settings.media_server_label} user matched {normalized_username}.")

        async def load_user_replay_context(user: dict[str, Any]) -> dict[str, Any]:
            current_username = str(user.get("Name") or "").strip() or "unknown"
            user_id = str(user.get("Id") or "")
            history, favorite_items = await asyncio.gather(
                self.media_server.get_playback_history(user_id, None),
                self._load_user_favorite_items(user_id, username=current_username),
            )
            baseline_history, holdout_history = self._split_history_for_backtest(history, cutoff_ts=cutoff_ts)
            return {
                "user": user,
                "username": current_username,
                "history": history,
                "baseline_history": baseline_history,
                "holdout_history": holdout_history,
                "favorite_items": favorite_items,
                "stored_payload": self.profile_store.read_payload(current_username),
            }

        loaded_contexts = await asyncio.gather(*(load_user_replay_context(user) for user in selected_users))

        replay_contexts: list[dict[str, Any]] = []
        baseline_profile_payloads: dict[str, dict[str, Any]] = {}
        for context in loaded_contexts:
            current_username = str(context["username"])
            baseline_history = list(context["baseline_history"])
            stored_payload = context["stored_payload"]
            if not baseline_history:
                replay_contexts.append(
                    {
                        **context,
                        "status": "insufficient_history",
                        "status_detail": "No playback history exists before the selected cutoff.",
                    }
                )
                continue

            history_summary = self._build_profile_history_context(
                baseline_history,
                favorite_items=None,
                top_limit=self.settings.profile_architect_top_titles_limit,
                recent_limit=self.settings.profile_architect_recent_momentum_limit,
                recent_weight_percent=self.settings.profile_recent_signal_weight_percent,
            )
            history_summary = self._apply_existing_profile_guidance(history_summary, stored_payload)
            recommendation_seeds = self._build_recommendation_seed_pool(
                baseline_history,
                favorite_items=None,
                profile_summary=history_summary,
                limit=self.settings.recommendation_seed_limit,
            )
            recommendation_seeds = self._resolve_tv_seed_media_ids_from_library_index(recommendation_seeds)
            history_summary = await self._enrich_profile_summary_with_seer(
                history_summary,
                recommendation_seeds=recommendation_seeds,
            )
            history_summary = await self._enrich_profile_summary_with_tmdb(
                history_summary,
                recommendation_seeds=recommendation_seeds,
            )
            preliminary_payload = self._build_profile_payload(
                current_username,
                history_summary,
                enrichment=self._build_heuristic_profile_enrichment(history_summary),
                existing_payload=stored_payload,
            )
            baseline_profile_payloads[current_username] = preliminary_payload
            replay_contexts.append(
                {
                    **context,
                    "status": "ready",
                    "history_summary": history_summary,
                    "recommendation_seeds": recommendation_seeds,
                    "preliminary_payload": preliminary_payload,
                }
            )

        with self.session_scope() as session:
            requested_rows = list(
                session.scalars(
                    select(RequestedMedia).where(RequestedMedia.created_at <= cutoff_at)
                )
            )
        requested_media_keys = {
            (str(row.media_type or "").strip(), int(row.media_id or 0))
            for row in requested_rows
            if str(row.media_type or "").strip() and int(row.media_id or 0) > 0
        }
        requested_title_keys = {
            (str(row.media_type or "").strip(), str(row.media_title or "").strip().lower())
            for row in requested_rows
            if str(row.media_type or "").strip() and str(row.media_title or "").strip()
        }

        user_reports: list[dict[str, Any]] = []
        aggregate_skip_reasons: Counter[str] = Counter()
        total_simulated = 0
        total_hits = 0
        total_favorite_overlaps = 0
        total_misses = 0
        profiles_with_hits = 0
        insufficient_history_profiles: list[str] = []
        errors: list[str] = []

        for context in replay_contexts:
            current_username = str(context["username"])
            if context.get("status") != "ready":
                insufficient_history_profiles.append(current_username)
                user_reports.append(
                    {
                        "username": current_username,
                        "profile_enabled": self.is_profile_enabled(current_username),
                        "status": str(context.get("status") or "insufficient_history"),
                        "status_detail": str(context.get("status_detail") or "Not enough baseline history to replay this user."),
                        "baseline_history_count": len(context.get("baseline_history", [])),
                        "holdout_history_count": len(context.get("holdout_history", [])),
                        "simulated_requests": [],
                        "skip_reasons": {},
                        "scored": 0,
                        "filtered_candidates": 0,
                        "watched_hits": 0,
                        "favorite_overlaps": 0,
                        "misses": 0,
                        "hit_rate": 0.0,
                        "primary_genres": [],
                        "recent_genres": [],
                        "similar_users": [],
                        "holdout_titles": [],
                    }
                )
                continue

            try:
                history_summary = dict(context["history_summary"])
                recommendation_seeds = list(context["recommendation_seeds"])
                stored_payload = context["stored_payload"]
                baseline_history = list(context["baseline_history"])
                holdout_history = list(context["holdout_history"])
                favorite_items = list(context["favorite_items"])

                history_summary = self._enrich_profile_summary_with_similar_users(
                    current_username,
                    history_summary,
                    peer_payload_overrides=baseline_profile_payloads,
                )
                profile_payload = self._build_profile_payload(
                    current_username,
                    history_summary,
                    enrichment=self._build_heuristic_profile_enrichment(history_summary),
                    existing_payload=stored_payload,
                )
                genre_seeds = self._build_genre_discovery_seeds(profile_payload)
                candidate_pool = await self.seer.discover_candidates(
                    recommendation_seeds,
                    genre_seeds=genre_seeds,
                    limit=self.settings.candidate_limit,
                    genre_limit=self.settings.genre_candidate_limit,
                    trending_limit=self.settings.trending_candidate_limit,
                )
                ranked_candidates = self._rank_candidate_pool(
                    candidate_pool,
                    profile_summary=profile_payload,
                )
                baseline_context = self._build_media_item_match_context(baseline_history)
                holdout_context = self._build_media_item_match_context(holdout_history)
                favorite_context = self._build_media_item_match_context(favorite_items)
                filtered_candidates: list[dict[str, Any]] = []
                skip_reasons: Counter[str] = Counter()
                scored = 0
                for candidate in ranked_candidates:
                    scored += 1
                    skip_reason = self._backtest_candidate_skip_reason(
                        candidate,
                        profile_summary=profile_payload,
                        prefilter_threshold=self._decision_prefilter_threshold(),
                        watched_media_keys=baseline_context["media_keys"],  # type: ignore[arg-type]
                        watched_external_keys=baseline_context["external_keys"],  # type: ignore[arg-type]
                        watched_title_keys=baseline_context["title_keys"],  # type: ignore[arg-type]
                        requested_media_keys=requested_media_keys,
                        requested_title_keys=requested_title_keys,
                    )
                    if skip_reason is not None:
                        skip_reasons[skip_reason] += 1
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
                shortlisted_candidates = self._diversify_candidates(
                    filtered_candidates,
                    limit=normalized_shortlist,
                )

                simulated_requests: list[dict[str, Any]] = []
                watched_hits = 0
                favorite_overlaps = 0
                misses = 0
                for candidate in shortlisted_candidates:
                    deterministic_score = float(candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0)
                    if deterministic_score < float(self.settings.request_threshold):
                        continue
                    result, result_detail = self._holdout_result_for_candidate(
                        candidate,
                        holdout_context=holdout_context,
                        current_favorite_context=favorite_context,
                    )
                    if result == "watched_later":
                        watched_hits += 1
                    elif result == "favorite_overlap":
                        favorite_overlaps += 1
                    else:
                        misses += 1
                    simulated_requests.append(
                        {
                            "title": str(candidate.get("title") or "Unknown"),
                            "media_type": str(candidate.get("media_type") or "unknown"),
                            "score": round(deterministic_score, 3),
                            "result": result,
                            "result_detail": result_detail,
                            "genres": self._normalize_genres(candidate.get("genres", []), limit=5),
                            "sources": self._normalize_string_list(candidate.get("sources", []), limit=4),
                            "analysis_summary": str(
                                candidate.get("recommendation_features", {}).get("analysis_summary") or ""
                            ).strip(),
                        }
                    )

                holdout_summary = self._build_profile_history_context(
                    holdout_history,
                    favorite_items=None,
                    top_limit=4,
                    recent_limit=3,
                    recent_weight_percent=self.settings.profile_recent_signal_weight_percent,
                ) if holdout_history else {"top_titles": []}
                holdout_titles = [
                    str(item.get("title") or "").strip()
                    for item in holdout_summary.get("top_titles", [])
                    if str(item.get("title") or "").strip()
                ]
                hit_rate = round((watched_hits / len(simulated_requests)) * 100, 1) if simulated_requests else 0.0
                if watched_hits > 0:
                    profiles_with_hits += 1

                total_simulated += len(simulated_requests)
                total_hits += watched_hits
                total_favorite_overlaps += favorite_overlaps
                total_misses += misses
                aggregate_skip_reasons.update(skip_reasons)

                user_reports.append(
                    {
                        "username": current_username,
                        "profile_enabled": self.is_profile_enabled(current_username),
                        "status": "ready",
                        "status_detail": "",
                        "baseline_history_count": len(baseline_history),
                        "holdout_history_count": len(holdout_history),
                        "scored": scored,
                        "filtered_candidates": len(filtered_candidates),
                        "simulated_requests": simulated_requests,
                        "skip_reasons": dict(skip_reasons),
                        "watched_hits": watched_hits,
                        "favorite_overlaps": favorite_overlaps,
                        "misses": misses,
                        "hit_rate": hit_rate,
                        "primary_genres": self._normalize_string_list(profile_payload.get("primary_genres", []), limit=4),
                        "recent_genres": self._normalize_string_list(profile_payload.get("recent_genres", []), limit=4),
                        "similar_users": self._normalize_string_list(profile_payload.get("similar_users", []), limit=3),
                        "holdout_titles": holdout_titles,
                    }
                )
            except Exception as exc:
                errors.append(f"{current_username}: {exc}")
                user_reports.append(
                    {
                        "username": current_username,
                        "profile_enabled": self.is_profile_enabled(current_username),
                        "status": "error",
                        "status_detail": str(exc),
                        "baseline_history_count": len(context.get("baseline_history", [])),
                        "holdout_history_count": len(context.get("holdout_history", [])),
                        "simulated_requests": [],
                        "skip_reasons": {},
                        "scored": 0,
                        "filtered_candidates": 0,
                        "watched_hits": 0,
                        "favorite_overlaps": 0,
                        "misses": 0,
                        "hit_rate": 0.0,
                        "primary_genres": [],
                        "recent_genres": [],
                        "similar_users": [],
                        "holdout_titles": [],
                    }
                )

        report = {
            "generated_at": generated_at.isoformat(sep=" "),
            "cutoff_at": cutoff_at.isoformat(sep=" "),
            "days": normalized_days,
            "shortlist_limit": normalized_shortlist,
            "target_username": normalized_username,
            "profiles_considered": len(selected_users),
            "profiles_analyzed": sum(1 for row in user_reports if row.get("status") == "ready"),
            "profiles_with_hits": profiles_with_hits,
            "disabled_profiles_skipped": disabled_profiles,
            "insufficient_history_profiles": insufficient_history_profiles,
            "simulated_requests": total_simulated,
            "watched_hits": total_hits,
            "favorite_overlaps": total_favorite_overlaps,
            "misses": total_misses,
            "hit_rate": round((total_hits / total_simulated) * 100, 1) if total_simulated else 0.0,
            "favorite_overlap_rate": round((total_favorite_overlaps / total_simulated) * 100, 1) if total_simulated else 0.0,
            "skip_reasons": dict(aggregate_skip_reasons),
            "users": user_reports,
            "notes": notes,
            "errors": errors,
        }

        self.record_operation_event(
            engine="backtesting",
            username=normalized_username or "system",
            media_type="profile",
            media_title=(
                f"Backtesting scorecard for {normalized_username}"
                if normalized_username
                else "Backtesting scorecard for enabled profiles"
            ),
            source="ui",
            decision="RUN" if not errors else "PARTIAL",
            reasoning=(
                f"Replay analyzed {report['profiles_analyzed']} profile(s), simulated {total_simulated} request(s), "
                f"and found {total_hits} watched-later hit(s) across the last {normalized_days} day(s)."
            ),
            error="; ".join(errors) if errors else None,
            detail_payload={
                "days": normalized_days,
                "target_username": normalized_username or "all-enabled",
                "profiles_considered": report["profiles_considered"],
                "profiles_analyzed": report["profiles_analyzed"],
                "simulated_requests": total_simulated,
                "watched_hits": total_hits,
                "favorite_overlaps": total_favorite_overlaps,
                "misses": total_misses,
                "hit_rate": report["hit_rate"],
            },
        )
        return report

    async def run_profile_architect(
        self,
        username: str | None = None,
        *,
        trigger_source: str = "manual",
    ) -> dict[str, Any]:
        logger.info("Profile Architect started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "profile_architect")

        target_username = str(username or "").strip()
        updated_users: list[str] = []
        processed_usernames: list[str] = []
        suggestion_refreshes = 0
        watched_request_updates = 0
        suggestion_targets: list[dict[str, Any]] = []
        errors: list[str] = []
        rebuilt_payloads: dict[str, dict[str, Any]] = {}
        user_step_bases: dict[str, int] = {}
        profile_steps_per_user = 6
        suggestion_steps_per_user = 5 if self.settings.suggestions_enabled else 0
        steps_per_user = profile_steps_per_user + suggestion_steps_per_user
        total_steps = 0
        completed_steps = 0

        try:
            users = await self.media_server.list_users()
            if username:
                users = [user for user in users if user.get("Name") == username]

            total_steps = len(users) * steps_per_user

            def build_detail_payload(**extra: Any) -> dict[str, Any]:
                payload = {
                    "target_username": target_username,
                    "processed_users": len(updated_users),
                    "total_users": len(users),
                    "processed_usernames": list(processed_usernames),
                    "updated_users": list(updated_users),
                    "suggestion_refreshes": suggestion_refreshes,
                    "watched_request_updates": watched_request_updates,
                    "profile_steps_per_user": profile_steps_per_user,
                    "suggestion_steps_per_user": suggestion_steps_per_user,
                    "errors": list(errors),
                }
                payload.update(extra)
                return payload

            def emit_task_progress(
                summary_text: str,
                *,
                progress_value: int,
                current_label: str,
                phase: str,
                detail: dict[str, Any] | None = None,
            ) -> None:
                self._update_task(
                    task.id,
                    status="running",
                    summary=summary_text,
                    progress_current=max(0, min(total_steps, progress_value)),
                    progress_total=total_steps,
                    current_label=current_label,
                    detail_payload=build_detail_payload(phase=phase, **(detail or {})),
                )

            self._update_task(
                task.id,
                status="running",
                summary=(
                    f"Preparing Profile Architect for {target_username}."
                    if target_username
                    else f"Preparing Profile Architect for {len(users)} user(s)."
                ),
                progress_current=0,
                progress_total=total_steps,
                current_label=target_username or "Preparing profiles",
                detail_payload=build_detail_payload(
                    processed_users=0,
                    processed_usernames=[],
                    updated_users=[],
                    suggestion_refreshes=0,
                    phase="prepare",
                ),
            )

            for user_index, user in enumerate(users):
                current_username = user.get("Name", "unknown")
                if current_username not in processed_usernames:
                    processed_usernames.append(current_username)
                user_base_step = user_index * steps_per_user
                user_step_bases[current_username] = user_base_step
                try:
                    emit_task_progress(
                        f"Loading playback history for {current_username}.",
                        progress_value=user_base_step,
                        current_label=current_username,
                        phase="history_load",
                    )
                    history = await self.media_server.get_playback_history(
                        user["Id"],
                        self._playback_history_limit(),
                    )
                    favorite_items = await self._load_user_favorite_items(str(user["Id"] or ""), username=current_username)
                    watch_sync = self.sync_watched_request_outcomes_from_history(
                        username=current_username,
                        history=history,
                        source=trigger_source,
                    )
                    watched_request_updates += int(watch_sync.get("count") or 0)
                    completed_steps = user_base_step + 1
                    stored_payload = self.profile_store.read_payload(current_username)
                    profile_payload, _recommendation_seeds = await self._compose_profile_payload(
                        current_username,
                        history=history,
                        favorite_items=favorite_items,
                        existing_payload=stored_payload,
                        peer_payload_overrides=rebuilt_payloads,
                        progress_callback=lambda label, step, phase: emit_task_progress(
                            label,
                            progress_value=user_base_step + 1 + step,
                            current_label=current_username,
                            phase=phase,
                        ),
                    )
                    self.profile_store.write_payload(current_username, profile_payload)
                    rebuilt_payloads[current_username] = profile_payload
                    updated_users.append(current_username)
                    suggestion_targets.append(user)
                    self._record_operation_log(
                        engine="profile_architect",
                        username=current_username,
                        media_type="profile",
                        media_title=f"Profile manifest rebuilt for {current_username}",
                        source="playback-history",
                        decision="REBUILD",
                        reasoning=(
                            "Profile Architect rebuilt this manifest from the latest playback history "
                            "plus Jellyfin favorites, Seer neighborhoods, local similar-user lift, TMDb metadata, "
                            "and LLM adjacent-lane synthesis."
                        ),
                        detail_payload={
                            "task_type": "profile_rebuild",
                            "target_username": current_username,
                            "run_scope": target_username or "all-users",
                        },
                    )
                    completed_steps = user_base_step + profile_steps_per_user
                    emit_task_progress(
                        f"Updated profile manifest for {current_username}.",
                        progress_value=completed_steps,
                        current_label=current_username,
                        phase="profile_saved",
                    )
                    logger.info("Profile Architect updated profile for user=%s", current_username)
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")
                    logger.exception("Profile Architect failed for user=%s", current_username)
                    completed_steps = max(completed_steps, user_base_step + steps_per_user)
                    emit_task_progress(
                        f"Profile Architect hit an error for {current_username}.",
                        progress_value=completed_steps,
                        current_label=current_username,
                        phase="profile_error",
                    )

            if self.settings.suggestions_enabled:
                for user in suggestion_targets:
                    current_username = str(user.get("Name") or "unknown")
                    user_base_step = user_step_bases.get(current_username, 0)
                    suggestion_base_step = user_base_step + profile_steps_per_user
                    try:
                        emit_task_progress(
                            f"Refreshing suggestions for {current_username}.",
                            progress_value=suggestion_base_step,
                            current_label=current_username,
                            phase="suggestion_refresh_prepare",
                        )
                        await self._refresh_user_suggestions(
                            user,
                            progress_callback=lambda label, step, detail: emit_task_progress(
                                label,
                                progress_value=suggestion_base_step + step + 1,
                                current_label=current_username,
                                phase=str(detail.get("phase") or "suggestion_refresh"),
                                detail={"suggestion_phase": detail.get("phase")},
                            ),
                        )
                        suggestion_refreshes += 1
                        completed_steps = max(completed_steps, suggestion_base_step + suggestion_steps_per_user)
                        emit_task_progress(
                            f"Refreshed suggestions for {current_username}.",
                            progress_value=completed_steps,
                            current_label=current_username,
                            phase="suggestion_refresh_complete",
                        )
                    except Exception as exc:
                        errors.append(f"{current_username} suggestions: {exc}")
                        logger.exception(
                            "Profile Architect follow-up suggestion refresh failed for user=%s",
                            current_username,
                        )
                        completed_steps = max(completed_steps, suggestion_base_step + suggestion_steps_per_user)
                        emit_task_progress(
                            f"Suggestion refresh hit an error for {current_username}.",
                            progress_value=completed_steps,
                            current_label=current_username,
                            phase="suggestion_refresh_error",
                        )

            if not users:
                status = "error"
                summary = f"No {self.settings.media_server_label} users matched the requested target."
            elif errors:
                status = "partial"
                summary = (
                    f"Updated {len(updated_users)} profile(s), refreshed {suggestion_refreshes} "
                    f"suggestion snapshot(s), inferred {watched_request_updates} watched request(s), "
                    f"with {len(errors)} error(s)."
                )
            else:
                status = "success"
                summary = (
                    f"Updated {len(updated_users)} profile(s), refreshed "
                    f"{suggestion_refreshes} suggestion snapshot(s), and inferred "
                    f"{watched_request_updates} watched request(s)."
                )
        except Exception as exc:
            status = "error"
            summary = f"Profile Architect failed: {exc}"
            errors.append(str(exc))

        self._update_task(
            task.id,
            status=status,
            summary=summary,
            progress_current=total_steps if total_steps > 0 else completed_steps,
            progress_total=total_steps,
            current_label=target_username or ("Complete" if status == "success" else "Finished"),
            detail_payload=(
                build_detail_payload()
                if 'users' in locals()
                else {
                    "target_username": target_username,
                    "processed_users": len(updated_users),
                    "total_users": 0,
                    "processed_usernames": list(processed_usernames),
                    "updated_users": list(updated_users),
                    "suggestion_refreshes": suggestion_refreshes,
                    "watched_request_updates": watched_request_updates,
                    "profile_steps_per_user": profile_steps_per_user,
                    "suggestion_steps_per_user": suggestion_steps_per_user,
                    "errors": list(errors),
                }
            ),
            finished=True,
        )
        self._record_operation_log(
            engine="profile_architect",
            username=target_username or "system",
            media_type="profile",
            media_title=(
                f"Profile Architect run for {target_username}"
                if target_username
                else "Profile Architect run for all users"
            ),
            source=trigger_source,
            decision="RUN",
            reasoning=summary,
            error="; ".join(errors) if errors else None,
            detail_payload={
                "task_type": "profile_architect_run",
                "run_scope": target_username or "all-users",
                "trigger": trigger_source,
                "updated_users": list(updated_users),
                "processed_usernames": list(processed_usernames),
                "suggestion_refreshes": suggestion_refreshes,
                "watched_request_updates": watched_request_updates,
                "status": status,
            },
        )

        logger.info("Profile Architect finished status=%s summary=%s", status, summary)

        return {
            "engine": "profile_architect",
            "status": status,
            "summary": summary,
            "updated_users": updated_users,
            "suggestion_refreshes": suggestion_refreshes,
            "watched_request_updates": watched_request_updates,
            "errors": errors,
        }

    async def run_decision_engine(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Decision Engine started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "decision_engine")

        target_username = str(username or "").strip()
        scored = 0
        shortlisted = 0
        evaluated = 0
        requested = 0
        skipped = 0
        processed_usernames: list[str] = []
        errors: list[str] = []
        exclusions = self._parse_global_exclusions()
        total_steps = 0
        completed_steps = 0
        matched_users: list[dict[str, Any]] = []
        users: list[dict[str, Any]] = []
        disabled_usernames: list[str] = []

        try:
            matched_users = await self.media_server.list_users()
            if username:
                matched_users = [user for user in matched_users if user.get("Name") == username]

            for user in matched_users:
                current_username = str(user.get("Name") or "").strip()
                if not current_username:
                    continue
                if self.is_profile_enabled(current_username):
                    users.append(user)
                    continue
                disabled_usernames.append(current_username)
                self.record_operation_event(
                    engine="decision_engine",
                    username=current_username,
                    media_type="profile",
                    media_title=f"Decision Engine skipped for {current_username}",
                    source="profile-toggle",
                    decision="SKIP",
                    reasoning=(
                        f"Decision Engine skipped {current_username} because that profile is disabled "
                        "for live requests."
                    ),
                    detail_payload={
                        "target_username": target_username or "all-users",
                        "profile_enabled": False,
                    },
                )

            total_steps = max(1, len(users) * 3)
            self._update_task(
                task.id,
                status="running",
                summary=(
                    f"Decision Engine is disabled for {target_username}."
                    if target_username and not users and bool(disabled_usernames)
                    else
                    f"Preparing Decision Engine for {target_username}."
                    if target_username
                    else f"Preparing Decision Engine for {len(users)} user(s)."
                ),
                progress_current=0,
                progress_total=total_steps,
                current_label=target_username or "Preparing decisions",
                detail_payload={
                    "target_username": target_username,
                    "processed_users": 0,
                    "total_users": len(users),
                    "processed_usernames": [],
                    "disabled_usernames": list(disabled_usernames),
                    "scored": 0,
                    "shortlisted": 0,
                    "evaluated": 0,
                    "requested": 0,
                    "skipped": 0,
                    "errors": [],
                },
            )

            for user in users:
                current_username = user.get("Name", "unknown")
                if current_username not in processed_usernames:
                    processed_usernames.append(current_username)
                try:
                    self._update_task(
                        task.id,
                        status="running",
                        summary=f"Loading history and profile context for {current_username}.",
                        progress_current=completed_steps,
                        progress_total=total_steps,
                        current_label=current_username,
                        detail_payload={
                            "target_username": target_username,
                            "processed_users": max(0, completed_steps // 3),
                            "total_users": len(users),
                            "processed_usernames": list(processed_usernames),
                            "disabled_usernames": list(disabled_usernames),
                            "scored": scored,
                            "shortlisted": shortlisted,
                            "evaluated": evaluated,
                            "requested": requested,
                            "skipped": skipped,
                            "errors": list(errors),
                        },
                    )
                    prepared = await self._prepare_decision_candidates_for_user(user)
                    profile_payload = prepared["profile_payload"]
                    viewing_history = prepared["viewing_history"]
                    candidates = prepared["shortlisted_candidates"]
                    requested_media_keys = set(prepared["requested_media_keys"])
                    for shared_match in prepared.get("shared_request_matches", []):
                        try:
                            self.add_request_supporter(
                                requested_media_id=int(shared_match.get("requested_media_id") or 0),
                                username=str(current_username),
                                source="decision_engine",
                                reason=(
                                    f"Decision Engine matched {current_username} to already-requested title "
                                    f"{shared_match.get('media_title') or 'Unknown'} owned by "
                                    f"{shared_match.get('owner_username') or 'another user'}."
                                ),
                            )
                        except ValueError:
                            continue
                    scored += int(prepared["scored"] or 0)
                    skipped += int(prepared["skipped"] or 0)
                    completed_steps += 1
                    self._update_task(
                        task.id,
                        status="running",
                        summary=f"Discovering and ranking candidates for {current_username}.",
                        progress_current=completed_steps,
                        progress_total=total_steps,
                        current_label=current_username,
                        detail_payload={
                            "target_username": target_username,
                            "processed_users": max(0, completed_steps // 3),
                            "total_users": len(users),
                            "processed_usernames": list(processed_usernames),
                            "disabled_usernames": list(disabled_usernames),
                            "scored": scored,
                            "shortlisted": shortlisted,
                            "evaluated": evaluated,
                            "requested": requested,
                            "skipped": skipped,
                            "errors": list(errors),
                        },
                    )
                    shortlisted += len(candidates)
                    completed_steps += 1
                    candidate_steps = max(1, len(candidates))
                    task_total_for_user = completed_steps + candidate_steps + max(0, (len(users) - 1) * 3)
                    total_steps = max(total_steps, task_total_for_user)
                    self._update_task(
                        task.id,
                        status="running",
                        summary=f"Evaluating {len(candidates)} shortlisted candidate(s) for {current_username}.",
                        progress_current=completed_steps,
                        progress_total=total_steps,
                        current_label=current_username,
                        detail_payload={
                            "target_username": target_username,
                            "processed_users": max(0, completed_steps // 3),
                            "total_users": len(users),
                            "processed_usernames": list(processed_usernames),
                            "disabled_usernames": list(disabled_usernames),
                            "scored": scored,
                            "shortlisted": shortlisted,
                            "evaluated": evaluated,
                            "requested": requested,
                            "skipped": skipped,
                            "errors": list(errors),
                        },
                    )

                    user_candidate_total = len(candidates)
                    user_candidate_index = 0
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
                            request_created = False
                            request_tracked = False
                            request_note = ""
                            if should_request:
                                try:
                                    external_ids = (
                                        candidate.get("external_ids", {})
                                        if isinstance(candidate.get("external_ids"), dict)
                                        else {}
                                    )
                                    request_result = await self.seer.request_media(
                                        candidate["media_type"],
                                        candidate["media_id"],
                                        tvdb_id=self._coerce_int(external_ids.get("tvdb")),
                                    )
                                    request_id = request_result.request_id
                                    if request_result.created:
                                        request_created = True
                                        request_tracked = True
                                        requested_media_keys.add(self._candidate_key(candidate))
                                        logger.info(
                                            "Decision Engine requested media user=%s title=%s type=%s request_id=%s",
                                            current_username,
                                            candidate["title"],
                                            candidate["media_type"],
                                            request_id,
                                        )
                                    else:
                                        request_tracked = self._should_track_request_result(
                                            candidate=candidate,
                                            request_result=request_result,
                                        )
                                        if request_tracked:
                                            requested_media_keys.add(self._candidate_key(candidate))
                                        request_note = request_result.message or "Seer did not create a request."
                                        if request_tracked and request_id is not None and not request_result.message:
                                            request_note = (
                                                f"Seer already had this title tracked as request {request_id}."
                                            )
                                        logger.info(
                                            "Decision Engine request skipped user=%s title=%s type=%s status=%s reason=%s",
                                            current_username,
                                            candidate["title"],
                                            candidate["media_type"],
                                            request_result.status_code,
                                            request_note,
                                        )
                                except Exception as exc:
                                    error = str(exc)
                                    errors.append(f"{current_username}::{candidate['title']}: {exc}")
                                    logger.exception(
                                        "Decision Engine request failed user=%s title=%s",
                                        current_username,
                                        candidate["title"],
                                    )

                            if request_note:
                                reasoning = f"{reasoning} Request outcome: {request_note}"

                            with self.session_scope() as session:
                                if should_request and error is None and request_tracked:
                                    existing_request = session.scalar(
                                        select(RequestedMedia)
                                        .where(
                                            RequestedMedia.username == current_username,
                                            RequestedMedia.media_type == candidate["media_type"],
                                            RequestedMedia.media_id == candidate["media_id"],
                                        )
                                    )
                                    if existing_request is None:
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
                                    else:
                                        if request_id is not None and existing_request.seer_request_id is None:
                                            existing_request.seer_request_id = request_id
                                        if not str(existing_request.source or "").strip():
                                            existing_request.source = ", ".join(candidate["sources"])
                                    if request_created:
                                        requested += 1

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
                                        requested=should_request and error is None and request_created,
                                        request_id=request_id,
                                        reasoning=reasoning,
                                        payload_json=json.dumps(candidate, ensure_ascii=True),
                                        error=error,
                                    )
                                )

                            evaluated += 1
                            user_candidate_index += 1
                            completed_steps = min(total_steps, completed_steps + 1)
                            self._update_task(
                                task.id,
                                status="running",
                                summary=(
                                    f"Processed {user_candidate_index}/{user_candidate_total} candidate(s) "
                                    f"for {current_username}."
                                ),
                                progress_current=completed_steps,
                                progress_total=total_steps,
                                current_label=current_username,
                                detail_payload={
                                    "target_username": target_username,
                                    "processed_users": max(0, completed_steps // 3),
                                    "total_users": len(users),
                                    "processed_usernames": list(processed_usernames),
                                    "disabled_usernames": list(disabled_usernames),
                                    "scored": scored,
                                    "shortlisted": shortlisted,
                                    "evaluated": evaluated,
                                    "requested": requested,
                                    "skipped": skipped,
                                    "errors": list(errors),
                                },
                            )
                        except Exception as exc:
                            errors.append(f"{current_username}::{candidate.get('title', 'unknown')}: {exc}")
                            logger.exception(
                                "Decision Engine evaluation failed user=%s title=%s",
                                current_username,
                                candidate.get("title", "unknown"),
                            )
                    completed_steps = max(completed_steps, total_steps - max(0, (len(users) - (users.index(user) + 1)) * 3))
                except Exception as exc:
                    errors.append(f"{current_username}: {exc}")
                    logger.exception("Decision Engine failed while preparing user=%s", current_username)

            disabled_summary = (
                f" Disabled profiles skipped: {len(disabled_usernames)}."
                if disabled_usernames
                else ""
            )
            if target_username and not matched_users:
                status = "error"
                summary = f"No {self.settings.media_server_label} users matched the requested target."
            elif target_username and not users and disabled_usernames:
                status = "success"
                summary = (
                    f"Decision Engine did not run for {target_username} because that profile is disabled "
                    "for live requests."
                )
            elif not users and disabled_usernames:
                status = "success"
                summary = (
                    f"Skipped {len(disabled_usernames)} disabled profile(s); "
                    "no enabled profiles were available for Decision Engine."
                )
            elif not users:
                status = "error"
                summary = f"No {self.settings.media_server_label} users matched the requested target."
            elif errors:
                status = "partial"
                summary = (
                    f"Scored {scored} candidates, shortlisted {shortlisted}, evaluated {evaluated}, requested {requested}, "
                    f"skipped {skipped}, errors {len(errors)}.{disabled_summary}"
                )
            else:
                status = "success"
                summary = (
                    f"Scored {scored} candidates, shortlisted {shortlisted}, "
                    f"evaluated {evaluated}, requested {requested}, skipped {skipped}.{disabled_summary}"
                )
        except Exception as exc:
            status = "error"
            summary = f"Decision Engine failed: {exc}"
            errors.append(str(exc))

        self._update_task(
            task.id,
            status=status,
            summary=summary,
            progress_current=total_steps if total_steps > 0 else completed_steps,
            progress_total=total_steps,
            current_label=target_username or ("Complete" if status == "success" else "Finished"),
            detail_payload={
                "target_username": target_username,
                "processed_users": len(users) if 'users' in locals() else 0,
                "total_users": len(users) if 'users' in locals() else 0,
                "processed_usernames": list(processed_usernames),
                "disabled_usernames": list(disabled_usernames),
                "scored": scored,
                "shortlisted": shortlisted,
                "evaluated": evaluated,
                "requested": requested,
                "skipped": skipped,
                "errors": list(errors),
            },
            finished=True,
        )

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

    async def _build_decision_preview(self, username: str, *, limit: int = 8) -> dict[str, Any]:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            raise ValueError("Select a profile before running a dry-run review.")

        users = await self.media_server.list_users()
        target_user = next((user for user in users if str(user.get("Name") or "") == cleaned_username), None)
        if target_user is None:
            raise ValueError(f"No {self.settings.media_server_label} user matched {cleaned_username}.")

        prepared = await self._prepare_decision_candidates_for_user(
            target_user,
            shortlist_limit=max(limit, self.settings.decision_shortlist_limit),
        )
        profile_payload = prepared["profile_payload"]
        viewing_history = prepared["viewing_history"]
        candidates = list(prepared["shortlisted_candidates"])[: max(1, int(limit))]
        preview_rows: list[dict[str, Any]] = []

        for candidate in candidates:
            deterministic_score = float(candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0)
            llm_vote = "UNAVAILABLE"
            llm_confidence: float | None = None
            llm_reasoning = ""

            try:
                llm_payload = await self.llm.generate_json(
                    messages=build_decision_messages(
                        username=cleaned_username,
                        profile_payload=profile_payload,
                        viewing_history=viewing_history,
                        candidate=candidate,
                        global_exclusions=self._parse_global_exclusions(),
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
                    "Decision preview LLM fallback triggered user=%s title=%s reason=%s",
                    cleaned_username,
                    candidate.get("title", "unknown"),
                    exc,
                )

            confidence = self._blend_confidences(
                deterministic_score=deterministic_score,
                llm_confidence=llm_confidence,
                llm_vote=llm_vote,
                llm_weight_percent=self.settings.decision_ai_weight_percent,
            )
            decision = "REQUEST" if confidence >= self.settings.request_threshold else "IGNORE"
            reasoning = self._compose_decision_reasoning(
                candidate,
                deterministic_score=deterministic_score,
                hybrid_confidence=confidence,
                decision=decision,
                request_threshold=self.settings.request_threshold,
                llm_vote=llm_vote,
                llm_reasoning=llm_reasoning,
            )
            preview_rows.append(
                {
                    "media_type": str(candidate.get("media_type") or "unknown"),
                    "media_id": int(candidate.get("media_id") or 0),
                    "title": str(candidate.get("title") or "Unknown"),
                    "overview": str(candidate.get("overview") or "").strip(),
                    "genres": self._normalize_genres(candidate.get("genres", []), limit=6),
                    "sources": self._normalize_string_list(candidate.get("sources", []), limit=6),
                    "release_date": candidate.get("release_date"),
                    "rating": self._coerce_optional_number(candidate.get("rating")),
                    "vote_count": int(candidate.get("vote_count") or 0),
                    "decision": decision,
                    "hybrid_confidence": round(confidence, 3),
                    "threshold": float(self.settings.request_threshold),
                    "llm_vote": llm_vote,
                    "llm_confidence": llm_confidence,
                    "llm_reasoning": llm_reasoning,
                    "reasoning": reasoning,
                    "features": candidate.get("recommendation_features", {}),
                }
            )

        summary = (
            f"Dry-run reviewed {len(preview_rows)} shortlisted candidate(s) for {cleaned_username}. "
            f"Scored {int(prepared['scored'] or 0)} total and skipped {int(prepared['skipped'] or 0)} before the shortlist."
        )
        return {
            "username": cleaned_username,
            "summary": summary,
            "profile_review": profile_payload.get("profile_review", {}),
            "candidates": preview_rows,
            "scored": int(prepared["scored"] or 0),
            "skipped": int(prepared["skipped"] or 0),
            "skip_reasons": prepared.get("skip_reasons", {}),
        }

    async def preview_decision_candidates(self, username: str, *, limit: int = 8) -> dict[str, Any]:
        return await self._build_decision_preview(username, limit=limit)

    async def run_decision_preview(
        self,
        username: str | None = None,
        *,
        trigger_source: str = "manual",
        limit: int = 8,
    ) -> dict[str, Any]:
        target_username = str(username or "").strip()
        logger.info("Decision Dry Run started for target=%s", target_username or "missing-user")
        with self.session_scope() as session:
            task = self._start_task(session, "decision_preview")

        preview: dict[str, Any] | None = None
        errors: list[str] = []
        try:
            self._update_task(
                task.id,
                status="running",
                summary=(
                    f"Preparing a dry-run review for {target_username}."
                    if target_username
                    else "Waiting for a selected profile before running dry run."
                ),
                progress_current=0,
                progress_total=3,
                current_label=target_username or "Awaiting profile",
                detail_payload={
                    "target_username": target_username,
                    "preview": None,
                    "errors": [],
                },
            )
            if not target_username:
                raise ValueError("Select a profile before running a dry-run review.")

            self._update_task(
                task.id,
                status="running",
                summary=f"Loading profile context and shortlist for {target_username}.",
                progress_current=1,
                progress_total=3,
                current_label=target_username,
                detail_payload={
                    "target_username": target_username,
                    "preview": None,
                    "errors": [],
                },
            )
            preview = await self._build_decision_preview(target_username, limit=limit)
            self._update_task(
                task.id,
                status="running",
                summary=f"Scoring the dry-run shortlist for {target_username}.",
                progress_current=2,
                progress_total=3,
                current_label=target_username,
                detail_payload={
                    "target_username": target_username,
                    "preview": preview,
                    "errors": [],
                },
            )
            status = "success"
            summary = str(preview.get("summary") or "Dry-run complete.")
        except Exception as exc:
            status = "error"
            summary = str(exc).strip() or "Decision dry run failed."
            errors.append(summary)

        self._update_task(
            task.id,
            status=status,
            summary=summary,
            progress_current=3 if status == "success" else min(2, 3),
            progress_total=3,
            current_label=target_username or "Finished",
            detail_payload={
                "target_username": target_username,
                "preview": preview,
                "errors": list(errors),
            },
            finished=True,
        )

        self._record_operation_log(
            engine="decision_preview",
            username=target_username or "system",
            media_type="candidate",
            media_title=(
                f"Decision dry run for {target_username}"
                if target_username
                else "Decision dry run without selected profile"
            ),
            source=trigger_source,
            decision="PREVIEW" if status == "success" else "ERROR",
            reasoning=summary,
            error="; ".join(errors) if errors else None,
            detail_payload={
                "task_type": "decision_preview",
                "run_scope": target_username or "missing-user",
                "trigger": trigger_source,
                "previewed": int((preview or {}).get("candidates") and len((preview or {}).get("candidates", [])) or 0),
                "scored": int((preview or {}).get("scored") or 0),
                "skipped": int((preview or {}).get("skipped") or 0),
                "skip_reasons": (preview or {}).get("skip_reasons", {}),
                "status": status,
            },
        )

        logger.info("Decision Dry Run finished status=%s summary=%s", status, summary)

        return {
            "engine": "decision_preview",
            "status": status,
            "summary": summary,
            "preview": preview,
            "errors": errors,
        }

    async def run_suggested_for_you(self, username: str | None = None) -> dict[str, Any]:
        logger.info("Suggested For You refresh started for target=%s", username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "suggested_for_you")

        target_username = str(username or "").strip()
        refreshed_users: list[str] = []
        processed_usernames: list[str] = []
        stored = 0
        scored = 0
        errors: list[str] = []
        total_steps = 0
        completed_steps = 0

        try:
            if not self.settings.suggestions_enabled:
                status = "success"
                summary = "Suggested For You is disabled in runtime settings."
            else:
                users = await self.media_server.list_users()
                if username:
                    users = [user for user in users if user.get("Name") == username]

                phase_steps_per_user = 5
                total_steps = len(users) * phase_steps_per_user
                self._update_task(
                    task.id,
                    status="running",
                    summary=(
                        f"Preparing Suggested For You refresh for {target_username}."
                        if target_username
                        else f"Preparing Suggested For You refresh for {len(users)} user(s)."
                    ),
                    progress_current=0,
                    progress_total=total_steps,
                    current_label=target_username or "Preparing suggestions",
                    detail_payload={
                        "target_username": target_username,
                        "processed_users": 0,
                        "total_users": len(users),
                        "processed_usernames": [],
                        "stored": 0,
                        "scored": 0,
                        "errors": [],
                    },
                )

                for user_index, user in enumerate(users):
                    current_username = str(user.get("Name") or "unknown")
                    if current_username not in processed_usernames:
                        processed_usernames.append(current_username)
                    try:
                        base_offset = user_index * phase_steps_per_user

                        def emit_suggestion_progress(label: str, step: int, detail: dict[str, Any] | None = None) -> None:
                            nonlocal completed_steps
                            completed_steps = base_offset + max(0, min(phase_steps_per_user, int(step)))
                            self._update_task(
                                task.id,
                                status="running",
                                summary=label,
                                progress_current=completed_steps,
                                progress_total=total_steps,
                                current_label=current_username,
                                detail_payload={
                                    "target_username": target_username,
                                    "processed_users": len(refreshed_users),
                                    "total_users": len(users),
                                    "processed_usernames": list(processed_usernames),
                                    "stored": stored,
                                    "scored": scored,
                                    "errors": list(errors),
                                    "phase": (detail or {}).get("phase", ""),
                                },
                            )

                        result = await self._refresh_user_suggestions(user, progress_callback=emit_suggestion_progress)
                        refreshed_users.append(current_username)
                        stored += int(result.get("stored") or 0)
                        scored += int(result.get("scored") or 0)
                        completed_steps = base_offset + phase_steps_per_user
                        self._update_task(
                            task.id,
                            status="running",
                            summary=f"Stored suggestion snapshot for {current_username}.",
                            progress_current=completed_steps,
                            progress_total=total_steps,
                            current_label=current_username,
                            detail_payload={
                                "target_username": target_username,
                                "processed_users": len(refreshed_users),
                                "total_users": len(users),
                                "processed_usernames": list(processed_usernames),
                                "stored": stored,
                                "scored": scored,
                                "errors": list(errors),
                                "phase": "complete",
                            },
                        )
                    except Exception as exc:
                        errors.append(f"{current_username}: {exc}")
                        logger.exception("Suggested For You refresh failed for user=%s", current_username)
                        completed_steps = base_offset + phase_steps_per_user
                        self._update_task(
                            task.id,
                            status="running",
                            summary=f"Suggested For You hit an error for {current_username}.",
                            progress_current=completed_steps,
                            progress_total=total_steps,
                            current_label=current_username,
                            detail_payload={
                                "target_username": target_username,
                                "processed_users": len(refreshed_users),
                                "total_users": len(users),
                                "processed_usernames": list(processed_usernames),
                                "stored": stored,
                                "scored": scored,
                                "errors": list(errors),
                                "phase": "error",
                            },
                        )

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

        self._update_task(
            task.id,
            status=status,
            summary=summary,
            progress_current=total_steps if total_steps > 0 else completed_steps,
            progress_total=total_steps,
            current_label=target_username or ("Complete" if status == "success" else "Finished"),
            detail_payload={
                "target_username": target_username,
                "processed_users": len(refreshed_users),
                "total_users": len(users) if 'users' in locals() else 0,
                "processed_usernames": list(processed_usernames),
                "refreshed_users": list(refreshed_users),
                "stored": stored,
                "scored": scored,
                "errors": list(errors),
                "phase": "complete" if status == "success" else status,
            },
            finished=True,
        )

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

    async def run_library_sync(self, *, trigger_source: str = "manual") -> dict[str, Any]:
        logger.info("Library Sync started.")
        with self.session_scope() as session:
            task = self._start_task(session, "library_sync")

        indexed = 0
        added = 0
        updated = 0
        unchanged = 0
        removed = 0
        skipped = 0
        material_changes = 0
        refreshed_users: list[str] = []
        errors: list[str] = []
        sync_libraries: list[dict[str, Any]] = []
        suggestion_refresh: dict[str, Any] = {
            "state": "pending",
            "completed_users": 0,
            "total_users": 0,
            "reason": "",
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
                    changed = False
                    if row is None:
                        row = LibraryMedia(
                            source_provider="jellyfin",
                            media_server_id=media_server_id,
                        )
                        session.add(row)
                        added += 1
                        material_changes += 1
                        changed = True
                    else:
                        current_fingerprint = str(row.content_fingerprint or "")
                        if current_fingerprint != str(payload["content_fingerprint"]) or row.state != "available":
                            updated += 1
                            material_changes += 1
                            changed = True
                        else:
                            unchanged += 1

                    if changed:
                        row.media_type = str(payload["media_type"])
                        row.title = str(payload["title"])
                        row.sort_title = str(payload["sort_title"])
                        row.overview = str(payload["overview"])
                        row.production_year = payload["production_year"]
                        row.release_date = payload["release_date"]
                        row.community_rating = payload["community_rating"]
                        row.genres_json = json.dumps(payload["genres"], ensure_ascii=True)
                        row.tmdb_id = payload["tmdb_id"]
                        row.tvdb_id = payload["tvdb_id"]
                        row.imdb_id = payload["imdb_id"]
                        row.content_fingerprint = str(payload["content_fingerprint"])
                        row.payload_json = str(payload["payload_json"])
                    row.state = "available"
                    row.last_seen_at = now
                    indexed += 1

                if allow_removals:
                    for media_server_id, row in existing_rows.items():
                        if media_server_id in seen_ids or row.state == "removed":
                            continue
                        row.state = "removed"
                        removed += 1
                        material_changes += 1

            if self.settings.suggestions_enabled:
                if material_changes > 0:
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
                    suggestion_refresh["state"] = "skipped"
                    suggestion_refresh["reason"] = "library_unchanged"
                    logger.info("Library Sync skipped suggestion refresh because no material library changes were detected.")
            else:
                suggestion_refresh["state"] = "disabled"

            suggestion_clause = (
                f"refreshed {len(refreshed_users)} suggestion snapshot(s)"
                if suggestion_refresh["state"] not in {"skipped", "disabled"}
                else "skipped suggestion refresh because the indexed library did not materially change"
                if suggestion_refresh["state"] == "skipped"
                else "suggestion refresh disabled"
            )
            if errors:
                status = "partial"
                summary = (
                    f"Indexed {indexed} Jellyfin item(s), added {added}, updated {updated}, "
                    f"unchanged {unchanged}, removed {removed}, skipped {skipped}, {suggestion_clause}, "
                    f"errors {len(errors)}."
                )
            else:
                status = "success"
                summary = (
                    f"Indexed {indexed} Jellyfin item(s), added {added}, updated {updated}, "
                    f"unchanged {unchanged}, removed {removed}, skipped {skipped}, {suggestion_clause}."
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

        self._record_operation_log(
            engine="library_sync",
            username="system",
            media_type="library",
            media_title="Jellyfin library sync",
            source="jellyfin",
            decision="SYNC",
            reasoning=summary,
            error="; ".join(errors) if errors else None,
            detail_payload={
                "task_type": "library_sync",
                "trigger": trigger_source,
                "indexed": indexed,
                "added": added,
                "updated": updated,
                "unchanged": unchanged,
                "removed": removed,
                "skipped": skipped,
                "refreshed_users": list(refreshed_users),
                "suggestion_refresh_state": suggestion_refresh["state"],
            },
        )

        logger.info("Library Sync finished status=%s summary=%s", status, summary)

        return {
            "engine": "library_sync",
            "status": status,
            "summary": summary,
            "indexed": indexed,
            "added": added,
            "updated": updated,
            "unchanged": unchanged,
            "removed": removed,
            "skipped": skipped,
            "material_changes": material_changes,
            "refreshed_users": refreshed_users,
            "suggestion_refresh_state": suggestion_refresh["state"],
            "errors": errors,
        }

    async def run_request_status_sync(
        self,
        username: str | None = None,
        *,
        trigger_source: str = "manual",
    ) -> dict[str, Any]:
        target_username = str(username or "").strip()
        logger.info("Request Status Sync started for target=%s", target_username or "all-users")
        with self.session_scope() as session:
            task = self._start_task(session, "request_status_sync")

        checked = 0
        recorded = 0
        processed_usernames: list[str] = []
        errors: list[str] = []
        outcome_counts: Counter[str] = Counter()

        try:
            if not self.seer.configured or not self.settings.seer_api_key:
                raise ClientConfigError("Configure Seer base URL and API key before syncing request status.")

            with self.session_scope() as session:
                stmt = (
                    select(RequestedMedia)
                    .where(RequestedMedia.seer_request_id.is_not(None))
                    .order_by(desc(RequestedMedia.created_at))
                )
                if target_username:
                    stmt = stmt.where(RequestedMedia.username == target_username)
                tracked_rows = list(session.scalars(stmt))

            total_steps = max(1, len(tracked_rows))
            self._update_task(
                task.id,
                status="running",
                summary=(
                    f"Preparing Seer request sync for {target_username}."
                    if target_username
                    else f"Preparing Seer request sync for {len(tracked_rows)} tracked request(s)."
                ),
                progress_current=0,
                progress_total=total_steps,
                current_label=target_username or "Preparing sync",
                detail_payload={
                    "target_username": target_username,
                    "checked": 0,
                    "recorded": 0,
                    "processed_usernames": [],
                    "outcome_counts": {},
                    "errors": [],
                },
            )

            for index, row in enumerate(tracked_rows, start=1):
                current_username = str(row.username or "").strip() or "unknown"
                if current_username not in processed_usernames:
                    processed_usernames.append(current_username)

                self._update_task(
                    task.id,
                    status="running",
                    summary=f"Checking Seer request status for {row.media_title}.",
                    progress_current=max(0, index - 1),
                    progress_total=total_steps,
                    current_label=current_username,
                    detail_payload={
                        "target_username": target_username,
                        "checked": checked,
                        "recorded": recorded,
                        "processed_usernames": list(processed_usernames),
                        "outcome_counts": dict(outcome_counts),
                        "errors": list(errors),
                    },
                )

                try:
                    payload = await self.seer.get_request(int(row.seer_request_id or 0))
                    checked += 1
                    outcome = self._request_outcome_from_seer_request(payload)
                    if outcome and self._record_request_outcome_from_seer_sync(
                        requested_media_id=row.id,
                        payload=payload,
                        outcome=outcome,
                        trigger_source=trigger_source,
                    ):
                        recorded += 1
                        outcome_counts[outcome] += 1
                except Exception as exc:
                    errors.append(f"{current_username}::{row.media_title}: {exc}")
                    logger.warning(
                        "Request status sync failed username=%s title=%s request_id=%s reason=%s",
                        current_username,
                        row.media_title,
                        row.seer_request_id,
                        exc,
                    )

                self._update_task(
                    task.id,
                    status="running",
                    summary=f"Checked {index}/{len(tracked_rows)} tracked request(s).",
                    progress_current=index,
                    progress_total=total_steps,
                    current_label=current_username,
                    detail_payload={
                        "target_username": target_username,
                        "checked": checked,
                        "recorded": recorded,
                        "processed_usernames": list(processed_usernames),
                        "outcome_counts": dict(outcome_counts),
                        "errors": list(errors),
                    },
                )

            if not tracked_rows:
                status = "success"
                summary = (
                    f"No Seer-linked requests are stored for {target_username}."
                    if target_username
                    else "No Seer-linked requests are stored yet."
                )
            elif errors and recorded == 0:
                status = "partial"
                summary = f"Checked {checked} tracked request(s) but hit {len(errors)} sync error(s)."
            elif errors:
                status = "partial"
                summary = (
                    f"Checked {checked} tracked request(s) and recorded {recorded} new status update(s), "
                    f"with {len(errors)} sync error(s)."
                )
            else:
                status = "success"
                summary = f"Checked {checked} tracked request(s) and recorded {recorded} new status update(s)."
        except Exception as exc:
            status = "error"
            summary = str(exc).strip() or "Request status sync failed."
            errors.append(summary)

        final_total_steps = max(1, locals().get("total_steps", 1))
        self._update_task(
            task.id,
            status=status,
            summary=summary,
            progress_current=final_total_steps if status != "error" else min(final_total_steps, checked),
            progress_total=final_total_steps,
            current_label=target_username or "Finished",
            detail_payload={
                "target_username": target_username,
                "checked": checked,
                "recorded": recorded,
                "processed_usernames": list(processed_usernames),
                "outcome_counts": dict(outcome_counts),
                "errors": list(errors),
            },
            finished=True,
        )

        self._record_operation_log(
            engine="request_status_sync",
            username=target_username or "system",
            media_type="request",
            media_title=(
                f"Request Status Sync for {target_username}"
                if target_username
                else "Request Status Sync for all users"
            ),
            source=trigger_source,
            decision="SYNC" if status != "error" else "ERROR",
            reasoning=summary,
            error="; ".join(errors) if errors else None,
            detail_payload={
                "task_type": "request_status_sync",
                "run_scope": target_username or "all-users",
                "trigger": trigger_source,
                "checked": checked,
                "recorded": recorded,
                "outcome_counts": dict(outcome_counts),
                "processed_usernames": list(processed_usernames),
                "status": status,
            },
        )

        logger.info("Request Status Sync finished status=%s summary=%s", status, summary)

        return {
            "engine": "request_status_sync",
            "status": status,
            "summary": summary,
            "checked": checked,
            "recorded": recorded,
            "outcome_counts": dict(outcome_counts),
            "errors": errors,
        }

    @classmethod
    def _request_outcome_from_seer_request(cls, payload: dict[str, Any]) -> str | None:
        if not isinstance(payload, dict):
            return None

        request_block = payload.get("request") if isinstance(payload.get("request"), dict) else {}
        media_block = {}
        for key in ("media", "mediaInfo", "requestedMedia"):
            value = payload.get(key)
            if isinstance(value, dict):
                media_block = value
                break

        combined = " ".join(
            part.strip().upper()
            for part in (
                str(payload.get("status") or ""),
                str(payload.get("requestStatus") or ""),
                str(request_block.get("status") or ""),
                str(media_block.get("status") or ""),
                str(media_block.get("mediaStatus") or ""),
                str(payload.get("mediaStatus") or ""),
            )
            if part.strip()
        )

        if "DECLIN" in combined or "DENIED" in combined:
            return "denied"
        if "FAILED" in combined or "UNAVAILABLE" in combined:
            return "unavailable"
        if "PARTIALLY_AVAILABLE" in combined or "AVAILABLE" in combined or "DOWNLOADED" in combined:
            return "downloaded"
        if "APPROV" in combined:
            return "approved"

        request_status = cls._coerce_int(
            payload.get("status")
            if isinstance(payload.get("status"), int | float | str)
            else request_block.get("status")
        )
        media_status = cls._coerce_int(
            media_block.get("status")
            if media_block.get("status") is not None
            else media_block.get("mediaStatus")
        )
        if request_status == 3:
            return "denied"
        if media_status is not None and media_status >= 4:
            return "downloaded"
        if request_status == 2:
            return "approved"
        return None

    @classmethod
    def _describe_seer_request_sync(cls, payload: dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return "Updated from Seer request status sync."

        request_block = payload.get("request") if isinstance(payload.get("request"), dict) else {}
        media_block = {}
        for key in ("media", "mediaInfo", "requestedMedia"):
            value = payload.get(key)
            if isinstance(value, dict):
                media_block = value
                break

        request_status = str(
            request_block.get("status")
            if request_block.get("status") is not None
            else payload.get("status")
            or ""
        ).strip()
        media_status = str(
            media_block.get("status")
            if media_block.get("status") is not None
            else media_block.get("mediaStatus")
            or payload.get("mediaStatus")
            or ""
        ).strip()
        if request_status and media_status:
            return f"Seer sync observed request status {request_status} and media status {media_status}."
        if request_status:
            return f"Seer sync observed request status {request_status}."
        if media_status:
            return f"Seer sync observed media status {media_status}."
        return "Updated from Seer request status sync."

    def _record_request_outcome_for_audience(
        self,
        *,
        session: Session,
        requested_row: RequestedMedia | None,
        usernames: list[str],
        outcome: str,
        source: str,
        detail: str,
        payload: dict[str, Any],
        suppress_positive_regression: bool = False,
    ) -> list[str]:
        normalized_outcome = self._normalize_request_outcome_label(outcome)
        positive_rank = {"approved": 1, "downloaded": 2, "watched": 3}
        recorded: list[str] = []

        requested_media_id = int(requested_row.id) if requested_row is not None else None
        request_id = requested_row.seer_request_id if requested_row is not None else self._coerce_int(payload.get("request_id"))
        media_type = requested_row.media_type if requested_row is not None else str(payload.get("media_type") or "unknown").strip()
        media_id = requested_row.media_id if requested_row is not None else int(self._coerce_int(payload.get("media_id")) or 0)
        media_title = requested_row.media_title if requested_row is not None else str(payload.get("subject") or "Unknown").strip() or "Unknown"

        for target_username in usernames:
            lookup_conditions: list[Any] = []
            if requested_media_id is not None:
                lookup_conditions.append(RequestOutcomeEvent.requested_media_id == requested_media_id)
            if request_id is not None:
                lookup_conditions.append(RequestOutcomeEvent.request_id == request_id)
            matching_events = list(
                session.scalars(
                    select(RequestOutcomeEvent)
                    .where(
                        RequestOutcomeEvent.username == target_username,
                        or_(*lookup_conditions),
                    )
                    .order_by(desc(RequestOutcomeEvent.created_at))
                )
            ) if lookup_conditions else []
            if any(
                self._normalize_request_outcome_label(event.outcome) == normalized_outcome
                for event in matching_events
            ):
                continue

            latest_outcome = (
                self._normalize_request_outcome_label(matching_events[0].outcome)
                if matching_events
                else ""
            )
            if suppress_positive_regression and positive_rank.get(latest_outcome, 0) > positive_rank.get(normalized_outcome, 0) > 0:
                continue

            session.add(
                RequestOutcomeEvent(
                    requested_media_id=requested_media_id,
                    username=target_username,
                    media_type=media_type,
                    media_id=media_id,
                    media_title=media_title,
                    request_id=request_id,
                    outcome=normalized_outcome,
                    source=source,
                    detail=detail,
                    payload_json=json.dumps(payload, ensure_ascii=True),
                )
            )
            recorded.append(target_username)
        return recorded

    def _record_request_outcome_from_seer_sync(
        self,
        *,
        requested_media_id: int,
        payload: dict[str, Any],
        outcome: str,
        trigger_source: str,
    ) -> bool:
        normalized_outcome = self._normalize_request_outcome_label(outcome)
        detail = self._describe_seer_request_sync(payload)

        affected_usernames: list[str] = []
        with self.session_scope() as session:
            requested_row = session.get(RequestedMedia, requested_media_id)
            if requested_row is None:
                return False

            affected_usernames = self._record_request_outcome_for_audience(
                session=session,
                requested_row=requested_row,
                usernames=self._request_audience_usernames(session, requested_row),
                outcome=normalized_outcome,
                source="seer_sync",
                detail=detail,
                payload=payload,
                suppress_positive_regression=True,
            )
            resolved_media_type = requested_row.media_type
            resolved_media_id = requested_row.media_id
            resolved_title = requested_row.media_title
            resolved_request_id = requested_row.seer_request_id

        if not affected_usernames:
            return False

        for resolved_username in affected_usernames:
            self.record_operation_event(
                engine="request_outcome",
                username=resolved_username,
                media_type=resolved_media_type,
                media_title=resolved_title,
                source="seer_sync",
                decision=normalized_outcome.upper(),
                reasoning=f"Seer request sync recorded request outcome {normalized_outcome} for {resolved_title}.",
                detail_payload={
                    "requested_media_id": requested_media_id,
                    "request_id": resolved_request_id,
                    "media_id": resolved_media_id,
                    "outcome": normalized_outcome,
                    "trigger": trigger_source,
                    "affected_usernames": affected_usernames,
                },
            )
            live_payload = self._with_live_profile_context(resolved_username, self.profile_store.read_payload(resolved_username))
            self.profile_store.write_payload(resolved_username, live_payload)
        return True

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

        recorded_outcome: str | None = None
        outcome_label = self._request_outcome_from_webhook(
            notification_type=notification_type,
            event_name=event_name,
            media_status=media_status,
        )
        if outcome_label and requested_by_username:
            recorded_outcome = self._record_request_outcome_from_webhook(
                username=requested_by_username,
                outcome=outcome_label,
                request_id=request_id,
                media_type=media_type,
                media_id=tmdb_id,
                media_title=subject,
                payload=payload,
                source=notification_type or "seer_webhook",
            )

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
            "recorded_outcome": recorded_outcome,
            "refreshed_suggestions": refreshed,
        }

    @staticmethod
    def _request_outcome_from_webhook(
        *,
        notification_type: str,
        event_name: str,
        media_status: str | None,
    ) -> str | None:
        combined = " ".join(
            part.strip().upper()
            for part in (notification_type, event_name, media_status or "")
            if str(part).strip()
        )
        if "AVAILABLE" in combined:
            return "downloaded"
        if "APPROV" in combined:
            return "approved"
        if "DECLIN" in combined or "DENIED" in combined:
            return "denied"
        if "FAILED" in combined or "UNAVAILABLE" in combined:
            return "unavailable"
        return None

    def _record_request_outcome_from_webhook(
        self,
        *,
        username: str,
        outcome: str,
        request_id: int | None,
        media_type: str | None,
        media_id: int | None,
        media_title: str,
        payload: dict[str, Any],
        source: str,
    ) -> str:
        cleaned_username = str(username or "").strip()
        if not cleaned_username:
            return outcome

        requested_row_id: int | None = None
        resolved_media_type = str(media_type or "unknown").strip().lower() or "unknown"
        resolved_media_id = int(media_id or 0)
        resolved_title = str(media_title or "").strip() or "Unknown"
        affected_usernames: list[str] = [cleaned_username]
        recorded_usernames: list[str] = []

        with self.session_scope() as session:
            requested_row = None
            if request_id is not None:
                requested_row = session.scalar(
                    select(RequestedMedia)
                    .where(
                        RequestedMedia.username == cleaned_username,
                        RequestedMedia.seer_request_id == request_id,
                    )
                    .order_by(desc(RequestedMedia.created_at))
                    .limit(1)
                )
            if requested_row is None and resolved_media_type in {"movie", "tv"} and resolved_media_id > 0:
                requested_row = session.scalar(
                    select(RequestedMedia)
                    .where(
                        RequestedMedia.username == cleaned_username,
                        RequestedMedia.media_type == resolved_media_type,
                        RequestedMedia.media_id == resolved_media_id,
                    )
                    .order_by(desc(RequestedMedia.created_at))
                    .limit(1)
                )

            if requested_row is not None:
                requested_row_id = requested_row.id
                resolved_media_type = requested_row.media_type
                resolved_media_id = requested_row.media_id
                resolved_title = requested_row.media_title
                affected_usernames = self._request_audience_usernames(session, requested_row)

            recorded_usernames = self._record_request_outcome_for_audience(
                session=session,
                requested_row=requested_row,
                usernames=affected_usernames,
                outcome=outcome,
                source=source,
                detail=str(payload.get("subject") or payload.get("event") or payload.get("media_status") or "").strip(),
                payload=payload,
                suppress_positive_regression=False,
            )
            if recorded_usernames:
                affected_usernames = recorded_usernames

        if not recorded_usernames:
            return outcome

        for target_username in affected_usernames:
            self.record_operation_event(
                engine="request_outcome",
                username=target_username,
                media_type=resolved_media_type,
                media_title=resolved_title,
                source=source,
                decision=outcome.upper(),
                reasoning=f"Seer webhook recorded request outcome {outcome} for {resolved_title}.",
                detail_payload={
                    "request_id": request_id,
                    "media_id": resolved_media_id,
                    "webhook_source": source,
                    "outcome": outcome,
                    "affected_usernames": affected_usernames,
                },
            )
            live_payload = self._with_live_profile_context(target_username, self.profile_store.read_payload(target_username))
            self.profile_store.write_payload(target_username, live_payload)
        return outcome

    def _jellyfin_client(self) -> JellyfinClient:
        if isinstance(self.media_server, JellyfinClient):
            return self.media_server
        client = getattr(self.media_server, "jellyfin", None)
        if isinstance(client, JellyfinClient):
            return client
        raise RuntimeError("Suggested For You requires a Jellyfin media server client.")

    async def _refresh_user_suggestions(
        self,
        user: dict[str, Any],
        progress_callback: Callable[[str, int, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        current_username = str(user.get("Name") or "unknown")
        jellyfin_user_id = normalize_jellyfin_user_id(str(user.get("Id") or ""))
        if not jellyfin_user_id:
            raise ValueError("Jellyfin user id is required for suggestion refresh.")

        def emit_progress(label: str, step: int, phase: str) -> None:
            if progress_callback is None:
                return
            progress_callback(label, step, {"phase": phase})

        emit_progress(f"Loading playback history for {current_username}.", 0, "history")
        history = await self.media_server.get_playback_history(
            jellyfin_user_id,
            self._playback_history_limit(),
        )
        favorite_items = await self._load_user_favorite_items(jellyfin_user_id, username=current_username)
        stored_profile = self.profile_store.read_payload(current_username)
        profile_payload, recommendation_seeds, should_persist = await self._prepare_runtime_profile_payload(
            current_username,
            history,
            favorite_items=favorite_items,
            existing_payload=stored_profile,
        )
        if should_persist:
            self.profile_store.write_payload(current_username, profile_payload)

        emit_progress(f"Loading indexed library candidates for {current_username}.", 1, "library")
        viewing_history = self._build_viewing_history_context(
            history,
            recommendation_seeds=recommendation_seeds,
            profile_summary=profile_payload,
        )
        existing_ai_cache = self._load_existing_suggestion_ai_cache(jellyfin_user_id)
        available_candidates = await self._build_available_library_candidates(jellyfin_user_id)
        in_progress_items = await self._load_in_progress_items(jellyfin_user_id)
        exclusion_context = self._build_suggestion_exclusion_context(
            history,
            in_progress_items,
            recent_cooldown_days=self.settings.suggestion_recent_cooldown_days,
            repeat_watch_cutoff=self.settings.suggestion_repeat_watch_cutoff,
        )
        emit_progress(f"Ranking suggestion candidates for {current_username}.", 2, "ranking")
        filtered_candidates = [
            candidate
            for candidate in available_candidates
            if self._candidate_feedback_block_reason(candidate, profile_payload) is None
            and self._suggestion_exclusion_reason(candidate, exclusion_context) is None
        ]
        ranked_candidates = self._rank_candidate_pool(
            filtered_candidates,
            profile_summary=profile_payload,
        )
        ai_candidates, ai_scored, ai_reused = await self._score_suggestion_candidates_with_ai(
            ranked_candidates,
            username=current_username,
            profile_payload=profile_payload,
            viewing_history=viewing_history,
            cached_llm_votes=existing_ai_cache,
        )
        emit_progress(f"Applying AI shortlist for {current_username}.", 3, "ai")
        display_candidates = self._filter_suggestion_candidates_for_display(
            ai_candidates,
            threshold=self.settings.suggestion_ai_threshold,
        )
        selected_candidates = self._diversify_candidates(
            self._sort_suggestion_candidates(display_candidates),
            limit=max(1, int(self.settings.suggestions_limit)),
        )

        emit_progress(f"Writing stored suggestion snapshot for {current_username}.", 4, "storage")
        with self.session_scope() as session:
            for existing in session.scalars(
                select(SuggestedMedia).where(SuggestedMedia.jellyfin_user_id == jellyfin_user_id)
            ):
                session.delete(existing)

            for index, candidate in enumerate(selected_candidates, start=1):
                features = candidate.get("recommendation_features", {})
                reasoning = self._compose_suggestion_reasoning(candidate)
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
                        score=float(
                            features.get("hybrid_score")
                            or features.get("final_score")
                            or features.get("deterministic_score")
                            or 0.0
                        ),
                        reasoning=reasoning,
                        state="available",
                        tmdb_id=self._coerce_int(external_ids.get("tmdb")),
                        tvdb_id=self._coerce_int(external_ids.get("tvdb")),
                        imdb_id=str(external_ids.get("imdb") or "").strip() or None,
                        payload_json=json.dumps(candidate, ensure_ascii=True),
                    )
                )
                session.add(
                    DecisionLog(
                        engine="suggested_for_you",
                        username=current_username,
                        media_type=str(candidate.get("media_type") or "unknown"),
                        media_id=int(candidate.get("media_id") or 0),
                        media_title=str(candidate.get("title") or "Unknown"),
                        source=", ".join(candidate.get("sources", [])) or "library:indexed",
                        decision="SUGGEST",
                        confidence=float(
                            features.get("hybrid_score")
                            or features.get("final_score")
                            or features.get("deterministic_score")
                            or 0.0
                        ),
                        threshold=float(self.settings.suggestion_ai_threshold),
                        requested=False,
                        request_id=None,
                        reasoning=reasoning,
                        payload_json=json.dumps(candidate, ensure_ascii=True),
                        error=None,
                    )
                )

        emit_progress(f"Completed suggestion refresh for {current_username}.", 5, "complete")
        return {
            "username": current_username,
            "stored": len(selected_candidates),
            "scored": len(filtered_candidates),
            "eligible": len(display_candidates),
            "ai_scored": ai_scored,
            "ai_reused": ai_reused,
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

    async def _load_in_progress_items(self, jellyfin_user_id: str) -> list[dict[str, Any]]:
        try:
            client = self._jellyfin_client()
        except RuntimeError:
            return []

        try:
            return await client.get_resumable_items(jellyfin_user_id, limit=150)
        except Exception as exc:
            logger.warning(
                "Suggested For You in-progress lookup skipped user=%s reason=%s",
                jellyfin_user_id,
                exc,
            )
            return []

    async def _score_suggestion_candidates_with_ai(
        self,
        candidates: list[dict[str, Any]],
        *,
        username: str,
        profile_payload: dict[str, Any],
        viewing_history: dict[str, Any],
        cached_llm_votes: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], int, int]:
        annotated = [dict(candidate) for candidate in candidates]
        if not annotated:
            return annotated, 0, 0

        shortlist = self._select_suggestion_ai_candidates(
            annotated,
            threshold=self.settings.suggestion_ai_threshold,
            limit=self.settings.suggestion_ai_candidate_limit,
        )
        if not shortlist:
            for candidate in annotated:
                self._finalize_suggestion_candidate(candidate)
            return annotated, 0, 0

        shortlist = await self._enrich_candidate_pool_with_tmdb(
            shortlist,
            limit=min(self.settings.tmdb_candidate_enrichment_limit, len(shortlist)),
        )
        shortlist = self._rank_candidate_pool(shortlist, profile_summary=profile_payload)

        scored_by_key: dict[tuple[str, int], dict[str, Any]] = {}
        cached_votes = cached_llm_votes or {}
        ai_scored = 0
        ai_reused = 0
        for candidate in shortlist:
            features = candidate.setdefault("recommendation_features", {})
            deterministic_score = float(features.get("deterministic_score") or 0.0)
            cache_key = self._build_suggestion_ai_cache_key(
                candidate,
                profile_payload=profile_payload,
                viewing_history=viewing_history,
            )
            features["suggestion_ai_cache_key"] = cache_key
            llm_vote = "UNAVAILABLE"
            llm_confidence: float | None = None
            llm_reasoning = ""
            cached_vote = cached_votes.get(cache_key)

            if cached_vote is not None:
                llm_vote = str(cached_vote.get("llm_vote") or "UNAVAILABLE")
                llm_confidence = self._coerce_optional_float(cached_vote.get("llm_confidence"))
                llm_reasoning = str(cached_vote.get("llm_reasoning") or "").strip()
                ai_reused += 1
            else:
                try:
                    llm_payload = await self.llm.generate_json(
                        messages=build_suggestion_messages(
                            username=username,
                            profile_payload=profile_payload,
                            viewing_history=viewing_history,
                            candidate=candidate,
                        ),
                        temperature=0,
                        purpose="decision",
                    )
                    llm_vote = str(llm_payload.get("decision", "PASS")).upper()
                    if llm_vote not in {"RECOMMEND", "PASS"}:
                        llm_vote = "PASS"
                    llm_confidence = self._coerce_float(llm_payload.get("confidence"))
                    llm_reasoning = str(llm_payload.get("reasoning", "No reasoning provided.")).strip()
                    ai_scored += 1
                except Exception as exc:
                    logger.warning(
                        "Suggested For You LLM fallback triggered user=%s title=%s reason=%s",
                        username,
                        candidate.get("title", "unknown"),
                        exc,
                    )

            hybrid_score = self._blend_suggestion_confidences(
                deterministic_score=deterministic_score,
                llm_confidence=llm_confidence,
                llm_vote=llm_vote,
                llm_weight_percent=self.settings.decision_ai_weight_percent,
            )
            features["llm_vote"] = llm_vote
            features["llm_confidence"] = llm_confidence
            features["llm_reasoning"] = llm_reasoning
            features["hybrid_score"] = hybrid_score
            features["final_score"] = hybrid_score
            scored_by_key[self._candidate_key(candidate)] = candidate

        merged: list[dict[str, Any]] = []
        for candidate in annotated:
            merged_candidate = scored_by_key.get(self._candidate_key(candidate), candidate)
            self._finalize_suggestion_candidate(merged_candidate)
            merged.append(merged_candidate)
        return merged, ai_scored, ai_reused

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
    def _task_detail_payload(task: TaskRun | None) -> dict[str, Any]:
        if task is None:
            return {}

        try:
            parsed = json.loads(task.detail_json or "{}")
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
        return {}

    @classmethod
    def _task_target_username(cls, task: TaskRun | None) -> str:
        detail = cls._task_detail_payload(task)
        return str(detail.get("target_username") or "").strip()

    @classmethod
    def _task_matches_username(cls, task: TaskRun | None, username: str) -> bool:
        normalized_username = str(username or "").strip().casefold()
        if not normalized_username or task is None:
            return False

        detail = cls._task_detail_payload(task)
        direct_target = str(detail.get("target_username") or "").strip().casefold()
        if direct_target == normalized_username:
            return True
        if not direct_target and int(detail.get("processed_users") or 0) > 0:
            return True

        for key in ("processed_usernames", "updated_users", "refreshed_users"):
            values = detail.get(key)
            if not isinstance(values, list):
                continue
            if any(str(value or "").strip().casefold() == normalized_username for value in values):
                return True

        return False

    def _record_operation_log(
        self,
        *,
        engine: str,
        username: str,
        media_type: str,
        media_title: str,
        source: str,
        decision: str,
        reasoning: str,
        error: str | None = None,
        detail_payload: dict[str, Any] | None = None,
    ) -> None:
        with self.session_scope() as session:
            session.add(
                DecisionLog(
                    engine=engine,
                    username=username,
                    media_type=media_type,
                    media_id=0,
                    media_title=media_title,
                    source=source,
                    decision=decision,
                    confidence=1.0 if error is None else 0.0,
                    threshold=0.0,
                    requested=False,
                    request_id=None,
                    reasoning=reasoning,
                    payload_json=json.dumps(detail_payload or {}, ensure_ascii=True),
                    error=error,
                )
            )

    def record_operation_event(
        self,
        *,
        engine: str,
        username: str,
        media_type: str,
        media_title: str,
        source: str,
        decision: str,
        reasoning: str,
        error: str | None = None,
        detail_payload: dict[str, Any] | None = None,
    ) -> None:
        self._record_operation_log(
            engine=engine,
            username=username,
            media_type=media_type,
            media_title=media_title,
            source=source,
            decision=decision,
            reasoning=reasoning,
            error=error,
            detail_payload=detail_payload,
        )

    @classmethod
    def _serialize_task_run(cls, task: TaskRun | None) -> dict[str, Any]:
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
                "target_username": "",
            }

        detail = cls._task_detail_payload(task)

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
            "target_username": str(detail.get("target_username") or ""),
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
    def _requested_media_keys(session: Session, username: str | None = None) -> set[tuple[str, int]]:
        stmt = select(RequestedMedia)
        if username:
            stmt = stmt.where(RequestedMedia.username == username)
        return {
            (item.media_type, item.media_id)
            for item in session.scalars(stmt)
        }

    @staticmethod
    def _requested_title_keys(session: Session, username: str | None = None) -> set[tuple[str, str]]:
        stmt = select(RequestedMedia)
        if username:
            stmt = stmt.where(RequestedMedia.username == username)
        keys: set[tuple[str, str]] = set()
        for item in session.scalars(stmt):
            media_type = str(item.media_type or "").strip()
            title = str(item.media_title or "").strip().lower()
            if media_type and title:
                keys.add((media_type, title))
        return keys

    @classmethod
    def _build_profile_history_context(
        cls,
        history: list[dict[str, Any]],
        *,
        favorite_items: list[dict[str, Any]] | None = None,
        top_limit: int = 8,
        recent_limit: int = 5,
        recent_window: int = 12,
        recent_weight_percent: int = 75,
    ) -> dict[str, Any]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
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
                    "release_year": None,
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

            release_year = cls._extract_history_release_year(item)
            if release_year is not None and grouped_entry.get("release_year") is None:
                grouped_entry["release_year"] = release_year

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

        top_titles = cls._sort_profile_entries(list(grouped.values()))
        recent_momentum = cls._sort_profile_entries(list(recent_grouped.values()))

        genre_counts: Counter[str] = Counter()
        genre_title_counts: Counter[str] = Counter()
        recent_genre_counts: Counter[str] = Counter()
        recent_genre_title_counts: Counter[str] = Counter()
        favorite_genre_counts: Counter[str] = Counter()

        for entry in grouped.values():
            entry_genres = cls._normalize_genres(entry.get("genres", []), limit=6)
            signal_weight = cls._profile_signal_weight(entry.get("play_count"))
            media_type = str(entry.get("media_type") or "")
            if media_type in {"movie", "tv"}:
                media_type_counts[media_type] += 1

            release_year = cls._coerce_int(entry.get("release_year"))
            if release_year is not None:
                release_years.append(release_year)

            for genre in entry_genres:
                genre_counts[genre] += signal_weight
                genre_title_counts[genre] += 1

            for source_genre in entry_genres:
                for target_genre in entry_genres:
                    if source_genre != target_genre:
                        genre_pairs[(source_genre, target_genre)] += signal_weight

        for entry in recent_grouped.values():
            entry_genres = cls._normalize_genres(entry.get("genres", []), limit=5)
            for genre in entry_genres:
                recent_genre_counts[genre] += 1
                recent_genre_title_counts[genre] += 1

        favorite_grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for item in favorite_items or []:
            media_type = cls._map_history_media_type(item.get("Type")) or "other"
            if media_type not in {"movie", "tv"}:
                continue

            title = cls._seed_title(item, media_type)
            genres = cls._normalize_genres(item.get("Genres", []), limit=6)
            key = (title, media_type)
            favorite_entry = favorite_grouped.setdefault(
                key,
                {
                    "title": title,
                    "media_type": media_type,
                    "play_count": 0,
                    "genres": [],
                    "community_rating": item.get("CommunityRating"),
                    "last_played": None,
                    "_last_played_score": 0.0,
                    "release_year": None,
                },
            )
            favorite_entry["genres"] = cls._merge_unique_strings(favorite_entry["genres"], genres[:4])

            if favorite_entry.get("community_rating") is None and item.get("CommunityRating") is not None:
                favorite_entry["community_rating"] = item.get("CommunityRating")

            last_played = item.get("UserData", {}).get("LastPlayedDate")
            last_played_score = cls._to_timestamp(last_played)
            if last_played_score >= favorite_entry["_last_played_score"]:
                favorite_entry["last_played"] = last_played
                favorite_entry["_last_played_score"] = last_played_score

            release_year = cls._extract_history_release_year(item)
            if release_year is not None and favorite_entry.get("release_year") is None:
                favorite_entry["release_year"] = release_year

        for key, entry in favorite_grouped.items():
            entry_genres = cls._normalize_genres(entry.get("genres", []), limit=6)
            if not entry_genres:
                continue

            watched_already = key in grouped
            signal_weight = 0.45 if watched_already else 0.85
            for genre in entry_genres:
                genre_counts[genre] += signal_weight
                favorite_genre_counts[genre] += 1
                if not watched_already:
                    genre_title_counts[genre] += 1

            for source_genre in entry_genres:
                for target_genre in entry_genres:
                    if source_genre != target_genre:
                        genre_pairs[(source_genre, target_genre)] += signal_weight

        favorite_titles = cls._sort_profile_entries(list(favorite_grouped.values()))

        normalized_top_titles = [cls._clean_profile_entry(item) for item in top_titles[:top_limit]]
        normalized_recent_momentum = [cls._clean_profile_entry(item) for item in recent_momentum[:recent_limit]]
        repeat_titles = [cls._clean_profile_entry(item) for item in top_titles if int(item.get("play_count") or 0) > 1][:5]
        normalized_favorite_titles = [cls._clean_profile_entry(item) for item in favorite_titles[:6]]
        ranked_genres = cls._rank_genres(
            genre_counts,
            recent_genre_counts,
            recent_weight_percent=recent_weight_percent,
        )
        primary_genres = [genre for genre, _score in ranked_genres[:4]]
        secondary_genres = [genre for genre, _score in ranked_genres[4:8]]
        recent_genres = [genre for genre, _count in recent_genre_counts.most_common(4)]
        ranked_genre_details = [
            {
                "genre": genre,
                "raw_count": int(genre_title_counts.get(genre, 0)),
                "recent_count": int(recent_genre_title_counts.get(genre, 0)),
                "weighted_score": round(score, 3),
            }
            for genre, score in ranked_genres[:8]
        ]

        total_genre_events = sum(float(count) for count in genre_counts.values())
        focus_share = 0.0
        if total_genre_events and primary_genres:
            focus_share = sum(float(genre_counts[genre]) for genre in primary_genres[:3]) / total_genre_events

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
            "favorite_titles": normalized_favorite_titles,
            "favorite_genres": [genre for genre, _count in favorite_genre_counts.most_common(6)],
            "favorite_signal_count": len(favorite_grouped),
            "format_preference": cls._determine_format_preference(media_type_counts),
            "release_year_preference": cls._build_release_year_preference(release_years),
            "average_top_rating": cls._average_rating(normalized_top_titles),
            "genre_focus_share": round(focus_share, 3),
            "recent_signal_weight_percent": max(0, int(recent_weight_percent)),
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
        favorite_items: list[dict[str, Any]] | None = None,
        profile_summary: dict[str, Any],
        limit: int,
    ) -> list[dict[str, Any]]:
        seeds = cls._collect_recommendation_seed_candidates(history)
        favorite_lookup: set[tuple[str, int]] = set()
        if favorite_items:
            merged_by_key: dict[tuple[str, int], dict[str, Any]] = {
                (str(seed.get("media_type") or ""), int(seed.get("media_id") or 0)): dict(seed)
                for seed in seeds
            }
            favorite_entries = cls._collect_recommendation_seed_candidates(favorite_items)
            for favorite_seed in favorite_entries:
                key = (str(favorite_seed.get("media_type") or ""), int(favorite_seed.get("media_id") or 0))
                favorite_lookup.add(key)
                existing = merged_by_key.get(key)
                if existing is None:
                    enriched = dict(favorite_seed)
                    enriched["favorite_seed"] = True
                    merged_by_key[key] = enriched
                    continue

                existing["favorite_seed"] = True
                existing["genres"] = cls._merge_unique_strings(existing.get("genres", []), favorite_seed.get("genres", []))
                if existing.get("overview") in {"", None} and favorite_seed.get("overview"):
                    existing["overview"] = favorite_seed.get("overview")
                if existing.get("community_rating") is None and favorite_seed.get("community_rating") is not None:
                    existing["community_rating"] = favorite_seed.get("community_rating")
                if float(favorite_seed.get("_last_played_score") or 0.0) > float(existing.get("_last_played_score") or 0.0):
                    existing["last_played"] = favorite_seed.get("last_played")
                    existing["_last_played_score"] = favorite_seed.get("_last_played_score")
            seeds = list(merged_by_key.values())
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
                favorite_lookup=favorite_lookup,
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

    def _resolve_tv_seed_media_ids_from_library_index(
        self,
        seeds: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        tv_seeds = [seed for seed in seeds if str(seed.get("media_type") or "") == "tv"]
        if not tv_seeds:
            return [dict(seed) for seed in seeds]

        title_keys = {
            self._normalize_seed_lookup_title(str(seed.get("title") or ""))
            for seed in tv_seeds
            if str(seed.get("title") or "").strip()
        }
        if not title_keys:
            return [dict(seed) for seed in seeds]

        with self.session_scope() as session:
            rows = list(
                session.scalars(
                    select(LibraryMedia).where(
                        LibraryMedia.media_type == "tv",
                        LibraryMedia.state == "available",
                        LibraryMedia.tmdb_id.is_not(None),
                    )
                )
            )

        resolved_by_title: dict[str, LibraryMedia] = {}
        ambiguous_titles: set[str] = set()

        for row in rows:
            lookup_values = {
                self._normalize_seed_lookup_title(str(row.title or "")),
                self._normalize_seed_lookup_title(str(row.sort_title or "")),
            }
            for lookup_value in lookup_values:
                if not lookup_value or lookup_value not in title_keys:
                    continue
                if lookup_value in ambiguous_titles:
                    continue
                existing = resolved_by_title.get(lookup_value)
                if existing is None:
                    resolved_by_title[lookup_value] = row
                    continue
                if existing.id != row.id:
                    ambiguous_titles.add(lookup_value)
                    resolved_by_title.pop(lookup_value, None)

        resolved: list[dict[str, Any]] = []
        for seed in seeds:
            enriched = dict(seed)
            if str(enriched.get("media_type") or "") != "tv":
                resolved.append(enriched)
                continue

            lookup_key = self._normalize_seed_lookup_title(str(enriched.get("title") or ""))
            row = resolved_by_title.get(lookup_key)
            if row is not None and row.tmdb_id is not None:
                enriched["media_id"] = int(row.tmdb_id)
                external_ids = dict(enriched.get("external_ids") or {})
                external_ids["tmdb"] = str(row.tmdb_id)
                if row.tvdb_id is not None:
                    external_ids.setdefault("tvdb", str(row.tvdb_id))
                if row.imdb_id:
                    external_ids.setdefault("imdb", str(row.imdb_id))
                enriched["external_ids"] = external_ids
            resolved.append(enriched)

        return resolved

    @classmethod
    def _build_seed_lanes(
        cls,
        seed: dict[str, Any],
        *,
        top_lookup: set[tuple[str, int]],
        recent_lookup: set[tuple[str, int]],
        favorite_lookup: set[tuple[str, int]],
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
        if key in favorite_lookup or bool(seed.get("favorite_seed")):
            lanes.append("favorite_seed")
        if cls._intersect_strings(seed_genres, primary_genres) or cls._intersect_strings(seed_genres, recent_genres):
            lanes.append("genre_anchor_seed")

        return lanes

    @staticmethod
    def _normalize_seed_lookup_title(value: str) -> str:
        return str(value or "").strip().lower()

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
            "favorite_titles": summary.get("favorite_titles", [])[:5],
            "top_genres": summary.get("top_genres", []),
            "ranked_genres": summary.get("ranked_genres", [])[:5],
            "primary_genres": summary.get("primary_genres", []),
            "secondary_genres": summary.get("secondary_genres", []),
            "repeat_titles": summary.get("repeat_titles", [])[:3],
            "recent_momentum": summary.get("recent_momentum", [])[:5],
            "favorite_genres": summary.get("favorite_genres", [])[:5],
            "format_preference": summary.get("format_preference", {}),
            "release_year_preference": summary.get("release_year_preference", {}),
            "discovery_lanes": summary.get("discovery_lanes", []),
            "adjacent_genres": summary.get("adjacent_genres", []),
            "adjacent_themes": summary.get("adjacent_themes", []),
            "seer_adjacent_titles": summary.get("seer_adjacent_titles", [])[:4],
            "seer_adjacent_genres": summary.get("seer_adjacent_genres", [])[:4],
            "similar_users": summary.get("similar_users", [])[:3],
            "similar_user_genres": summary.get("similar_user_genres", [])[:4],
            "similar_user_titles": summary.get("similar_user_titles", [])[:4],
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
        seer_adjacent_genres = cls._normalize_genres(profile_summary.get("seer_adjacent_genres", []), limit=2)
        similar_user_genres = cls._normalize_genres(profile_summary.get("similar_user_genres", []), limit=2)
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
            (seer_adjacent_genres, "seer_genre_seed"),
            (similar_user_genres, "similar_user_genre_seed"),
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
        return cls._build_media_item_match_context(history)["media_keys"]  # type: ignore[return-value]

    @classmethod
    def _build_media_item_match_context(
        cls,
        items: list[dict[str, Any]] | None,
    ) -> dict[str, set[tuple[str, Any]]]:
        media_keys: set[tuple[str, int]] = set()
        external_keys: set[tuple[str, str, str]] = set()
        title_keys: set[tuple[str, str]] = set()

        for item in items or []:
            media_type = cls._map_history_media_type(item.get("Type"))
            if media_type not in {"movie", "tv"}:
                continue

            tmdb_id = cls._extract_tmdb_id(item)
            if tmdb_id is not None:
                media_keys.add((media_type, tmdb_id))

            title_key = cls._history_title_key(item)
            if title_key is not None:
                title_keys.add(title_key)

            external_ids = cls._extract_external_ids(item)
            for provider_key, provider_id in external_ids.items():
                external_keys.add((media_type, str(provider_key), str(provider_id)))

        return {
            "media_keys": media_keys,
            "external_keys": external_keys,
            "title_keys": title_keys,
        }

    @staticmethod
    def _build_library_match_context(session: Session) -> dict[str, set[tuple[str, Any]]]:
        media_keys: set[tuple[str, int]] = set()
        external_keys: set[tuple[str, str, str]] = set()
        title_keys: set[tuple[str, str]] = set()

        rows = session.scalars(
            select(LibraryMedia).where(LibraryMedia.state.in_(("available", "partial", "processing", "pending")))
        )
        for row in rows:
            media_type = str(row.media_type or "").strip()
            if media_type not in {"movie", "tv"}:
                continue

            if row.tmdb_id is not None:
                media_keys.add((media_type, int(row.tmdb_id)))
            for provider_key, provider_id in {
                "tmdb": row.tmdb_id,
                "tvdb": row.tvdb_id,
                "imdb": row.imdb_id,
            }.items():
                if provider_id not in (None, ""):
                    external_keys.add((media_type, provider_key, str(provider_id)))

            title = str(row.title or "").strip().lower()
            if title:
                title_keys.add((media_type, title))

        return {
            "media_keys": media_keys,
            "external_keys": external_keys,
            "title_keys": title_keys,
        }

    @classmethod
    def _build_history_watch_timestamps(
        cls,
        history: list[dict[str, Any]],
    ) -> dict[str, dict[tuple[str, Any], float]]:
        media_keys: dict[tuple[str, int], float] = {}
        title_keys: dict[tuple[str, str], float] = {}

        for item in history:
            media_type = cls._map_history_media_type(item.get("Type"))
            if media_type not in {"movie", "tv"}:
                continue

            last_played_ts = cls._to_timestamp(item.get("UserData", {}).get("LastPlayedDate"))
            if last_played_ts <= 0:
                last_played_ts = cls._to_timestamp(item.get("DatePlayed"))

            tmdb_id = cls._extract_tmdb_id(item)
            if tmdb_id is not None:
                media_key = (media_type, tmdb_id)
                media_keys[media_key] = max(media_keys.get(media_key, 0.0), last_played_ts)

            title_key = cls._history_title_key(item)
            if title_key is not None:
                title_keys[title_key] = max(title_keys.get(title_key, 0.0), last_played_ts)

        return {
            "media_keys": media_keys,
            "title_keys": title_keys,
        }

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
        discovery_lanes = cls._profile_extension_genres(profile_summary, limit=6)
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
        profile_favorite_genres = cls._normalize_genres(profile_summary.get("favorite_genres", []), limit=6)
        profile_theme_hints = cls._normalize_string_list(profile_summary.get("adjacent_themes", []), limit=4)
        request_outcomes = cls._normalize_request_outcome_insights(profile_summary.get("request_outcome_insights", {}))

        matched_primary = cls._intersect_strings(candidate_genres, primary_genres)
        matched_secondary = cls._intersect_strings(candidate_genres, secondary_genres)
        matched_recent = cls._intersect_strings(candidate_genres, recent_genres)
        matched_discovery = cls._intersect_strings(candidate_genres, discovery_lanes)
        matched_keywords = cls._intersect_strings(candidate_keywords, profile_keywords)
        matched_people = cls._intersect_strings(candidate_people, profile_people)
        matched_brands = cls._intersect_strings(candidate_brands, profile_brands)
        matched_favorite_genres = cls._intersect_strings(candidate_genres, profile_favorite_genres)
        theme_matches = cls._match_theme_hints(candidate_keywords, profile_theme_hints)
        collection_match = candidate_collection if candidate_collection and candidate_collection.lower() in {
            value.lower() for value in profile_collections
        } else None
        positive_outcome_genres = cls._intersect_strings(
            candidate_genres,
            cls._normalize_genres(request_outcomes.get("positive_genres", []), limit=5),
        )
        negative_outcome_genres = cls._intersect_strings(
            candidate_genres,
            cls._normalize_genres(request_outcomes.get("negative_genres", []), limit=5),
        )
        positive_outcome_titles = {
            value.lower()
            for value in cls._normalize_string_list(request_outcomes.get("positive_titles", []), limit=8)
        }
        negative_outcome_titles = {
            value.lower()
            for value in cls._normalize_string_list(request_outcomes.get("negative_titles", []), limit=8)
        }
        outcome_title_signal = str(candidate.get("title") or "").strip().lower()
        feedback_block_reason = cls._candidate_feedback_block_reason(candidate, profile_summary)

        source_titles = cls._extract_source_titles(candidate.get("sources", []))
        top_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("top_titles", [])}
        repeat_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("repeat_titles", [])}
        recent_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("recent_momentum", [])}
        favorite_titles = {str(item.get("title") or "").strip().lower() for item in profile_summary.get("favorite_titles", [])}
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
        score_breakdown["favorite_affinity"] = cls._score_favorite_affinity(
            matched_favorite_genres=matched_favorite_genres,
            source_titles=source_titles,
            favorite_titles=favorite_titles,
        )
        score_breakdown["genre_guardrail"] = cls._score_genre_guardrail(
            candidate_genres=candidate_genres,
            matched_primary=matched_primary,
            matched_secondary=matched_secondary,
            matched_recent=matched_recent,
            matched_discovery=matched_discovery,
            primary_genres=primary_genres,
            secondary_genres=secondary_genres,
            recent_genres=recent_genres,
            discovery_lanes=discovery_lanes,
            ranked_genres=profile_summary.get("ranked_genres", []),
            genre_focus_share=profile_summary.get("genre_focus_share"),
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
        score_breakdown["outcome_fit"] = cls._score_request_outcome_fit(
            candidate_title=str(candidate.get("title") or ""),
            candidate_genres=candidate_genres,
            request_outcome_insights=request_outcomes,
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
            "matched_favorite_genres": matched_favorite_genres,
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
                positive_outcome_genres=positive_outcome_genres,
                negative_outcome_genres=negative_outcome_genres,
                positive_title_signal=outcome_title_signal in positive_outcome_titles,
                negative_title_signal=outcome_title_signal in negative_outcome_titles,
            ),
            "feedback_block_reason": feedback_block_reason,
            "positive_outcome_genres": positive_outcome_genres,
            "negative_outcome_genres": negative_outcome_genres,
            "positive_title_signal": outcome_title_signal in positive_outcome_titles,
            "negative_title_signal": outcome_title_signal in negative_outcome_titles,
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

    @classmethod
    def _select_suggestion_ai_candidates(
        cls,
        candidates: list[dict[str, Any]],
        *,
        threshold: float,
        limit: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []

        shortlisted: list[dict[str, Any]] = []
        threshold_value = max(0.0, min(1.0, float(threshold)))
        for candidate in candidates:
            score = float(candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0)
            if score < threshold_value:
                continue
            shortlisted.append(dict(candidate))
            if len(shortlisted) >= limit:
                break
        return shortlisted

    @classmethod
    def _sort_suggestion_candidates(cls, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = [dict(candidate) for candidate in candidates]
        ranked.sort(
            key=lambda item: (
                -float(
                    item.get("recommendation_features", {}).get("hybrid_score")
                    or item.get("recommendation_features", {}).get("final_score")
                    or item.get("recommendation_features", {}).get("deterministic_score")
                    or 0.0
                ),
                -float(item.get("recommendation_features", {}).get("deterministic_score") or 0.0),
                -float(item.get("rating") or 0.0),
                str(item.get("title") or "").lower(),
            )
        )
        return ranked

    @classmethod
    def _filter_suggestion_candidates_for_display(
        cls,
        candidates: list[dict[str, Any]],
        *,
        threshold: float,
    ) -> list[dict[str, Any]]:
        floor = max(0.0, min(1.0, float(threshold)))
        eligible: list[dict[str, Any]] = []
        for candidate in candidates:
            features = candidate.get("recommendation_features", {})
            final_score = float(
                features.get("hybrid_score")
                or features.get("final_score")
                or features.get("deterministic_score")
                or 0.0
            )
            if final_score < floor:
                continue
            eligible.append(dict(candidate))
        return eligible

    @classmethod
    def _finalize_suggestion_candidate(cls, candidate: dict[str, Any]) -> dict[str, Any]:
        features = candidate.setdefault("recommendation_features", {})
        deterministic_score = float(features.get("deterministic_score") or 0.0)
        final_score = float(features.get("hybrid_score") or features.get("final_score") or deterministic_score)
        features["deterministic_score"] = deterministic_score
        features["hybrid_score"] = final_score
        features["final_score"] = final_score
        features["llm_vote"] = str(features.get("llm_vote") or "UNAVAILABLE")
        if features.get("llm_reasoning") is None:
            features["llm_reasoning"] = ""
        return candidate

    def _load_existing_suggestion_ai_cache(self, jellyfin_user_id: str) -> dict[str, dict[str, Any]]:
        with self.session_scope() as session:
            existing_rows = list(
                session.scalars(
                    select(SuggestedMedia).where(SuggestedMedia.jellyfin_user_id == jellyfin_user_id)
                )
            )

        cache: dict[str, dict[str, Any]] = {}
        for row in existing_rows:
            try:
                payload = json.loads(row.payload_json or "{}")
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            features = payload.get("recommendation_features", {}) if isinstance(payload.get("recommendation_features"), dict) else {}
            cache_key = str(features.get("suggestion_ai_cache_key") or "").strip()
            if not cache_key:
                continue
            cache[cache_key] = {
                "llm_vote": str(features.get("llm_vote") or "UNAVAILABLE"),
                "llm_confidence": features.get("llm_confidence"),
                "llm_reasoning": str(features.get("llm_reasoning") or "").strip(),
            }
        return cache

    @classmethod
    def _build_suggestion_ai_cache_key(
        cls,
        candidate: dict[str, Any],
        *,
        profile_payload: dict[str, Any],
        viewing_history: dict[str, Any],
    ) -> str:
        features = candidate.get("recommendation_features", {}) if isinstance(candidate.get("recommendation_features"), dict) else {}
        fingerprint_payload = {
            "prompt_version": "suggestion-ai-v1",
            "profile": {
                "summary_block": str(profile_payload.get("summary_block") or "").strip(),
                "primary_genres": profile_payload.get("primary_genres", []),
                "secondary_genres": profile_payload.get("secondary_genres", []),
                "recent_genres": profile_payload.get("recent_genres", []),
                "adjacent_genres": profile_payload.get("adjacent_genres", []),
                "adjacent_themes": profile_payload.get("adjacent_themes", []),
                "seer_adjacent_titles": profile_payload.get("seer_adjacent_titles", []),
                "seer_adjacent_genres": profile_payload.get("seer_adjacent_genres", []),
                "similar_users": profile_payload.get("similar_users", []),
                "similar_user_genres": profile_payload.get("similar_user_genres", []),
                "similar_user_titles": profile_payload.get("similar_user_titles", []),
                "repeat_titles": profile_payload.get("repeat_titles", []),
                "recent_momentum": profile_payload.get("recent_momentum", []),
                "format_preference": profile_payload.get("format_preference", {}),
                "release_year_preference": profile_payload.get("release_year_preference", {}),
            },
            "viewing_history": {
                "recent_plays": viewing_history.get("recent_plays", []),
                "top_titles": viewing_history.get("top_titles", []),
                "recent_momentum": viewing_history.get("recent_momentum", []),
                "repeat_titles": viewing_history.get("repeat_titles", []),
                "primary_genres": viewing_history.get("primary_genres", []),
                "seer_adjacent_titles": viewing_history.get("seer_adjacent_titles", []),
                "seer_adjacent_genres": viewing_history.get("seer_adjacent_genres", []),
                "similar_users": viewing_history.get("similar_users", []),
                "similar_user_genres": viewing_history.get("similar_user_genres", []),
                "similar_user_titles": viewing_history.get("similar_user_titles", []),
                "top_keywords": viewing_history.get("top_keywords", []),
                "favorite_people": viewing_history.get("favorite_people", []),
                "preferred_brands": viewing_history.get("preferred_brands", []),
                "favorite_collections": viewing_history.get("favorite_collections", []),
            },
            "candidate": {
                "media_type": candidate.get("media_type"),
                "media_id": candidate.get("media_id"),
                "title": candidate.get("title"),
                "overview": candidate.get("overview"),
                "genres": candidate.get("genres", []),
                "rating": candidate.get("rating"),
                "release_date": candidate.get("release_date"),
                "sources": candidate.get("sources", []),
                "media_info": candidate.get("media_info", {}),
                "external_ids": candidate.get("external_ids", {}),
                "tmdb_details": candidate.get("tmdb_details", {}),
                "recommendation_features": {
                    "analysis_summary": features.get("analysis_summary"),
                    "deterministic_score": float(features.get("deterministic_score") or 0.0),
                    "score_breakdown": features.get("score_breakdown", {}),
                    "lane_tags": features.get("lane_tags", []),
                    "matched_keywords": features.get("matched_keywords", []),
                    "matched_people": features.get("matched_people", []),
                    "matched_brands": features.get("matched_brands", []),
                    "collection_match": features.get("collection_match"),
                },
            },
        }
        return cls._stable_json_fingerprint(fingerprint_payload)

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

    @classmethod
    def _blend_suggestion_confidences(
        cls,
        *,
        deterministic_score: float,
        llm_confidence: float | None,
        llm_vote: str,
        llm_weight_percent: int,
    ) -> float:
        request_vote = "UNAVAILABLE"
        if llm_vote == "RECOMMEND":
            request_vote = "REQUEST"
        elif llm_vote == "PASS":
            request_vote = "IGNORE"
        return cls._blend_confidences(
            deterministic_score=deterministic_score,
            llm_confidence=llm_confidence,
            llm_vote=request_vote,
            llm_weight_percent=llm_weight_percent,
        )

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
            f"feedback {float(breakdown.get('feedback_fit', 0.0)):.2f}, "
            f"outcomes {float(breakdown.get('outcome_fit', 0.0)):.2f}, "
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

    @classmethod
    def _compose_suggestion_reasoning(cls, candidate: dict[str, Any]) -> str:
        features = candidate.get("recommendation_features", {})
        breakdown = features.get("score_breakdown", {})
        deterministic_score = float(features.get("deterministic_score") or 0.0)
        hybrid_score = float(features.get("hybrid_score") or features.get("final_score") or deterministic_score)
        summary = str(features.get("analysis_summary") or "Limited alignment signals.").strip()
        reasoning = (
            f"Suggestion score {hybrid_score:.2f}. Code score {deterministic_score:.2f}. {summary} "
            f"Breakdown: source {float(breakdown.get('source_affinity', 0.0)):.2f}, "
            f"genres {float(breakdown.get('genre_affinity', 0.0)):.2f}, "
            f"format {float(breakdown.get('format_fit', 0.0)):.2f}, "
            f"freshness {float(breakdown.get('freshness_fit', 0.0)):.2f}, "
            f"quality {float(breakdown.get('quality', 0.0)):.2f}, "
            f"feedback {float(breakdown.get('feedback_fit', 0.0)):.2f}, "
            f"outcomes {float(breakdown.get('outcome_fit', 0.0)):.2f}, "
            f"themes {float(breakdown.get('tmdb_themes', 0.0)):.2f}, "
            f"people {float(breakdown.get('tmdb_people', 0.0)):.2f}, "
            f"brands {float(breakdown.get('tmdb_brands', 0.0)):.2f}."
        )
        llm_vote = str(features.get("llm_vote") or "UNAVAILABLE")
        llm_reasoning = str(features.get("llm_reasoning") or "").strip()
        if llm_vote == "UNAVAILABLE":
            return reasoning + " AI vote unavailable, so the shelf order used the code-driven score only."
        if llm_reasoning:
            return reasoning + f" AI vote: {llm_vote}. {llm_reasoning}"
        return reasoning + f" AI vote: {llm_vote}."

    async def _suggest_profile_enrichment(
        self,
        username: str,
        history_summary: dict[str, Any],
        *,
        existing_payload: dict[str, Any] | None = None,
    ) -> dict[str, list[str]]:
        if not self.settings.profile_llm_enrichment_enabled:
            return {}
        if int(history_summary.get("history_count") or 0) == 0:
            return {}

        prompt_summary = dict(history_summary)
        existing = existing_payload if isinstance(existing_payload, dict) else {}
        prompt_summary["explicit_feedback"] = self._normalize_explicit_feedback(existing.get("explicit_feedback", {}))
        prompt_summary["profile_exclusions"] = self._normalize_string_list(existing.get("profile_exclusions", []), limit=8)
        prompt_summary["operator_notes"] = str(existing.get("operator_notes") or "").strip()
        prompt_summary["adjacent_genres"] = self._normalize_string_list(existing.get("adjacent_genres", []), limit=4)
        prompt_summary["adjacent_themes"] = self._normalize_string_list(existing.get("adjacent_themes", []), limit=3)

        try:
            payload = await self.llm.generate_json(
                messages=build_profile_enrichment_messages(username, prompt_summary),
                temperature=0.1,
                purpose="profile_enrichment",
            )
        except Exception as exc:
            logger.warning("Profile enrichment skipped for user=%s reason=%s", username, exc)
            return {}

        blocked_genres = self._blocked_profile_genres(
            prompt_summary,
            extra_genres=prompt_summary.get("adjacent_genres", []),
        )
        adjacent_genres: list[str] = []
        for raw in payload.get("adjacent_genres", []):
            value = str(raw).strip()
            if value and value.lower() not in blocked_genres:
                adjacent_genres.append(value)

        existing_adjacent_themes = {
            value.lower()
            for value in self._normalize_string_list(prompt_summary.get("adjacent_themes", []), limit=3)
        }
        adjacent_themes = [
            str(raw).strip()
            for raw in payload.get("adjacent_themes", [])
            if str(raw).strip() and str(raw).strip().lower() not in existing_adjacent_themes
        ]
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
            tmdb_id = None
            external_ids = item.get("external_ids", {}) if isinstance(item.get("external_ids"), dict) else {}
            if external_ids.get("tmdb") not in (None, ""):
                tmdb_id = self._coerce_int(external_ids.get("tmdb"))
            elif item.get("media_id") is not None:
                tmdb_id = self._coerce_int(item.get("media_id"))

            if media_type not in {"movie", "tv"} or tmdb_id is None:
                continue
            if isinstance(item.get("tmdb_details"), dict) and item.get("tmdb_details"):
                continue
            targets.append((index, media_type, int(tmdb_id)))

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
        payload["favorite_titles"] = cls._normalize_profile_entries(payload.get("favorite_titles", []), limit=6)
        payload["top_genres"] = cls._normalize_string_list(payload.get("top_genres", []), limit=8)
        payload["primary_genres"] = cls._normalize_string_list(payload.get("primary_genres", []), limit=4)
        payload["secondary_genres"] = cls._normalize_string_list(payload.get("secondary_genres", []), limit=4)
        payload["recent_genres"] = cls._normalize_string_list(payload.get("recent_genres", []), limit=4)
        payload["favorite_genres"] = cls._normalize_string_list(payload.get("favorite_genres", []), limit=6)
        payload["favorite_signal_count"] = max(0, int(payload.get("favorite_signal_count") or 0))
        payload["ranked_genres"] = cls._normalize_ranked_genres(payload.get("ranked_genres", []), limit=8)
        payload["discovery_lanes"] = cls._normalize_string_list(payload.get("discovery_lanes", []), limit=4)
        payload["top_keywords"] = cls._normalize_string_list(payload.get("top_keywords", []), limit=8)
        payload["favorite_people"] = cls._normalize_string_list(payload.get("favorite_people", []), limit=6)
        payload["preferred_brands"] = cls._normalize_string_list(payload.get("preferred_brands", []), limit=6)
        payload["favorite_collections"] = cls._normalize_string_list(payload.get("favorite_collections", []), limit=4)
        payload["seer_adjacent_titles"] = cls._normalize_string_list(payload.get("seer_adjacent_titles", []), limit=4)
        payload["seer_adjacent_genres"] = cls._normalize_string_list(payload.get("seer_adjacent_genres", []), limit=4)
        payload["similar_users"] = cls._normalize_string_list(payload.get("similar_users", []), limit=3)
        payload["similar_user_genres"] = cls._normalize_string_list(payload.get("similar_user_genres", []), limit=4)
        payload["similar_user_titles"] = cls._normalize_string_list(payload.get("similar_user_titles", []), limit=4)
        if enrichment is None or enrichment == {}:
            payload["adjacent_genres"] = cls._normalize_string_list(existing.get("adjacent_genres", []), limit=4)
            payload["adjacent_themes"] = cls._normalize_string_list(existing.get("adjacent_themes", []), limit=3)
        else:
            payload["adjacent_genres"] = cls._normalize_string_list(
                (enrichment or {}).get("adjacent_genres", []),
                limit=4,
            )
            payload["adjacent_themes"] = cls._normalize_string_list(
                (enrichment or {}).get("adjacent_themes", []),
                limit=3,
            )
        payload["seed_lanes"] = cls._build_profile_seed_lanes(payload)
        payload["format_preference"] = cls._normalize_format_preference(payload.get("format_preference", {}))
        payload["release_year_preference"] = cls._normalize_release_year_preference(
            payload.get("release_year_preference", {})
        )
        payload["explicit_feedback"] = cls._normalize_explicit_feedback(existing.get("explicit_feedback", {}))
        payload["blocked_titles"] = cls._normalize_string_list(existing.get("blocked_titles", []), limit=12)
        payload["profile_exclusions"] = cls._normalize_string_list(existing.get("profile_exclusions", []), limit=8)
        payload["operator_notes"] = str(existing.get("operator_notes") or "").strip()
        payload["enabled"] = cls._normalize_profile_enabled(existing.get("enabled", True))
        payload["summary_block"] = (
            cls._limit_words(cls._render_profile_block(username, payload), max_words=500)
            if cls._has_profile_signal(payload)
            else ProfileStore.default_block(username)
        )
        payload["request_outcome_insights"] = cls._normalize_request_outcome_insights(
            existing.get("request_outcome_insights", {})
        )
        payload["profile_review"] = cls._build_profile_review(
            payload,
            previous_payload=existing if isinstance(existing, dict) else None,
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
        normalized["enabled"] = cls._normalize_profile_enabled(normalized.get("enabled", True))
        normalized["history_count"] = max(0, int(normalized.get("history_count") or 0))
        normalized["unique_titles"] = max(0, int(normalized.get("unique_titles") or 0))
        normalized["average_top_rating"] = cls._coerce_optional_number(normalized.get("average_top_rating"))
        normalized["genre_focus_share"] = max(0.0, min(1.0, float(normalized.get("genre_focus_share") or 0.0)))
        normalized["top_titles"] = cls._normalize_profile_entries(normalized.get("top_titles", []), limit=8)
        normalized["repeat_titles"] = cls._normalize_profile_entries(normalized.get("repeat_titles", []), limit=5)
        normalized["recent_momentum"] = cls._normalize_profile_entries(normalized.get("recent_momentum", []), limit=5)
        normalized["favorite_titles"] = cls._normalize_profile_entries(normalized.get("favorite_titles", []), limit=6)
        normalized["top_genres"] = cls._normalize_string_list(normalized.get("top_genres", []), limit=8)
        normalized["primary_genres"] = cls._normalize_string_list(normalized.get("primary_genres", []), limit=4)
        normalized["secondary_genres"] = cls._normalize_string_list(normalized.get("secondary_genres", []), limit=4)
        normalized["recent_genres"] = cls._normalize_string_list(normalized.get("recent_genres", []), limit=4)
        normalized["favorite_genres"] = cls._normalize_string_list(normalized.get("favorite_genres", []), limit=6)
        normalized["favorite_signal_count"] = max(0, int(normalized.get("favorite_signal_count") or 0))
        normalized["ranked_genres"] = cls._normalize_ranked_genres(normalized.get("ranked_genres", []), limit=8)
        normalized["discovery_lanes"] = cls._normalize_string_list(normalized.get("discovery_lanes", []), limit=4)
        normalized["top_keywords"] = cls._normalize_string_list(normalized.get("top_keywords", []), limit=8)
        normalized["favorite_people"] = cls._normalize_string_list(normalized.get("favorite_people", []), limit=6)
        normalized["preferred_brands"] = cls._normalize_string_list(normalized.get("preferred_brands", []), limit=6)
        normalized["favorite_collections"] = cls._normalize_string_list(
            normalized.get("favorite_collections", []),
            limit=4,
        )
        normalized["seer_adjacent_titles"] = cls._normalize_string_list(normalized.get("seer_adjacent_titles", []), limit=4)
        normalized["seer_adjacent_genres"] = cls._normalize_string_list(normalized.get("seer_adjacent_genres", []), limit=4)
        normalized["similar_users"] = cls._normalize_string_list(normalized.get("similar_users", []), limit=3)
        normalized["similar_user_genres"] = cls._normalize_string_list(normalized.get("similar_user_genres", []), limit=4)
        normalized["similar_user_titles"] = cls._normalize_string_list(normalized.get("similar_user_titles", []), limit=4)
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
        normalized["blocked_titles"] = cls._normalize_string_list(normalized.get("blocked_titles", []), limit=12)
        normalized["profile_exclusions"] = cls._normalize_string_list(normalized.get("profile_exclusions", []), limit=8)
        normalized["operator_notes"] = str(normalized.get("operator_notes") or "").strip()
        normalized["profile_state"] = "ready" if cls._has_profile_signal(normalized) else "default"
        normalized["summary_block"] = (
            cls._limit_words(cls._render_profile_block(username, normalized), max_words=500)
            if cls._has_profile_signal(normalized)
            else ProfileStore.default_block(username)
        )
        normalized["request_outcome_insights"] = cls._normalize_request_outcome_insights(
            normalized.get("request_outcome_insights", {})
        )
        normalized["profile_review"] = cls._normalize_profile_review(normalized.get("profile_review", {}))
        return normalized

    @staticmethod
    def _normalize_profile_enabled(raw_value: Any) -> bool:
        if isinstance(raw_value, bool):
            return raw_value
        if isinstance(raw_value, (int, float)):
            return bool(raw_value)
        normalized = str(raw_value or "").strip().lower()
        if normalized in {"false", "0", "off", "no", "disabled"}:
            return False
        if normalized in {"true", "1", "on", "yes", "enabled"}:
            return True
        return True

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
        if profile_summary.get("favorite_titles"):
            lanes.append("favorite_seed")
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

    @classmethod
    def _normalize_request_outcome_label(cls, value: Any) -> str:
        normalized = str(value or "").strip().lower().replace(" ", "_")
        aliases = {
            "approve": "approved",
            "approved": "approved",
            "deny": "denied",
            "declined": "denied",
            "denied": "denied",
            "ignore": "ignored",
            "ignored": "ignored",
            "unavailable": "unavailable",
            "failed": "unavailable",
            "downloaded": "downloaded",
            "available": "downloaded",
            "watched": "watched",
        }
        return aliases.get(normalized, normalized)

    @classmethod
    def _normalize_request_outcome_insights(cls, raw_value: Any) -> dict[str, Any]:
        value = raw_value if isinstance(raw_value, dict) else {}
        raw_counts = value.get("counts", {}) if isinstance(value.get("counts"), dict) else {}
        counts: dict[str, int] = {}
        for raw_key, raw_count in raw_counts.items():
            key = cls._normalize_request_outcome_label(raw_key)
            if not key:
                continue
            try:
                counts[key] = max(0, int(raw_count or 0))
            except (TypeError, ValueError):
                continue

        recent_outcomes: list[dict[str, Any]] = []
        raw_recent = value.get("recent_outcomes", [])
        if isinstance(raw_recent, list):
            for item in raw_recent:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title") or "").strip()
                outcome = cls._normalize_request_outcome_label(item.get("outcome"))
                if not title or not outcome:
                    continue
                recent_outcomes.append(
                    {
                        "title": title,
                        "outcome": outcome,
                        "created_at": str(item.get("created_at") or "").strip() or None,
                        "source": str(item.get("source") or "").strip() or None,
                    }
                )
                if len(recent_outcomes) >= 8:
                    break

        return {
            "counts": counts,
            "positive_titles": cls._normalize_string_list(value.get("positive_titles", []), limit=6),
            "negative_titles": cls._normalize_string_list(value.get("negative_titles", []), limit=6),
            "positive_genres": cls._normalize_string_list(value.get("positive_genres", []), limit=6),
            "negative_genres": cls._normalize_string_list(value.get("negative_genres", []), limit=6),
            "recent_outcomes": recent_outcomes,
        }

    @classmethod
    def _normalize_profile_review(cls, raw_value: Any) -> dict[str, Any]:
        value = raw_value if isinstance(raw_value, dict) else {}
        health_status = str(value.get("health_status") or "unknown").strip().lower()
        if health_status not in {"unknown", "weak", "watch", "healthy", "strong"}:
            health_status = "unknown"

        confidence = str(value.get("confidence") or "low").strip().lower()
        if confidence not in {"low", "watch", "medium", "high"}:
            confidence = "low"

        freshness = str(value.get("freshness") or "unknown").strip().lower()
        if freshness not in {"unknown", "fresh", "aging", "stale"}:
            freshness = "unknown"

        evidence_raw = value.get("evidence", {}) if isinstance(value.get("evidence"), dict) else {}
        evidence: dict[str, int] = {}
        for key, raw_count in evidence_raw.items():
            try:
                evidence[str(key)] = max(0, int(raw_count or 0))
            except (TypeError, ValueError):
                continue

        return {
            "health_score": max(0, min(100, int(value.get("health_score") or 0))),
            "health_status": health_status,
            "confidence": confidence,
            "freshness": freshness,
            "warnings": cls._normalize_string_list(value.get("warnings", []), limit=8),
            "strengths": cls._normalize_string_list(value.get("strengths", []), limit=8),
            "changed_fields": cls._normalize_string_list(value.get("changed_fields", []), limit=12),
            "diff_summary": cls._normalize_string_list(value.get("diff_summary", []), limit=12),
            "evidence": evidence,
            "summary": str(value.get("summary") or "").strip(),
            "last_reviewed_at": str(value.get("last_reviewed_at") or "").strip() or None,
        }

    @classmethod
    def _profile_diff_titles(cls, raw_items: Any, *, limit: int) -> list[str]:
        return [
            str(item.get("title") or "").strip()
            for item in cls._normalize_profile_entries(raw_items, limit=limit)
            if str(item.get("title") or "").strip()
        ]

    @classmethod
    def _profile_diff_strings(
        cls,
        previous_items: list[str],
        current_items: list[str],
    ) -> tuple[list[str], list[str]]:
        previous_lookup = {value.lower(): value for value in previous_items}
        current_lookup = {value.lower(): value for value in current_items}
        added = [current_lookup[key] for key in current_lookup.keys() - previous_lookup.keys()]
        removed = [previous_lookup[key] for key in previous_lookup.keys() - current_lookup.keys()]
        added.sort(key=str.lower)
        removed.sort(key=str.lower)
        return added, removed

    @classmethod
    def _build_profile_diff_summary(
        cls,
        previous_payload: dict[str, Any] | None,
        current_payload: dict[str, Any],
    ) -> tuple[list[str], list[str]]:
        if not isinstance(previous_payload, dict) or not ProfileStore.is_structured_payload(previous_payload):
            return [], []

        comparisons = (
            ("Primary genres", cls._normalize_string_list(previous_payload.get("primary_genres", []), limit=6), cls._normalize_string_list(current_payload.get("primary_genres", []), limit=6)),
            ("Secondary genres", cls._normalize_string_list(previous_payload.get("secondary_genres", []), limit=6), cls._normalize_string_list(current_payload.get("secondary_genres", []), limit=6)),
            ("Recent genres", cls._normalize_string_list(previous_payload.get("recent_genres", []), limit=6), cls._normalize_string_list(current_payload.get("recent_genres", []), limit=6)),
            ("Discovery lanes", cls._normalize_string_list(previous_payload.get("discovery_lanes", []), limit=6), cls._normalize_string_list(current_payload.get("discovery_lanes", []), limit=6)),
            ("Adjacent genres", cls._normalize_string_list(previous_payload.get("adjacent_genres", []), limit=6), cls._normalize_string_list(current_payload.get("adjacent_genres", []), limit=6)),
            ("Seer lift", cls._normalize_string_list(previous_payload.get("seer_adjacent_genres", []), limit=6), cls._normalize_string_list(current_payload.get("seer_adjacent_genres", []), limit=6)),
            ("Similar-user lift", cls._normalize_string_list(previous_payload.get("similar_user_genres", []), limit=6), cls._normalize_string_list(current_payload.get("similar_user_genres", []), limit=6)),
            ("Top titles", cls._profile_diff_titles(previous_payload.get("top_titles", []), limit=5), cls._profile_diff_titles(current_payload.get("top_titles", []), limit=5)),
            ("Favorite titles", cls._profile_diff_titles(previous_payload.get("favorite_titles", []), limit=5), cls._profile_diff_titles(current_payload.get("favorite_titles", []), limit=5)),
            ("Recent momentum", cls._profile_diff_titles(previous_payload.get("recent_momentum", []), limit=5), cls._profile_diff_titles(current_payload.get("recent_momentum", []), limit=5)),
            ("Favorite genres", cls._normalize_string_list(previous_payload.get("favorite_genres", []), limit=6), cls._normalize_string_list(current_payload.get("favorite_genres", []), limit=6)),
            ("TMDb keywords", cls._normalize_string_list(previous_payload.get("top_keywords", []), limit=6), cls._normalize_string_list(current_payload.get("top_keywords", []), limit=6)),
            ("Favorite people", cls._normalize_string_list(previous_payload.get("favorite_people", []), limit=6), cls._normalize_string_list(current_payload.get("favorite_people", []), limit=6)),
        )

        changed_fields: list[str] = []
        diff_summary: list[str] = []
        for label, previous_items, current_items in comparisons:
            added, removed = cls._profile_diff_strings(previous_items, current_items)
            if not added and not removed:
                continue
            changed_fields.append(label)
            parts: list[str] = []
            if added:
                parts.append(f"+{cls._human_join(added[:3])}")
            if removed:
                parts.append(f"-{cls._human_join(removed[:3])}")
            diff_summary.append(f"{label}: {'; '.join(parts)}")
            if len(diff_summary) >= 8:
                break
        return changed_fields, diff_summary

    @staticmethod
    def _days_since_iso_timestamp(value: Any) -> int | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        delta = datetime.utcnow() - parsed
        return max(0, int(delta.total_seconds() // 86400))

    @classmethod
    def _build_profile_review(
        cls,
        payload: dict[str, Any],
        *,
        previous_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        history_count = max(0, int(payload.get("history_count") or 0))
        unique_titles = max(0, int(payload.get("unique_titles") or 0))
        favorite_signal_count = max(0, int(payload.get("favorite_signal_count") or 0))
        tmdb_signal_count = sum(
            1
            for key in ("top_keywords", "favorite_people", "preferred_brands", "favorite_collections")
            if payload.get(key)
        )
        seer_signal_count = len(cls._normalize_string_list(payload.get("seer_adjacent_titles", []), limit=6)) + len(
            cls._normalize_string_list(payload.get("seer_adjacent_genres", []), limit=6)
        )
        similar_signal_count = len(cls._normalize_string_list(payload.get("similar_users", []), limit=4))
        outcome_insights = cls._normalize_request_outcome_insights(payload.get("request_outcome_insights", {}))
        outcome_count = sum(int(value) for value in outcome_insights.get("counts", {}).values())
        days_old = cls._days_since_iso_timestamp(payload.get("generated_at"))

        score = 0
        score += min(34, history_count * 2)
        score += min(18, unique_titles * 2)
        if favorite_signal_count:
            score += min(8, favorite_signal_count * 2)
        if payload.get("primary_genres"):
            score += 10
        if payload.get("recent_momentum"):
            score += 10
        if tmdb_signal_count:
            score += 8 + min(8, tmdb_signal_count * 2)
        if seer_signal_count:
            score += 8
        if similar_signal_count:
            score += 6
        if outcome_count:
            score += min(6, outcome_count)
        if days_old is not None and days_old > 21:
            score -= 10
        elif days_old is not None and days_old > 7:
            score -= 4
        score = max(0, min(100, score))

        if score >= 80:
            health_status = "strong"
            confidence = "high"
        elif score >= 62:
            health_status = "healthy"
            confidence = "medium"
        elif score >= 42:
            health_status = "watch"
            confidence = "watch"
        else:
            health_status = "weak"
            confidence = "low"

        if days_old is None:
            freshness = "unknown"
        elif days_old <= 7:
            freshness = "fresh"
        elif days_old <= 21:
            freshness = "aging"
        else:
            freshness = "stale"

        warnings: list[str] = []
        if history_count == 0:
            if favorite_signal_count:
                warnings.append("No playback history has been captured yet, so the profile is leaning on Jellyfin favorites until viewing data grows.")
            else:
                warnings.append("No playback history has been captured for this profile yet.")
        elif history_count < 10:
            warnings.append("This profile is still sparse and may overreact to a small sample.")
        if unique_titles < 5 and history_count > 0:
            warnings.append("Title diversity is low, so a single binge can still tilt the profile.")
        if freshness == "stale":
            warnings.append(f"This profile has not been rebuilt in {days_old} days.")
        elif freshness == "aging":
            warnings.append(f"This profile is aging and was last rebuilt {days_old} days ago.")
        if tmdb_signal_count == 0:
            warnings.append("TMDb enrichment is missing, so theme and talent matching is thin.")
        if seer_signal_count == 0:
            warnings.append("Seer neighborhood lift is missing, so discovery is relying on profile-only signals.")
        if not payload.get("recent_momentum"):
            warnings.append("Recent momentum is empty, so short-term taste may lag behind current viewing.")

        strengths: list[str] = []
        if history_count >= 20:
            strengths.append("Durable playback history gives this profile a solid long-term base.")
        if favorite_signal_count:
            strengths.append("Jellyfin favorites are reinforcing durable preference signals.")
        if payload.get("recent_momentum"):
            strengths.append("Recent momentum is captured, so current taste still has a live voice.")
        if tmdb_signal_count:
            strengths.append("TMDb enrichment is active across themes, people, brands, or collections.")
        if seer_signal_count:
            strengths.append("Seer recommendation neighborhoods are adding adjacent lanes.")
        if similar_signal_count:
            strengths.append("Local similar-user lift is contributing collaborative hints.")
        if outcome_count:
            strengths.append("Past request outcomes are being tracked for review and future tuning.")

        changed_fields, diff_summary = cls._build_profile_diff_summary(previous_payload, payload)
        if not diff_summary and not warnings and strengths:
            diff_summary = [strengths[0]]

        summary_parts: list[str] = [f"Profile health {score}/100 ({health_status})."]
        if warnings:
            summary_parts.append(warnings[0])
        elif strengths:
            summary_parts.append(strengths[0])

        return cls._normalize_profile_review(
            {
                "health_score": score,
                "health_status": health_status,
                "confidence": confidence,
                "freshness": freshness,
                "warnings": warnings,
                "strengths": strengths,
                "changed_fields": changed_fields,
                "diff_summary": diff_summary,
                "evidence": {
                    "history_items": history_count,
                    "unique_titles": unique_titles,
                    "favorite_signals": favorite_signal_count,
                    "tmdb_signals": tmdb_signal_count,
                    "seer_signals": seer_signal_count,
                    "similar_users": similar_signal_count,
                    "outcomes": outcome_count,
                },
                "summary": " ".join(summary_parts),
                "last_reviewed_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            }
        )

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
                bool(payload.get("favorite_titles")),
                bool(payload.get("primary_genres")),
                bool(payload.get("favorite_genres")),
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
        if not cls._has_profile_signal(history_summary):
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
        favorite_titles = history_summary.get("favorite_titles", [])
        favorite_genres = cls._normalize_string_list(history_summary.get("favorite_genres", []), limit=4)
        favorite_collections = history_summary.get("favorite_collections", [])
        history_count = int(history_summary.get("history_count") or 0)
        unique_titles = int(history_summary.get("unique_titles") or 0)
        favorite_signal_count = int(history_summary.get("favorite_signal_count") or 0)
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
        if favorite_titles:
            lines.append(f"Jellyfin favorites reinforce titles like {cls._format_title_entries(favorite_titles[:3])}.")
        if favorite_genres:
            lines.append(f"Favorite-tagged genre pull leans toward {cls._human_join(favorite_genres[:3])}.")
        if favorite_collections:
            lines.append(f"Recurring franchise pull: {cls._human_join(favorite_collections[:2])}.")
        if history_count == 0 and favorite_signal_count > 0:
            lines.append(f"Current core is favorite-led, using {favorite_signal_count} Jellyfin favorite title signals before playback history grows.")

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
        favorite_titles = cls._normalize_profile_entries(history_summary.get("favorite_titles", []), limit=3)
        favorite_genres = cls._normalize_string_list(history_summary.get("favorite_genres", []), limit=3)
        top_keywords = cls._normalize_string_list(history_summary.get("top_keywords", []), limit=4)
        favorite_people = cls._normalize_string_list(history_summary.get("favorite_people", []), limit=3)
        preferred_brands = cls._normalize_string_list(history_summary.get("preferred_brands", []), limit=3)
        seer_adjacent_titles = cls._normalize_string_list(history_summary.get("seer_adjacent_titles", []), limit=3)
        similar_users = cls._normalize_string_list(history_summary.get("similar_users", []), limit=2)
        similar_user_titles = cls._normalize_string_list(history_summary.get("similar_user_titles", []), limit=3)

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

        if favorite_titles or favorite_genres:
            favorite_parts: list[str] = []
            if favorite_titles:
                favorite_parts.append(f"Jellyfin favorites include {cls._format_title_entries(favorite_titles[:2])}")
            if favorite_genres:
                favorite_parts.append(f"those favorites cluster around {cls._human_join(favorite_genres)}")
            lines.append(f"{' and '.join(favorite_parts)}.")

        if seer_adjacent_titles:
            lines.append(f"Seer recommendation neighborhoods keep clustering around {cls._human_join(seer_adjacent_titles)}.")
        if similar_users and similar_user_titles:
            lines.append(
                f"Local overlap with profiles like {cls._human_join(similar_users)} also reinforces titles near {cls._human_join(similar_user_titles)}."
            )
        elif similar_users:
            lines.append(f"Local overlap is strongest with profiles like {cls._human_join(similar_users)}.")

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
        favorite_titles = cls._normalize_profile_entries(history_summary.get("favorite_titles", []), limit=2)
        seer_adjacent_genres = cls._normalize_string_list(history_summary.get("seer_adjacent_genres", []), limit=3)
        similar_users = cls._normalize_string_list(history_summary.get("similar_users", []), limit=2)
        similar_user_genres = cls._normalize_string_list(history_summary.get("similar_user_genres", []), limit=3)
        preferred = str(format_preference.get("preferred") or "balanced")
        lines: list[str] = []

        if primary_genres:
            lines.append(
                f"Favor candidates that match {cls._human_join(primary_genres[:3])} and connect to anchor titles or repeat-watch neighborhoods."
            )
        if favorite_titles:
            lines.append(
                f"Treat Jellyfin favorites like {cls._format_title_entries(favorite_titles)} as durable positive evidence after watch history."
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

        if seer_adjacent_genres:
            lines.append(
                f"Let Seer neighborhood signals around {cls._human_join(seer_adjacent_genres)} act as a secondary tiebreaker after the personal profile."
            )
        if similar_users and similar_user_genres:
            lines.append(
                f"Use local similar-user lift from {cls._human_join(similar_users)} only as a supporting boost toward {cls._human_join(similar_user_genres)}."
            )

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
        favorite_titles = history_summary.get("favorite_titles", [])
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

        if favorite_titles:
            return f"Favorite-led; Jellyfin favorites cluster around {cls._format_title_entries(favorite_titles[:2])}."

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

    @classmethod
    def _apply_existing_profile_guidance(
        cls,
        history_summary: dict[str, Any],
        existing_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        summary = dict(history_summary)
        existing = existing_payload if isinstance(existing_payload, dict) else {}
        summary["explicit_feedback"] = cls._normalize_explicit_feedback(existing.get("explicit_feedback", {}))
        summary["profile_exclusions"] = cls._normalize_string_list(existing.get("profile_exclusions", []), limit=8)
        summary["operator_notes"] = str(existing.get("operator_notes") or "").strip()
        return summary

    async def _compose_profile_payload(
        self,
        username: str,
        history: list[dict[str, Any]],
        *,
        favorite_items: list[dict[str, Any]] | None = None,
        existing_payload: dict[str, Any] | None = None,
        peer_payload_overrides: dict[str, dict[str, Any]] | None = None,
        include_llm_enrichment: bool = True,
        progress_callback: Callable[[str, int, str], None] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        def emit_progress(summary_text: str, step: int, phase: str) -> None:
            if progress_callback is not None:
                progress_callback(summary_text, step, phase)

        stored_payload = existing_payload if isinstance(existing_payload, dict) else self.profile_store.read_payload(username)
        emit_progress(f"Building core playback profile for {username}.", 1, "profile_history")
        history_summary = self._build_profile_history_context(
            history,
            favorite_items=favorite_items,
            top_limit=self.settings.profile_architect_top_titles_limit,
            recent_limit=self.settings.profile_architect_recent_momentum_limit,
            recent_weight_percent=self.settings.profile_recent_signal_weight_percent,
        )
        history_summary = self._apply_existing_profile_guidance(history_summary, stored_payload)
        recommendation_seeds = self._build_recommendation_seed_pool(
            history,
            favorite_items=favorite_items,
            profile_summary=history_summary,
            limit=self.settings.recommendation_seed_limit,
        )
        recommendation_seeds = self._resolve_tv_seed_media_ids_from_library_index(recommendation_seeds)
        emit_progress(f"Mapping Seer neighborhoods for {username}.", 2, "seer_enrichment")
        history_summary = await self._enrich_profile_summary_with_seer(
            history_summary,
            recommendation_seeds=recommendation_seeds,
        )
        emit_progress(f"Blending local similar-user lift for {username}.", 3, "similar_user_enrichment")
        history_summary = self._enrich_profile_summary_with_similar_users(
            username,
            history_summary,
            peer_payload_overrides=peer_payload_overrides,
        )
        emit_progress(f"Enriching TMDb metadata for {username}.", 4, "tmdb_enrichment")
        history_summary = await self._enrich_profile_summary_with_tmdb(
            history_summary,
            recommendation_seeds=recommendation_seeds,
        )
        emit_progress(f"Finalizing profile manifest for {username}.", 5, "profile_finalize")
        heuristic_enrichment = self._build_heuristic_profile_enrichment(history_summary)
        llm_enrichment = (
            await self._suggest_profile_enrichment(
                username,
                history_summary,
                existing_payload=stored_payload,
            )
            if include_llm_enrichment
            else {}
        )
        profile_payload = self._build_profile_payload(
            username,
            history_summary,
            enrichment=self._merge_profile_enrichment_layers(heuristic_enrichment, llm_enrichment),
            existing_payload=stored_payload,
        )
        return profile_payload, recommendation_seeds

    async def _prepare_runtime_profile_payload(
        self,
        username: str,
        history: list[dict[str, Any]],
        *,
        favorite_items: list[dict[str, Any]] | None = None,
        existing_payload: dict[str, Any] | None = None,
        peer_payload_overrides: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
        stored_payload = existing_payload if isinstance(existing_payload, dict) else self.profile_store.read_payload(username)
        if ProfileStore.is_structured_payload(stored_payload):
            history_summary = self._build_profile_history_context(
                history,
                favorite_items=favorite_items,
                top_limit=self.settings.profile_architect_top_titles_limit,
                recent_limit=self.settings.profile_architect_recent_momentum_limit,
                recent_weight_percent=self.settings.profile_recent_signal_weight_percent,
            )
            recommendation_seeds = self._build_recommendation_seed_pool(
                history,
                favorite_items=favorite_items,
                profile_summary=history_summary,
                limit=self.settings.recommendation_seed_limit,
            )
            live_payload = dict(stored_payload)
            for key in ("favorite_titles", "favorite_genres", "favorite_signal_count"):
                if key in history_summary:
                    live_payload[key] = history_summary[key]
            live_payload = self._with_live_profile_context(username, live_payload)
            return live_payload, self._resolve_tv_seed_media_ids_from_library_index(recommendation_seeds), False

        profile_payload, recommendation_seeds = await self._compose_profile_payload(
            username,
            history,
            favorite_items=favorite_items,
            existing_payload=stored_payload,
            peer_payload_overrides=peer_payload_overrides,
            include_llm_enrichment=False,
        )
        live_payload = self._with_live_profile_context(username, profile_payload)
        return live_payload, recommendation_seeds, int(live_payload.get("history_count") or 0) > 0

    @staticmethod
    def _candidate_title_key(candidate: dict[str, Any]) -> str:
        return str(candidate.get("title") or "").strip().lower()

    @classmethod
    def _candidate_feedback_block_reason(
        cls,
        candidate: dict[str, Any],
        profile_summary: dict[str, Any],
    ) -> str | None:
        blocked_titles = {
            value.lower()
            for value in cls._normalize_string_list(profile_summary.get("blocked_titles", []), limit=24)
        }
        title_key = cls._candidate_title_key(candidate)
        if title_key and title_key in blocked_titles:
            return "blocked_title"
        return None

    def _decision_candidate_skip_reason(
        self,
        candidate: dict[str, Any],
        *,
        profile_summary: dict[str, Any],
        watched_media_keys: set[tuple[str, int]],
        watched_external_keys: set[tuple[str, str, str]],
        watched_title_keys: set[tuple[str, str]],
        favorite_external_keys: set[tuple[str, str, str]],
        favorite_title_keys: set[tuple[str, str]],
        requested_media_keys: set[tuple[str, int]],
        requested_title_keys: set[tuple[str, str]],
        library_media_keys: set[tuple[str, int]],
        library_external_keys: set[tuple[str, str, str]],
        library_title_keys: set[tuple[str, str]],
    ) -> str | None:
        if (
            self._is_managed_candidate(candidate)
            or self._candidate_key(candidate) in library_media_keys
            or self._candidate_matches_external_keys(candidate, library_external_keys)
            or self._candidate_matches_title_keys(candidate, library_title_keys)
        ):
            return "managed"
        if (
            self._candidate_key(candidate) in watched_media_keys
            or self._candidate_matches_external_keys(candidate, watched_external_keys)
            or self._candidate_matches_title_keys(candidate, watched_title_keys)
        ):
            return "already_watched"
        if (
            self._candidate_matches_external_keys(candidate, favorite_external_keys)
            or self._candidate_matches_title_keys(candidate, favorite_title_keys)
        ):
            return "already_favorited"
        if (
            self._candidate_key(candidate) in requested_media_keys
            or self._candidate_matches_title_keys(candidate, requested_title_keys)
        ):
            return "already_requested"
        block_reason = self._candidate_feedback_block_reason(candidate, profile_summary)
        if block_reason is not None:
            return block_reason

        deterministic_score = float(candidate.get("recommendation_features", {}).get("deterministic_score") or 0.0)
        if deterministic_score < self._decision_prefilter_threshold():
            return "below_threshold"
        return None

    async def _prepare_decision_candidates_for_user(
        self,
        user: dict[str, Any],
        *,
        shortlist_limit: int | None = None,
        existing_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_username = str(user.get("Name") or "unknown")
        history = await self.media_server.get_playback_history(
            str(user.get("Id") or ""),
            self._playback_history_limit(),
        )
        favorite_items = await self._load_user_favorite_items(str(user.get("Id") or ""), username=current_username)
        stored_profile = existing_payload if isinstance(existing_payload, dict) else self.profile_store.read_payload(current_username)
        profile_payload, recommendation_seeds, should_persist = await self._prepare_runtime_profile_payload(
            current_username,
            history,
            favorite_items=favorite_items,
            existing_payload=stored_profile,
        )
        if should_persist:
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
        watched_context = self._build_media_item_match_context(history)
        favorite_context = self._build_media_item_match_context(favorite_items)
        ranked_candidates = self._rank_candidate_pool(
            candidate_pool,
            profile_summary=profile_payload,
        )
        with self.session_scope() as session:
            requested_rows = list(
                session.scalars(
                    select(RequestedMedia).order_by(
                        RequestedMedia.seer_request_id.is_(None).asc(),
                        desc(RequestedMedia.created_at),
                    )
                )
            )
            requested_media_keys = self._requested_media_keys(session)
            requested_title_keys = self._requested_title_keys(session)
            library_context = self._build_library_match_context(session)
            request_supporters = self._request_supporter_lookup(session, [row.id for row in requested_rows])
        requested_by_key: dict[tuple[str, int], RequestedMedia] = {}
        requested_by_title: dict[tuple[str, str], RequestedMedia] = {}
        for row in requested_rows:
            requested_by_key.setdefault((row.media_type, row.media_id), row)
            title_key = (str(row.media_type or "").strip(), str(row.media_title or "").strip().lower())
            if title_key[0] and title_key[1]:
                requested_by_title.setdefault(title_key, row)

        filtered_candidates: list[dict[str, Any]] = []
        shared_request_matches: list[dict[str, Any]] = []
        skip_reasons: Counter[str] = Counter()
        scored = 0
        skipped = 0
        for candidate in ranked_candidates:
            scored += 1
            skip_reason = self._decision_candidate_skip_reason(
                candidate,
                profile_summary=profile_payload,
                watched_media_keys=watched_context["media_keys"],  # type: ignore[arg-type]
                watched_external_keys=watched_context["external_keys"],  # type: ignore[arg-type]
                watched_title_keys=watched_context["title_keys"],  # type: ignore[arg-type]
                favorite_external_keys=favorite_context["external_keys"],  # type: ignore[arg-type]
                favorite_title_keys=favorite_context["title_keys"],  # type: ignore[arg-type]
                requested_media_keys=requested_media_keys,
                requested_title_keys=requested_title_keys,
                library_media_keys=library_context["media_keys"],  # type: ignore[arg-type]
                library_external_keys=library_context["external_keys"],  # type: ignore[arg-type]
                library_title_keys=library_context["title_keys"],  # type: ignore[arg-type]
            )
            if skip_reason is not None:
                skipped += 1
                skip_reasons[skip_reason] += 1
                if skip_reason == "already_requested":
                    matching_request = requested_by_key.get(self._candidate_key(candidate))
                    if matching_request is None:
                        matching_request = requested_by_title.get(
                            (str(candidate.get("media_type") or "").strip(), self._candidate_title_key(candidate))
                        )
                    if matching_request is not None:
                        supporter_usernames = request_supporters.get(int(matching_request.id), [])
                        if (
                            str(matching_request.username or "").strip().lower() != current_username.lower()
                            and current_username.lower() not in {value.lower() for value in supporter_usernames}
                        ):
                            shared_request_matches.append(
                                {
                                    "requested_media_id": int(matching_request.id),
                                    "owner_username": str(matching_request.username or ""),
                                    "media_title": str(matching_request.media_title or ""),
                                }
                            )
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
        shortlisted_candidates = self._diversify_candidates(
            filtered_candidates,
            limit=shortlist_limit or self.settings.decision_shortlist_limit,
        )
        return {
            "username": current_username,
            "history": history,
            "profile_payload": profile_payload,
            "recommendation_seeds": recommendation_seeds,
            "viewing_history": viewing_history,
            "candidate_pool": candidate_pool,
            "ranked_candidates": ranked_candidates,
            "filtered_candidates": filtered_candidates,
            "shortlisted_candidates": shortlisted_candidates,
            "requested_media_keys": requested_media_keys,
            "watched_media_keys": watched_context["media_keys"],
            "shared_request_matches": shared_request_matches,
            "scored": scored,
            "skipped": skipped,
            "skip_reasons": dict(skip_reasons),
            "persisted_profile": should_persist,
        }

    def _load_peer_profile_payloads(
        self,
        username: str,
        *,
        peer_payload_overrides: dict[str, dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        peers: dict[str, dict[str, Any]] = {}
        for candidate_username in self.profile_store.list_profiles():
            if candidate_username == username:
                continue
            peers[candidate_username] = self.profile_store.read_payload(candidate_username)

        if peer_payload_overrides:
            for candidate_username, payload in peer_payload_overrides.items():
                if candidate_username == username:
                    continue
                peers[candidate_username] = payload

        return [
            payload
            for payload in peers.values()
            if isinstance(payload, dict) and ProfileStore.is_structured_payload(payload)
        ]

    @classmethod
    def _profile_extension_genres(
        cls,
        profile_summary: dict[str, Any],
        *,
        limit: int,
    ) -> list[str]:
        merged: list[str] = []
        for source in (
            profile_summary.get("discovery_lanes", []),
            profile_summary.get("adjacent_genres", []),
            profile_summary.get("seer_adjacent_genres", []),
            profile_summary.get("similar_user_genres", []),
        ):
            merged = cls._merge_unique_strings(merged, cls._normalize_genres(source, limit=limit))
        return merged[:limit]

    @classmethod
    def _blocked_profile_genres(
        cls,
        profile_summary: dict[str, Any],
        *,
        extra_genres: list[Any] | None = None,
        limit: int = 8,
    ) -> set[str]:
        merged: list[str] = []
        for source in (
            profile_summary.get("primary_genres", []),
            profile_summary.get("secondary_genres", []),
            profile_summary.get("recent_genres", []),
            profile_summary.get("discovery_lanes", []),
        ):
            merged = cls._merge_unique_strings(merged, cls._normalize_string_list(source, limit=limit))
        if extra_genres:
            merged = cls._merge_unique_strings(merged, cls._normalize_string_list(extra_genres, limit=limit))

        blocked = {value.lower() for value in merged}
        feedback = profile_summary.get("explicit_feedback", {})
        blocked.update(
            value.lower()
            for value in cls._normalize_string_list(
                feedback.get("disliked_genres", []) if isinstance(feedback, dict) else [],
                limit=limit,
            )
        )
        return blocked

    @classmethod
    def _profile_neighbor_similarity(
        cls,
        profile_summary: dict[str, Any],
        peer_payload: dict[str, Any],
    ) -> float:
        current_primary = {value.lower() for value in cls._normalize_string_list(profile_summary.get("primary_genres", []), limit=4)}
        peer_primary = {value.lower() for value in cls._normalize_string_list(peer_payload.get("primary_genres", []), limit=4)}
        current_secondary = {value.lower() for value in cls._normalize_string_list(profile_summary.get("secondary_genres", []), limit=4)}
        peer_secondary = {value.lower() for value in cls._normalize_string_list(peer_payload.get("secondary_genres", []), limit=4)}
        current_recent = {value.lower() for value in cls._normalize_string_list(profile_summary.get("recent_genres", []), limit=4)}
        peer_recent = {value.lower() for value in cls._normalize_string_list(peer_payload.get("recent_genres", []), limit=4)}
        current_keywords = {value.lower() for value in cls._normalize_string_list(profile_summary.get("top_keywords", []), limit=6)}
        peer_keywords = {value.lower() for value in cls._normalize_string_list(peer_payload.get("top_keywords", []), limit=6)}
        current_titles = {
            str(item.get("title") or "").strip().lower()
            for item in cls._normalize_profile_entries(profile_summary.get("top_titles", []), limit=5)
            if str(item.get("title") or "").strip()
        }
        peer_titles = {
            str(item.get("title") or "").strip().lower()
            for item in cls._normalize_profile_entries(peer_payload.get("top_titles", []), limit=5)
            if str(item.get("title") or "").strip()
        }

        score = 0.0
        score += min(0.36, 0.18 * len(current_primary & peer_primary))
        score += min(0.16, 0.08 * len(current_secondary & peer_secondary))
        score += min(0.14, 0.07 * len(current_recent & peer_recent))
        score += min(0.2, 0.1 * len(current_titles & peer_titles))
        score += min(0.1, 0.05 * len(current_keywords & peer_keywords))

        current_format = str((profile_summary.get("format_preference") or {}).get("preferred") or "balanced")
        peer_format = str((peer_payload.get("format_preference") or {}).get("preferred") or "balanced")
        if current_format != "balanced" and current_format == peer_format:
            score += 0.05

        current_release = profile_summary.get("release_year_preference", {})
        peer_release = peer_payload.get("release_year_preference", {})
        current_bias = str(current_release.get("bias") or "balanced")
        peer_bias = str(peer_release.get("bias") or "balanced")
        current_average = cls._coerce_int(current_release.get("average_year"))
        peer_average = cls._coerce_int(peer_release.get("average_year"))
        if current_bias == peer_bias and current_average is not None and peer_average is not None:
            if abs(current_average - peer_average) <= 6:
                score += 0.03

        return round(score, 3)

    def _enrich_profile_summary_with_similar_users(
        self,
        username: str,
        history_summary: dict[str, Any],
        *,
        peer_payload_overrides: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        summary = dict(history_summary)
        summary.setdefault("similar_users", [])
        summary.setdefault("similar_user_genres", [])
        summary.setdefault("similar_user_titles", [])

        peer_payloads = self._load_peer_profile_payloads(username, peer_payload_overrides=peer_payload_overrides)
        if not peer_payloads:
            return summary

        scored_peers: list[tuple[float, dict[str, Any]]] = []
        for payload in peer_payloads:
            score = self._profile_neighbor_similarity(summary, payload)
            if score >= 0.18:
                scored_peers.append((score, payload))

        scored_peers.sort(
            key=lambda item: (
                -item[0],
                -int(item[1].get("history_count") or 0),
                str(item[1].get("username") or "").lower(),
            )
        )
        if not scored_peers:
            return summary
        if len(scored_peers) < 2 and scored_peers[0][0] < 0.3:
            return summary

        top_peers = scored_peers[:3]
        blocked_genres = self._blocked_profile_genres(
            summary,
            extra_genres=self._normalize_string_list(summary.get("seer_adjacent_genres", []), limit=4),
        )
        blocked_titles = {
            str(item.get("title") or "").strip().lower()
            for item in self._normalize_profile_entries(summary.get("top_titles", []), limit=8)
            if str(item.get("title") or "").strip()
        }
        blocked_titles.update(
            value.lower()
            for value in self._normalize_string_list(
                summary.get("explicit_feedback", {}).get("disliked_titles", []),
                limit=8,
            )
        )

        genre_scores: Counter[str] = Counter()
        title_scores: Counter[str] = Counter()
        for score, payload in top_peers:
            genre_sources = (
                (self._normalize_string_list(payload.get("adjacent_genres", []), limit=4), 1.3),
                (self._normalize_string_list(payload.get("discovery_lanes", []), limit=4), 1.1),
                (self._normalize_string_list(payload.get("primary_genres", []), limit=4), 0.8),
                (self._normalize_string_list(payload.get("recent_genres", []), limit=4), 0.7),
            )
            for genres, multiplier in genre_sources:
                for genre in genres:
                    lowered = genre.lower()
                    if lowered in blocked_genres:
                        continue
                    genre_scores[genre] += score * multiplier

            for entry in self._normalize_profile_entries(payload.get("top_titles", []), limit=4):
                title = str(entry.get("title") or "").strip()
                lowered = title.lower()
                if not title or lowered in blocked_titles:
                    continue
                title_scores[title] += score * (1.0 + min(0.5, int(entry.get("play_count") or 0) * 0.1))

        summary["similar_users"] = [
            str(payload.get("username") or "").strip()
            for _score, payload in top_peers
            if str(payload.get("username") or "").strip()
        ][:3]
        summary["similar_user_genres"] = self._rank_counter(genre_scores, limit=4)
        summary["similar_user_titles"] = self._rank_counter(title_scores, limit=4)
        return summary

    async def _enrich_profile_summary_with_seer(
        self,
        history_summary: dict[str, Any],
        *,
        recommendation_seeds: list[dict[str, Any]],
    ) -> dict[str, Any]:
        summary = dict(history_summary)
        summary.setdefault("seer_adjacent_titles", [])
        summary.setdefault("seer_adjacent_genres", [])

        if not recommendation_seeds or not hasattr(self.seer, "discover_candidates"):
            return summary

        seed_limit = max(1, min(4, int(self.settings.recommendation_seed_limit or 4)))
        try:
            candidates = await self.seer.discover_candidates(
                recommendation_seeds[:seed_limit],
                genre_seeds=[],
                limit=max(8, seed_limit * 6),
                genre_limit=0,
                trending_limit=0,
            )
        except Exception as exc:
            logger.warning("Seer profile enrichment skipped reason=%s", exc)
            return summary

        if not candidates:
            return summary

        blocked_genres = self._blocked_profile_genres(summary)
        blocked_titles = {
            str(item.get("title") or "").strip().lower()
            for item in self._normalize_profile_entries(summary.get("top_titles", []), limit=8)
            if str(item.get("title") or "").strip()
        }
        blocked_titles.update(
            value.lower()
            for value in self._normalize_string_list(
                summary.get("explicit_feedback", {}).get("disliked_titles", []),
                limit=8,
            )
        )

        genre_scores: Counter[str] = Counter()
        title_scores: Counter[str] = Counter()
        for candidate in candidates:
            source_count = max(1, len(self._normalize_string_list(candidate.get("sources", []), limit=6)))
            try:
                rating_value = float(candidate.get("rating") or 0.0)
            except (TypeError, ValueError):
                rating_value = 0.0
            weight = 1.0 + min(1.0, (source_count - 1) * 0.5) + max(0.0, min(0.4, (rating_value - 6.5) / 5.0))

            title = str(candidate.get("title") or "").strip()
            if title and title.lower() not in blocked_titles:
                title_scores[title] += weight
            for genre in self._normalize_genres(candidate.get("genres", []), limit=4):
                if genre.lower() in blocked_genres:
                    continue
                genre_scores[genre] += weight

        summary["seer_adjacent_titles"] = self._rank_counter(title_scores, limit=4)
        summary["seer_adjacent_genres"] = self._rank_counter(genre_scores, limit=4)
        return summary

    @classmethod
    def _build_heuristic_profile_enrichment(cls, history_summary: dict[str, Any]) -> dict[str, list[str]]:
        blocked_genres = cls._blocked_profile_genres(history_summary)
        adjacent_genres: list[str] = []
        for source_list in (
            history_summary.get("seer_adjacent_genres", []),
            history_summary.get("similar_user_genres", []),
        ):
            for value in cls._normalize_string_list(source_list, limit=4):
                if value.lower() in blocked_genres:
                    continue
                adjacent_genres.append(value)

        return {
            "adjacent_genres": cls._merge_unique_strings([], adjacent_genres)[:4],
            "adjacent_themes": [],
        }

    @classmethod
    def _merge_profile_enrichment_layers(
        cls,
        base: dict[str, list[str]] | None,
        overlay: dict[str, list[str]] | None,
    ) -> dict[str, list[str]]:
        base_genres = cls._normalize_string_list((base or {}).get("adjacent_genres", []), limit=4)
        base_themes = cls._normalize_string_list((base or {}).get("adjacent_themes", []), limit=3)
        overlay_genres = cls._normalize_string_list((overlay or {}).get("adjacent_genres", []), limit=4)
        overlay_themes = cls._normalize_string_list((overlay or {}).get("adjacent_themes", []), limit=3)
        return {
            "adjacent_genres": cls._merge_unique_strings(overlay_genres, base_genres)[:4],
            "adjacent_themes": cls._merge_unique_strings(overlay_themes, base_themes)[:3],
        }

    @staticmethod
    def _rank_genres(
        genre_counts: Counter[str],
        recent_genre_counts: Counter[str],
        *,
        recent_weight_percent: int = 75,
    ) -> list[tuple[str, float]]:
        recent_weight = max(0.0, float(recent_weight_percent) / 100.0)
        ranked: list[tuple[str, float, int]] = []
        for genre in set(genre_counts) | set(recent_genre_counts):
            score = float(genre_counts.get(genre, 0)) + (float(recent_genre_counts.get(genre, 0)) * recent_weight)
            ranked.append((genre, score, int(genre_counts.get(genre, 0))))

        ranked.sort(key=lambda item: (-item[1], -item[2], item[0].lower()))
        return [(genre, score) for genre, score, _count in ranked]

    @staticmethod
    def _profile_signal_weight(play_count: Any) -> float:
        try:
            count = max(1, int(play_count or 0))
        except (TypeError, ValueError):
            count = 1
        return 1.0 + min(1.0, max(0, count - 1) * 0.2)

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
        if "favorite_seed" in lane_lookup:
            score += 0.06
        if "genre_anchor_seed" in lane_lookup:
            score += 0.04
        if "primary_genre_seed" in lane_lookup:
            score += 0.05
        if "recent_genre_seed" in lane_lookup:
            score += 0.04
        if "adjacent_genre_seed" in lane_lookup:
            score += 0.02
        if "seer_genre_seed" in lane_lookup:
            score += 0.03
        if "similar_user_genre_seed" in lane_lookup:
            score += 0.03
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
    def _score_favorite_affinity(
        *,
        matched_favorite_genres: list[str],
        source_titles: list[str],
        favorite_titles: set[str],
    ) -> float:
        score = min(0.06, 0.03 * len(matched_favorite_genres))
        for title in source_titles:
            if title.lower() in favorite_titles:
                score += 0.04
        return min(0.08, score)

    @classmethod
    def _score_genre_guardrail(
        cls,
        *,
        candidate_genres: list[str],
        matched_primary: list[str],
        matched_secondary: list[str],
        matched_recent: list[str],
        matched_discovery: list[str],
        primary_genres: list[str],
        secondary_genres: list[str],
        recent_genres: list[str],
        discovery_lanes: list[str],
        ranked_genres: list[dict[str, Any]],
        genre_focus_share: Any,
    ) -> float:
        if not candidate_genres:
            return 0.0

        if matched_primary or matched_secondary or matched_recent or matched_discovery:
            return 0.0

        try:
            focus_share = max(0.0, min(1.0, float(genre_focus_share or 0.0)))
        except (TypeError, ValueError):
            focus_share = 0.0

        if focus_share < 0.35 and not primary_genres and not recent_genres:
            return 0.0

        candidate_set = {genre.lower() for genre in candidate_genres}
        preference_genres = cls._merge_unique_strings(primary_genres, secondary_genres)
        preference_genres = cls._merge_unique_strings(preference_genres, recent_genres)
        preference_genres = cls._merge_unique_strings(preference_genres, discovery_lanes)
        preference_set = {genre.lower() for genre in preference_genres}
        ranked_lookup = {
            str(item.get("genre") or "").strip().lower(): float(item.get("weighted_score") or 0.0)
            for item in ranked_genres
            if isinstance(item, dict) and str(item.get("genre") or "").strip()
        }

        if candidate_set & preference_set:
            return 0.0

        ranked_overlap = [genre for genre in candidate_set if genre in ranked_lookup]
        if ranked_overlap:
            strongest_overlap = max(ranked_lookup.get(genre, 0.0) for genre in ranked_overlap)
            if strongest_overlap >= 1.5:
                return 0.0

        penalty = 0.0
        if focus_share >= 0.7:
            penalty -= 0.14
        elif focus_share >= 0.55:
            penalty -= 0.1
        elif focus_share >= 0.45:
            penalty -= 0.07
        else:
            penalty -= 0.04

        if preference_set and len(candidate_set) >= 2:
            penalty -= 0.02
        if ranked_lookup and not ranked_overlap:
            penalty -= 0.03

        return max(-0.18, round(penalty, 3))

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

    @classmethod
    def _score_request_outcome_fit(
        cls,
        *,
        candidate_title: str,
        candidate_genres: list[str],
        request_outcome_insights: dict[str, Any],
    ) -> float:
        insights = cls._normalize_request_outcome_insights(request_outcome_insights)
        positive_titles = {
            value.lower() for value in cls._normalize_string_list(insights.get("positive_titles", []), limit=8)
        }
        negative_titles = {
            value.lower() for value in cls._normalize_string_list(insights.get("negative_titles", []), limit=8)
        }
        positive_genres = {
            value.lower() for value in cls._normalize_string_list(insights.get("positive_genres", []), limit=6)
        }
        negative_genres = {
            value.lower() for value in cls._normalize_string_list(insights.get("negative_genres", []), limit=6)
        }

        score = 0.0
        lowered_title = candidate_title.strip().lower()
        if lowered_title and lowered_title in positive_titles:
            score += 0.06
        if lowered_title and lowered_title in negative_titles:
            score -= 0.08

        for genre in candidate_genres:
            lowered = genre.lower()
            if lowered in positive_genres:
                score += 0.025
            if lowered in negative_genres:
                score -= 0.03

        return max(-0.14, min(0.14, score))

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
        if "seer_genre_seed" in lane_lookup:
            tags.append("seer_neighbor_lane")
        if "similar_user_genre_seed" in lane_lookup:
            tags.append("similar_user_lane")
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
        positive_outcome_genres: list[str],
        negative_outcome_genres: list[str],
        positive_title_signal: bool,
        negative_title_signal: bool,
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
        if positive_title_signal:
            parts.append("Past request outcomes on this exact title trended positive.")
        elif negative_title_signal:
            parts.append("Past request outcomes on this exact title trended negative.")
        elif positive_outcome_genres:
            parts.append(f"Past request outcomes leaned positive on {cls._human_join(positive_outcome_genres[:2])}.")
        elif negative_outcome_genres:
            parts.append(f"Past request outcomes leaned negative on {cls._human_join(negative_outcome_genres[:2])}.")
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

    @classmethod
    def _managed_media_status_label(cls, media_info: Any) -> str:
        if not isinstance(media_info, dict):
            return ""

        combined = " ".join(
            str(media_info.get(key) or "").strip().upper()
            for key in ("status", "mediaStatus")
            if str(media_info.get(key) or "").strip()
        )
        if "PARTIALLY_AVAILABLE" in combined or "PARTIAL" in combined:
            return "partial"
        if "AVAILABLE" in combined:
            return "available"
        if "PROCESSING" in combined:
            return "processing"
        if "PENDING" in combined:
            return "pending"

        status_value = cls._coerce_int(
            media_info.get("status") if media_info.get("status") is not None else media_info.get("mediaStatus")
        )
        if status_value == 5:
            return "available"
        if status_value == 4:
            return "partial"
        if status_value == 3:
            return "processing"
        if status_value == 2:
            return "pending"
        return ""

    @classmethod
    def _is_managed_candidate(cls, candidate: dict[str, Any]) -> bool:
        status = cls._managed_media_status_label(candidate.get("media_info") or {})
        return status in {"available", "partial", "processing", "pending"}

    @classmethod
    def _should_track_request_result(
        cls,
        *,
        candidate: dict[str, Any],
        request_result: SeerRequestResult,
    ) -> bool:
        if request_result.created or request_result.request_id is not None:
            return True

        message = str(request_result.message or "").strip().lower()
        if any(
            phrase in message
            for phrase in (
                "already requested",
                "already exists",
                "already been requested",
                "request already exists",
            )
        ):
            return True

        payload = request_result.payload if isinstance(request_result.payload, dict) else {}
        for key in ("media", "mediaInfo", "requestedMedia"):
            block = payload.get(key)
            if cls._managed_media_status_label(block) in {"available", "partial", "processing", "pending"}:
                return True

        return cls._is_managed_candidate(candidate)

    @staticmethod
    def _coerce_float(value: Any) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _coerce_optional_float(value: Any) -> float | None:
        if value in ("", None):
            return None
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return None

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
        payload = {
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
        payload["content_fingerprint"] = cls._build_library_content_fingerprint(payload)
        return payload

    @classmethod
    def _build_library_content_fingerprint(cls, payload: dict[str, Any]) -> str:
        fingerprint_payload = {
            "media_type": str(payload.get("media_type") or "unknown"),
            "title": str(payload.get("title") or ""),
            "sort_title": str(payload.get("sort_title") or ""),
            "overview": str(payload.get("overview") or ""),
            "production_year": payload.get("production_year"),
            "release_date": payload.get("release_date"),
            "community_rating": payload.get("community_rating"),
            "genres": cls._normalize_genres(payload.get("genres", []), limit=12),
            "tmdb_id": payload.get("tmdb_id"),
            "tvdb_id": payload.get("tvdb_id"),
            "imdb_id": payload.get("imdb_id"),
        }
        return cls._stable_json_fingerprint(fingerprint_payload)

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

    @classmethod
    def _build_suggestion_exclusion_context(
        cls,
        history: list[dict[str, Any]],
        in_progress_items: list[dict[str, Any]],
        *,
        recent_cooldown_days: int,
        repeat_watch_cutoff: int,
    ) -> dict[str, set[tuple[str, str]] | set[tuple[str, str, str]]]:
        watched_external_keys: set[tuple[str, str, str]] = set()
        recent_external_keys: set[tuple[str, str, str]] = set()
        repeat_external_keys: set[tuple[str, str, str]] = set()
        in_progress_external_keys: set[tuple[str, str, str]] = set()

        watched_title_keys: set[tuple[str, str]] = set()
        recent_title_keys: set[tuple[str, str]] = set()
        repeat_title_keys: set[tuple[str, str]] = set()
        in_progress_title_keys: set[tuple[str, str]] = set()

        repeat_threshold = max(1, int(repeat_watch_cutoff))
        recent_cutoff_ts = 0.0
        if int(recent_cooldown_days) > 0:
            recent_cutoff_ts = (datetime.utcnow() - timedelta(days=int(recent_cooldown_days))).timestamp()

        external_counts: Counter[tuple[str, str, str]] = Counter()
        title_counts: Counter[tuple[str, str]] = Counter()

        for item in history:
            media_type = cls._map_history_media_type(item.get("Type"))
            if media_type not in {"movie", "tv"}:
                continue

            title_key = cls._history_title_key(item)
            if title_key is not None:
                watched_title_keys.add(title_key)
                title_counts[title_key] += 1

            external_ids = cls._extract_external_ids(item)
            external_keys = [(media_type, provider_key, provider_id) for provider_key, provider_id in external_ids.items()]
            for key in external_keys:
                watched_external_keys.add(key)
                external_counts[key] += 1

            if recent_cutoff_ts > 0 and cls._to_timestamp(item.get("UserData", {}).get("LastPlayedDate")) >= recent_cutoff_ts:
                if title_key is not None:
                    recent_title_keys.add(title_key)
                recent_external_keys.update(external_keys)

        for key, count in external_counts.items():
            if count >= repeat_threshold:
                repeat_external_keys.add(key)
        for key, count in title_counts.items():
            if count >= repeat_threshold:
                repeat_title_keys.add(key)

        for item in in_progress_items:
            media_type = cls._map_history_media_type(item.get("Type"))
            if media_type not in {"movie", "tv"}:
                continue

            title_key = cls._history_title_key(item)
            if title_key is not None:
                in_progress_title_keys.add(title_key)

            external_ids = cls._extract_external_ids(item)
            for provider_key, provider_id in external_ids.items():
                in_progress_external_keys.add((media_type, provider_key, provider_id))

        return {
            "watched_external_keys": watched_external_keys,
            "watched_title_keys": watched_title_keys,
            "recent_external_keys": recent_external_keys,
            "recent_title_keys": recent_title_keys,
            "repeat_external_keys": repeat_external_keys,
            "repeat_title_keys": repeat_title_keys,
            "in_progress_external_keys": in_progress_external_keys,
            "in_progress_title_keys": in_progress_title_keys,
        }

    @classmethod
    def _suggestion_exclusion_reason(
        cls,
        candidate: dict[str, Any],
        context: dict[str, set[tuple[str, str]] | set[tuple[str, str, str]]],
    ) -> str | None:
        if cls._candidate_matches_external_keys(
            candidate,
            context.get("in_progress_external_keys", set()),  # type: ignore[arg-type]
        ) or cls._candidate_matches_title_keys(
            candidate,
            context.get("in_progress_title_keys", set()),  # type: ignore[arg-type]
        ):
            return "in_progress"
        if cls._candidate_matches_external_keys(
            candidate,
            context.get("recent_external_keys", set()),  # type: ignore[arg-type]
        ) or cls._candidate_matches_title_keys(
            candidate,
            context.get("recent_title_keys", set()),  # type: ignore[arg-type]
        ):
            return "recently_watched"
        if cls._candidate_matches_external_keys(
            candidate,
            context.get("repeat_external_keys", set()),  # type: ignore[arg-type]
        ) or cls._candidate_matches_title_keys(
            candidate,
            context.get("repeat_title_keys", set()),  # type: ignore[arg-type]
        ):
            return "repeat_watch"
        if cls._candidate_matches_external_keys(
            candidate,
            context.get("watched_external_keys", set()),  # type: ignore[arg-type]
        ) or cls._candidate_matches_title_keys(
            candidate,
            context.get("watched_title_keys", set()),  # type: ignore[arg-type]
        ):
            return "already_watched"
        return None

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
    def _candidate_matches_title_keys(
        candidate: dict[str, Any],
        title_keys: set[tuple[str, str]],
    ) -> bool:
        media_type = str(candidate.get("media_type") or "")
        title = str(candidate.get("title") or "").strip().lower()
        if not media_type or not title:
            return False
        return (media_type, title) in title_keys

    @classmethod
    def _history_title_key(cls, item: dict[str, Any]) -> tuple[str, str] | None:
        media_type = cls._map_history_media_type(item.get("Type"))
        if media_type not in {"movie", "tv"}:
            return None
        title = cls._seed_title(item, media_type).strip().lower()
        if not title:
            return None
        return media_type, title

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
    def _stable_json_fingerprint(payload: Any) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

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
