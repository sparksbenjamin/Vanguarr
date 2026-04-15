from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base


class DecisionLog(Base):
    __tablename__ = "decision_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    engine: Mapped[str] = mapped_column(String(64), default="decision_engine", index=True)
    username: Mapped[str] = mapped_column(String(255), index=True)
    media_type: Mapped[str] = mapped_column(String(32), index=True)
    media_id: Mapped[int] = mapped_column(Integer, index=True)
    media_title: Mapped[str] = mapped_column(String(255), index=True)
    source: Mapped[str] = mapped_column(String(255), default="unknown")
    decision: Mapped[str] = mapped_column(String(32), index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    threshold: Mapped[float] = mapped_column(Float, default=0.0)
    requested: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reasoning: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class RequestedMedia(Base):
    __tablename__ = "requested_media"
    __table_args__ = (
        UniqueConstraint("username", "media_type", "media_id", name="uq_requested_media_per_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    username: Mapped[str] = mapped_column(String(255), index=True)
    media_type: Mapped[str] = mapped_column(String(32), index=True)
    media_id: Mapped[int] = mapped_column(Integer, index=True)
    media_title: Mapped[str] = mapped_column(String(255))
    source: Mapped[str] = mapped_column(String(255), default="unknown")
    seer_request_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


class RequestedMediaSupporter(Base):
    __tablename__ = "requested_media_supporters"
    __table_args__ = (
        UniqueConstraint("requested_media_id", "username", name="uq_requested_media_supporter"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    requested_media_id: Mapped[int] = mapped_column(Integer, index=True)
    username: Mapped[str] = mapped_column(String(255), index=True)
    source: Mapped[str] = mapped_column(String(64), default="manual", index=True)


class RequestOutcomeEvent(Base):
    __tablename__ = "request_outcome_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    requested_media_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    username: Mapped[str] = mapped_column(String(255), index=True)
    media_type: Mapped[str] = mapped_column(String(32), index=True)
    media_id: Mapped[int] = mapped_column(Integer, default=0, index=True)
    media_title: Mapped[str] = mapped_column(String(255), index=True)
    request_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    outcome: Mapped[str] = mapped_column(String(32), index=True)
    source: Mapped[str] = mapped_column(String(64), default="manual", index=True)
    detail: Mapped[str] = mapped_column(Text, default="")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class TaskRun(Base):
    __tablename__ = "task_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    engine: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    summary: Mapped[str] = mapped_column(Text, default="")
    progress_current: Mapped[int | None] = mapped_column(Integer, nullable=True)
    progress_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_label: Mapped[str] = mapped_column(String(255), default="")
    detail_json: Mapped[str] = mapped_column(Text, default="{}")


class SuggestedMedia(Base):
    __tablename__ = "suggested_media"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        index=True,
    )
    jellyfin_user_id: Mapped[str] = mapped_column(String(64), index=True)
    username: Mapped[str] = mapped_column(String(255), index=True)
    rank: Mapped[int] = mapped_column(Integer, index=True)
    media_type: Mapped[str] = mapped_column(String(32), index=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    overview: Mapped[str] = mapped_column(Text, default="")
    production_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float] = mapped_column(Float, default=0.0)
    reasoning: Mapped[str] = mapped_column(Text, default="")
    state: Mapped[str] = mapped_column(String(32), default="available", index=True)
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    tvdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    imdb_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class LibraryMedia(Base):
    __tablename__ = "library_media"
    __table_args__ = (
        UniqueConstraint("source_provider", "media_server_id", name="uq_library_media_source_item"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        index=True,
    )
    source_provider: Mapped[str] = mapped_column(String(32), default="jellyfin", index=True)
    media_server_id: Mapped[str] = mapped_column(String(128), index=True)
    media_type: Mapped[str] = mapped_column(String(32), index=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    sort_title: Mapped[str] = mapped_column(String(255), default="", index=True)
    overview: Mapped[str] = mapped_column(Text, default="")
    production_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    release_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    community_rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    genres_json: Mapped[str] = mapped_column(Text, default="[]")
    state: Mapped[str] = mapped_column(String(32), default="available", index=True)
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    tvdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    imdb_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    content_fingerprint: Mapped[str] = mapped_column(String(64), default="", index=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class SeerWebhookEvent(Base):
    __tablename__ = "seer_webhook_events"
    __table_args__ = (
        UniqueConstraint("delivery_key", name="uq_seer_webhook_delivery_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    delivery_key: Mapped[str] = mapped_column(String(255), index=True)
    notification_type: Mapped[str] = mapped_column(String(64), index=True)
    event_name: Mapped[str] = mapped_column(String(255), default="")
    request_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    requested_by_username: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    media_type: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    media_status: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    tmdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    tvdb_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    subject: Mapped[str] = mapped_column(String(255), default="")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        index=True,
    )


class LLMProviderConfig(Base):
    __tablename__ = "llm_provider_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255), default="Provider")
    provider: Mapped[str] = mapped_column(String(64), index=True)
    model: Mapped[str] = mapped_column(String(255))
    priority: Mapped[int] = mapped_column(Integer, default=1, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    api_base: Mapped[str | None] = mapped_column(Text, nullable=True)
    api_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    timeout_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    use_for_decision: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    use_for_profile_enrichment: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
