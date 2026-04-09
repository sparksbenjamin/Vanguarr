from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.settings import get_settings


settings = get_settings()
connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def init_db() -> None:
    from app.core import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _migrate_runtime_schema()


def _migrate_runtime_schema() -> None:
    inspector = inspect(engine)
    if "llm_provider_configs" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("llm_provider_configs")}
    statements: list[str] = []

    if "max_output_tokens" not in columns:
        statements.append("ALTER TABLE llm_provider_configs ADD COLUMN max_output_tokens INTEGER")
    if "use_for_decision" not in columns:
        statements.append(
            "ALTER TABLE llm_provider_configs ADD COLUMN use_for_decision BOOLEAN NOT NULL DEFAULT 1"
        )
    if "use_for_profile_enrichment" not in columns:
        statements.append(
            "ALTER TABLE llm_provider_configs ADD COLUMN use_for_profile_enrichment BOOLEAN NOT NULL DEFAULT 1"
        )

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def get_db() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
