from __future__ import annotations

from contextlib import contextmanager
from threading import Lock
from time import monotonic
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.models import AppSetting, LLMProviderConfig
from app.core.settings import (
    DB_MANAGED_SETTING_KEYS,
    LLMProviderSettings,
    Settings,
    serialize_setting_value,
)


class SettingsManager:
    def __init__(
        self,
        bootstrap_settings: Settings,
        session_factory: sessionmaker[Session],
        *,
        cache_ttl_seconds: float = 1.0,
    ) -> None:
        self.bootstrap_settings = bootstrap_settings
        self.session_factory = session_factory
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache_lock = Lock()
        self._cached_settings: Settings | None = None
        self._cached_at = 0.0

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

    def invalidate(self) -> None:
        with self._cache_lock:
            self._cached_settings = None
            self._cached_at = 0.0

    def sync_bootstrap_defaults(self) -> Settings:
        with self.session_scope() as session:
            existing_settings = {
                row.key: row
                for row in session.scalars(select(AppSetting)).all()
            }
            for key in DB_MANAGED_SETTING_KEYS:
                if key in existing_settings:
                    continue
                session.add(
                    AppSetting(
                        key=key,
                        value=serialize_setting_value(getattr(self.bootstrap_settings, key)),
                    )
                )

            provider_rows = list(
                session.scalars(
                    select(LLMProviderConfig).order_by(LLMProviderConfig.priority.asc(), LLMProviderConfig.id.asc())
                )
            )
            if not provider_rows:
                legacy_provider = self.bootstrap_settings.legacy_llm_provider
                if legacy_provider is not None and legacy_provider.model:
                    session.add(
                        LLMProviderConfig(
                            name=legacy_provider.name,
                            provider=legacy_provider.provider,
                            model=legacy_provider.model,
                            priority=legacy_provider.priority,
                            enabled=legacy_provider.enabled,
                            api_base=legacy_provider.api_base,
                            api_key=legacy_provider.api_key,
                            timeout_seconds=legacy_provider.timeout_seconds,
                            max_output_tokens=legacy_provider.max_output_tokens,
                            use_for_decision=legacy_provider.use_for_decision,
                            use_for_profile_enrichment=legacy_provider.use_for_profile_enrichment,
                        )
                    )

        return self.get_runtime_settings(force=True)

    def get_runtime_settings(self, *, force: bool = False) -> Settings:
        with self._cache_lock:
            if (
                not force
                and self._cached_settings is not None
                and (monotonic() - self._cached_at) < self.cache_ttl_seconds
            ):
                return self._cached_settings

        with self.session_scope() as session:
            settings = self._load_runtime_settings(session)

        with self._cache_lock:
            self._cached_settings = settings
            self._cached_at = monotonic()
        return settings

    def save_settings(
        self,
        setting_values: dict[str, Any],
        provider_payloads: list[dict[str, Any]],
    ) -> Settings:
        with self.session_scope() as session:
            current_setting_rows = {
                row.key: row
                for row in session.scalars(select(AppSetting)).all()
            }
            merged_setting_values = {
                key: row.value
                for key, row in current_setting_rows.items()
                if key in DB_MANAGED_SETTING_KEYS
            }
            for key in DB_MANAGED_SETTING_KEYS:
                if key not in setting_values:
                    continue
                merged_setting_values[key] = serialize_setting_value(setting_values[key])

            normalized_provider_payloads = self._normalize_provider_payloads(provider_payloads)
            validated_providers = [
                LLMProviderSettings.model_validate(payload)
                for payload in normalized_provider_payloads
            ]
            validated_settings = self._build_settings_from_rows(
                merged_setting_values,
                tuple(validated_providers),
            )

            for key in DB_MANAGED_SETTING_KEYS:
                row = current_setting_rows.get(key)
                serialized_value = serialize_setting_value(getattr(validated_settings, key))
                if row is None:
                    session.add(AppSetting(key=key, value=serialized_value))
                else:
                    row.value = serialized_value

            provider_rows = {
                row.id: row
                for row in session.scalars(select(LLMProviderConfig)).all()
            }

            for payload in provider_payloads:
                row_id = payload.get("id")
                delete_requested = bool(payload.get("delete"))
                if row_id in ("", None):
                    continue
                normalized_id = int(row_id)
                row = provider_rows.get(normalized_id)
                if row is None:
                    continue
                if delete_requested:
                    session.delete(row)

            for provider in validated_providers:
                if provider.id is not None and provider.id in provider_rows:
                    row = provider_rows[provider.id]
                    row.name = provider.name
                    row.provider = provider.provider
                    row.model = provider.model
                    row.priority = provider.priority
                    row.enabled = provider.enabled
                    row.api_base = provider.api_base
                    row.api_key = provider.api_key
                    row.timeout_seconds = provider.timeout_seconds
                    row.max_output_tokens = provider.max_output_tokens
                    row.use_for_decision = provider.use_for_decision
                    row.use_for_profile_enrichment = provider.use_for_profile_enrichment
                    continue

                session.add(
                    LLMProviderConfig(
                        name=provider.name,
                        provider=provider.provider,
                        model=provider.model,
                        priority=provider.priority,
                        enabled=provider.enabled,
                        api_base=provider.api_base,
                        api_key=provider.api_key,
                        timeout_seconds=provider.timeout_seconds,
                        max_output_tokens=provider.max_output_tokens,
                        use_for_decision=provider.use_for_decision,
                        use_for_profile_enrichment=provider.use_for_profile_enrichment,
                    )
                )

        self.invalidate()
        return self.get_runtime_settings(force=True)

    def _load_runtime_settings(self, session: Session) -> Settings:
        setting_rows = {
            row.key: row.value
            for row in session.scalars(select(AppSetting)).all()
            if row.key in DB_MANAGED_SETTING_KEYS
        }
        provider_rows = tuple(
            LLMProviderSettings(
                id=row.id,
                name=row.name,
                provider=row.provider,
                model=row.model,
                priority=row.priority,
                enabled=row.enabled,
                api_base=row.api_base,
                api_key=row.api_key,
                timeout_seconds=row.timeout_seconds,
                max_output_tokens=row.max_output_tokens,
                use_for_decision=row.use_for_decision,
                use_for_profile_enrichment=row.use_for_profile_enrichment,
            )
            for row in session.scalars(
                select(LLMProviderConfig).order_by(LLMProviderConfig.priority.asc(), LLMProviderConfig.id.asc())
            )
        )
        return self._build_settings_from_rows(setting_rows, provider_rows)

    def _build_settings_from_rows(
        self,
        setting_rows: dict[str, Any],
        provider_rows: tuple[LLMProviderSettings, ...],
    ) -> Settings:
        payload = self.bootstrap_settings.model_dump()
        payload.update(setting_rows)
        payload["llm_providers"] = provider_rows
        settings = Settings(**payload)
        settings.ensure_runtime_dirs()
        return settings

    @staticmethod
    def _normalize_provider_payloads(provider_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for payload in provider_payloads:
            row_id = payload.get("id")
            provider = str(payload.get("provider") or "").strip().lower()
            model = str(payload.get("model") or "").strip()
            name = str(payload.get("name") or "").strip()
            api_base = str(payload.get("api_base") or "").strip() or None
            api_key = str(payload.get("api_key") or "").strip() or None
            timeout_seconds = payload.get("timeout_seconds", "")
            max_output_tokens = payload.get("max_output_tokens", "")
            priority = payload.get("priority", 1)
            enabled = bool(payload.get("enabled"))
            use_for_decision = bool(payload.get("use_for_decision"))
            use_for_profile_enrichment = bool(payload.get("use_for_profile_enrichment"))
            delete_requested = bool(payload.get("delete"))

            normalized_id = None if row_id in ("", None) else int(row_id)
            if delete_requested:
                continue

            has_meaningful_new_input = any(
                [
                    name,
                    model,
                    api_base,
                    api_key,
                    str(timeout_seconds).strip(),
                    str(max_output_tokens).strip(),
                ]
            )
            if normalized_id is None and not has_meaningful_new_input:
                continue

            normalized.append(
                {
                    "id": normalized_id,
                    "name": name or (provider.title() if provider else "Provider"),
                    "provider": provider,
                    "model": model,
                    "priority": priority,
                    "enabled": enabled,
                    "api_base": api_base,
                    "api_key": api_key,
                    "timeout_seconds": timeout_seconds,
                    "max_output_tokens": max_output_tokens,
                    "use_for_decision": use_for_decision,
                    "use_for_profile_enrichment": use_for_profile_enrichment,
                }
            )
        return normalized


class LiveSettings:
    def __init__(self, manager: SettingsManager) -> None:
        self.manager = manager

    def snapshot(self, *, force: bool = False) -> Settings:
        return self.manager.get_runtime_settings(force=force)

    def invalidate(self) -> None:
        self.manager.invalidate()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.snapshot(), name)

    def __repr__(self) -> str:
        return f"LiveSettings({self.snapshot()!r})"
