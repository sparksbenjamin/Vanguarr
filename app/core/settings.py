from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DEFAULT_PROFILES_DIR = DATA_DIR / "profiles"
DEFAULT_LOGS_DIR = DATA_DIR / "logs"
DEFAULT_LOG_FILE = DEFAULT_LOGS_DIR / "vanguarr.log"


@dataclass(frozen=True, slots=True)
class SettingFieldDefinition:
    key: str
    label: str
    group: str
    description: str
    input_type: str = "text"
    placeholder: str = ""
    choices: tuple[tuple[str, str], ...] = ()
    min_value: str = ""
    max_value: str = ""
    step: str = "any"


class LLMProviderSettings(BaseModel):
    id: int | None = None
    name: str = "Provider"
    provider: str = "ollama"
    model: str = ""
    priority: int = 1
    enabled: bool = True
    api_base: str | None = None
    api_key: str | None = None
    timeout_seconds: int | None = None
    max_output_tokens: int | None = None
    use_for_decision: bool = True
    use_for_profile_enrichment: bool = True

    @field_validator("name", "provider", "model", mode="before")
    @classmethod
    def strip_required_strings(cls, value: object) -> object:
        if value is None:
            return value
        return str(value).strip()

    @field_validator("api_base", "api_key", mode="before")
    @classmethod
    def blank_optional_strings_to_none(cls, value: object) -> object:
        if value in ("", None):
            return None
        return str(value).strip()

    @field_validator("timeout_seconds", "max_output_tokens", mode="before")
    @classmethod
    def blank_timeout_to_none(cls, value: object) -> object:
        if value in ("", None):
            return None
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return value
        return numeric if numeric > 0 else None

    @field_validator("priority", mode="before")
    @classmethod
    def default_priority(cls, value: object) -> object:
        if value in ("", None):
            return 1
        return value


class Settings(BaseSettings):
    app_name: str = "Vanguarr"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    timezone: str = Field(default="America/New_York", alias="TZ")

    data_dir: Path = DATA_DIR
    database_url: str = f"sqlite:///{(DATA_DIR / 'vanguarr.db').as_posix()}"
    profiles_dir: Path = DEFAULT_PROFILES_DIR
    logs_dir: Path = DEFAULT_LOGS_DIR
    log_file: Path = DEFAULT_LOG_FILE
    log_level: str = "INFO"
    global_exclusions: str = "No Horror,No Reality TV"
    request_threshold: float = 0.72
    decision_ai_weight_percent: int = 25
    scheduler_enabled: bool = True
    profile_cron: str = "0 3 * * 0"
    decision_cron: str = "0 4 * * *"
    health_cache_seconds: int = 30
    profile_history_limit: int = 40
    profile_architect_max_output_tokens: int = 384
    profile_architect_top_titles_limit: int = 8
    profile_architect_recent_momentum_limit: int = 5
    profile_llm_enrichment_enabled: bool = True
    profile_llm_enrichment_max_output_tokens: int = 120
    candidate_limit: int = 160
    genre_candidate_limit: int = 30
    trending_candidate_limit: int = 100
    decision_shortlist_limit: int = 15
    recommendation_seed_limit: int = 6
    tmdb_seed_enrichment_limit: int = 6
    tmdb_candidate_enrichment_limit: int = 30
    decision_page_size: int = 100
    suggestions_enabled: bool = True
    suggestions_limit: int = 20

    media_server_provider: str = "jellyfin"
    jellyfin_base_url: str | None = None
    jellyfin_api_key: str | None = None
    plex_base_url: str | None = None
    plex_api_token: str | None = None
    plex_client_identifier: str = "vanguarr"
    seer_base_url: str | None = None
    seer_api_key: str | None = None
    seer_request_user_id: int | None = None
    seer_webhook_token: str | None = None
    suggestions_api_key: str | None = None
    tmdb_base_url: str = "https://api.themoviedb.org/3"
    tmdb_api_read_access_token: str | None = None
    tmdb_api_key: str | None = None
    tmdb_language: str = "en-US"
    tmdb_watch_region: str = "US"

    llm_provider: str = "ollama"
    llm_model: str = "ollama/llama3.1:8b"
    llm_temperature: float = 0.2
    llm_max_output_tokens: int | None = None
    llm_timeout_seconds: int | None = None
    ollama_api_base: str = "http://ollama:11434"
    openai_api_key: str | None = None
    openai_api_base: str | None = None
    anthropic_api_key: str | None = None
    anthropic_api_base: str | None = None
    llm_providers: tuple[LLMProviderSettings, ...] = ()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @field_validator("seer_request_user_id", mode="before")
    @classmethod
    def blank_int_to_none(cls, value: object) -> object:
        if value in ("", None):
            return None
        return value

    @field_validator("llm_timeout_seconds", mode="before")
    @classmethod
    def blank_timeout_to_none(cls, value: object) -> object:
        if value in ("", None):
            return None
        return value

    @field_validator("decision_ai_weight_percent", mode="before")
    @classmethod
    def validate_decision_ai_weight_percent(cls, value: object) -> object:
        if value in ("", None):
            return 25
        numeric = int(value)
        if not 0 <= numeric <= 100:
            raise ValueError("AI decision weight must be between 0 and 100.")
        return numeric

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        ZoneInfo(value)
        return value

    @field_validator("media_server_provider", mode="before")
    @classmethod
    def validate_media_server_provider(cls, value: object) -> str:
        provider = str(value or "jellyfin").strip().lower()
        valid = {option for option, _label in MEDIA_SERVER_PROVIDER_OPTIONS}
        if provider not in valid:
            raise ValueError(f"Media server provider must be one of: {', '.join(sorted(valid))}.")
        return provider

    @field_validator("profile_cron", "decision_cron")
    @classmethod
    def validate_cron_expression(cls, value: str) -> str:
        CronTrigger.from_crontab(value)
        return value

    @field_validator(
        "jellyfin_base_url",
        "jellyfin_api_key",
        "plex_base_url",
        "plex_api_token",
        "seer_base_url",
        "seer_api_key",
        "seer_webhook_token",
        "suggestions_api_key",
        "tmdb_api_read_access_token",
        "tmdb_api_key",
        "openai_api_key",
        "openai_api_base",
        "anthropic_api_key",
        "anthropic_api_base",
        mode="before",
    )
    @classmethod
    def blank_string_to_none(cls, value: object) -> object:
        if value in ("", None):
            return None
        return value

    @property
    def normalized_media_server_provider(self) -> str:
        provider = str(self.media_server_provider or "jellyfin").strip().lower()
        valid = {option for option, _label in MEDIA_SERVER_PROVIDER_OPTIONS}
        if provider in valid:
            return provider
        return "jellyfin"

    @property
    def media_server_label(self) -> str:
        labels = dict(MEDIA_SERVER_PROVIDER_OPTIONS)
        return labels.get(self.normalized_media_server_provider, "Jellyfin")

    @property
    def effective_llm_timeout_seconds(self) -> int:
        primary_provider = self.primary_llm_provider
        if primary_provider is not None:
            return self.resolve_llm_timeout(primary_provider.provider, primary_provider.timeout_seconds)
        return self.resolve_llm_timeout(self.llm_provider, self.llm_timeout_seconds)

    @property
    def active_llm_providers(self) -> tuple[LLMProviderSettings, ...]:
        return self.providers_for_use()

    @property
    def decision_llm_providers(self) -> tuple[LLMProviderSettings, ...]:
        return self.providers_for_use("decision")

    @property
    def profile_enrichment_llm_providers(self) -> tuple[LLMProviderSettings, ...]:
        return self.providers_for_use("profile_enrichment")

    @property
    def primary_llm_provider(self) -> LLMProviderSettings | None:
        providers = self.active_llm_providers
        if not providers:
            return None
        return providers[0]

    @property
    def llm_provider_label(self) -> str:
        primary = self.primary_llm_provider
        if primary is None:
            return "disabled"
        if len(self.active_llm_providers) == 1:
            return primary.provider
        return f"{primary.provider} +{len(self.active_llm_providers) - 1}"

    @property
    def legacy_llm_provider(self) -> LLMProviderSettings | None:
        model = str(self.llm_model or "").strip()
        provider = str(self.llm_provider or "").strip().lower()
        if not provider or not model:
            return None

        api_base: str | None = None
        api_key: str | None = None
        if provider == "ollama":
            api_base = self.ollama_api_base
        elif provider == "openai":
            api_base = self.openai_api_base
            api_key = self.openai_api_key
        elif provider == "anthropic":
            api_base = self.anthropic_api_base
            api_key = self.anthropic_api_key

        return LLMProviderSettings(
            name=f"Legacy {provider.title()}",
            provider=provider,
            model=model,
            priority=1,
            enabled=True,
            api_base=api_base,
            api_key=api_key,
            timeout_seconds=self.llm_timeout_seconds,
            max_output_tokens=self.llm_max_output_tokens,
            use_for_decision=True,
            use_for_profile_enrichment=True,
        )

    def providers_for_use(self, use_case: str | None = None) -> tuple[LLMProviderSettings, ...]:
        configured = tuple(
            sorted(
                (
                    provider
                    for provider in self.llm_providers
                    if provider.enabled and self._provider_supports_use_case(provider, use_case)
                ),
                key=lambda provider: (provider.priority, provider.id or 0, provider.name.lower()),
            )
        )
        if configured:
            return configured
        if self.llm_providers:
            return ()

        legacy = self.legacy_llm_provider
        if legacy is not None and legacy.model:
            return (legacy,)
        return ()

    @staticmethod
    def _provider_supports_use_case(provider: LLMProviderSettings, use_case: str | None) -> bool:
        if use_case == "decision":
            return provider.use_for_decision
        if use_case == "profile_enrichment":
            return provider.use_for_profile_enrichment
        return provider.use_for_decision or provider.use_for_profile_enrichment

    def resolve_llm_timeout(self, provider: str, provider_timeout: int | None = None) -> int:
        if provider_timeout is not None:
            return provider_timeout
        if self.llm_timeout_seconds is not None:
            return self.llm_timeout_seconds
        if provider.lower() == "ollama":
            return 180
        return 45

    def ensure_runtime_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        if self.database_url.startswith("sqlite:///") and ":memory:" not in self.database_url:
            db_path = Path(self.database_url.replace("sqlite:///", "", 1))
            db_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings


LLM_PROVIDER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("ollama", "Ollama"),
    ("openai", "OpenAI"),
    ("anthropic", "Anthropic"),
)

MEDIA_SERVER_PROVIDER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("jellyfin", "Jellyfin"),
    ("plex", "Plex"),
)


DB_MANAGED_SETTING_FIELDS: tuple[SettingFieldDefinition, ...] = (
    SettingFieldDefinition(
        key="app_name",
        label="App Name",
        group="General",
        description="Display name shown throughout the UI.",
    ),
    SettingFieldDefinition(
        key="timezone",
        label="Timezone",
        group="General",
        description="Used for the scheduler and time-based displays.",
        placeholder="America/New_York",
    ),
    SettingFieldDefinition(
        key="log_level",
        label="Log Level",
        group="General",
        description="Application logging threshold.",
        input_type="select",
        choices=(("DEBUG", "DEBUG"), ("INFO", "INFO"), ("WARNING", "WARNING"), ("ERROR", "ERROR")),
    ),
    SettingFieldDefinition(
        key="health_cache_seconds",
        label="Health Cache Seconds",
        group="General",
        description="How long health checks are cached before refreshing.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="media_server_provider",
        label="Media Server Provider",
        group="Integrations",
        description="Choose which media server Vanguarr reads watch history from.",
        input_type="select",
        choices=MEDIA_SERVER_PROVIDER_OPTIONS,
    ),
    SettingFieldDefinition(
        key="jellyfin_base_url",
        label="Jellyfin Base URL",
        group="Integrations",
        description="Base Jellyfin URL when Jellyfin is the active media server.",
        placeholder="http://jellyfin:8096",
    ),
    SettingFieldDefinition(
        key="jellyfin_api_key",
        label="Jellyfin API Key",
        group="Integrations",
        description="API key used for Jellyfin access.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="plex_base_url",
        label="Plex Base URL",
        group="Integrations",
        description="Base Plex Media Server URL when Plex is the active media server.",
        placeholder="http://plex:32400",
    ),
    SettingFieldDefinition(
        key="plex_api_token",
        label="Plex API Token",
        group="Integrations",
        description="Plex token used for history and metadata requests.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="plex_client_identifier",
        label="Plex Client Identifier",
        group="Integrations",
        description="Stable identifier sent with Plex API requests.",
        placeholder="vanguarr",
    ),
    SettingFieldDefinition(
        key="seer_base_url",
        label="Seer Base URL",
        group="Integrations",
        description="Base URL for Jellyseerr or another Seer-compatible API.",
        placeholder="http://jellyseerr:5055",
    ),
    SettingFieldDefinition(
        key="seer_api_key",
        label="Seer API Key",
        group="Integrations",
        description="API key used for discovery and request creation.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="seer_request_user_id",
        label="Seer Request User ID",
        group="Integrations",
        description="Optional request owner override.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="seer_webhook_token",
        label="Seer Webhook Token",
        group="Integrations",
        description="Bearer token expected on Seer webhook deliveries.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="suggestions_api_key",
        label="Suggestions API Key",
        group="Integrations",
        description="Bearer token the Jellyfin Vanguarr plugin uses when fetching per-user suggestions.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="tmdb_base_url",
        label="TMDb Base URL",
        group="TMDb",
        description="TMDb API base URL.",
        placeholder="https://api.themoviedb.org/3",
    ),
    SettingFieldDefinition(
        key="tmdb_api_read_access_token",
        label="TMDb Read Token",
        group="TMDb",
        description="Preferred TMDb auth method.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="tmdb_api_key",
        label="TMDb API Key",
        group="TMDb",
        description="Alternative TMDb auth method.",
        input_type="password",
    ),
    SettingFieldDefinition(
        key="tmdb_language",
        label="TMDb Language",
        group="TMDb",
        description="Language used for TMDb metadata lookups.",
        placeholder="en-US",
    ),
    SettingFieldDefinition(
        key="tmdb_watch_region",
        label="TMDb Watch Region",
        group="TMDb",
        description="Region used for watch provider and certification lookups.",
        placeholder="US",
    ),
    SettingFieldDefinition(
        key="scheduler_enabled",
        label="Scheduler Enabled",
        group="Scheduling",
        description="Turns the built-in scheduler on or off immediately.",
        input_type="checkbox",
    ),
    SettingFieldDefinition(
        key="profile_cron",
        label="Profile Cron",
        group="Scheduling",
        description="Cron expression for Profile Architect.",
        placeholder="0 3 * * 0",
    ),
    SettingFieldDefinition(
        key="decision_cron",
        label="Decision Cron",
        group="Scheduling",
        description="Cron expression for Decision Engine.",
        placeholder="0 4 * * *",
    ),
    SettingFieldDefinition(
        key="global_exclusions",
        label="Global Exclusions",
        group="Tuning",
        description="Comma-separated guardrails applied to every decision.",
        input_type="textarea",
        placeholder="No Horror,No Reality TV",
    ),
    SettingFieldDefinition(
        key="request_threshold",
        label="Request Threshold",
        group="Tuning",
        description="Minimum final blended score required to request media.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="decision_ai_weight_percent",
        label="AI Decision Weight",
        group="Tuning",
        description="Choose how much the final decision score leans on the LLM versus the code-driven score. 0% is all code. 100% is all AI.",
        input_type="range",
        min_value="0",
        max_value="100",
        step="5",
    ),
    SettingFieldDefinition(
        key="profile_history_limit",
        label="Profile History Limit",
        group="Tuning",
        description="How many playback events are used per user.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="profile_architect_max_output_tokens",
        label="Profile Architect Max Output Tokens",
        group="Tuning",
        description="Maximum tokens for profile architect prompts.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="profile_architect_top_titles_limit",
        label="Profile Top Titles Limit",
        group="Tuning",
        description="Maximum top titles retained from history.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="profile_architect_recent_momentum_limit",
        label="Profile Recent Momentum Limit",
        group="Tuning",
        description="How many recent momentum items are retained.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="profile_llm_enrichment_enabled",
        label="Profile LLM Enrichment Enabled",
        group="Tuning",
        description="Toggle profile-side adjacent-lane enrichment.",
        input_type="checkbox",
    ),
    SettingFieldDefinition(
        key="candidate_limit",
        label="Candidate Limit",
        group="Tuning",
        description="Maximum blended recommendation pool size.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="genre_candidate_limit",
        label="Genre Candidate Limit",
        group="Tuning",
        description="Maximum candidates pulled from Seer genre discovery across primary, recent, and adjacent genres.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="trending_candidate_limit",
        label="Trending Candidate Limit",
        group="Tuning",
        description="Maximum trending titles mixed into the pool.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="decision_shortlist_limit",
        label="Decision Shortlist Limit",
        group="Tuning",
        description="Diversified shortlist size before final voting.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="recommendation_seed_limit",
        label="Recommendation Seed Limit",
        group="Tuning",
        description="Maximum watch-history seeds per user.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="tmdb_seed_enrichment_limit",
        label="TMDb Seed Enrichment Limit",
        group="Tuning",
        description="How many watched seeds receive TMDb enrichment.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="tmdb_candidate_enrichment_limit",
        label="TMDb Candidate Enrichment Limit",
        group="Tuning",
        description="How many ranked candidates receive TMDb enrichment.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="decision_page_size",
        label="Decision Page Size",
        group="Tuning",
        description="Maximum decision rows shown in the War Room.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="suggestions_enabled",
        label="Suggested For You Enabled",
        group="Tuning",
        description="Toggle per-user suggested playlist generation for Jellyfin.",
        input_type="checkbox",
    ),
    SettingFieldDefinition(
        key="suggestions_limit",
        label="Suggested For You Limit",
        group="Tuning",
        description="How many ranked available titles are stored per user for the Jellyfin plugin.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="llm_temperature",
        label="LLM Temperature",
        group="LLM",
        description="Fallback temperature used across LLM providers.",
        input_type="number",
    ),
    SettingFieldDefinition(
        key="llm_timeout_seconds",
        label="LLM Timeout Seconds",
        group="LLM",
        description="Global timeout fallback when a provider timeout is blank.",
        input_type="number",
    ),
)

DB_MANAGED_SETTING_KEYS = frozenset(field.key for field in DB_MANAGED_SETTING_FIELDS)

BOOTSTRAP_ONLY_SETTING_KEYS = frozenset(
    {
        "app_env",
        "app_host",
        "app_port",
        "data_dir",
        "database_url",
        "profiles_dir",
        "logs_dir",
        "log_file",
        "llm_provider",
        "llm_model",
        "ollama_api_base",
        "openai_api_key",
        "openai_api_base",
        "anthropic_api_key",
        "anthropic_api_base",
        "llm_providers",
    }
)

LEGACY_LLM_ENV_KEYS = frozenset(
    {
        "llm_provider",
        "llm_model",
        "llm_timeout_seconds",
        "ollama_api_base",
        "openai_api_key",
        "openai_api_base",
        "anthropic_api_key",
        "anthropic_api_base",
    }
)


def serialize_setting_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
