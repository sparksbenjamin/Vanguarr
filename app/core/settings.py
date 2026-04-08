from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "config"
DEFAULT_PROFILES_DIR = CONFIG_DIR / "profiles"


class Settings(BaseSettings):
    app_name: str = "Vanguarr"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    timezone: str = Field(default="America/New_York", alias="TZ")

    database_url: str = f"sqlite:///{(CONFIG_DIR / 'vanguarr.db').as_posix()}"
    profiles_dir: Path = DEFAULT_PROFILES_DIR
    global_exclusions: str = "No Horror,No Reality TV"
    request_threshold: float = 0.72
    scheduler_enabled: bool = True
    profile_cron: str = "0 3 * * 0"
    decision_cron: str = "0 4 * * *"
    health_cache_seconds: int = 30
    profile_history_limit: int = 40
    candidate_limit: int = 25
    recommendation_seed_limit: int = 3
    decision_page_size: int = 100

    jellyfin_base_url: str | None = None
    jellyfin_api_key: str | None = None
    seer_base_url: str | None = None
    seer_api_key: str | None = None
    seer_request_user_id: int | None = None

    llm_provider: str = "ollama"
    llm_model: str = "ollama/llama3.1:8b"
    llm_temperature: float = 0.2
    llm_max_output_tokens: int = 700
    llm_timeout_seconds: int = 45
    ollama_api_base: str = "http://ollama:11434"
    openai_api_key: str | None = None
    openai_api_base: str | None = None
    anthropic_api_key: str | None = None
    anthropic_api_base: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    def ensure_runtime_dirs(self) -> None:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        self.profiles_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
