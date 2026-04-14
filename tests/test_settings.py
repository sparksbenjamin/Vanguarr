from app.api.tmdb import TMDbClient
from app.core.settings import LLMProviderSettings, Settings


def test_blank_seer_request_user_id_is_none() -> None:
    settings = Settings(SEER_REQUEST_USER_ID="")

    assert settings.seer_request_user_id is None


def test_ollama_timeout_defaults_to_180_when_blank() -> None:
    settings = Settings(llm_provider="ollama", llm_timeout_seconds="")

    assert settings.llm_timeout_seconds is None
    assert settings.effective_llm_timeout_seconds == 180


def test_hosted_timeout_defaults_to_45_when_blank() -> None:
    settings = Settings(llm_provider="openai", llm_timeout_seconds="")

    assert settings.llm_timeout_seconds is None
    assert settings.effective_llm_timeout_seconds == 45


def test_profile_architect_max_tokens_default() -> None:
    settings = Settings()

    assert settings.profile_architect_max_output_tokens == 384


def test_profile_architect_grouping_defaults() -> None:
    settings = Settings()

    assert settings.profile_use_full_history is False
    assert settings.profile_architect_top_titles_limit == 8
    assert settings.profile_architect_recent_momentum_limit == 5


def test_profile_llm_enrichment_defaults() -> None:
    settings = Settings()

    assert settings.profile_llm_enrichment_enabled is True
    assert settings.profile_llm_enrichment_max_output_tokens == 120


def test_decision_funnel_defaults() -> None:
    settings = Settings()

    assert settings.decision_ai_weight_percent == 25
    assert settings.candidate_limit == 160
    assert settings.genre_candidate_limit == 30
    assert settings.trending_candidate_limit == 100
    assert settings.decision_shortlist_limit == 15
    assert settings.recommendation_seed_limit == 6
    assert settings.tmdb_seed_enrichment_limit == 6
    assert settings.tmdb_candidate_enrichment_limit == 30


def test_suggested_for_you_defaults() -> None:
    settings = Settings()

    assert settings.suggestions_enabled is True
    assert settings.suggestions_limit == 20
    assert settings.suggestion_ai_threshold == 0.58
    assert settings.suggestion_ai_candidate_limit == 24
    assert settings.suggestion_recent_cooldown_days == 14
    assert settings.suggestion_repeat_watch_cutoff == 3
    assert settings.library_sync_enabled is True
    assert settings.library_sync_cron == "0 */4 * * *"


def test_tmdb_defaults() -> None:
    settings = Settings()

    assert settings.tmdb_base_url == "https://api.themoviedb.org/3"
    assert settings.tmdb_language == "en-US"
    assert settings.tmdb_watch_region == "US"


def test_blank_tmdb_credentials_become_none() -> None:
    settings = Settings(TMDB_API_READ_ACCESS_TOKEN="", TMDB_API_KEY="")

    assert settings.tmdb_api_read_access_token is None
    assert settings.tmdb_api_key is None


def test_tmdb_client_is_enabled_with_api_key() -> None:
    settings = Settings(tmdb_api_key="test-key")

    assert TMDbClient(settings).enabled is True


def test_blank_suggestion_tokens_become_none() -> None:
    settings = Settings(SEER_WEBHOOK_TOKEN="", SUGGESTIONS_API_KEY="")

    assert settings.seer_webhook_token is None
    assert settings.suggestions_api_key is None


def test_provider_role_selection_supports_separate_chains() -> None:
    settings = Settings(
        llm_provider="openai",
        llm_model="gpt-4.1-mini",
        llm_providers=(
            LLMProviderSettings(
                id=2,
                name="Decision Ollama",
                provider="ollama",
                model="qwen3:8b",
                priority=2,
                enabled=True,
                use_for_decision=True,
                use_for_profile_enrichment=False,
            ),
            LLMProviderSettings(
                id=1,
                name="Enrichment OpenAI",
                provider="openai",
                model="gpt-4.1-mini",
                priority=1,
                enabled=True,
                use_for_decision=False,
                use_for_profile_enrichment=True,
            ),
        ),
    )

    assert [provider.name for provider in settings.active_llm_providers] == [
        "Enrichment OpenAI",
        "Decision Ollama",
    ]
    assert [provider.name for provider in settings.decision_llm_providers] == ["Decision Ollama"]
    assert [provider.name for provider in settings.profile_enrichment_llm_providers] == [
        "Enrichment OpenAI",
    ]


def test_role_specific_provider_selection_does_not_fall_back_to_legacy_when_rows_exist() -> None:
    settings = Settings(
        llm_provider="openai",
        llm_model="gpt-4.1-mini",
        llm_providers=(
            LLMProviderSettings(
                name="Enrichment Only",
                provider="ollama",
                model="qwen3:8b",
                enabled=True,
                use_for_decision=False,
                use_for_profile_enrichment=True,
            ),
        ),
    )

    assert settings.decision_llm_providers == ()
    assert [provider.name for provider in settings.profile_enrichment_llm_providers] == ["Enrichment Only"]


def test_provider_max_output_tokens_blank_or_zero_becomes_none() -> None:
    blank = LLMProviderSettings(provider="ollama", model="qwen3:8b", max_output_tokens="")
    zero = LLMProviderSettings(provider="ollama", model="qwen3:8b", max_output_tokens="0")

    assert blank.max_output_tokens is None
    assert zero.max_output_tokens is None


def test_decision_ai_weight_percent_must_stay_within_range() -> None:
    settings = Settings(decision_ai_weight_percent="75")

    assert settings.decision_ai_weight_percent == 75


def test_suggestion_ai_threshold_must_stay_within_range() -> None:
    settings = Settings(suggestion_ai_threshold="0.67")

    assert settings.suggestion_ai_threshold == 0.67
