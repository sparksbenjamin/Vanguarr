from app.core.settings import Settings


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

    assert settings.profile_architect_top_titles_limit == 8
    assert settings.profile_architect_recent_momentum_limit == 5


def test_profile_llm_enrichment_defaults() -> None:
    settings = Settings()

    assert settings.profile_llm_enrichment_enabled is True
    assert settings.profile_llm_enrichment_max_output_tokens == 120
