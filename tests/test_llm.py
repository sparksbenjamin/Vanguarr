import asyncio

from app.api.llm import LLMClient
from app.core.settings import LLMProviderSettings, Settings


def test_ollama_bare_model_name_is_normalized() -> None:
    settings = Settings(
        llm_provider="ollama",
        llm_model="glm-4.7-flash:latest",
        ollama_api_base="http://host.docker.internal:11434",
    )

    client = LLMClient(settings)
    kwargs = client._build_completion_kwargs(max_tokens=32, temperature=0, timeout_seconds=12)

    assert kwargs["model"] == "ollama/glm-4.7-flash:latest"


def test_ollama_prefixed_model_name_is_preserved() -> None:
    settings = Settings(
        llm_provider="ollama",
        llm_model="ollama/glm-4.7-flash:latest",
        ollama_api_base="http://host.docker.internal:11434",
    )

    client = LLMClient(settings)
    kwargs = client._build_completion_kwargs(max_tokens=32, temperature=0, timeout_seconds=12)

    assert kwargs["model"] == "ollama/glm-4.7-flash:latest"


def test_provider_max_output_tokens_are_omitted_when_blank() -> None:
    provider = LLMProviderSettings(
        name="Runtime Ollama",
        provider="ollama",
        model="qwen3:8b",
        api_base="http://host.docker.internal:11434",
        max_output_tokens=None,
    )
    settings = Settings(
        llm_max_output_tokens=999,
        llm_providers=(provider,),
        ollama_api_base="http://host.docker.internal:11434",
    )

    client = LLMClient(settings)
    kwargs = client._build_completion_kwargs(
        settings=settings,
        provider=provider,
        max_tokens=None,
        temperature=0,
        timeout_seconds=12,
    )

    assert "max_tokens" not in kwargs


def test_provider_max_output_tokens_are_used_when_present() -> None:
    provider = LLMProviderSettings(
        name="Runtime Ollama",
        provider="ollama",
        model="qwen3:8b",
        api_base="http://host.docker.internal:11434",
        max_output_tokens=1200,
    )
    settings = Settings(
        llm_providers=(provider,),
        ollama_api_base="http://host.docker.internal:11434",
    )

    client = LLMClient(settings)
    kwargs = client._build_completion_kwargs(
        settings=settings,
        provider=provider,
        max_tokens=None,
        temperature=0,
        timeout_seconds=12,
    )

    assert kwargs["max_tokens"] == 1200


def test_generate_messages_uses_role_specific_provider_chain(monkeypatch) -> None:
    decision_provider = LLMProviderSettings(
        name="Decision Ollama",
        provider="ollama",
        model="qwen3:8b",
        api_base="http://host.docker.internal:11434",
        use_for_decision=True,
        use_for_profile_enrichment=False,
    )
    enrichment_provider = LLMProviderSettings(
        name="Enrichment OpenAI",
        provider="openai",
        model="gpt-4.1-mini",
        api_key="test-key",
        use_for_decision=False,
        use_for_profile_enrichment=True,
    )
    settings = Settings(
        llm_providers=(decision_provider, enrichment_provider),
        ollama_api_base="http://host.docker.internal:11434",
        openai_api_key="test-key",
    )

    async def fake_generate_with_provider(self, *, settings, provider, messages, max_tokens, temperature, timeout_seconds):
        return provider.name

    monkeypatch.setattr(LLMClient, "_generate_messages_with_provider", fake_generate_with_provider)

    client = LLMClient(settings)
    decision_result = asyncio.run(
        client.generate_messages(messages=[{"role": "user", "content": "hi"}], purpose="decision")
    )
    enrichment_result = asyncio.run(
        client.generate_messages(messages=[{"role": "user", "content": "hi"}], purpose="profile_enrichment")
    )

    assert decision_result == "Decision Ollama"
    assert enrichment_result == "Enrichment OpenAI"
