from app.api.llm import LLMClient
from app.core.settings import Settings


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
