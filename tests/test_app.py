from fastapi.testclient import TestClient

from app.api.base import ConnectionCheck
from app.api.llm import LLMClient
from app.main import app


def test_healthz_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_dashboard_renders() -> None:
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "Vanguarr" in response.text


def test_settings_page_renders() -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    assert "General settings" in response.text
    assert "Save General Settings" in response.text
    assert "/settings/llm-providers" in response.text
    assert 'data-settings-group' in response.text
    assert 'data-settings-open="true"' in response.text
    assert 'href="/manifest"' in response.text


def test_settings_root_redirects_to_general() -> None:
    with TestClient(app) as client:
        response = client.get("/settings", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/settings/general"


def test_llm_providers_page_renders() -> None:
    with TestClient(app) as client:
        response = client.get("/settings/llm-providers")

    assert response.status_code == 200
    assert "Priority-ordered failover chain" in response.text
    assert "Test Provider" in response.text
    assert "Load Ollama Models" in response.text


def test_llm_provider_test_endpoint(monkeypatch) -> None:
    async def fake_test_provider(self: LLMClient, provider) -> ConnectionCheck:
        assert provider.provider == "ollama"
        assert provider.model == "llama3.1:8b"
        return ConnectionCheck(
            service="LLM",
            ok=True,
            detail="Provider test passed.",
            meta={"provider": provider.provider, "model": provider.model},
        )

    monkeypatch.setattr(LLMClient, "test_provider", fake_test_provider)

    with TestClient(app) as client:
        response = client.post(
            "/api/settings/llm/provider-test",
            json={
                "name": "Primary Ollama",
                "provider": "ollama",
                "model": "llama3.1:8b",
                "priority": 1,
                "enabled": True,
                "api_base": "http://ollama:11434",
                "api_key": "",
                "timeout_seconds": "",
            },
        )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["detail"] == "Provider test passed."


def test_ollama_models_endpoint(monkeypatch) -> None:
    async def fake_list_ollama_models(self: LLMClient, provider) -> list[str]:
        assert provider.provider == "ollama"
        return ["llama3.1:8b", "qwen3:8b"]

    monkeypatch.setattr(LLMClient, "list_ollama_models", fake_list_ollama_models)

    with TestClient(app) as client:
        response = client.post(
            "/api/settings/llm/ollama-models",
            json={
                "name": "Primary Ollama",
                "provider": "ollama",
                "model": "",
                "priority": 1,
                "enabled": True,
                "api_base": "http://ollama:11434",
                "api_key": "",
                "timeout_seconds": "",
            },
        )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["models"] == ["llama3.1:8b", "qwen3:8b"]


def test_manifest_page_shows_profiles_under_settings_group() -> None:
    with TestClient(app) as client:
        response = client.get("/manifest")

    assert response.status_code == 200
    assert 'data-settings-open="true"' in response.text
    assert 'href="/manifest"' in response.text
    assert "settings-subnav-link-active" in response.text
