from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.api.base import ClientConfigError, ConnectionCheck
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
    assert "Suggested For You" in response.text


def test_startup_recovers_interrupted_tasks(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_recover(self) -> int:
        calls["count"] += 1
        return 0

    monkeypatch.setattr("app.main.VanguarrService.recover_interrupted_tasks", fake_recover)

    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert calls["count"] == 1


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


def test_tuning_settings_page_shows_ai_weight_slider() -> None:
    with TestClient(app) as client:
        response = client.get("/settings/tuning")

    assert response.status_code == 200
    assert "AI Decision Weight" in response.text
    assert "Genre Candidate Limit" in response.text
    assert 'type="range"' in response.text
    assert "% AI /" in response.text


def test_integrations_settings_page_shows_jellyfin_plugin_install_action() -> None:
    with TestClient(app) as client:
        response = client.get("/settings/integrations")

    assert response.status_code == 200
    assert "Install the Vanguarr Jellyfin plugin" in response.text
    assert "/api/settings/integrations/jellyfin-plugin/install" in response.text
    assert "jellyfin-plugin/manifest.json" in response.text


def test_scheduling_settings_page_shows_library_sync_box() -> None:
    with TestClient(app) as client:
        response = client.get("/settings/scheduling")

    assert response.status_code == 200
    assert "Library Sync Enabled" in response.text
    assert "Library Sync Cron" in response.text
    assert "Keep Suggested For You aligned with the real library" in response.text
    assert "/api/settings/scheduling/library-sync/run" in response.text
    assert "/api/settings/scheduling/library-sync/status" in response.text
    assert "Current Sync Status" in response.text
    assert "Suggestion refresh" in response.text


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
    assert "Delete Provider" in response.text
    assert "Test Provider" in response.text
    assert "Load Ollama Models" in response.text
    assert "Use For Decisions" in response.text
    assert "Use For Profile Enrichment" in response.text
    assert "Blank for unlimited" in response.text


def test_llm_provider_delete_endpoint(monkeypatch) -> None:
    deleted: dict[str, object] = {}

    with TestClient(app) as client:
        existing_provider = SimpleNamespace(id=7, name="Primary Ollama")
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(llm_providers=(existing_provider,)),
        )

        def fake_save_settings(setting_values, provider_payloads):
            deleted["setting_values"] = setting_values
            deleted["provider_payloads"] = provider_payloads
            return SimpleNamespace(llm_providers=())

        monkeypatch.setattr(client.app.state.settings.manager, "save_settings", fake_save_settings)
        monkeypatch.setattr("app.main.apply_runtime_settings", lambda app, force=False: SimpleNamespace())

        response = client.post("/api/settings/llm/provider-delete/7", json={})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["provider_id"] == 7
    assert deleted["setting_values"] == {}
    assert deleted["provider_payloads"] == [{"id": 7, "delete": True}]


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
                "max_output_tokens": "",
                "use_for_decision": True,
                "use_for_profile_enrichment": True,
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
                "max_output_tokens": "",
                "use_for_decision": True,
                "use_for_profile_enrichment": True,
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
    assert "Suggested For You Preview" in response.text
    assert "/manifest/actions/suggested-for-you" in response.text


def test_manifest_page_renders_suggestion_preview_for_selected_user(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(client.app.state.vanguarr, "list_profiles", lambda: ["alice"])
        monkeypatch.setattr(client.app.state.vanguarr, "read_profile", lambda username: '{"username": "alice"}')
        monkeypatch.setattr(client.app.state.vanguarr, "read_profile_summary", lambda username: "profile summary")
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_suggestions",
            lambda username=None, jellyfin_user_id=None, limit=None: [
                SimpleNamespace(
                    rank=1,
                    title="Arrival",
                    media_type="movie",
                    production_year=2016,
                    score=0.91,
                    state="available",
                    overview="First contact drama.",
                    reasoning="Matches sci-fi preference and avoids top-repeat comfort titles.",
                )
            ],
        )

        response = client.get("/manifest?username=alice")

    assert response.status_code == 200
    assert "Showing 1 of" in response.text
    assert "Arrival" in response.text
    assert "Matches sci-fi preference and avoids top-repeat comfort titles." in response.text


def test_manifest_suggested_for_you_action_redirects_back_to_manifest(monkeypatch) -> None:
    with TestClient(app) as client:
        launches: list[str | None] = []

        def fake_launch(username: str | None) -> tuple[bool, str]:
            launches.append(username)
            return True, "Suggested For You started in the background for alice."

        monkeypatch.setattr(client.app.state.background_runner, "launch_suggested_for_you", fake_launch)

        response = client.post(
            "/manifest/actions/suggested-for-you",
            data={"username": "alice"},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/manifest?username=alice&toast=Suggested+For+You+started+in+the+background+for+alice."
    assert launches == ["alice"]


def test_profile_architect_action_redirects_immediately_with_background_toast(monkeypatch) -> None:
    with TestClient(app) as client:
        launches: list[str | None] = []

        def fake_launch(username: str | None) -> tuple[bool, str]:
            launches.append(username)
            return True, "Profile Architect started in the background for admin."

        monkeypatch.setattr(client.app.state.background_runner, "launch_profile_architect", fake_launch)

        response = client.post(
            "/actions/profile-architect",
            data={"username": "admin"},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/?toast=Profile+Architect+started+in+the+background+for+admin."
    assert launches == ["admin"]


def test_decision_engine_action_reports_existing_background_run(monkeypatch) -> None:
    with TestClient(app) as client:
        launches: list[str | None] = []

        def fake_launch(username: str | None) -> tuple[bool, str]:
            launches.append(username)
            return False, "Decision Engine is already running."

        monkeypatch.setattr(client.app.state.background_runner, "launch_decision_engine", fake_launch)

        response = client.post(
            "/actions/decision-engine",
            data={"username": ""},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/?toast=Decision+Engine+is+already+running."
    assert launches == [None]


def test_suggested_for_you_action_redirects_immediately_with_background_toast(monkeypatch) -> None:
    with TestClient(app) as client:
        launches: list[str | None] = []

        def fake_launch(username: str | None) -> tuple[bool, str]:
            launches.append(username)
            return True, "Suggested For You started in the background for admin."

        monkeypatch.setattr(client.app.state.background_runner, "launch_suggested_for_you", fake_launch)

        response = client.post(
            "/actions/suggested-for-you",
            data={"username": "admin"},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/?toast=Suggested+For+You+started+in+the+background+for+admin."
    assert launches == ["admin"]


def test_jellyfin_suggestions_api_requires_bearer_token(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(suggestions_api_key="top-secret"),
        )

        response = client.get("/api/jellyfin/suggestions?username=alice")

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid suggestions API token."


def test_jellyfin_suggestions_api_returns_ranked_items(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(suggestions_api_key="top-secret"),
        )

        captured: dict[str, object] = {}

        def fake_get_suggestions(username=None, jellyfin_user_id=None, limit=None):
            captured["username"] = username
            captured["jellyfin_user_id"] = jellyfin_user_id
            captured["limit"] = limit
            return [
                SimpleNamespace(
                    username="alice",
                    jellyfin_user_id="66456a3a4cd346e383ce254e99d4b09a",
                    rank=1,
                    media_type="movie",
                    title="Arrival",
                    overview="First contact drama.",
                    production_year=2016,
                    score=0.91,
                    reasoning="Matches sci-fi preference.",
                    state="available",
                    tmdb_id=329865,
                    tvdb_id=None,
                    imdb_id="tt2543164",
                )
            ]

        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_suggestions",
            fake_get_suggestions,
        )

        response = client.get(
            "/api/jellyfin/suggestions?username=alice&user_id=66456a3a-4cd3-46e3-83ce-254e99d4b09a&limit=5",
            headers={"Authorization": "Bearer top-secret"},
        )

    assert response.status_code == 200
    assert captured["username"] == "alice"
    assert captured["jellyfin_user_id"] == "66456a3a4cd346e383ce254e99d4b09a"
    assert captured["limit"] == 5
    payload = response.json()
    assert payload["username"] == "alice"
    assert payload["jellyfin_user_id"] == "66456a3a4cd346e383ce254e99d4b09a"
    assert payload["count"] == 1
    assert payload["items"][0]["title"] == "Arrival"
    assert payload["items"][0]["external_ids"] == {
        "tmdb": "329865",
        "imdb": "tt2543164",
    }


def test_seer_webhook_requires_bearer_token(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(seer_webhook_token="hook-secret"),
        )

        response = client.post("/api/webhooks/seer", json={"notification_type": "MEDIA_AVAILABLE"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid Seer webhook token."


def test_seer_webhook_accepts_payload_and_calls_service(monkeypatch) -> None:
    received: dict[str, object] = {}

    async def fake_ingest(payload: dict[str, object]) -> dict[str, object]:
        received["payload"] = payload
        return {"status": "accepted", "refreshed_suggestions": True}

    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(seer_webhook_token="hook-secret"),
        )
        monkeypatch.setattr(client.app.state.vanguarr, "ingest_seer_webhook", fake_ingest)

        response = client.post(
            "/api/webhooks/seer",
            headers={"Authorization": "Bearer hook-secret"},
            json={
                "notification_type": "MEDIA_AVAILABLE",
                "requested_by": "alice",
                "media_tmdbid": 329865,
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "refreshed_suggestions": True}
    assert received["payload"] == {
        "notification_type": "MEDIA_AVAILABLE",
        "requested_by": "alice",
        "media_tmdbid": 329865,
    }


def test_install_jellyfin_plugin_endpoint_returns_success(monkeypatch) -> None:
    async def fake_install() -> dict[str, object]:
        return {
            "plugin_name": "Vanguarr",
            "repository_added": True,
            "plugin_install_requested": True,
            "restart_required": True,
            "detail": "Added the Vanguarr plugin repository to Jellyfin. Requested Vanguarr plugin installation from the configured Jellyfin repository. Restart Jellyfin after the install finishes so the plugin can load.",
        }

    with TestClient(app) as client:
        monkeypatch.setattr(client.app.state.vanguarr, "install_jellyfin_plugin", fake_install)
        response = client.post("/api/settings/integrations/jellyfin-plugin/install")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["plugin_name"] == "Vanguarr"
    assert response.json()["repository_added"] is True
    assert response.json()["plugin_install_requested"] is True


def test_install_jellyfin_plugin_endpoint_returns_validation_error(monkeypatch) -> None:
    async def fake_install() -> dict[str, object]:
        raise ClientConfigError("JELLYFIN_API_KEY is required to install Jellyfin plugins.")

    with TestClient(app) as client:
        monkeypatch.setattr(client.app.state.vanguarr, "install_jellyfin_plugin", fake_install)
        response = client.post("/api/settings/integrations/jellyfin-plugin/install")

    assert response.status_code == 400
    assert response.json() == {
        "ok": False,
        "detail": "JELLYFIN_API_KEY is required to install Jellyfin plugins.",
    }


def test_run_library_sync_endpoint_queues_background_job(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(normalized_media_server_provider="jellyfin"),
        )

        monkeypatch.setattr(
            client.app.state.background_runner,
            "launch_library_sync",
            lambda: (True, "Library Sync started in the background for the Jellyfin library."),
        )

        response = client.post("/api/settings/scheduling/library-sync/run")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "started": True,
        "detail": "Library Sync started in the background for the Jellyfin library.",
    }


def test_run_library_sync_endpoint_requires_jellyfin(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.settings,
            "snapshot",
            lambda force=False: SimpleNamespace(normalized_media_server_provider="plex"),
        )

        response = client.post("/api/settings/scheduling/library-sync/run")

    assert response.status_code == 400
    assert response.json() == {
        "ok": False,
        "detail": "Library Sync currently requires Jellyfin as the active media server.",
    }


def test_library_sync_status_endpoint_returns_snapshot(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_task_snapshot",
            lambda engine_name: {
                "id": 21,
                "engine": engine_name,
                "status": "running",
                "summary": "Indexing Movies.",
                "started_at": "2026-04-09T22:15:00",
                "finished_at": None,
                "progress_current": 1,
                "progress_total": 3,
                "percent": 33.3,
                "current_label": "Movies",
                "detail": {
                    "phase": "indexing",
                    "libraries": [{"name": "Movies", "state": "running"}],
                    "suggestion_refresh": {"state": "pending", "completed_users": 0, "total_users": 5},
                },
            },
        )
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_library_sync_snapshot",
            lambda: {
                "total_items": 1200,
                "available_items": 1190,
                "removed_items": 10,
                "movies": 700,
                "series": 490,
                "last_seen_at": None,
                "last_task": None,
            },
        )

        response = client.get("/api/settings/scheduling/library-sync/status")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "task": {
            "id": 21,
            "engine": "library_sync",
            "status": "running",
            "summary": "Indexing Movies.",
            "started_at": "2026-04-09T22:15:00",
            "finished_at": None,
            "progress_current": 1,
            "progress_total": 3,
            "percent": 33.3,
            "current_label": "Movies",
            "detail": {
                "phase": "indexing",
                "libraries": [{"name": "Movies", "state": "running"}],
                "suggestion_refresh": {"state": "pending", "completed_users": 0, "total_users": 5},
            },
        },
        "snapshot": {
            "total_items": 1200,
            "available_items": 1190,
            "removed_items": 10,
            "movies": 700,
            "series": 490,
            "last_seen_at": None,
            "last_task": {
                "id": 21,
                "engine": "library_sync",
                "status": "running",
                "summary": "Indexing Movies.",
                "started_at": "2026-04-09T22:15:00",
                "finished_at": None,
                "progress_current": 1,
                "progress_total": 3,
                "percent": 33.3,
                "current_label": "Movies",
                "detail": {
                    "phase": "indexing",
                    "libraries": [{"name": "Movies", "state": "running"}],
                    "suggestion_refresh": {"state": "pending", "completed_users": 0, "total_users": 5},
                },
            },
        },
    }
