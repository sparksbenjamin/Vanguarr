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
    assert "v0.2.2" in response.text
    assert "Version 0.2.2" in response.text


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


def test_tuning_settings_page_shows_history_and_weight_controls() -> None:
    with TestClient(app) as client:
        response = client.get("/settings/tuning")

    assert response.status_code == 200
    assert "AI Decision Weight" in response.text
    assert "Use Full Playback History" in response.text
    assert "Recent Momentum Weight" in response.text
    assert "Genre Candidate Limit" in response.text
    assert "Suggestion AI Threshold" in response.text
    assert "Suggestion Recent Cooldown Days" in response.text
    assert 'type="range"' in response.text
    assert "% AI" in response.text
    assert "% code" in response.text
    assert "% recent boost" in response.text
    assert "More long-term" in response.text
    assert "More recent" in response.text


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


def test_logs_page_shows_filters_and_live_feed_controls() -> None:
    with TestClient(app) as client:
        response = client.get("/logs")

    assert response.status_code == 200
    assert "Live operations feed" in response.text
    assert "Suggestions" in response.text
    assert "/api/logs" in response.text
    assert "Previous" in response.text
    assert "Next" in response.text


def test_logs_api_returns_feed_payload(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_log_feed",
            lambda **kwargs: {
                "rows": [
                    {
                        "id": 1,
                        "created_at": "2026-04-10T12:00:00Z",
                        "created_at_display": "2026-04-10 12:00:00",
                        "engine": "suggested_for_you",
                        "engine_label": "Suggestion",
                        "username": "alice",
                        "media_type": "movie",
                        "media_id": 101,
                        "media_title": "Arrival",
                        "source": "library:indexed",
                        "decision": "SUGGEST",
                        "confidence": 0.88,
                        "threshold": 0.58,
                        "requested": False,
                        "request_id": None,
                        "reasoning": "Strong fit.",
                        "error": None,
                    }
                ],
                "raw_rows": [],
                "query": "arrival",
                "view": "suggestions",
                "sort_by": "confidence",
                "sort_direction": "desc",
                "page": 1,
                "page_size": 25,
                "total_rows": 1,
                "total_pages": 1,
                "has_previous": False,
                "has_next": False,
                "view_counts": {"all": 5, "requests": 3, "suggestions": 2},
                "error_rows": 0,
                "generated_at": "2026-04-10T12:00:00Z",
            },
        )

        response = client.get("/api/logs?q=arrival&view=suggestions&sort=confidence&dir=desc&page=1")

    assert response.status_code == 200
    assert response.json()["view"] == "suggestions"
    assert response.json()["rows"][0]["decision"] == "SUGGEST"


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
    assert "Profile Actions" in response.text
    assert "Suggested For You Preview" in response.text
    assert "/api/manifest/task-status" in response.text
    assert "Load a profile first to run Profile Architect" in response.text


def test_manifest_page_renders_suggestion_preview_for_selected_user(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(client.app.state.vanguarr, "list_profiles", lambda: ["alice"])
        monkeypatch.setattr(client.app.state.vanguarr, "read_profile", lambda username: '{"username": "alice"}')
        monkeypatch.setattr(client.app.state.vanguarr, "read_profile_summary", lambda username: "profile summary")
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_profile_payload_with_live_context",
            lambda username: {
                "profile_review": {
                    "health_score": 78,
                    "health_status": "healthy",
                    "freshness": "fresh",
                    "confidence": "medium",
                    "summary": "Profile health 78/100.",
                    "warnings": ["Recent rebuild looks healthy."],
                    "strengths": ["Recent momentum is captured."],
                    "diff_summary": ["Primary genres: +Sci-Fi"],
                    "evidence": {"history_items": 24, "unique_titles": 12},
                },
                "explicit_feedback": {
                    "liked_titles": ["Arrival"],
                    "disliked_titles": [],
                    "liked_genres": ["Sci-Fi"],
                    "disliked_genres": [],
                },
                "blocked_titles": ["Anime Trap"],
                "request_outcome_insights": {
                    "counts": {"approved": 2, "downloaded": 1},
                    "positive_titles": ["Arrival"],
                    "negative_titles": [],
                    "positive_genres": ["Sci-Fi"],
                    "negative_genres": [],
                },
            },
        )
        monkeypatch.setattr(client.app.state.vanguarr, "get_request_history", lambda username, limit=8: [])
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_profile_task_snapshots",
            lambda username: {
                "profile_architect": {"status": "idle", "summary": "No runs yet.", "progress_total": 0, "progress_current": 0, "percent": 0.0, "current_label": "Ready"},
                "decision_engine": {"status": "idle", "summary": "No runs yet.", "progress_total": 0, "progress_current": 0, "percent": 0.0, "current_label": "Ready"},
                "suggested_for_you": {"status": "success", "summary": "Stored snapshot.", "progress_total": 5, "progress_current": 5, "percent": 100.0, "current_label": "alice"},
            },
        )
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
    assert "Run Profile Architect" in response.text
    assert "Run Decision Engine" in response.text
    assert "Refresh Suggestions" in response.text
    assert "Last run: Never" in response.text or "Last run:" in response.text
    assert "Health, freshness, and drift" in response.text
    assert "What happened after requests" in response.text
    assert "Anime Trap" in response.text


def test_manifest_page_renders_decision_sandbox_preview(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(client.app.state.vanguarr, "list_profiles", lambda: ["alice"])
        monkeypatch.setattr(client.app.state.vanguarr, "read_profile", lambda username: '{"username": "alice"}')
        monkeypatch.setattr(client.app.state.vanguarr, "read_profile_summary", lambda username: "profile summary")
        monkeypatch.setattr(client.app.state.vanguarr, "get_profile_payload_with_live_context", lambda username: {})
        monkeypatch.setattr(client.app.state.vanguarr, "get_request_history", lambda username, limit=8: [])
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_profile_task_snapshots",
            lambda username: {
                "profile_architect": {"status": "idle", "summary": "No runs yet.", "progress_total": 0, "progress_current": 0, "percent": 0.0, "current_label": "Ready"},
                "decision_engine": {"status": "idle", "summary": "No runs yet.", "progress_total": 0, "progress_current": 0, "percent": 0.0, "current_label": "Ready"},
                "suggested_for_you": {"status": "idle", "summary": "No runs yet.", "progress_total": 0, "progress_current": 0, "percent": 0.0, "current_label": "Ready"},
            },
        )
        monkeypatch.setattr(client.app.state.vanguarr, "get_suggestions", lambda **kwargs: [])

        async def fake_preview(username: str, limit: int = 8) -> dict[str, object]:
            return {
                "summary": "Dry-run reviewed 1 shortlisted candidate.",
                "candidates": [
                    {
                        "media_type": "movie",
                        "title": "Arrival",
                        "overview": "First contact drama.",
                        "genres": ["Sci-Fi", "Drama"],
                        "sources": ["recommended:Interstellar"],
                        "release_date": "2016-11-11",
                        "rating": 8.2,
                        "decision": "REQUEST",
                        "hybrid_confidence": 0.82,
                        "llm_vote": "REQUEST",
                        "reasoning": "Final score 0.82.",
                        "features": {"score_breakdown": {"genre_affinity": 0.22, "outcome_fit": 0.05}},
                    }
                ],
            }

        monkeypatch.setattr(client.app.state.vanguarr, "preview_decision_candidates", fake_preview)

        response = client.get("/manifest?username=alice&review=1")

    assert response.status_code == 200
    assert "Top candidate review" in response.text
    assert "Dry-run reviewed 1 shortlisted candidate." in response.text
    assert "Arrival" in response.text
    assert "More Like This" in response.text
    assert "Never Again" in response.text


def test_manifest_profile_feedback_action_redirects_back_to_manifest(monkeypatch) -> None:
    with TestClient(app) as client:
        received: dict[str, object] = {}

        def fake_update_profile_feedback(**kwargs):
            received.update(kwargs)
            return {}

        monkeypatch.setattr(client.app.state.vanguarr, "update_profile_feedback", fake_update_profile_feedback)

        response = client.post(
            "/manifest/actions/profile-feedback",
            data={
                "username": "alice",
                "action": "more_like_this",
                "title": "Arrival",
                "genres": "Sci-Fi,Drama",
                "media_type": "movie",
                "review": "1",
            },
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/manifest?username=alice&review=1&toast=Saved+more+like+this+feedback+for+Arrival."
    assert received == {
        "username": "alice",
        "action": "more_like_this",
        "title": "Arrival",
        "genres": ["Sci-Fi", "Drama"],
        "media_type": "movie",
        "source": "manifest",
    }


def test_manifest_request_outcome_action_redirects_back_to_manifest(monkeypatch) -> None:
    with TestClient(app) as client:
        def fake_record_request_outcome(**kwargs):
            assert kwargs["username"] == "alice"
            assert kwargs["requested_media_id"] == 7
            assert kwargs["outcome"] == "watched"
            assert kwargs["source"] == "manifest"
            return {"outcome": "watched", "media_title": "Arrival"}

        monkeypatch.setattr(client.app.state.vanguarr, "record_request_outcome", fake_record_request_outcome)

        response = client.post(
            "/manifest/actions/request-outcome",
            data={
                "username": "alice",
                "requested_media_id": "7",
                "outcome": "watched",
                "review": "1",
            },
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/manifest?username=alice&review=1&toast=Recorded+watched+for+Arrival."


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


def test_manifest_task_status_endpoint_returns_profile_snapshots(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_profile_task_snapshots",
            lambda username: {
                "profile_architect": {"status": "running", "target_username": username, "summary": "Rebuilding profile."},
                "decision_engine": {"status": "idle", "target_username": username, "summary": "No runs yet."},
                "suggested_for_you": {"status": "success", "target_username": username, "summary": "Stored snapshot."},
            },
        )
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_task_snapshot",
            lambda engine_name: {"engine": engine_name, "status": "running" if engine_name == "profile_architect" else "idle"},
        )
        monkeypatch.setattr(
            client.app.state.background_runner,
            "is_running",
            lambda engine_name: engine_name == "profile_architect",
        )

        response = client.get("/api/manifest/task-status?username=alice")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "username": "alice",
        "tasks": {
            "profile_architect": {"status": "running", "target_username": "alice", "summary": "Rebuilding profile."},
            "decision_engine": {"status": "idle", "target_username": "alice", "summary": "No runs yet."},
            "suggested_for_you": {"status": "success", "target_username": "alice", "summary": "Stored snapshot."},
        },
        "active_tasks": {
            "profile_architect": {"engine": "profile_architect", "status": "running"},
            "decision_engine": {"engine": "decision_engine", "status": "idle"},
            "suggested_for_you": {"engine": "suggested_for_you", "status": "idle"},
        },
        "global_running": {
            "profile_architect": True,
            "decision_engine": False,
            "suggested_for_you": False,
        },
    }


def test_manifest_profile_architect_api_starts_background_run(monkeypatch) -> None:
    with TestClient(app) as client:
        monkeypatch.setattr(
            client.app.state.background_runner,
            "launch_profile_architect",
            lambda username: (True, f"Profile Architect started in the background for {username}."),
        )
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_task_snapshot_for_target",
            lambda engine_name, username: {"engine": engine_name, "target_username": username, "status": "running"},
        )
        monkeypatch.setattr(
            client.app.state.vanguarr,
            "get_task_snapshot",
            lambda engine_name: {"engine": engine_name, "status": "running"},
        )

        response = client.post("/api/manifest/actions/profile-architect", data={"username": "alice"})

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "started": True,
        "detail": "Profile Architect started in the background for alice.",
        "task": {"engine": "profile_architect", "target_username": "alice", "status": "running"},
        "active_task": {"engine": "profile_architect", "status": "running"},
    }


def test_manifest_decision_engine_api_requires_selected_profile() -> None:
    with TestClient(app) as client:
        response = client.post("/api/manifest/actions/decision-engine", data={"username": ""})

    assert response.status_code == 400
    assert response.json() == {
        "ok": False,
        "detail": "Select a profile before running Decision Engine.",
    }


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
