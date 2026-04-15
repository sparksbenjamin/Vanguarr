import asyncio
import json
import logging
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from app.api.base import ClientConfigError, ExternalServiceError
from app import __version__ as APP_VERSION
from app.api.jellyfin import (
    VANGUARR_JELLYFIN_PLUGIN_NAME,
    VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
)
from app.api.llm import LLMClient
from app.api.media_server import MediaServerClient
from app.api.seer import SeerClient
from app.api.tmdb import TMDbClient
from app.core.background_runner import BackgroundEngineRunner
from app.core.config_store import LiveSettings, SettingsManager
from app.core.db import SessionLocal, init_db
from app.core.health import HealthMonitor
from app.core.logging import setup_logging
from app.core.scheduler import EngineScheduler
from app.core.settings import (
    DB_MANAGED_SETTING_FIELDS,
    LLM_PROVIDER_OPTIONS,
    LLMProviderSettings,
    SettingFieldDefinition,
    Settings,
    get_settings,
)
from app.core.services import VanguarrService, normalize_jellyfin_user_id


bootstrap_settings = get_settings()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
templates.env.globals["app_version"] = APP_VERSION


@dataclass(frozen=True, slots=True)
class SettingsPageDefinition:
    slug: str
    title: str
    kind: str
    description: str
    href: str
    fields: tuple[SettingFieldDefinition, ...] = ()


def build_setting_sections() -> list[dict[str, object]]:
    grouped: dict[str, list[object]] = {}
    for field in DB_MANAGED_SETTING_FIELDS:
        grouped.setdefault(field.group, []).append(field)
    return [{"title": title, "fields": fields} for title, fields in grouped.items()]


SECTION_DESCRIPTIONS: dict[str, str] = {
    "General": "Core runtime identity, timezone, logging, and health polling behavior.",
    "Integrations": "Media server, Seer, and upstream integration settings that Vanguarr uses live.",
    "TMDb": "Metadata enrichment defaults for TMDb lookups, language, and region handling.",
    "Scheduling": "Control the built-in scheduler and its live cron expressions.",
    "Tuning": "Adjust thresholds, history depth, pool sizing, and recommendation heuristics.",
    "LLM": "Set global LLM defaults that apply across the provider chain.",
}


def slugify_settings_title(value: str) -> str:
    return value.strip().lower().replace(" ", "-")


def build_settings_pages() -> tuple[SettingsPageDefinition, ...]:
    pages: list[SettingsPageDefinition] = []
    for section in build_setting_sections():
        title = str(section["title"])
        slug = slugify_settings_title(title)
        pages.append(
            SettingsPageDefinition(
                slug=slug,
                title=title,
                kind="fields",
                description=SECTION_DESCRIPTIONS.get(title, f"Manage the {title.lower()} runtime settings for Vanguarr."),
                href=f"/settings/{slug}",
                fields=tuple(section["fields"]),
            )
        )

    pages.append(
        SettingsPageDefinition(
            slug="llm-providers",
            title="LLM Providers",
            kind="providers",
            description="Manage the live, priority-ordered LLM provider chain and verify each provider before saving.",
            href="/settings/llm-providers",
        )
    )
    return tuple(pages)


SETTINGS_PAGES = build_settings_pages()
SETTINGS_PAGE_MAP = {page.slug: page for page in SETTINGS_PAGES}
DEFAULT_SETTINGS_PAGE = SETTINGS_PAGE_MAP["general"]
SETTINGS_NAV_ITEMS = SETTINGS_PAGES + (
    SettingsPageDefinition(
        slug="profiles",
        title="Profiles",
        kind="editor",
        description="Open the manifest editor for live profile documents and summaries.",
        href="/manifest",
    ),
)
templates.env.globals["settings_nav_items"] = SETTINGS_NAV_ITEMS


def get_settings_page_or_404(section_slug: str) -> SettingsPageDefinition:
    page = SETTINGS_PAGE_MAP.get(section_slug)
    if page is None:
        raise HTTPException(status_code=404, detail="Settings section not found.")
    return page


def redirect_with_toast(path: str, message: str, **params: str) -> RedirectResponse:
    payload = {key: value for key, value in params.items() if value}
    payload["toast"] = message
    return RedirectResponse(f"{path}?{urlencode(payload)}", status_code=303)


def redirect_to_manifest(message: str, *, username: str = "", review: str = "") -> RedirectResponse:
    return redirect_with_toast("/manifest", message, username=username, review=review)


def parse_csv_values(raw: str) -> list[str]:
    return [value.strip() for value in str(raw or "").split(",") if value.strip()]


def _extract_bearer_token(header_value: str | None) -> str | None:
    raw = str(header_value or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("bearer "):
        return raw[7:].strip() or None
    return raw


def require_bearer_token(request: Request, expected_token: str | None, *, purpose: str) -> None:
    configured_token = str(expected_token or "").strip()
    if not configured_token:
        raise HTTPException(status_code=503, detail=f"{purpose} token is not configured.")

    supplied_token = _extract_bearer_token(request.headers.get("Authorization"))
    if not supplied_token or not secrets.compare_digest(supplied_token, configured_token):
        raise HTTPException(status_code=401, detail=f"Invalid {purpose} token.")


def current_settings(app: FastAPI, *, force: bool = False) -> Settings:
    if hasattr(app.state, "settings"):
        return app.state.settings.snapshot(force=force)
    return bootstrap_settings


def apply_runtime_settings(app: FastAPI, *, force: bool = False) -> Settings:
    settings = current_settings(app, force=force)
    setup_logging(settings)
    app.title = settings.app_name
    if hasattr(app.state, "health_monitor"):
        app.state.health_monitor.reset(ttl_seconds=settings.health_cache_seconds)
    if hasattr(app.state, "scheduler"):
        app.state.scheduler.refresh()
    return settings


def parse_provider_payloads(form: object) -> list[dict[str, object]]:
    get = form.get
    getlist = form.getlist
    provider_payloads: list[dict[str, object]] = []
    for raw_id in getlist("provider_row_ids"):
        row_id = str(raw_id).strip()
        if not row_id:
            continue
        prefix = f"provider-{row_id}-"
        provider_payloads.append(
            {
                "id": row_id,
                "name": get(f"{prefix}name", ""),
                "provider": get(f"{prefix}provider", ""),
                "model": get(f"{prefix}model", ""),
                "priority": get(f"{prefix}priority", "1"),
                "enabled": f"{prefix}enabled" in form,
                "api_base": get(f"{prefix}api_base", ""),
                "api_key": get(f"{prefix}api_key", ""),
                "timeout_seconds": get(f"{prefix}timeout_seconds", ""),
                "max_output_tokens": get(f"{prefix}max_output_tokens", ""),
                "use_for_decision": f"{prefix}use_for_decision" in form,
                "use_for_profile_enrichment": f"{prefix}use_for_profile_enrichment" in form,
                "delete": f"{prefix}delete" in form,
            }
        )

    provider_payloads.append(
        {
            "id": None,
            "name": get("provider-new-name", ""),
            "provider": get("provider-new-provider", ""),
            "model": get("provider-new-model", ""),
            "priority": get("provider-new-priority", "1"),
            "enabled": "provider-new-enabled" in form,
            "api_base": get("provider-new-api_base", ""),
            "api_key": get("provider-new-api_key", ""),
            "timeout_seconds": get("provider-new-timeout_seconds", ""),
            "max_output_tokens": get("provider-new-max_output_tokens", ""),
            "use_for_decision": "provider-new-use_for_decision" in form,
            "use_for_profile_enrichment": "provider-new-use_for_profile_enrichment" in form,
            "delete": False,
        }
    )
    return provider_payloads


def format_validation_error(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        details = exc.errors()
        if details:
            first = details[0]
            location = ".".join(str(part) for part in first.get("loc", []))
            message = str(first.get("msg", "Invalid settings payload."))
            if location:
                return f"{location}: {message}"
            return message
    return str(exc)


def normalize_provider_payload(raw_payload: dict[str, object] | None) -> dict[str, object]:
    payload = raw_payload or {}
    row_id = payload.get("id")
    return {
        "id": None if row_id in ("", None) else int(row_id),
        "name": str(payload.get("name") or "").strip(),
        "provider": str(payload.get("provider") or "").strip().lower(),
        "model": str(payload.get("model") or "").strip(),
        "priority": payload.get("priority", 1),
        "enabled": bool(payload.get("enabled")),
        "api_base": str(payload.get("api_base") or "").strip() or None,
        "api_key": str(payload.get("api_key") or "").strip() or None,
        "timeout_seconds": payload.get("timeout_seconds", ""),
        "max_output_tokens": payload.get("max_output_tokens", ""),
        "use_for_decision": bool(payload.get("use_for_decision", True)),
        "use_for_profile_enrichment": bool(payload.get("use_for_profile_enrichment", True)),
    }


def build_provider_settings(raw_payload: dict[str, object] | None) -> LLMProviderSettings:
    normalized = normalize_provider_payload(raw_payload)
    normalized["name"] = normalized.get("name") or (
        str(normalized.get("provider") or "").title() or "Provider"
    )
    return LLMProviderSettings.model_validate(normalized)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    settings_manager = SettingsManager(bootstrap_settings, SessionLocal)
    settings_manager.sync_bootstrap_defaults()
    live_settings = LiveSettings(settings_manager)
    media_server = MediaServerClient(live_settings)
    seer = SeerClient(live_settings)
    tmdb = TMDbClient(live_settings)
    llm = LLMClient(live_settings)
    service = VanguarrService(
        settings=live_settings,
        media_server=media_server,
        seer=seer,
        tmdb=tmdb,
        llm=llm,
        session_factory=SessionLocal,
    )
    background_runner = BackgroundEngineRunner(service)
    recovered_task_runs = service.recover_interrupted_tasks()
    scheduler = EngineScheduler(live_settings, service, background_runner)
    health_monitor = HealthMonitor(
        media_server=media_server,
        seer=seer,
        tmdb=tmdb,
        llm=llm,
        ttl_seconds=live_settings.snapshot(force=True).health_cache_seconds,
    )

    app.state.settings_manager = settings_manager
    app.state.settings = live_settings
    app.state.media_server = media_server
    app.state.seer = seer
    app.state.tmdb = tmdb
    app.state.llm = llm
    app.state.vanguarr = service
    app.state.background_runner = background_runner
    app.state.health_monitor = health_monitor
    app.state.scheduler = scheduler

    runtime_settings = apply_runtime_settings(app, force=True)
    if recovered_task_runs:
        logging.getLogger("vanguarr").warning(
            "Recovered %s interrupted task run(s) during startup.",
            recovered_task_runs,
        )
    logging.getLogger("vanguarr").info(
        "Preparing runtime data_dir=%s profiles_dir=%s log_file=%s",
        runtime_settings.data_dir,
        runtime_settings.profiles_dir,
        runtime_settings.log_file,
    )
    logging.getLogger("vanguarr").info("Vanguarr startup complete.")
    yield
    logging.getLogger("vanguarr").info("Vanguarr shutting down.")
    scheduler.shutdown()
    await background_runner.shutdown()


app = FastAPI(
    title="Vanguarr",
    summary="AI-driven proactive media curation bridge for the Arr stack.",
    lifespan=lifespan,
)


@app.get("/")
async def root(request: Request) -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
    settings = current_settings(request.app)
    health = await request.app.state.health_monitor.snapshot()
    dashboard = service.get_dashboard_snapshot()
    media_server_meta = health.get("services", {}).get("media_server", {}).get("meta", {})
    tmdb_meta = health.get("services", {}).get("tmdb", {}).get("meta", {})
    llm_meta = health.get("services", {}).get("llm", {}).get("meta", {})
    dashboard["connected_users"] = int(media_server_meta.get("users") or 0) if isinstance(media_server_meta, dict) else 0
    dashboard["media_server_label"] = str(
        media_server_meta.get("provider_name") or settings.media_server_label
    )
    dashboard["tmdb_enabled"] = bool(tmdb_meta.get("enabled")) if isinstance(tmdb_meta, dict) else False
    dashboard["llm_provider"] = str(
        llm_meta.get("provider_name") or llm_meta.get("provider") or settings.llm_provider_label
    )
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "request": request,
            "page_title": "Vanguarr Dashboard",
            "toast": request.query_params.get("toast"),
            "health": health,
            "dashboard": dashboard,
            "recent_logs": service.get_logs(limit=8),
            "task_runs": service.get_task_runs(limit=6),
            "profiles": service.list_profiles(),
            "scheduler_jobs": request.app.state.scheduler.snapshot(),
            "settings": settings,
            "health_service_labels": {
                "media_server": dashboard["media_server_label"],
                "seer": "Seer",
                "tmdb": "TMDb",
                "llm": "LLM",
            },
        },
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    settings = current_settings(app)
    return {"status": "ok", "service": settings.app_name}


@app.get("/api/health")
async def api_health(request: Request, force: bool = False) -> dict[str, object]:
    return await request.app.state.health_monitor.snapshot(force=force)


@app.get("/api/jellyfin/suggestions")
async def jellyfin_suggestions_api(
    request: Request,
    username: str = "",
    user_id: str = "",
    limit: int | None = None,
) -> dict[str, object]:
    settings = current_settings(request.app, force=True)
    require_bearer_token(request, settings.suggestions_api_key, purpose="suggestions API")

    service: VanguarrService = request.app.state.vanguarr
    normalized_username = username.strip() or None
    normalized_user_id = normalize_jellyfin_user_id(user_id)
    suggestions = service.get_suggestions(
        username=normalized_username,
        jellyfin_user_id=normalized_user_id,
        limit=limit,
    )

    return {
        "username": normalized_username or (suggestions[0].username if suggestions else ""),
        "jellyfin_user_id": normalized_user_id or (suggestions[0].jellyfin_user_id if suggestions else ""),
        "count": len(suggestions),
        "items": [
            {
                "rank": item.rank,
                "media_type": item.media_type,
                "title": item.title,
                "overview": item.overview,
                "production_year": item.production_year,
                "score": item.score,
                "reasoning": item.reasoning,
                "state": item.state,
                "external_ids": {
                    key: value
                    for key, value in {
                        "tmdb": str(item.tmdb_id) if item.tmdb_id is not None else None,
                        "tvdb": str(item.tvdb_id) if item.tvdb_id is not None else None,
                        "imdb": item.imdb_id,
                    }.items()
                    if value not in (None, "")
                },
            }
            for item in suggestions
        ],
    }


@app.post("/api/webhooks/seer")
async def seer_webhook(request: Request) -> JSONResponse:
    settings = current_settings(request.app, force=True)
    require_bearer_token(request, settings.seer_webhook_token, purpose="Seer webhook")

    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Seer webhook payload must be a JSON object.")

    service: VanguarrService = request.app.state.vanguarr
    result = await service.ingest_seer_webhook(payload)
    return JSONResponse(result)


@app.get("/logs", response_class=HTMLResponse)
async def logs(
    request: Request,
    q: str = "",
    view: str = "all",
    sort: str = "created_at",
    dir: str = "desc",
    page: int = 1,
) -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
    settings = current_settings(request.app)
    log_feed = service.get_log_feed(
        search=q,
        view=view,
        sort_by=sort,
        sort_direction=dir,
        page=page,
        limit=settings.decision_page_size,
    )
    return templates.TemplateResponse(
        request=request,
        name="logs.html",
        context={
            "request": request,
            "page_title": "Vanguarr War Room",
            "toast": request.query_params.get("toast"),
            "log_feed": log_feed,
            "query": log_feed["query"],
            "current_view": log_feed["view"],
            "sort_by": log_feed["sort_by"],
            "sort_direction": log_feed["sort_direction"],
            "current_page": log_feed["page"],
        },
    )


@app.get("/api/logs")
async def logs_api(
    request: Request,
    q: str = "",
    view: str = "all",
    sort: str = "created_at",
    dir: str = "desc",
    page: int = 1,
) -> JSONResponse:
    service: VanguarrService = request.app.state.vanguarr
    settings = current_settings(request.app)
    feed = service.get_log_feed(
        search=q,
        view=view,
        sort_by=sort,
        sort_direction=dir,
        page=page,
        limit=settings.decision_page_size,
    )
    payload = dict(feed)
    payload.pop("raw_rows", None)
    return JSONResponse(payload)


@app.get("/settings")
async def settings_root() -> RedirectResponse:
    return RedirectResponse(DEFAULT_SETTINGS_PAGE.href, status_code=303)


@app.get("/settings/{section_slug}", response_class=HTMLResponse)
async def settings_page(request: Request, section_slug: str) -> HTMLResponse:
    settings_page_def = get_settings_page_or_404(section_slug)
    settings = current_settings(request.app, force=True)
    library_sync_snapshot = request.app.state.vanguarr.get_library_sync_snapshot()
    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context={
            "request": request,
            "page_title": f"Vanguarr Settings - {settings_page_def.title}",
            "toast": request.query_params.get("toast"),
            "settings": settings,
            "settings_page": settings_page_def,
            "active_settings_slug": settings_page_def.slug,
            "llm_providers": settings.llm_providers,
            "llm_provider_options": LLM_PROVIDER_OPTIONS,
            "jellyfin_plugin_name": VANGUARR_JELLYFIN_PLUGIN_NAME,
            "jellyfin_plugin_repository_url": VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
            "library_sync_snapshot": library_sync_snapshot,
        },
    )


@app.post("/settings/save")
async def settings_save(request: Request) -> RedirectResponse:
    form = await request.form()
    section_slug = str(form.get("settings_section_slug") or DEFAULT_SETTINGS_PAGE.slug).strip().lower()
    settings_page_def = get_settings_page_or_404(section_slug)
    setting_values: dict[str, object] = {}
    provider_payloads: list[dict[str, object]] = []

    if settings_page_def.kind == "fields":
        for field in settings_page_def.fields:
            if field.input_type == "checkbox":
                setting_values[field.key] = field.key in form
                continue
            setting_values[field.key] = form.get(field.key, "")
    elif settings_page_def.kind == "providers":
        provider_payloads = parse_provider_payloads(form)

    try:
        request.app.state.settings.manager.save_settings(setting_values, provider_payloads)
        apply_runtime_settings(request.app, force=True)
    except (ValidationError, ValueError) as exc:
        return redirect_with_toast(settings_page_def.href, format_validation_error(exc))

    return redirect_with_toast(settings_page_def.href, f"Saved {settings_page_def.title.lower()} settings.")


@app.post("/api/settings/llm/provider-test")
async def test_llm_provider(request: Request) -> JSONResponse:
    try:
        provider = build_provider_settings(await request.json())
        check = await request.app.state.llm.test_provider(provider)
        return JSONResponse(check.to_dict())
    except (ValidationError, ValueError, ClientConfigError, ExternalServiceError) as exc:
        return JSONResponse(
            status_code=400,
            content={
                "service": "LLM",
                "ok": False,
                "state": "down",
                "detail": format_validation_error(exc),
                "meta": {},
            },
        )


@app.post("/api/settings/llm/ollama-models")
async def list_ollama_models(request: Request) -> JSONResponse:
    try:
        provider = build_provider_settings(await request.json())
        models = await request.app.state.llm.list_ollama_models(provider)
        return JSONResponse(
            {
                "ok": True,
                "provider": provider.provider,
                "provider_name": provider.name,
                "models": models,
                "detail": f"Loaded {len(models)} Ollama model(s).",
            }
        )
    except (ValidationError, ValueError, ClientConfigError, ExternalServiceError) as exc:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "provider": "ollama",
                "models": [],
                "detail": format_validation_error(exc),
            },
        )


@app.post("/api/settings/llm/provider-delete/{provider_id}")
async def delete_llm_provider(request: Request, provider_id: int) -> JSONResponse:
    settings = current_settings(request.app, force=True)
    provider = next((item for item in settings.llm_providers if item.id == provider_id), None)
    if provider is None:
        raise HTTPException(status_code=404, detail="LLM provider not found.")

    updated_settings = request.app.state.settings.manager.save_settings({}, [{"id": provider_id, "delete": True}])
    apply_runtime_settings(request.app, force=True)
    return JSONResponse(
        {
            "ok": True,
            "detail": f"Deleted {provider.name}.",
            "provider_id": provider_id,
            "providers_remaining": len(updated_settings.llm_providers),
        }
    )


@app.post("/api/settings/integrations/jellyfin-plugin/install")
async def install_jellyfin_plugin(request: Request) -> JSONResponse:
    try:
        result = await request.app.state.vanguarr.install_jellyfin_plugin()
        return JSONResponse({"ok": True, **result})
    except (ClientConfigError, ExternalServiceError, ValueError) as exc:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "detail": format_validation_error(exc),
            },
        )


@app.post("/api/settings/scheduling/library-sync/run")
async def run_library_sync_now(request: Request) -> JSONResponse:
    settings = current_settings(request.app, force=True)
    if settings.normalized_media_server_provider != "jellyfin":
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "detail": "Library Sync currently requires Jellyfin as the active media server.",
            },
        )

    started, message = request.app.state.background_runner.launch_library_sync()
    return JSONResponse(
        {
            "ok": True,
            "started": started,
            "detail": message,
        }
    )


@app.get("/api/settings/scheduling/library-sync/status")
async def library_sync_status(request: Request) -> JSONResponse:
    snapshot = request.app.state.vanguarr.get_library_sync_snapshot()
    task_snapshot = request.app.state.vanguarr.get_task_snapshot("library_sync")
    return JSONResponse(
        {
            "ok": True,
            "task": task_snapshot,
            "snapshot": {
                "total_items": snapshot["total_items"],
                "available_items": snapshot["available_items"],
                "removed_items": snapshot["removed_items"],
                "movies": snapshot["movies"],
                "series": snapshot["series"],
                "last_seen_at": snapshot["last_seen_at"].isoformat() if snapshot["last_seen_at"] else None,
                "last_task": task_snapshot,
            },
        }
    )


@app.get("/manifest", response_class=HTMLResponse)
async def manifest(request: Request, username: str = "", review: str = "") -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
    settings = current_settings(request.app, force=True)
    profiles = service.list_profiles()
    selected_user = username or (profiles[0] if profiles else "")
    profile_content = service.read_profile(selected_user) if selected_user else ""
    profile_summary = service.read_profile_summary(selected_user) if selected_user else ""
    profile_payload_live = service.get_profile_payload_with_live_context(selected_user) if selected_user else {}
    profile_json_path = service.profile_store.json_path_for(selected_user) if selected_user else None
    profile_summary_path = service.profile_store.summary_path_for(selected_user) if selected_user else None
    suggestions_preview = (
        service.get_suggestions(username=selected_user, limit=settings.suggestions_limit)
        if selected_user
        else []
    )
    request_history = service.get_request_history(selected_user, limit=8) if selected_user else []
    profile_task_snapshots = service.get_profile_task_snapshots(selected_user) if selected_user else {}
    review_requested = str(review or "").strip() in {"1", "true", "yes", "on"}
    decision_preview: dict[str, object] | None = None
    decision_preview_error = ""
    if selected_user and review_requested:
        try:
            decision_preview = await service.preview_decision_candidates(selected_user, limit=8)
        except Exception as exc:
            decision_preview_error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="manifest.html",
        context={
            "request": request,
            "page_title": "Vanguarr Manifest Editor",
            "toast": request.query_params.get("toast"),
            "active_settings_slug": "profiles",
            "profiles": profiles,
            "selected_user": selected_user,
            "profile_content": profile_content,
            "profile_summary": profile_summary,
            "profile_payload_live": profile_payload_live,
            "profile_json_path": profile_json_path,
            "profile_summary_path": profile_summary_path,
            "suggestions_preview": suggestions_preview,
            "suggestions_limit": settings.suggestions_limit,
            "request_history": request_history,
            "profile_task_snapshots": profile_task_snapshots,
            "review_requested": review_requested,
            "decision_preview": decision_preview,
            "decision_preview_error": decision_preview_error,
        },
    )


@app.post("/manifest/save")
async def manifest_save(
    request: Request,
    username: str = Form(...),
    content: str = Form(...),
) -> RedirectResponse:
    service: VanguarrService = request.app.state.vanguarr
    cleaned_username = username.strip()
    if not cleaned_username:
        return redirect_with_toast("/manifest", "A username is required before saving a manifest.")

    try:
        service.save_profile(cleaned_username, content)
    except json.JSONDecodeError:
        return redirect_to_manifest("Profile manifest must be valid JSON.", username=cleaned_username)
    except ValueError as exc:
        return redirect_to_manifest(str(exc), username=cleaned_username)

    return redirect_to_manifest(f"Saved profile manifest for {cleaned_username}.", username=cleaned_username)


@app.post("/manifest/actions/profile-feedback")
async def manifest_action_profile_feedback(
    request: Request,
    username: str = Form(""),
    action: str = Form(""),
    title: str = Form(""),
    genres: str = Form(""),
    media_type: str = Form("unknown"),
    review: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip()
    if not cleaned_username:
        return redirect_to_manifest("Select a profile before sending feedback.")

    try:
        request.app.state.vanguarr.update_profile_feedback(
            username=cleaned_username,
            action=action,
            title=title,
            genres=parse_csv_values(genres),
            media_type=media_type,
            source="manifest",
        )
    except ValueError as exc:
        return redirect_to_manifest(str(exc), username=cleaned_username, review=review)

    return redirect_to_manifest(
        f"Saved {action.replace('_', ' ')} feedback for {title.strip() or 'that title'}.",
        username=cleaned_username,
        review=review,
    )


@app.post("/manifest/actions/request-outcome")
async def manifest_action_request_outcome(
    request: Request,
    username: str = Form(""),
    requested_media_id: int = Form(...),
    outcome: str = Form(""),
    detail: str = Form(""),
    review: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip()
    if not cleaned_username:
        return redirect_to_manifest("Select a profile before recording a request outcome.")

    try:
        result = request.app.state.vanguarr.record_request_outcome(
            username=cleaned_username,
            requested_media_id=requested_media_id,
            outcome=outcome,
            source="manifest",
            detail=detail,
        )
    except ValueError as exc:
        return redirect_to_manifest(str(exc), username=cleaned_username, review=review)

    return redirect_to_manifest(
        f"Recorded {result['outcome']} for {result['media_title']}.",
        username=cleaned_username,
        review=review,
    )


@app.post("/manifest/actions/suggested-for-you")
async def manifest_action_suggested_for_you(
    request: Request,
    username: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip()
    if not cleaned_username:
        return redirect_with_toast("/manifest", "Select a profile before refreshing suggestions.")

    _started, message = request.app.state.background_runner.launch_suggested_for_you(cleaned_username)
    return redirect_with_toast("/manifest", message, username=cleaned_username)


@app.get("/api/manifest/task-status")
async def manifest_task_status(request: Request, username: str = "") -> JSONResponse:
    cleaned_username = username.strip()
    engines = ("profile_architect", "decision_engine", "suggested_for_you")
    service: VanguarrService = request.app.state.vanguarr
    return JSONResponse(
        {
            "ok": True,
            "username": cleaned_username,
            "tasks": service.get_profile_task_snapshots(cleaned_username),
            "active_tasks": {
                engine: service.get_task_snapshot(engine)
                for engine in engines
            },
            "global_running": {
                engine: request.app.state.background_runner.is_running(engine)
                for engine in engines
            },
        }
    )


@app.post("/api/manifest/actions/profile-architect")
async def manifest_action_profile_architect_api(
    request: Request,
    username: str = Form(""),
) -> JSONResponse:
    cleaned_username = username.strip()
    if not cleaned_username:
        return JSONResponse({"ok": False, "detail": "Select a profile before running Profile Architect."}, status_code=400)

    started, detail = request.app.state.background_runner.launch_profile_architect(cleaned_username)
    if started:
        await asyncio.sleep(0)
    service: VanguarrService = request.app.state.vanguarr
    return JSONResponse(
        {
            "ok": True,
            "started": started,
            "detail": detail,
            "task": service.get_task_snapshot_for_target("profile_architect", cleaned_username),
            "active_task": service.get_task_snapshot("profile_architect"),
        }
    )


@app.post("/api/manifest/actions/decision-engine")
async def manifest_action_decision_engine_api(
    request: Request,
    username: str = Form(""),
) -> JSONResponse:
    cleaned_username = username.strip()
    if not cleaned_username:
        return JSONResponse({"ok": False, "detail": "Select a profile before running Decision Engine."}, status_code=400)

    started, detail = request.app.state.background_runner.launch_decision_engine(cleaned_username)
    if started:
        await asyncio.sleep(0)
    service: VanguarrService = request.app.state.vanguarr
    return JSONResponse(
        {
            "ok": True,
            "started": started,
            "detail": detail,
            "task": service.get_task_snapshot_for_target("decision_engine", cleaned_username),
            "active_task": service.get_task_snapshot("decision_engine"),
        }
    )


@app.post("/api/manifest/actions/suggested-for-you")
async def manifest_action_suggested_for_you_api(
    request: Request,
    username: str = Form(""),
) -> JSONResponse:
    cleaned_username = username.strip()
    if not cleaned_username:
        return JSONResponse({"ok": False, "detail": "Select a profile before refreshing suggestions."}, status_code=400)

    started, detail = request.app.state.background_runner.launch_suggested_for_you(cleaned_username)
    if started:
        await asyncio.sleep(0)
    service: VanguarrService = request.app.state.vanguarr
    return JSONResponse(
        {
            "ok": True,
            "started": started,
            "detail": detail,
            "task": service.get_task_snapshot_for_target("suggested_for_you", cleaned_username),
            "active_task": service.get_task_snapshot("suggested_for_you"),
        }
    )


@app.post("/actions/profile-architect")
async def action_profile_architect(
    request: Request,
    username: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip() or None
    _started, message = request.app.state.background_runner.launch_profile_architect(cleaned_username)
    return redirect_with_toast("/", message)


@app.post("/actions/decision-engine")
async def action_decision_engine(
    request: Request,
    username: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip() or None
    _started, message = request.app.state.background_runner.launch_decision_engine(cleaned_username)
    return redirect_with_toast("/", message)


@app.post("/actions/suggested-for-you")
async def action_suggested_for_you(
    request: Request,
    username: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip() or None
    _started, message = request.app.state.background_runner.launch_suggested_for_you(cleaned_username)
    return redirect_with_toast("/", message)
