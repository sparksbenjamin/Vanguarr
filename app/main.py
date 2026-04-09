import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from app.api.base import ClientConfigError, ExternalServiceError
from app.api.llm import LLMClient
from app.api.media_server import MediaServerClient
from app.api.seer import SeerClient
from app.api.tmdb import TMDbClient
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
from app.core.services import VanguarrService


bootstrap_settings = get_settings()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


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
templates.env.globals["settings_nav_items"] = SETTINGS_PAGES


def get_settings_page_or_404(section_slug: str) -> SettingsPageDefinition:
    page = SETTINGS_PAGE_MAP.get(section_slug)
    if page is None:
        raise HTTPException(status_code=404, detail="Settings section not found.")
    return page


def redirect_with_toast(path: str, message: str, **params: str) -> RedirectResponse:
    payload = {key: value for key, value in params.items() if value}
    payload["toast"] = message
    return RedirectResponse(f"{path}?{urlencode(payload)}", status_code=303)


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
    scheduler = EngineScheduler(live_settings, service)
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
    app.state.health_monitor = health_monitor
    app.state.scheduler = scheduler

    runtime_settings = apply_runtime_settings(app, force=True)
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


@app.get("/logs", response_class=HTMLResponse)
async def logs(request: Request, q: str = "") -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
    settings = current_settings(request.app)
    return templates.TemplateResponse(
        request=request,
        name="logs.html",
        context={
            "request": request,
            "page_title": "Vanguarr War Room",
            "toast": request.query_params.get("toast"),
            "query": q,
            "logs": service.get_logs(search=q, limit=settings.decision_page_size),
        },
    )


@app.get("/settings")
async def settings_root() -> RedirectResponse:
    return RedirectResponse(DEFAULT_SETTINGS_PAGE.href, status_code=303)


@app.get("/settings/{section_slug}", response_class=HTMLResponse)
async def settings_page(request: Request, section_slug: str) -> HTMLResponse:
    settings_page_def = get_settings_page_or_404(section_slug)
    settings = current_settings(request.app, force=True)
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


@app.get("/manifest", response_class=HTMLResponse)
async def manifest(request: Request, username: str = "") -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
    profiles = service.list_profiles()
    selected_user = username or (profiles[0] if profiles else "")
    profile_content = service.read_profile(selected_user) if selected_user else ""
    profile_summary = service.read_profile_summary(selected_user) if selected_user else ""
    profile_json_path = service.profile_store.json_path_for(selected_user) if selected_user else None
    profile_summary_path = service.profile_store.summary_path_for(selected_user) if selected_user else None
    return templates.TemplateResponse(
        request=request,
        name="manifest.html",
        context={
            "request": request,
            "page_title": "Vanguarr Manifest Editor",
            "toast": request.query_params.get("toast"),
            "profiles": profiles,
            "selected_user": selected_user,
            "profile_content": profile_content,
            "profile_summary": profile_summary,
            "profile_json_path": profile_json_path,
            "profile_summary_path": profile_summary_path,
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
        return redirect_with_toast("/manifest", "Profile manifest must be valid JSON.", username=cleaned_username)
    except ValueError as exc:
        return redirect_with_toast("/manifest", str(exc), username=cleaned_username)

    return redirect_with_toast("/manifest", f"Saved profile manifest for {cleaned_username}.", username=cleaned_username)


@app.post("/actions/profile-architect")
async def action_profile_architect(
    request: Request,
    username: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip() or None
    result = await request.app.state.vanguarr.run_profile_architect(cleaned_username)
    return redirect_with_toast("/", result["summary"])


@app.post("/actions/decision-engine")
async def action_decision_engine(
    request: Request,
    username: str = Form(""),
) -> RedirectResponse:
    cleaned_username = username.strip() or None
    result = await request.app.state.vanguarr.run_decision_engine(cleaned_username)
    return redirect_with_toast("/", result["summary"])
