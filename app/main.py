import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.seer import SeerClient
from app.api.tmdb import TMDbClient
from app.core.db import SessionLocal, init_db
from app.core.health import HealthMonitor
from app.core.logging import setup_logging
from app.core.scheduler import EngineScheduler
from app.core.settings import get_settings
from app.core.services import VanguarrService


settings = get_settings()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


def redirect_with_toast(path: str, message: str, **params: str) -> RedirectResponse:
    payload = {key: value for key, value in params.items() if value}
    payload["toast"] = message
    return RedirectResponse(f"{path}?{urlencode(payload)}", status_code=303)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(settings)
    logging.getLogger("vanguarr").info(
        "Preparing runtime data_dir=%s profiles_dir=%s log_file=%s",
        settings.data_dir,
        settings.profiles_dir,
        settings.log_file,
    )
    init_db()
    jellyfin = JellyfinClient(settings)
    seer = SeerClient(settings)
    tmdb = TMDbClient(settings)
    llm = LLMClient(settings)
    service = VanguarrService(
        settings=settings,
        jellyfin=jellyfin,
        seer=seer,
        tmdb=tmdb,
        llm=llm,
        session_factory=SessionLocal,
    )
    scheduler = EngineScheduler(settings, service)

    app.state.settings = settings
    app.state.jellyfin = jellyfin
    app.state.seer = seer
    app.state.tmdb = tmdb
    app.state.llm = llm
    app.state.vanguarr = service
    app.state.health_monitor = HealthMonitor(
        jellyfin=jellyfin,
        seer=seer,
        tmdb=tmdb,
        llm=llm,
        ttl_seconds=settings.health_cache_seconds,
    )
    app.state.scheduler = scheduler
    scheduler.start()
    logging.getLogger("vanguarr").info("Vanguarr startup complete.")
    yield
    logging.getLogger("vanguarr").info("Vanguarr shutting down.")
    scheduler.shutdown()


app = FastAPI(
    title=settings.app_name,
    summary="AI-driven proactive media curation bridge for the Arr stack.",
    lifespan=lifespan,
)


@app.get("/")
async def root(request: Request) -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
    health = await request.app.state.health_monitor.snapshot()
    dashboard = service.get_dashboard_snapshot()
    jellyfin_meta = health.get("services", {}).get("jellyfin", {}).get("meta", {})
    tmdb_meta = health.get("services", {}).get("tmdb", {}).get("meta", {})
    llm_meta = health.get("services", {}).get("llm", {}).get("meta", {})
    dashboard["connected_users"] = int(jellyfin_meta.get("users") or 0) if isinstance(jellyfin_meta, dict) else 0
    dashboard["tmdb_enabled"] = bool(tmdb_meta.get("enabled")) if isinstance(tmdb_meta, dict) else False
    dashboard["llm_provider"] = str(llm_meta.get("provider") or settings.llm_provider)
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
        },
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


@app.get("/api/health")
async def api_health(request: Request, force: bool = False) -> dict[str, object]:
    return await request.app.state.health_monitor.snapshot(force=force)


@app.get("/logs", response_class=HTMLResponse)
async def logs(request: Request, q: str = "") -> HTMLResponse:
    service: VanguarrService = request.app.state.vanguarr
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
