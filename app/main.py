from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.seer import SeerClient
from app.core.db import SessionLocal, init_db
from app.core.health import HealthMonitor
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
    init_db()
    jellyfin = JellyfinClient(settings)
    seer = SeerClient(settings)
    llm = LLMClient(settings)
    service = VanguarrService(
        settings=settings,
        jellyfin=jellyfin,
        seer=seer,
        llm=llm,
        session_factory=SessionLocal,
    )
    scheduler = EngineScheduler(settings, service)

    app.state.settings = settings
    app.state.jellyfin = jellyfin
    app.state.seer = seer
    app.state.llm = llm
    app.state.vanguarr = service
    app.state.health_monitor = HealthMonitor(
        jellyfin=jellyfin,
        seer=seer,
        llm=llm,
        ttl_seconds=settings.health_cache_seconds,
    )
    app.state.scheduler = scheduler
    scheduler.start()
    yield
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
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "request": request,
            "page_title": "Vanguarr Dashboard",
            "toast": request.query_params.get("toast"),
            "health": health,
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
    profile_path = service.profile_store.path_for(selected_user) if selected_user else None
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
            "profile_path": profile_path,
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

    service.save_profile(cleaned_username, content)
    return redirect_with_toast("/manifest", f"Saved profile block for {cleaned_username}.", username=cleaned_username)


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
