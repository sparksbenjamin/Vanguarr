from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.seer import SeerClient
from app.core.health import HealthMonitor
from app.core.settings import get_settings


settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    jellyfin = JellyfinClient(settings)
    seer = SeerClient(settings)
    llm = LLMClient(settings)

    app.state.settings = settings
    app.state.jellyfin = jellyfin
    app.state.seer = seer
    app.state.llm = llm
    app.state.health_monitor = HealthMonitor(
        jellyfin=jellyfin,
        seer=seer,
        llm=llm,
        ttl_seconds=settings.health_cache_seconds,
    )
    yield


app = FastAPI(
    title=settings.app_name,
    summary="AI-driven proactive media curation bridge for the Arr stack.",
    lifespan=lifespan,
)


@app.get("/")
async def root(request: Request) -> dict[str, object]:
    return {
        "name": settings.app_name,
        "status": "providers-online" if (await request.app.state.health_monitor.snapshot())["overall_ok"] else "degraded",
        "message": "Vanguarr provider layer is online.",
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


@app.get("/api/health")
async def api_health(request: Request, force: bool = False) -> dict[str, object]:
    return await request.app.state.health_monitor.snapshot(force=force)
