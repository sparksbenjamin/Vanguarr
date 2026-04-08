from fastapi import FastAPI

from app.core.settings import get_settings


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    summary="AI-driven proactive media curation bridge for the Arr stack.",
)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "status": "bootstrap-online",
        "message": "Vanguarr scaffold is ready for provider integration.",
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}
