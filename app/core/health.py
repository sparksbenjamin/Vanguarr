from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from app.api.base import ConnectionCheck
from app.api.jellyfin import JellyfinClient
from app.api.llm import LLMClient
from app.api.seer import SeerClient
from app.api.tmdb import TMDbClient


class HealthMonitor:
    def __init__(
        self,
        *,
        jellyfin: JellyfinClient,
        seer: SeerClient,
        tmdb: TMDbClient,
        llm: LLMClient,
        ttl_seconds: int = 30,
    ) -> None:
        self.jellyfin = jellyfin
        self.seer = seer
        self.tmdb = tmdb
        self.llm = llm
        self.ttl_seconds = ttl_seconds
        self._cached_payload: dict[str, Any] | None = None
        self._expires_at: datetime | None = None

    async def snapshot(self, *, force: bool = False) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        if (
            not force
            and self._cached_payload is not None
            and self._expires_at is not None
            and now < self._expires_at
        ):
            return self._cached_payload

        checks = await asyncio.gather(
            self._safe_check("Jellyfin", self.jellyfin.test_connection()),
            self._safe_check("Seer", self.seer.test_connection()),
            self._safe_check("TMDb", self.tmdb.test_connection()),
            self._safe_check("LLM", self.llm.test_connection()),
        )

        payload = {
            "generated_at": now.isoformat(),
            "overall_ok": all(check.ok for check in checks),
            "services": {
                "jellyfin": checks[0].to_dict(),
                "seer": checks[1].to_dict(),
                "tmdb": checks[2].to_dict(),
                "llm": checks[3].to_dict(),
            },
        }
        self._cached_payload = payload
        self._expires_at = now + timedelta(seconds=self.ttl_seconds)
        return payload

    @staticmethod
    async def _safe_check(service_name: str, coroutine: Any) -> ConnectionCheck:
        try:
            return await asyncio.wait_for(coroutine, timeout=8)
        except Exception as exc:  # pragma: no cover - defensive catch for dashboard health
            return ConnectionCheck(service=service_name, ok=False, detail=str(exc))
