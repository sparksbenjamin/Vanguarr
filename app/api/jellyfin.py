from __future__ import annotations

from typing import Any

from app.api.base import BaseAPIClient, ClientConfigError, ConnectionCheck
from app.core.settings import Settings


class JellyfinClient(BaseAPIClient):
    service_name = "Jellyfin"

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            settings.jellyfin_base_url,
            headers=self._build_headers(settings.jellyfin_api_key),
        )
        self.settings = settings

    @staticmethod
    def _build_headers(api_key: str | None) -> dict[str, str]:
        headers = {}
        if api_key:
            headers["Authorization"] = f'MediaBrowser Token="{api_key}"'
            headers["X-Emby-Token"] = api_key
        return headers

    async def test_connection(self) -> ConnectionCheck:
        if not self.base_url:
            return ConnectionCheck(
                service="Jellyfin",
                ok=False,
                detail="JELLYFIN_BASE_URL is not configured.",
            )

        public_info = await self._request("GET", "/System/Info/Public")
        server_name = public_info.get("ServerName", "Jellyfin")
        version = public_info.get("Version", "unknown")

        if not self.settings.jellyfin_api_key:
            return ConnectionCheck(
                service="Jellyfin",
                ok=False,
                detail="Server is reachable, but JELLYFIN_API_KEY is missing.",
                meta={"server_name": server_name, "version": version},
            )

        users = await self._request("GET", "/Users")
        return ConnectionCheck(
            service="Jellyfin",
            ok=True,
            detail=f"Connected to {server_name} {version}.",
            meta={"server_name": server_name, "version": version, "users": len(users)},
        )

    async def list_users(self) -> list[dict[str, Any]]:
        if not self.settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query users.")

        payload = await self._request("GET", "/Users")
        users = payload if isinstance(payload, list) else payload.get("Items", [])
        return [
            user
            for user in users
            if not user.get("Policy", {}).get("IsDisabled", False)
        ]

    async def get_playback_history(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        if not self.settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query playback history.")

        payload = await self._request(
            "GET",
            "/Items",
            params={
                "userId": user_id,
                "limit": limit or self.settings.profile_history_limit,
                "recursive": "true",
                "includeItemTypes": "Movie,Series,Episode",
                "filters": "IsPlayed",
                "sortBy": "DatePlayed",
                "sortOrder": "Descending",
                "fields": (
                    "Overview,Genres,CommunityRating,ProviderIds,ProductionYear,"
                    "PremiereDate,Taglines,UserData"
                ),
            },
        )
        return payload.get("Items", []) if isinstance(payload, dict) else []
