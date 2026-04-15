from __future__ import annotations

from typing import Any, Protocol

from app.api.base import ConnectionCheck
from app.api.jellyfin import JellyfinClient
from app.api.plex import PlexClient
from app.core.settings import Settings


class MediaServerClientProtocol(Protocol):
    provider_key: str
    provider_label: str

    async def test_connection(self) -> ConnectionCheck: ...

    async def list_users(self) -> list[dict[str, Any]]: ...

    async def get_playback_history(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]: ...

    async def get_favorite_items(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]: ...


class MediaServerClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.jellyfin = JellyfinClient(settings)
        self.plex = PlexClient(settings)

    def _current_settings(self) -> Settings:
        if hasattr(self.settings, "snapshot"):
            return self.settings.snapshot()
        return self.settings

    def _active_client(self) -> MediaServerClientProtocol:
        settings = self._current_settings()
        if settings.normalized_media_server_provider == "plex":
            return self.plex
        return self.jellyfin

    @property
    def provider_key(self) -> str:
        return self._active_client().provider_key

    @property
    def provider_label(self) -> str:
        return self._active_client().provider_label

    async def test_connection(self) -> ConnectionCheck:
        return await self._active_client().test_connection()

    async def list_users(self) -> list[dict[str, Any]]:
        return await self._active_client().list_users()

    async def get_playback_history(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        return await self._active_client().get_playback_history(user_id, limit)

    async def get_favorite_items(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        return await self._active_client().get_favorite_items(user_id, limit)
