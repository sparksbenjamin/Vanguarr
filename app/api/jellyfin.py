from __future__ import annotations

from typing import Any
from urllib.parse import quote

from app.api.base import BaseAPIClient, ClientConfigError, ConnectionCheck
from app.core.settings import Settings


VANGUARR_JELLYFIN_PLUGIN_NAME = "Vanguarr"
VANGUARR_JELLYFIN_PLUGIN_GUID = "7d7e8c4f-0fbe-48d0-95a9-8ca4c7d7c5e8"
VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_NAME = "Vanguarr"
VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL = (
    "https://raw.githubusercontent.com/sparksbenjamin/Vanguarr/main/jellyfin-plugin/manifest.json"
)


class JellyfinClient(BaseAPIClient):
    service_name = "Jellyfin"
    provider_key = "jellyfin"
    provider_label = "Jellyfin"
    library_sync_timeout_seconds = 60.0

    def __init__(self, settings: Settings) -> None:
        super().__init__(None)
        self.settings = settings
        self._refresh_connection()

    def _current_settings(self) -> Settings:
        if hasattr(self.settings, "snapshot"):
            return self.settings.snapshot()
        return self.settings

    def _refresh_connection(self) -> Settings:
        settings = self._current_settings()
        self._set_connection(
            settings.jellyfin_base_url,
            headers=self._build_headers(settings.jellyfin_api_key),
        )
        return settings

    @staticmethod
    def _build_headers(api_key: str | None) -> dict[str, str]:
        headers = {}
        if api_key:
            headers["Authorization"] = f'MediaBrowser Token="{api_key}"'
            headers["X-Emby-Token"] = api_key
        return headers

    async def test_connection(self) -> ConnectionCheck:
        settings = self._refresh_connection()
        if not self.base_url:
            return ConnectionCheck(
                service="Jellyfin",
                ok=False,
                detail="JELLYFIN_BASE_URL is not configured.",
            )

        public_info = await self._request("GET", "/System/Info/Public")
        server_name = public_info.get("ServerName", "Jellyfin")
        version = public_info.get("Version", "unknown")

        if not settings.jellyfin_api_key:
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
            meta={
                "provider": self.provider_key,
                "provider_name": self.provider_label,
                "server_name": server_name,
                "version": version,
                "users": len(users),
            },
        )

    async def list_users(self) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query users.")

        payload = await self._request("GET", "/Users")
        users = payload if isinstance(payload, list) else payload.get("Items", [])
        return [
            user
            for user in users
            if not user.get("Policy", {}).get("IsDisabled", False)
        ]

    async def get_playback_history(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query playback history.")

        payload = await self._request(
            "GET",
            "/Items",
            params={
                "userId": user_id,
                "limit": limit or settings.profile_history_limit,
                "recursive": "true",
                "includeItemTypes": "Movie,Series,Episode",
                "filters": "IsPlayed",
                "sortBy": "DatePlayed",
                "sortOrder": "Descending",
                "fields": (
                    "Overview,Genres,CommunityRating,ProviderIds,ProductionYear,"
                    "PremiereDate,SeriesName,SeriesId,Taglines,UserData"
                ),
            },
        )
        return payload.get("Items", []) if isinstance(payload, dict) else []

    async def get_resumable_items(self, user_id: str, limit: int = 150) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query resumable items.")

        payload = await self._request(
            "GET",
            "/Items",
            params={
                "userId": user_id,
                "limit": max(1, int(limit)),
                "recursive": "true",
                "includeItemTypes": "Movie,Episode",
                "filters": "IsResumable",
                "sortBy": "DatePlayed",
                "sortOrder": "Descending",
                "fields": (
                    "Overview,Genres,CommunityRating,ProviderIds,ProductionYear,"
                    "PremiereDate,SeriesName,UserData"
                ),
            },
        )
        return payload.get("Items", []) if isinstance(payload, dict) else []

    async def get_library_items(
        self,
        *,
        user_id: str | None = None,
        limit: int | None = None,
        search_term: str | None = None,
        media_type: str | None = None,
        parent_id: str | None = None,
    ) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query library items.")

        include_item_types = "Movie,Series"
        if media_type == "movie":
            include_item_types = "Movie"
        elif media_type == "tv":
            include_item_types = "Series"

        page_size = min(limit or 150, 150)
        items: list[dict[str, Any]] = []
        start_index = 0

        while True:
            params: dict[str, Any] = {
                "limit": page_size,
                "startIndex": start_index,
                "recursive": "true",
                "includeItemTypes": include_item_types,
                "sortBy": "SortName",
                "sortOrder": "Ascending",
                "fields": (
                    "Overview,Genres,CommunityRating,ProviderIds,ProductionYear,PremiereDate"
                ),
            }
            if user_id:
                params["userId"] = user_id
            if search_term:
                params["searchTerm"] = search_term
            if parent_id:
                params["parentId"] = parent_id

            payload = await self._request(
                "GET",
                "/Items",
                params=params,
                timeout=self.library_sync_timeout_seconds,
            )
            batch = payload.get("Items", []) if isinstance(payload, dict) else []
            items.extend(batch)

            total_record_count = int(payload.get("TotalRecordCount") or 0) if isinstance(payload, dict) else 0
            if not batch:
                break
            if limit is not None and len(items) >= limit:
                break
            if len(batch) < page_size:
                break
            if total_record_count and len(items) >= total_record_count:
                break

            start_index += len(batch)

        if limit is not None:
            return items[:limit]
        return items

    async def get_library_folders(self) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query Jellyfin libraries.")

        payload = await self._request(
            "GET",
            "/Library/VirtualFolders",
            timeout=self.library_sync_timeout_seconds,
        )
        folders = payload if isinstance(payload, list) else payload.get("Items", [])
        return [item for item in folders if isinstance(item, dict)]

    async def get_repositories(self) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to manage Jellyfin plugin repositories.")

        payload = await self._request("GET", "/Repositories")
        return payload if isinstance(payload, list) else []

    async def set_repositories(self, repositories: list[dict[str, Any]]) -> None:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to manage Jellyfin plugin repositories.")

        await self._request("POST", "/Repositories", json_body=repositories)

    async def get_plugins(self) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to query installed Jellyfin plugins.")

        payload = await self._request("GET", "/Plugins")
        return payload if isinstance(payload, list) else []

    async def install_package(
        self,
        name: str,
        *,
        assembly_guid: str | None = None,
        version: str | None = None,
        repository_url: str | None = None,
    ) -> None:
        settings = self._refresh_connection()
        if not settings.jellyfin_api_key:
            raise ClientConfigError("JELLYFIN_API_KEY is required to install Jellyfin plugins.")

        params: dict[str, Any] = {}
        if assembly_guid:
            params["assemblyGuid"] = assembly_guid
        if version:
            params["version"] = version
        if repository_url:
            params["repositoryUrl"] = repository_url

        await self._request(
            "POST",
            f"/Packages/Installed/{quote(name, safe='')}",
            params=params or None,
        )

    @staticmethod
    def _normalize_repository(repository: dict[str, Any]) -> dict[str, Any]:
        return {
            "Name": str(repository.get("Name") or repository.get("name") or "").strip(),
            "Url": str(repository.get("Url") or repository.get("url") or "").strip(),
            "Enabled": bool(
                repository["Enabled"] if "Enabled" in repository else repository.get("enabled", False)
            ),
        }

    @staticmethod
    def _upsert_repository(
        repositories: list[dict[str, Any]],
        *,
        name: str,
        url: str,
    ) -> tuple[list[dict[str, Any]], bool, bool, bool]:
        normalized_url = url.strip().rstrip("/")
        updated: list[dict[str, Any]] = []
        found = False
        changed = False
        added = False
        enabled = False

        for repository in repositories:
            normalized = JellyfinClient._normalize_repository(repository)
            repository_url = normalized["Url"].rstrip("/")
            if repository_url.lower() == normalized_url.lower():
                found = True
                if not normalized["Enabled"]:
                    normalized["Enabled"] = True
                    enabled = True
                    changed = True
                if not normalized["Name"]:
                    normalized["Name"] = name
                    changed = True
            updated.append(normalized)

        if not found:
            updated.append(
                {
                    "Name": name,
                    "Url": url.strip(),
                    "Enabled": True,
                }
            )
            added = True
            changed = True

        return updated, added, enabled, changed

    async def install_vanguarr_plugin(self) -> dict[str, Any]:
        repositories = await self.get_repositories()
        updated_repositories, repository_added, repository_enabled, repositories_changed = self._upsert_repository(
            repositories,
            name=VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_NAME,
            url=VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
        )

        if repositories_changed:
            await self.set_repositories(updated_repositories)

        plugins = await self.get_plugins()
        plugin_already_installed = any(
            str(plugin.get("Id") or plugin.get("id") or "").strip().lower()
            == VANGUARR_JELLYFIN_PLUGIN_GUID.lower()
            for plugin in plugins
            if isinstance(plugin, dict)
        )

        plugin_install_requested = False
        if not plugin_already_installed:
            await self.install_package(
                VANGUARR_JELLYFIN_PLUGIN_NAME,
                assembly_guid=VANGUARR_JELLYFIN_PLUGIN_GUID,
                repository_url=VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
            )
            plugin_install_requested = True

        details: list[str] = []
        if repository_added:
            details.append("Added the Vanguarr plugin repository to Jellyfin.")
        elif repository_enabled:
            details.append("Enabled the existing Vanguarr plugin repository in Jellyfin.")
        else:
            details.append("Vanguarr plugin repository was already configured in Jellyfin.")

        if plugin_already_installed:
            details.append("The Vanguarr Jellyfin plugin is already installed.")
        else:
            details.append("Requested Vanguarr plugin installation from the configured Jellyfin repository.")
            details.append("Restart Jellyfin after the install finishes so the plugin can load.")

        return {
            "plugin_name": VANGUARR_JELLYFIN_PLUGIN_NAME,
            "plugin_guid": VANGUARR_JELLYFIN_PLUGIN_GUID,
            "repository_name": VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_NAME,
            "repository_url": VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
            "repository_added": repository_added,
            "repository_enabled": repository_enabled,
            "plugin_already_installed": plugin_already_installed,
            "plugin_install_requested": plugin_install_requested,
            "restart_required": plugin_install_requested,
            "detail": " ".join(details),
        }
