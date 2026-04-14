from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from app.api.base import BaseAPIClient, ClientConfigError, ConnectionCheck
from app.core.settings import Settings


logger = logging.getLogger("vanguarr.plex")


class PlexClient(BaseAPIClient):
    service_name = "Plex"
    provider_key = "plex"
    provider_label = "Plex"

    def __init__(self, settings: Settings) -> None:
        super().__init__(None)
        self.settings = settings
        self._metadata_cache: dict[str, dict[str, Any]] = {}
        self._cache_signature: tuple[str, str, str] | None = None
        self._refresh_connection()

    def _current_settings(self) -> Settings:
        if hasattr(self.settings, "snapshot"):
            return self.settings.snapshot()
        return self.settings

    def _refresh_connection(self) -> Settings:
        settings = self._current_settings()
        base_url = (settings.plex_base_url or "").strip().rstrip("/")
        token = str(settings.plex_api_token or "").strip()
        client_identifier = str(settings.plex_client_identifier or "vanguarr").strip() or "vanguarr"
        signature = (base_url, token, client_identifier)
        if self._cache_signature != signature:
            self._metadata_cache.clear()
            self._cache_signature = signature

        self._set_connection(
            base_url,
            headers=self._build_headers(
                api_token=token,
                client_identifier=client_identifier,
            ),
        )
        return settings

    @staticmethod
    def _build_headers(*, api_token: str, client_identifier: str) -> dict[str, str]:
        headers = {
            "X-Plex-Client-Identifier": client_identifier,
            "X-Plex-Product": "Vanguarr",
        }
        if api_token:
            headers["X-Plex-Token"] = api_token
        return headers

    async def test_connection(self) -> ConnectionCheck:
        settings = self._refresh_connection()
        if not self.base_url:
            return ConnectionCheck(
                service="Plex",
                ok=False,
                detail="PLEX_BASE_URL is not configured.",
            )

        if not settings.plex_api_token:
            return ConnectionCheck(
                service="Plex",
                ok=False,
                detail="PLEX_API_TOKEN is not configured.",
                meta={
                    "provider": self.provider_key,
                    "provider_name": self.provider_label,
                },
            )

        info_payload = await self._request("GET", "/")
        container = self._extract_container(info_payload)
        server_name = str(container.get("friendlyName") or "Plex").strip() or "Plex"
        version = str(container.get("version") or "unknown").strip() or "unknown"
        users = await self.list_users()
        return ConnectionCheck(
            service="Plex",
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
        if not settings.plex_api_token:
            raise ClientConfigError("PLEX_API_TOKEN is required to query Plex users.")

        history_items = await self._get_history_items(limit=500)
        users: dict[str, dict[str, Any]] = {}
        for item in history_items:
            user_id = self._extract_history_account_id(item)
            if user_id is None:
                continue
            username = self._extract_history_user_name(item) or f"plex-user-{user_id}"
            existing = users.get(user_id)
            if existing is None or existing["Name"].startswith("plex-user-"):
                users[user_id] = {"Id": user_id, "Name": username}

        return sorted(users.values(), key=lambda user: (user["Name"].lower(), user["Id"]))

    async def get_playback_history(self, user_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        if not settings.plex_api_token:
            raise ClientConfigError("PLEX_API_TOKEN is required to query playback history.")

        max_items = limit
        if max_items is None and not settings.profile_use_full_history:
            max_items = settings.profile_history_limit
        history_items = await self._get_history_items(account_id=user_id, limit=max_items)
        rating_keys = {
            rating_key
            for rating_key in (
                self._normalize_rating_key(item)
                for item in history_items
            )
            if rating_key
        }
        metadata_map = await self._fetch_metadata_map(rating_keys)

        normalized: list[dict[str, Any]] = []
        for item in history_items:
            rating_key = self._normalize_rating_key(item)
            metadata = metadata_map.get(rating_key or "", {})
            normalized_item = self._normalize_history_item(item, metadata)
            if normalized_item is not None:
                normalized.append(normalized_item)
        return normalized

    async def _get_history_items(
        self,
        *,
        account_id: str | None = None,
        limit: int | None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        target_limit = max(1, int(limit)) if limit is not None else None
        start = 0
        items: list[dict[str, Any]] = []

        while True:
            batch_size = page_size if target_limit is None else min(page_size, target_limit - len(items))
            payload = await self._request(
                "GET",
                "/status/sessions/history/all",
                params=self._build_history_params(
                    account_id=account_id,
                    start=start,
                    size=batch_size,
                ),
            )
            container = self._extract_container(payload)
            page_items = [
                item
                for item in container.get("Metadata", [])
                if isinstance(item, dict)
            ]
            if not page_items:
                break

            items.extend(page_items)
            total_size = int(container.get("totalSize") or 0)
            if target_limit is not None and len(items) >= target_limit:
                break
            start += len(page_items)
            if total_size:
                if start >= total_size:
                    break
                continue
            if len(page_items) < batch_size:
                break

        if target_limit is not None:
            return items[:target_limit]
        return items

    @staticmethod
    def _build_history_params(
        *,
        account_id: str | None,
        start: int,
        size: int,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "sort": "viewedAt:desc",
            "X-Plex-Container-Start": start,
            "X-Plex-Container-Size": size,
        }
        if account_id:
            params["accountID"] = account_id
        return params

    async def _fetch_metadata_map(self, rating_keys: set[str]) -> dict[str, dict[str, Any]]:
        if not rating_keys:
            return {}

        tasks = [self._get_metadata_item(rating_key) for rating_key in sorted(rating_keys)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        metadata_map: dict[str, dict[str, Any]] = {}
        for rating_key, result in zip(sorted(rating_keys), results):
            if isinstance(result, Exception):
                logger.warning("Plex metadata enrichment skipped rating_key=%s reason=%s", rating_key, result)
                metadata_map[rating_key] = {}
                continue
            metadata_map[rating_key] = result
        return metadata_map

    async def _get_metadata_item(self, rating_key: str) -> dict[str, Any]:
        cached = self._metadata_cache.get(rating_key)
        if cached is not None:
            return cached

        payload = await self._request(
            "GET",
            f"/library/metadata/{rating_key}",
            params={"includeGuids": 1},
        )
        container = self._extract_container(payload)
        items = container.get("Metadata", [])
        metadata = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else {}
        self._metadata_cache[rating_key] = metadata
        return metadata

    @classmethod
    def _normalize_history_item(
        cls,
        item: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any] | None:
        item_type = str(item.get("type") or metadata.get("type") or "").strip().lower()
        if item_type == "movie":
            normalized_type = "Movie"
            name = str(item.get("title") or metadata.get("title") or "Unknown Movie").strip()
            series_name = None
        elif item_type in {"show", "series"}:
            normalized_type = "Series"
            name = str(item.get("title") or metadata.get("title") or "Unknown Series").strip()
            series_name = None
        elif item_type == "episode":
            normalized_type = "Episode"
            name = str(item.get("title") or metadata.get("title") or "Unknown Episode").strip()
            series_name = str(
                item.get("grandparentTitle")
                or metadata.get("grandparentTitle")
                or metadata.get("parentTitle")
                or ""
            ).strip() or None
        else:
            return None

        normalized = {
            "Name": name,
            "Type": normalized_type,
            "Genres": cls._extract_genres(metadata),
            "CommunityRating": metadata.get("rating") or item.get("rating"),
            "ProviderIds": cls._extract_provider_ids(metadata),
            "ProductionYear": metadata.get("year") or cls._parse_year(item.get("originallyAvailableAt")),
            "PremiereDate": metadata.get("originallyAvailableAt") or item.get("originallyAvailableAt"),
            "UserData": {
                "LastPlayedDate": cls._format_viewed_at(item.get("viewedAt")),
            },
        }
        if series_name:
            normalized["SeriesName"] = series_name
        return normalized

    @staticmethod
    def _extract_container(payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            container = payload.get("MediaContainer")
            if isinstance(container, dict):
                return container
            return payload
        return {}

    @staticmethod
    def _extract_history_account_id(item: dict[str, Any]) -> str | None:
        raw_value = item.get("accountID") or item.get("accountId") or item.get("userID") or item.get("userId")
        if raw_value in ("", None):
            return None
        return str(raw_value).strip() or None

    @staticmethod
    def _extract_history_user_name(item: dict[str, Any]) -> str | None:
        direct_keys = (
            "username",
            "userName",
            "userTitle",
            "accountTitle",
            "accountName",
            "friendlyName",
        )
        for key in direct_keys:
            value = str(item.get(key) or "").strip()
            if value:
                return value

        for container_key in ("User", "Account", "user", "account"):
            nested = item.get(container_key)
            if not isinstance(nested, dict):
                continue
            for key in ("title", "name", "username", "email"):
                value = str(nested.get(key) or "").strip()
                if value:
                    return value
        return None

    @staticmethod
    def _normalize_rating_key(item: dict[str, Any]) -> str | None:
        rating_key = str(item.get("ratingKey") or "").strip()
        if rating_key:
            return rating_key

        key = str(item.get("key") or "").strip()
        marker = "/library/metadata/"
        if marker not in key:
            return None
        suffix = key.split(marker, 1)[1]
        normalized = suffix.split("/", 1)[0].strip()
        return normalized or None

    @staticmethod
    def _extract_genres(metadata: dict[str, Any]) -> list[str]:
        raw_genres = metadata.get("Genre") or metadata.get("Genres") or metadata.get("genres") or []
        if not isinstance(raw_genres, list):
            return []

        genres: list[str] = []
        seen: set[str] = set()
        for raw in raw_genres:
            if isinstance(raw, dict):
                value = str(raw.get("tag") or raw.get("title") or raw.get("name") or "").strip()
            else:
                value = str(raw).strip()
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            genres.append(value)
            seen.add(lowered)
        return genres

    @classmethod
    def _extract_provider_ids(cls, metadata: dict[str, Any]) -> dict[str, str]:
        provider_ids: dict[str, str] = {}
        for raw_value in cls._collect_guid_values(metadata):
            tmdb_id = cls._extract_external_guid(raw_value, "tmdb")
            if tmdb_id:
                provider_ids.setdefault("Tmdb", tmdb_id)
            imdb_id = cls._extract_external_guid(raw_value, "imdb")
            if imdb_id:
                provider_ids.setdefault("Imdb", imdb_id)
            tvdb_id = cls._extract_external_guid(raw_value, "tvdb")
            if tvdb_id:
                provider_ids.setdefault("Tvdb", tvdb_id)
        return provider_ids

    @staticmethod
    def _collect_guid_values(metadata: dict[str, Any]) -> list[str]:
        values: list[str] = []
        for key in ("Guid", "Guids", "guids"):
            raw_values = metadata.get(key)
            if not isinstance(raw_values, list):
                continue
            for raw in raw_values:
                if isinstance(raw, dict):
                    value = str(raw.get("id") or raw.get("guid") or raw.get("tag") or "").strip()
                else:
                    value = str(raw).strip()
                if value:
                    values.append(value)

        direct_guid = str(metadata.get("guid") or "").strip()
        if direct_guid:
            values.append(direct_guid)
        return values

    @staticmethod
    def _extract_external_guid(value: str, scheme: str) -> str | None:
        lowered = value.lower()
        marker = f"{scheme}://"
        if marker not in lowered:
            return None

        start = lowered.index(marker) + len(marker)
        identifier = value[start:]
        for separator in ("?", "/", "&"):
            if separator in identifier:
                identifier = identifier.split(separator, 1)[0]
        cleaned = identifier.strip()
        if not cleaned:
            return None
        return cleaned

    @staticmethod
    def _parse_year(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip()[:4])
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_viewed_at(value: Any) -> str | None:
        if value in (None, ""):
            return None
        try:
            timestamp = int(value)
        except (TypeError, ValueError):
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")
