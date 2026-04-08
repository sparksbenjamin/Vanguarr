from __future__ import annotations

from typing import Any

from app.api.base import BaseAPIClient, ClientConfigError, ConnectionCheck
from app.core.settings import Settings


class SeerClient(BaseAPIClient):
    service_name = "Seer"

    def __init__(self, settings: Settings) -> None:
        base_url = (settings.seer_base_url or "").strip().rstrip("/")
        if base_url.endswith("/api/v1"):
            base_url = base_url[:-7]

        headers = {}
        if settings.seer_api_key:
            headers["X-Api-Key"] = settings.seer_api_key

        super().__init__(base_url, headers=headers)
        self.settings = settings
        self._genre_cache: dict[str, dict[int, str]] = {}

    async def test_connection(self) -> ConnectionCheck:
        if not self.base_url:
            return ConnectionCheck(
                service="Seer",
                ok=False,
                detail="SEER_BASE_URL is not configured.",
            )

        status = await self._request("GET", "/api/v1/status")
        app_version = status.get("version", "unknown")

        if not self.settings.seer_api_key:
            return ConnectionCheck(
                service="Seer",
                ok=False,
                detail="Server is reachable, but SEER_API_KEY is missing.",
                meta={"version": app_version},
            )

        await self._request("GET", "/api/v1/request/count")
        return ConnectionCheck(
            service="Seer",
            ok=True,
            detail=f"Connected to Seer API {app_version}.",
            meta={"version": app_version},
        )

    async def get_trending(self, page: int = 1) -> list[dict[str, Any]]:
        payload = await self._request(
            "GET",
            "/api/v1/discover/trending",
            params={"page": page},
        )
        return self._extract_results(payload)

    async def get_recommendations(self, media_type: str, media_id: int) -> list[dict[str, Any]]:
        if media_type == "movie":
            path = f"/api/v1/movie/{media_id}/recommendations"
        elif media_type == "tv":
            path = f"/api/v1/tv/{media_id}/recommendations"
        else:
            return []

        payload = await self._request("GET", path)
        return self._extract_results(payload)

    async def request_media(self, media_type: str, media_id: int) -> dict[str, Any]:
        if not self.settings.seer_api_key:
            raise ClientConfigError("SEER_API_KEY is required to create requests.")

        body: dict[str, Any] = {"mediaType": media_type, "mediaId": media_id}
        if self.settings.seer_request_user_id is not None:
            body["userId"] = self.settings.seer_request_user_id

        payload = await self._request("POST", "/api/v1/request", json_body=body)
        return payload if isinstance(payload, dict) else {}

    async def discover_candidates(
        self,
        history: list[dict[str, Any]],
        *,
        limit: int | None = None,
        seed_limit: int | None = None,
    ) -> list[dict[str, Any]]:
        max_candidates = limit or self.settings.candidate_limit
        seed_count = seed_limit or self.settings.recommendation_seed_limit

        movie_genres = await self.get_genre_map("movie")
        tv_genres = await self.get_genre_map("tv")

        candidates: list[dict[str, Any]] = []
        seen: dict[tuple[str, int], dict[str, Any]] = {}

        def add_candidate(item: dict[str, Any], source: str) -> None:
            candidate = self._normalize_candidate(item, movie_genres, tv_genres, source)
            if not candidate:
                return

            key = (candidate["media_type"], candidate["media_id"])
            existing = seen.get(key)
            if existing:
                if source not in existing["sources"]:
                    existing["sources"].append(source)
                return

            seen[key] = candidate
            candidates.append(candidate)

        for item in await self.get_trending():
            add_candidate(item, "trending")
            if len(candidates) >= max_candidates:
                return candidates

        seeded = 0
        for entry in history:
            tmdb_id = self._extract_tmdb_id(entry)
            media_type = self._map_jellyfin_media_type(entry.get("Type"))
            if not tmdb_id or media_type not in {"movie", "tv"}:
                continue

            for item in await self.get_recommendations(media_type, tmdb_id):
                add_candidate(item, f"recommended:{entry.get('Name', 'seed')}")
                if len(candidates) >= max_candidates:
                    return candidates

            seeded += 1
            if seeded >= seed_count:
                break

        return candidates

    async def get_genre_map(self, media_type: str) -> dict[int, str]:
        if media_type in self._genre_cache:
            return self._genre_cache[media_type]

        payload = await self._request("GET", f"/api/v1/genres/{media_type}")
        items = payload if isinstance(payload, list) else []
        genre_map = {int(item["id"]): item["name"] for item in items if "id" in item and "name" in item}
        self._genre_cache[media_type] = genre_map
        return genre_map

    @staticmethod
    def _extract_results(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            for key in ("results", "Items", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]

        return []

    @staticmethod
    def _extract_tmdb_id(item: dict[str, Any]) -> int | None:
        provider_ids = item.get("ProviderIds", {})
        raw_tmdb = (
            provider_ids.get("Tmdb")
            or provider_ids.get("TMDB")
            or provider_ids.get("tmdb")
        )
        if raw_tmdb is None:
            return None

        try:
            return int(raw_tmdb)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _map_jellyfin_media_type(item_type: str | None) -> str | None:
        if item_type == "Movie":
            return "movie"
        if item_type in {"Series", "Episode"}:
            return "tv"
        return None

    @staticmethod
    def _normalize_candidate(
        item: dict[str, Any],
        movie_genres: dict[int, str],
        tv_genres: dict[int, str],
        source: str,
    ) -> dict[str, Any] | None:
        media_type = item.get("mediaType")
        if media_type not in {"movie", "tv"}:
            return None

        media_id = item.get("id")
        if media_id is None:
            return None

        title = item.get("title") or item.get("name")
        if not title:
            return None

        genre_map = movie_genres if media_type == "movie" else tv_genres
        genre_names = [
            genre_map[genre_id]
            for genre_id in item.get("genreIds", [])
            if genre_id in genre_map
        ]

        return {
            "media_type": media_type,
            "media_id": int(media_id),
            "title": title,
            "overview": item.get("overview", ""),
            "genres": genre_names,
            "rating": item.get("voteAverage"),
            "vote_count": item.get("voteCount"),
            "popularity": item.get("popularity"),
            "release_date": item.get("releaseDate") or item.get("firstAirDate"),
            "poster_path": item.get("posterPath"),
            "backdrop_path": item.get("backdropPath"),
            "media_info": item.get("mediaInfo", {}),
            "sources": [source],
        }
