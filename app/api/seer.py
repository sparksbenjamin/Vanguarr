from __future__ import annotations

from typing import Any

from app.api.base import BaseAPIClient, ClientConfigError, ConnectionCheck
from app.core.settings import Settings


class SeerClient(BaseAPIClient):
    service_name = "Seer"

    def __init__(self, settings: Settings) -> None:
        super().__init__(None)
        self.settings = settings
        self._genre_cache: dict[str, dict[int, str]] = {}
        self._cache_signature: tuple[str, str, int] | None = None
        self._refresh_connection()

    def _current_settings(self) -> Settings:
        if hasattr(self.settings, "snapshot"):
            return self.settings.snapshot()
        return self.settings

    def _refresh_connection(self) -> Settings:
        settings = self._current_settings()
        base_url = (settings.seer_base_url or "").strip().rstrip("/")
        if base_url.endswith("/api/v1"):
            base_url = base_url[:-7]

        headers = {}
        if settings.seer_api_key:
            headers["X-Api-Key"] = settings.seer_api_key

        signature = (base_url, headers.get("X-Api-Key", ""), settings.candidate_limit)
        if self._cache_signature != signature:
            self._genre_cache.clear()
            self._cache_signature = signature

        self._set_connection(base_url, headers=headers)
        return settings

    async def test_connection(self) -> ConnectionCheck:
        settings = self._refresh_connection()
        if not self.base_url:
            return ConnectionCheck(
                service="Seer",
                ok=False,
                detail="SEER_BASE_URL is not configured.",
            )

        status = await self._request("GET", "/api/v1/status")
        app_version = status.get("version", "unknown")

        if not settings.seer_api_key:
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
        settings = self._refresh_connection()
        if not settings.seer_api_key:
            raise ClientConfigError("SEER_API_KEY is required to create requests.")

        body: dict[str, Any] = {"mediaType": media_type, "mediaId": media_id}
        if settings.seer_request_user_id is not None:
            body["userId"] = settings.seer_request_user_id

        payload = await self._request("POST", "/api/v1/request", json_body=body)
        return payload if isinstance(payload, dict) else {}

    async def discover_candidates(
        self,
        seed_items: list[dict[str, Any]],
        *,
        limit: int | None = None,
        trending_limit: int | None = None,
    ) -> list[dict[str, Any]]:
        settings = self._refresh_connection()
        max_candidates = limit or settings.candidate_limit
        max_trending = trending_limit or settings.trending_candidate_limit

        movie_genres = await self.get_genre_map("movie")
        tv_genres = await self.get_genre_map("tv")

        candidates: list[dict[str, Any]] = []
        seen: dict[tuple[str, int], dict[str, Any]] = {}

        def add_candidate(item: dict[str, Any], source: str, source_lanes: list[str]) -> None:
            candidate = self._normalize_candidate(item, movie_genres, tv_genres, source, source_lanes)
            if not candidate:
                return

            key = (candidate["media_type"], candidate["media_id"])
            existing = seen.get(key)
            if existing:
                if source not in existing["sources"]:
                    existing["sources"].append(source)
                for lane in source_lanes:
                    if lane not in existing["source_lanes"]:
                        existing["source_lanes"].append(lane)
                return

            seen[key] = candidate
            candidates.append(candidate)

        per_seed_limit = max(8, min(20, max_candidates // max(1, len(seed_items) or 1)))
        for seed in seed_items:
            media_id = seed.get("media_id")
            media_type = seed.get("media_type")
            seed_title = seed.get("title") or "seed"
            seed_lanes = [
                str(lane).strip()
                for lane in seed.get("seed_lanes", [])
                if str(lane).strip()
            ] or ["top_seed"]
            if media_type not in {"movie", "tv"} or media_id is None:
                continue

            added_for_seed = 0
            for item in await self.get_recommendations(media_type, int(media_id)):
                add_candidate(item, f"recommended:{seed_title}", seed_lanes)
                added_for_seed += 1
                if len(candidates) >= max_candidates:
                    return candidates
                if added_for_seed >= per_seed_limit:
                    break

        page = 1
        trending_added = 0
        while len(candidates) < max_candidates:
            trending_results = await self.get_trending(page=page)
            if not trending_results:
                break

            for item in trending_results:
                add_candidate(item, "trending", ["trending_lane"])
                trending_added += 1
                if len(candidates) >= max_candidates:
                    return candidates
                if trending_added >= max_trending:
                    return candidates

            page += 1

        return candidates

    async def get_genre_map(self, media_type: str) -> dict[int, str]:
        self._refresh_connection()
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
        source_lanes: list[str],
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
            "source_lanes": list(source_lanes),
        }
