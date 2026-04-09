from __future__ import annotations

from typing import Any

from app.api.base import BaseAPIClient, ConnectionCheck
from app.core.settings import Settings


class TMDbClient(BaseAPIClient):
    service_name = "TMDb"

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            settings.tmdb_base_url,
            headers=self._build_headers(settings.tmdb_api_read_access_token),
        )
        self.settings = settings
        self._details_cache: dict[tuple[str, int], dict[str, Any]] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.settings.tmdb_api_read_access_token or self.settings.tmdb_api_key)

    @staticmethod
    def _build_headers(read_access_token: str | None) -> dict[str, str]:
        if not read_access_token:
            return {}
        return {"Authorization": f"Bearer {read_access_token}"}

    def _auth_params(self) -> dict[str, Any]:
        if self.settings.tmdb_api_key:
            return {"api_key": self.settings.tmdb_api_key}
        return {}

    async def test_connection(self) -> ConnectionCheck:
        if not self.enabled:
            return ConnectionCheck(
                service="TMDb",
                ok=True,
                detail="TMDb enrichment is disabled because no TMDB credentials are configured.",
                meta={"enabled": False},
            )

        payload = await self._request("GET", "/configuration", params=self._auth_params())
        images = payload.get("images", {}) if isinstance(payload, dict) else {}
        return ConnectionCheck(
            service="TMDb",
            ok=True,
            detail="TMDb enrichment is ready.",
            meta={
                "enabled": True,
                "base_url": self.base_url,
                "poster_sizes": images.get("poster_sizes", [])[:5],
                "watch_region": self.settings.tmdb_watch_region,
                "language": self.settings.tmdb_language,
            },
        )

    async def get_details(self, media_type: str, media_id: int) -> dict[str, Any]:
        if not self.enabled:
            return {}

        key = (media_type, int(media_id))
        cached = self._details_cache.get(key)
        if cached is not None:
            return cached

        append_parts = {
            "movie": "keywords,credits,release_dates,watch/providers",
            "tv": "keywords,aggregate_credits,content_ratings,watch/providers",
        }.get(media_type)
        if not append_parts:
            return {}

        params = self._auth_params()
        params["language"] = self.settings.tmdb_language
        params["append_to_response"] = append_parts

        payload = await self._request("GET", f"/{media_type}/{int(media_id)}", params=params)
        normalized = self._normalize_details(media_type, payload)
        self._details_cache[key] = normalized
        return normalized

    def _normalize_details(self, media_type: str, payload: Any) -> dict[str, Any]:
        item = payload if isinstance(payload, dict) else {}
        keywords = self._extract_keywords(item.get("keywords"))
        cast = self._extract_cast(item, media_type)
        creative_leads = self._extract_creative_leads(item, media_type)
        brands = self._extract_brands(item)
        collection = item.get("belongs_to_collection") if isinstance(item.get("belongs_to_collection"), dict) else {}
        providers = self._extract_watch_providers(item.get("watch/providers"))

        return {
            "keywords": keywords,
            "top_billed_cast": cast,
            "creative_leads": creative_leads,
            "featured_people": self._dedupe_strings(cast + creative_leads)[:8],
            "networks": [network.get("name") for network in item.get("networks", []) if network.get("name")],
            "production_companies": [
                company.get("name")
                for company in item.get("production_companies", [])
                if company.get("name")
            ],
            "brands": brands,
            "collection_name": str(collection.get("name") or "").strip() or None,
            "collection_id": collection.get("id"),
            "certification": self._extract_certification(item, media_type),
            "providers": providers,
            "original_language": item.get("original_language"),
            "origin_countries": item.get("origin_country", []),
            "status": item.get("status"),
            "adult": bool(item.get("adult")),
            "tmdb_vote_average": item.get("vote_average"),
            "tmdb_vote_count": item.get("vote_count"),
        }

    @staticmethod
    def _dedupe_strings(values: list[Any]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in values:
            value = str(raw or "").strip()
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            normalized.append(value)
            seen.add(lowered)
        return normalized

    @classmethod
    def _extract_keywords(cls, payload: Any) -> list[str]:
        if not isinstance(payload, dict):
            return []
        items = payload.get("keywords") or payload.get("results") or []
        if not isinstance(items, list):
            return []
        return cls._dedupe_strings([item.get("name") for item in items if isinstance(item, dict)])[:12]

    @classmethod
    def _extract_cast(cls, payload: dict[str, Any], media_type: str) -> list[str]:
        credits_key = "aggregate_credits" if media_type == "tv" else "credits"
        credits = payload.get(credits_key, {})
        cast = credits.get("cast", []) if isinstance(credits, dict) else []
        if not isinstance(cast, list):
            return []
        names = [item.get("name") for item in cast[:6] if isinstance(item, dict) and item.get("name")]
        return cls._dedupe_strings(names)

    @classmethod
    def _extract_creative_leads(cls, payload: dict[str, Any], media_type: str) -> list[str]:
        credits_key = "aggregate_credits" if media_type == "tv" else "credits"
        credits = payload.get(credits_key, {})
        crew = credits.get("crew", []) if isinstance(credits, dict) else []
        if not isinstance(crew, list):
            return []

        prioritized: list[str] = []
        wanted_jobs = {"creator", "director", "writer", "screenplay", "executive producer", "producer"}
        for item in crew:
            if not isinstance(item, dict):
                continue

            jobs: list[str] = []
            if isinstance(item.get("jobs"), list):
                jobs.extend(str(job.get("job") or "").strip().lower() for job in item["jobs"] if isinstance(job, dict))
            if item.get("job"):
                jobs.append(str(item.get("job")).strip().lower())
            department = str(item.get("department") or "").strip().lower()
            if department in {"writing", "directing", "production"}:
                jobs.append(department)

            if wanted_jobs.intersection(job for job in jobs if job):
                prioritized.append(str(item.get("name") or "").strip())

        return cls._dedupe_strings(prioritized)[:6]

    @classmethod
    def _extract_brands(cls, payload: dict[str, Any]) -> list[str]:
        networks = [item.get("name") for item in payload.get("networks", []) if isinstance(item, dict) and item.get("name")]
        companies = [
            item.get("name")
            for item in payload.get("production_companies", [])
            if isinstance(item, dict) and item.get("name")
        ]
        return cls._dedupe_strings(networks + companies)[:8]

    def _extract_watch_providers(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {"region": self.settings.tmdb_watch_region, "flatrate": [], "rent": [], "buy": [], "link": None}

        results = payload.get("results", {})
        if not isinstance(results, dict):
            return {"region": self.settings.tmdb_watch_region, "flatrate": [], "rent": [], "buy": [], "link": None}

        region_payload = results.get(self.settings.tmdb_watch_region, {})
        if not isinstance(region_payload, dict):
            return {"region": self.settings.tmdb_watch_region, "flatrate": [], "rent": [], "buy": [], "link": None}

        return {
            "region": self.settings.tmdb_watch_region,
            "flatrate": self._dedupe_strings(
                [item.get("provider_name") for item in region_payload.get("flatrate", []) if isinstance(item, dict)]
            )[:8],
            "rent": self._dedupe_strings(
                [item.get("provider_name") for item in region_payload.get("rent", []) if isinstance(item, dict)]
            )[:8],
            "buy": self._dedupe_strings(
                [item.get("provider_name") for item in region_payload.get("buy", []) if isinstance(item, dict)]
            )[:8],
            "link": region_payload.get("link"),
        }

    def _extract_certification(self, payload: dict[str, Any], media_type: str) -> str | None:
        if media_type == "movie":
            release_dates = payload.get("release_dates", {})
            results = release_dates.get("results", []) if isinstance(release_dates, dict) else []
            for item in results:
                if not isinstance(item, dict) or item.get("iso_3166_1") != self.settings.tmdb_watch_region:
                    continue
                for release in item.get("release_dates", []):
                    if isinstance(release, dict):
                        certification = str(release.get("certification") or "").strip()
                        if certification:
                            return certification
            return None

        content_ratings = payload.get("content_ratings", {})
        results = content_ratings.get("results", []) if isinstance(content_ratings, dict) else []
        for item in results:
            if not isinstance(item, dict) or item.get("iso_3166_1") != self.settings.tmdb_watch_region:
                continue
            rating = str(item.get("rating") or "").strip()
            if rating:
                return rating
        return None
