import asyncio

from app.api.seer import SeerClient
from app.core.settings import Settings


def test_resolve_genre_id_handles_exact_and_compound_matches() -> None:
    movie_genres = {18: "Drama", 28: "Action"}
    tv_genres = {18: "Drama", 10759: "Action & Adventure", 10765: "Sci-Fi & Fantasy"}

    assert (
        SeerClient._resolve_genre_id(
            "movie",
            "Drama",
            movie_genres=movie_genres,
            tv_genres=tv_genres,
        )
        == 18
    )
    assert (
        SeerClient._resolve_genre_id(
            "tv",
            "Action",
            movie_genres=movie_genres,
            tv_genres=tv_genres,
        )
        == 10759
    )
    assert (
        SeerClient._resolve_genre_id(
            "tv",
            "Sci-Fi",
            movie_genres=movie_genres,
            tv_genres=tv_genres,
        )
        == 10765
    )


def test_discover_candidates_blends_recommendation_genre_and_trending_sources() -> None:
    async def scenario() -> None:
        settings = Settings(
            seer_base_url="http://seer.local",
            seer_api_key="token",
            candidate_limit=10,
            genre_candidate_limit=2,
            trending_candidate_limit=1,
        )
        client = SeerClient(settings)

        async def fake_get_genre_map(media_type: str) -> dict[int, str]:
            if media_type == "movie":
                return {18: "Drama"}
            return {18: "Drama"}

        async def fake_get_recommendations(media_type: str, media_id: int) -> list[dict]:
            assert media_type == "tv"
            assert media_id == 101
            return [
                {
                    "id": 201,
                    "mediaType": "tv",
                    "name": "Recommended Show",
                    "genreIds": [18],
                    "voteAverage": 8.2,
                    "voteCount": 100,
                    "popularity": 44.0,
                    "firstAirDate": "2025-01-01",
                }
            ]

        genre_calls: list[tuple[str, int, int]] = []

        async def fake_get_genre_discover(media_type: str, genre_id: int, *, page: int = 1) -> list[dict]:
            genre_calls.append((media_type, genre_id, page))
            if page > 1:
                return []
            if media_type == "tv":
                return [
                    {
                        "id": 202,
                        "mediaType": "tv",
                        "name": "Genre TV Pick",
                        "genreIds": [18],
                        "voteAverage": 7.9,
                        "voteCount": 80,
                        "popularity": 33.0,
                        "firstAirDate": "2024-03-01",
                    }
                ]
            return [
                {
                    "id": 203,
                    "mediaType": "movie",
                    "title": "Genre Movie Pick",
                    "genreIds": [18],
                    "voteAverage": 7.4,
                    "voteCount": 60,
                    "popularity": 22.0,
                    "releaseDate": "2024-05-01",
                }
            ]

        async def fake_get_trending(page: int = 1) -> list[dict]:
            assert page == 1
            return [
                {
                    "id": 204,
                    "mediaType": "tv",
                    "name": "Trending Pick",
                    "genreIds": [18],
                    "voteAverage": 8.0,
                    "voteCount": 120,
                    "popularity": 55.0,
                    "firstAirDate": "2025-07-01",
                }
            ]

        client.get_genre_map = fake_get_genre_map  # type: ignore[method-assign]
        client.get_recommendations = fake_get_recommendations  # type: ignore[method-assign]
        client.get_genre_discover = fake_get_genre_discover  # type: ignore[method-assign]
        client.get_trending = fake_get_trending  # type: ignore[method-assign]

        candidates = await client.discover_candidates(
            [
                {
                    "media_type": "tv",
                    "media_id": 101,
                    "title": "Seed Show",
                    "seed_lanes": ["top_seed"],
                }
            ],
            genre_seeds=[
                {
                    "genre_name": "Drama",
                    "source": "genre:Drama",
                    "source_lanes": ["primary_genre_seed"],
                    "media_types": ["tv", "movie"],
                }
            ],
            limit=10,
            genre_limit=2,
            trending_limit=1,
        )

        assert [candidate["title"] for candidate in candidates] == [
            "Recommended Show",
            "Genre TV Pick",
            "Genre Movie Pick",
            "Trending Pick",
        ]
        assert candidates[0]["sources"] == ["recommended:Seed Show"]
        assert candidates[1]["sources"] == ["genre:Drama"]
        assert candidates[2]["sources"] == ["genre:Drama"]
        assert candidates[3]["sources"] == ["trending"]
        assert genre_calls == [("tv", 18, 1), ("tv", 18, 2), ("movie", 18, 1)]

    asyncio.run(scenario())
