from app.core.prompts import build_decision_messages, build_profile_architect_user_prompt
from app.core.services import VanguarrService


def test_select_recommendation_seeds_prefers_top_watched_titles() -> None:
    history = [
        {
            "Name": "Movie Alpha",
            "Type": "Movie",
            "Genres": ["Sci-Fi"],
            "CommunityRating": 8.4,
            "Overview": "Alpha overview",
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Movie Beta",
            "Type": "Movie",
            "Genres": ["Drama"],
            "CommunityRating": 7.1,
            "Overview": "Beta overview",
            "ProviderIds": {"Tmdb": "202"},
            "UserData": {"LastPlayedDate": "2026-04-08T09:00:00Z"},
        },
        {
            "Name": "Movie Alpha",
            "Type": "Movie",
            "Genres": ["Sci-Fi", "Thriller"],
            "CommunityRating": 8.4,
            "Overview": "Alpha overview",
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-07T09:00:00Z"},
        },
    ]

    seeds = VanguarrService._select_recommendation_seeds(history, limit=2)

    assert [seed["media_id"] for seed in seeds] == [101, 202]
    assert seeds[0]["play_count"] == 2
    assert seeds[0]["genres"] == ["Sci-Fi", "Thriller"]


def test_decision_prompt_includes_viewing_history_block() -> None:
    messages = build_decision_messages(
        username="alice",
        profile_block="[VANGUARR_PROFILE_V3]\nUser: alice",
        viewing_history={
            "history_count": 12,
            "top_content": [{"title": "Movie Alpha", "play_count": 3}],
            "top_genres": ["Sci-Fi"],
            "recent_plays": [{"name": "Movie Alpha"}],
        },
        candidate={
            "media_type": "movie",
            "media_id": 303,
            "title": "Movie Gamma",
            "overview": "Gamma overview",
            "genres": ["Sci-Fi"],
            "rating": 8.8,
            "vote_count": 1000,
            "popularity": 90,
            "release_date": "2026-01-01",
            "sources": ["recommended:Movie Alpha"],
            "media_info": {},
        },
        global_exclusions=["No Horror"],
    )

    prompt = messages[1]["content"]

    assert "Block 2 (Observed Signals): User Viewing History" in prompt
    assert "Movie Alpha" in prompt
    assert "Base the score on the viewing history first" in prompt


def test_profile_history_context_compacts_repeated_titles() -> None:
    history = [
        {
            "Name": "Episode 1",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Drama"],
            "CommunityRating": 8.1,
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Episode 2",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi"],
            "CommunityRating": 8.1,
            "UserData": {"LastPlayedDate": "2026-04-07T10:00:00Z"},
        },
        {
            "Name": "Movie Beta",
            "Type": "Movie",
            "Genres": ["Drama"],
            "CommunityRating": 7.4,
            "UserData": {"LastPlayedDate": "2026-04-06T10:00:00Z"},
        },
    ]

    summary = VanguarrService._build_profile_history_context(history, top_limit=5, recent_limit=3, recent_window=3)

    assert summary["history_count"] == 3
    assert summary["top_titles"][0]["title"] == "Show Alpha"
    assert summary["top_titles"][0]["play_count"] == 2
    assert summary["top_titles"][0]["media_type"] == "tv"
    assert "Sci-Fi" in summary["top_genres"]
    assert summary["recent_momentum"][0]["title"] == "Show Alpha"
    assert summary["recent_momentum"][0]["play_count"] == 2
    assert "recent_plays" not in summary


def test_profile_architect_prompt_uses_viewing_summary() -> None:
    prompt = build_profile_architect_user_prompt(
        "alice",
        {
            "history_count": 12,
            "top_titles": [{"title": "Show Alpha", "play_count": 4}],
            "top_genres": ["Sci-Fi"],
            "recent_momentum": [{"title": "Show Alpha", "play_count": 2}],
        },
        "[VANGUARR_PROFILE_V3]\nUser: alice",
    )

    assert "Observed Jellyfin viewing summary" in prompt
    assert "Show Alpha" in prompt
    assert "Use grouped watch counts for shows and movies as durable taste signals" in prompt
