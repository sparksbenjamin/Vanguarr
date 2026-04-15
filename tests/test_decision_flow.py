from datetime import datetime, timedelta
import json
from types import SimpleNamespace
import asyncio
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.api.seer import SeerRequestResult
from app.core.db import Base
from app.core.models import DecisionLog, LibraryMedia, RequestOutcomeEvent, RequestedMedia, SuggestedMedia, TaskRun
from app.core.prompts import build_decision_messages, build_profile_enrichment_messages, build_suggestion_messages
from app.core.settings import Settings
from app.core.services import ProfileStore, VanguarrService, normalize_jellyfin_user_id


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


def test_task_snapshot_for_target_matches_global_run_that_processed_user() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings()
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=TestingSessionLocal,
    )

    with TestingSessionLocal() as session:
        session.add(
            TaskRun(
                engine="profile_architect",
                status="success",
                summary="Updated 2 profile(s).",
                current_label="Complete",
                detail_json=json.dumps(
                    {
                        "target_username": "",
                        "processed_usernames": ["alice", "bob"],
                        "updated_users": ["alice", "bob"],
                        "errors": [],
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.commit()

    snapshot = service.get_task_snapshot_for_target("profile_architect", "alice")

    assert snapshot["status"] == "success"
    assert snapshot["summary"] == "Updated 2 profile(s)."
    assert snapshot["detail"]["processed_usernames"] == ["alice", "bob"]


def test_task_snapshot_for_target_matches_global_run_without_explicit_user_list() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings()
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=TestingSessionLocal,
    )

    with TestingSessionLocal() as session:
        session.add(
            TaskRun(
                engine="profile_architect",
                status="success",
                summary="Updated 3 profile(s).",
                current_label="Complete",
                detail_json=json.dumps(
                    {
                        "target_username": "",
                        "processed_users": 3,
                        "updated_users": [],
                        "errors": [],
                    },
                    ensure_ascii=True,
                ),
            )
        )
        session.commit()

    snapshot = service.get_task_snapshot_for_target("profile_architect", "charlie")

    assert snapshot["status"] == "success"
    assert snapshot["summary"] == "Updated 3 profile(s)."


def test_decision_prompt_includes_viewing_history_block() -> None:
    messages = build_decision_messages(
        username="alice",
        profile_payload={
            "profile_version": "v5",
            "profile_state": "ready",
            "username": "alice",
            "primary_genres": ["Sci-Fi"],
            "summary_block": "[VANGUARR_PROFILE_SUMMARY_V1]\nUser: alice",
        },
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
            "tmdb_details": {"keywords": ["space opera"], "featured_people": ["Actor Prime"]},
            "recommendation_features": {
                "deterministic_score": 0.83,
                "lane_tags": ["because_you_watched", "top_genre_lane"],
            },
        },
        global_exclusions=["No Horror"],
    )

    prompt = messages[1]["content"]

    assert "Block 1 (Target): Canonical User Profile JSON" in prompt
    assert "Block 3 (Observed Signals): User Viewing History" in prompt
    assert "Movie Alpha" in prompt
    assert "recommendation_features" in prompt
    assert "tmdb_details" in prompt
    assert "profile manifest and summary" in prompt


def test_suggestion_prompt_uses_available_title_context() -> None:
    messages = build_suggestion_messages(
        username="alice",
        profile_payload={
            "profile_version": "v5",
            "profile_state": "ready",
            "username": "alice",
            "primary_genres": ["Sci-Fi"],
            "summary_block": "[VANGUARR_PROFILE_SUMMARY_V1]\nUser: alice",
        },
        viewing_history={
            "history_count": 12,
            "top_titles": [{"title": "Show Alpha", "play_count": 3}],
            "recent_momentum": [{"title": "Movie Beta", "play_count": 1}],
        },
        candidate={
            "media_type": "movie",
            "media_id": 303,
            "title": "Movie Gamma",
            "overview": "Gamma overview",
            "genres": ["Sci-Fi"],
            "rating": 8.8,
            "release_date": "2026-01-01",
            "sources": ["library:indexed"],
            "media_info": {"status": "available"},
            "tmdb_details": {"keywords": ["space opera"]},
            "recommendation_features": {"deterministic_score": 0.83},
        },
    )

    prompt = messages[1]["content"]

    assert "Suggested For You" in messages[0]["content"]
    assert "Block 3 (Observed Signals): User Viewing History" in prompt
    assert "Block 4 (Candidate): Available Library Title" in prompt
    assert "Movie Gamma" in prompt
    assert "library:indexed" in prompt


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
    assert summary["ranked_genres"][0]["genre"] in {"Sci-Fi", "Drama"}
    assert any(item["genre"] == "Sci-Fi" and item["raw_count"] == 1 for item in summary["ranked_genres"])
    assert summary["recent_momentum"][0]["title"] == "Show Alpha"
    assert summary["recent_momentum"][0]["play_count"] == 2
    assert summary["release_year_preference"]["bias"] == "balanced"
    assert "recent_plays" not in summary


def test_profile_enrichment_prompt_uses_viewing_summary() -> None:
    messages = build_profile_enrichment_messages(
        "alice",
        {
            "history_count": 12,
            "primary_genres": ["Sci-Fi"],
            "secondary_genres": ["Drama"],
            "recent_genres": ["Mystery"],
            "top_titles": [{"title": "Show Alpha", "play_count": 4}],
            "discovery_lanes": ["Thriller"],
            "explicit_feedback": {"disliked_genres": ["Anime"]},
            "profile_exclusions": ["reality tv"],
            "operator_notes": "Keep this grounded.",
        },
    )

    assert "adjacent_genres" in messages[0]["content"]
    assert "Show Alpha" in messages[1]["content"]
    assert "code-derived viewing summary" in messages[1]["content"]
    assert "Anime" in messages[1]["content"]
    assert "Keep this grounded." in messages[1]["content"]


def test_render_profile_block_uses_code_derived_signals() -> None:
    history = [
        {
            "Name": "Episode 1",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Drama"],
            "CommunityRating": 8.3,
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Episode 2",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Thriller"],
            "CommunityRating": 8.3,
            "UserData": {"LastPlayedDate": "2026-04-07T10:00:00Z"},
        },
        {
            "Name": "Movie Beta",
            "Type": "Movie",
            "Genres": ["Drama", "Mystery"],
            "CommunityRating": 7.8,
            "UserData": {"LastPlayedDate": "2026-04-06T10:00:00Z"},
        },
    ]

    summary = VanguarrService._build_profile_history_context(history, top_limit=5, recent_limit=3, recent_window=3)
    block = VanguarrService._render_profile_block(
        "alice",
        {
            **summary,
            "adjacent_genres": ["Adventure"],
            "adjacent_themes": ["found family"],
            "explicit_feedback": {"liked_titles": [], "disliked_titles": [], "liked_genres": [], "disliked_genres": []},
            "profile_exclusions": [],
            "operator_notes": "",
        },
    )

    assert "[VANGUARR_PROFILE_SUMMARY_V1]" in block
    assert "Primary genres:" in block
    assert "Ranked genre stack:" in block
    assert "Format bias:" in block
    assert "Anchor titles:" in block
    assert "Add-on lanes worth testing:" in block
    assert "Adventure" in block
    assert "found family" in block


def test_profile_history_context_caps_repeat_episode_weighting() -> None:
    history = [
        {
            "Name": f"Episode {index}",
            "SeriesName": "Anime Loop",
            "Type": "Episode",
            "Genres": ["Animation", "Anime"],
            "CommunityRating": 7.5,
            "UserData": {"LastPlayedDate": f"2026-04-{index:02d}T10:00:00Z"},
        }
        for index in range(1, 6)
    ] + [
        {
            "Name": "Courtroom One",
            "Type": "Movie",
            "Genres": ["Drama", "Crime"],
            "CommunityRating": 8.1,
            "UserData": {"LastPlayedDate": "2026-04-06T10:00:00Z"},
        },
        {
            "Name": "Courtroom Two",
            "Type": "Movie",
            "Genres": ["Drama", "Crime"],
            "CommunityRating": 8.0,
            "UserData": {"LastPlayedDate": "2026-04-05T10:00:00Z"},
        },
    ]

    summary = VanguarrService._build_profile_history_context(history, top_limit=5, recent_limit=3, recent_window=6)

    assert summary["format_preference"]["preferred"] == "movie"
    assert summary["primary_genres"][:2] == ["Crime", "Drama"]
    assert "Animation" not in summary["primary_genres"][:2]


def test_profile_history_context_can_raise_recent_momentum_weight() -> None:
    history = [
        {
            "Name": "Space Case",
            "Type": "Movie",
            "Genres": ["Sci-Fi"],
            "CommunityRating": 8.4,
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Courtroom One",
            "Type": "Movie",
            "Genres": ["Drama"],
            "CommunityRating": 7.9,
            "UserData": {"LastPlayedDate": "2026-04-07T10:00:00Z"},
        },
        {
            "Name": "Courtroom Two",
            "Type": "Movie",
            "Genres": ["Drama"],
            "CommunityRating": 7.8,
            "UserData": {"LastPlayedDate": "2026-04-06T10:00:00Z"},
        },
    ]

    low_recent = VanguarrService._build_profile_history_context(
        history,
        top_limit=5,
        recent_limit=3,
        recent_window=1,
        recent_weight_percent=0,
    )
    high_recent = VanguarrService._build_profile_history_context(
        history,
        top_limit=5,
        recent_limit=3,
        recent_window=1,
        recent_weight_percent=200,
    )

    assert low_recent["primary_genres"][0] == "Drama"
    assert high_recent["primary_genres"][0] == "Sci-Fi"
    assert high_recent["recent_signal_weight_percent"] == 200


def test_recover_interrupted_tasks_marks_running_rows(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    with session_factory() as session:
        session.add(TaskRun(engine="decision_engine", status="running", summary="Task started."))
        session.add(
            TaskRun(
                engine="profile_architect",
                status="success",
                summary="Updated 4 profile(s).",
                finished_at=datetime.utcnow(),
            )
        )
        session.commit()

    recovered_count = service.recover_interrupted_tasks()

    assert recovered_count == 1

    with session_factory() as session:
        task_runs = list(session.scalars(select(TaskRun).order_by(TaskRun.id.asc())))

    assert task_runs[0].status == "interrupted"
    assert task_runs[0].finished_at is not None
    assert "restart before completion" in task_runs[0].summary.lower()
    assert task_runs[1].status == "success"


def test_build_available_library_candidates_prefers_indexed_library_rows(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    with session_factory() as session:
        session.add(
            LibraryMedia(
                source_provider="jellyfin",
                media_server_id="item-1",
                media_type="movie",
                title="Arrival",
                sort_title="Arrival",
                overview="First contact drama.",
                production_year=2016,
                release_date="2016-11-11T00:00:00.0000000Z",
                community_rating=8.1,
                genres_json='["Sci-Fi","Drama"]',
                state="available",
                tmdb_id=329865,
                imdb_id="tt2543164",
                payload_json="{}",
            )
        )
        session.commit()

    import asyncio

    candidates = asyncio.run(service._build_available_library_candidates("user-1"))

    assert len(candidates) == 1
    assert candidates[0]["title"] == "Arrival"
    assert candidates[0]["external_ids"]["tmdb"] == 329865
    assert candidates[0]["sources"] == ["library:indexed"]


def test_viewing_history_context_reuses_profile_summary_signals() -> None:
    history = [
        {
            "Name": "Episode 1",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Drama"],
            "CommunityRating": 8.3,
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Episode 2",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Thriller"],
            "CommunityRating": 8.3,
            "UserData": {"LastPlayedDate": "2026-04-07T10:00:00Z"},
        },
        {
            "Name": "Movie Beta",
            "Type": "Movie",
            "Genres": ["Drama", "Mystery"],
            "CommunityRating": 7.8,
            "UserData": {"LastPlayedDate": "2026-04-06T10:00:00Z"},
        },
    ]

    summary = VanguarrService._build_profile_history_context(history, top_limit=5, recent_limit=3, recent_window=3)
    summary.update(
        {
            "seer_adjacent_titles": ["Galaxy Court"],
            "seer_adjacent_genres": ["Space Opera"],
            "similar_users": ["bob"],
            "similar_user_genres": ["Mystery"],
            "similar_user_titles": ["Orbit Docket"],
        }
    )
    viewing_history = VanguarrService._build_viewing_history_context(
        history,
        recommendation_seeds=[{"title": "Show Alpha", "media_type": "tv", "media_id": 101}],
        profile_summary=summary,
    )

    assert "Sci-Fi" in viewing_history["primary_genres"]
    assert any(item["genre"] == "Sci-Fi" for item in viewing_history["ranked_genres"])
    assert viewing_history["format_preference"]["preferred"] == "balanced"
    assert viewing_history["recent_momentum"][0]["title"] == "Show Alpha"
    assert viewing_history["seer_adjacent_titles"] == ["Galaxy Court"]
    assert viewing_history["similar_users"] == ["bob"]


def test_candidate_pool_ranking_favors_anchor_and_genre_overlap() -> None:
    profile_summary = {
        "primary_genres": ["Sci-Fi", "Thriller"],
        "secondary_genres": ["Drama"],
        "recent_genres": ["Sci-Fi"],
        "discovery_lanes": ["Mystery"],
        "adjacent_genres": ["Adventure"],
        "top_titles": [{"title": "Show Alpha", "play_count": 4}],
        "repeat_titles": [{"title": "Show Alpha", "play_count": 4}],
        "recent_momentum": [{"title": "Show Alpha", "play_count": 2}],
        "format_preference": {"preferred": "tv", "movie_plays": 1, "tv_plays": 4},
        "release_year_preference": {"bias": "recent", "average_year": 2022},
        "ranked_genres": [{"genre": "Sci-Fi", "raw_count": 4, "recent_count": 2, "weighted_score": 5.5}],
        "top_keywords": ["space opera", "bounty hunter"],
        "favorite_people": ["Actor Prime", "Showrunner Nova"],
        "preferred_brands": ["HBO"],
        "favorite_collections": ["Alpha Saga"],
        "explicit_feedback": {"liked_titles": [], "disliked_titles": [], "liked_genres": [], "disliked_genres": []},
    }
    candidates = [
        {
            "media_type": "tv",
            "media_id": 303,
            "title": "Show Gamma",
            "genres": ["Sci-Fi", "Thriller"],
            "rating": 8.5,
            "vote_count": 800,
            "popularity": 100,
            "release_date": "2025-01-01",
            "sources": ["recommended:Show Alpha"],
            "source_lanes": ["top_seed", "repeat_watch_seed"],
            "tmdb_details": {
                "keywords": ["space opera", "rebellion"],
                "featured_people": ["Actor Prime", "Director Echo"],
                "brands": ["HBO"],
                "collection_name": "Alpha Saga",
                "adult": False,
            },
            "media_info": {},
        },
        {
            "media_type": "movie",
            "media_id": 404,
            "title": "Movie Delta",
            "genres": ["Comedy"],
            "rating": 7.1,
            "vote_count": 50,
            "popularity": 80,
            "release_date": "2010-01-01",
            "sources": ["trending"],
            "source_lanes": ["trending_lane"],
            "tmdb_details": {
                "keywords": ["slapstick"],
                "featured_people": ["Actor Comic"],
                "brands": ["Studio Lite"],
                "adult": False,
            },
            "media_info": {},
        },
    ]

    ranked = VanguarrService._rank_candidate_pool(candidates, profile_summary=profile_summary)

    assert ranked[0]["title"] == "Show Gamma"
    assert ranked[0]["recommendation_features"]["deterministic_score"] > ranked[1]["recommendation_features"]["deterministic_score"]
    assert "because_you_watched" in ranked[0]["recommendation_features"]["lane_tags"]
    assert "space opera" in ranked[0]["recommendation_features"]["matched_keywords"]
    assert "Actor Prime" in ranked[0]["recommendation_features"]["matched_people"]
    assert ranked[0]["recommendation_features"]["collection_match"] == "Alpha Saga"


def test_candidate_pool_penalizes_strong_off_profile_genre_mismatch() -> None:
    profile_summary = {
        "primary_genres": ["Drama", "Crime", "History"],
        "secondary_genres": ["Thriller"],
        "recent_genres": ["Drama", "Crime"],
        "discovery_lanes": ["Mystery"],
        "adjacent_genres": ["Biography"],
        "genre_focus_share": 0.76,
        "format_preference": {"preferred": "tv", "movie_plays": 1, "tv_plays": 8},
        "release_year_preference": {"bias": "balanced", "average_year": 2020},
        "ranked_genres": [
            {"genre": "Drama", "raw_count": 8, "recent_count": 4, "weighted_score": 9.2},
            {"genre": "Crime", "raw_count": 6, "recent_count": 3, "weighted_score": 7.6},
            {"genre": "History", "raw_count": 3, "recent_count": 1, "weighted_score": 3.5},
        ],
        "explicit_feedback": {"liked_titles": [], "disliked_titles": [], "liked_genres": [], "disliked_genres": []},
    }
    candidates = [
        {
            "media_type": "tv",
            "media_id": 501,
            "title": "Courtroom Echoes",
            "genres": ["Drama", "Crime"],
            "rating": 8.1,
            "vote_count": 600,
            "popularity": 120,
            "release_date": "2022-05-01",
            "sources": ["similar:Prestige Drama"],
            "source_lanes": ["top_seed"],
            "tmdb_details": {"adult": False},
            "media_info": {},
        },
        {
            "media_type": "tv",
            "media_id": 502,
            "title": "Mecha Academy",
            "genres": ["Animation", "Anime", "Action"],
            "rating": 8.6,
            "vote_count": 900,
            "popularity": 200,
            "release_date": "2023-01-10",
            "sources": ["trending"],
            "source_lanes": ["trending_lane"],
            "tmdb_details": {"adult": False},
            "media_info": {},
        },
    ]

    ranked = VanguarrService._rank_candidate_pool(candidates, profile_summary=profile_summary)

    assert ranked[0]["title"] == "Courtroom Echoes"
    assert ranked[1]["title"] == "Mecha Academy"
    assert ranked[1]["recommendation_features"]["score_breakdown"]["genre_guardrail"] < 0
    assert ranked[0]["recommendation_features"]["score_breakdown"]["genre_guardrail"] == 0
    assert (
        ranked[1]["recommendation_features"]["deterministic_score"]
        < ranked[0]["recommendation_features"]["deterministic_score"]
    )


def test_build_recommendation_seed_pool_blends_behavior_lanes() -> None:
    history = [
        {
            "Name": "Episode 1",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Drama"],
            "CommunityRating": 8.4,
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Episode 2",
            "SeriesName": "Show Alpha",
            "Type": "Episode",
            "Genres": ["Sci-Fi", "Thriller"],
            "CommunityRating": 8.4,
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-07T10:00:00Z"},
        },
        {
            "Name": "Movie Beta",
            "Type": "Movie",
            "Genres": ["Drama"],
            "CommunityRating": 7.8,
            "ProviderIds": {"Tmdb": "202"},
            "UserData": {"LastPlayedDate": "2026-04-08T09:00:00Z"},
        },
    ]

    summary = VanguarrService._build_profile_history_context(history, top_limit=5, recent_limit=3, recent_window=3)
    seeds = VanguarrService._build_recommendation_seed_pool(history, profile_summary=summary, limit=3)

    assert seeds
    assert seeds[0]["media_id"] == 101
    assert "top_seed" in seeds[0]["seed_lanes"]
    assert "repeat_watch_seed" in seeds[0]["seed_lanes"]
    assert "genre_anchor_seed" in seeds[0]["seed_lanes"]
    assert any("recent_seed" in seed["seed_lanes"] for seed in seeds)


def test_build_genre_discovery_seeds_prioritizes_primary_recent_adjacent_and_enrichment_lanes() -> None:
    seeds = VanguarrService._build_genre_discovery_seeds(
        {
            "primary_genres": ["Drama", "History"],
            "recent_genres": ["Drama", "Crime"],
            "adjacent_genres": ["Mystery", "Thriller"],
            "seer_adjacent_genres": ["Legal Thriller", "Mystery"],
            "similar_user_genres": ["Procedural", "Mystery"],
            "format_preference": {"preferred": "tv"},
        }
    )

    assert [seed["genre_name"] for seed in seeds] == [
        "Drama",
        "History",
        "Crime",
        "Mystery",
        "Thriller",
        "Legal Thriller",
        "Procedural",
    ]
    assert seeds[0]["source_lanes"] == ["primary_genre_seed"]
    assert seeds[2]["source_lanes"] == ["recent_genre_seed"]
    assert seeds[3]["source_lanes"] == ["adjacent_genre_seed"]
    assert seeds[5]["source_lanes"] == ["seer_genre_seed"]
    assert seeds[6]["source_lanes"] == ["similar_user_genre_seed"]
    assert all(seed["media_types"] == ["tv", "movie"] for seed in seeds)


def test_normalize_saved_profile_payload_regenerates_summary() -> None:
    payload = VanguarrService._normalize_saved_profile_payload(
        "alice",
        {
            "history_count": 4,
            "unique_titles": 2,
            "primary_genres": ["Sci-Fi", "Thriller"],
            "top_genres": ["Sci-Fi", "Thriller"],
            "ranked_genres": [{"genre": "Sci-Fi", "raw_count": 3, "recent_count": 2, "weighted_score": 4.5}],
            "top_titles": [{"title": "Show Alpha", "play_count": 3, "media_type": "tv"}],
            "recent_momentum": [{"title": "Show Alpha", "play_count": 2, "media_type": "tv"}],
            "repeat_titles": [{"title": "Show Alpha", "play_count": 3, "media_type": "tv"}],
            "adjacent_genres": ["Adventure"],
            "operator_notes": "Prefer high-conviction Sci-Fi.",
        },
    )

    assert payload["profile_state"] == "ready"
    assert payload["summary_block"].startswith("[VANGUARR_PROFILE_SUMMARY_V1]")
    assert "Operator note: Prefer high-conviction Sci-Fi." in payload["summary_block"]


def test_build_profile_payload_replaces_stale_adjacent_lanes_when_enrichment_succeeds() -> None:
    payload = VanguarrService._build_profile_payload(
        "alice",
        {
            "history_count": 4,
            "unique_titles": 2,
            "primary_genres": ["Sci-Fi", "Thriller"],
            "top_genres": ["Sci-Fi", "Thriller"],
            "ranked_genres": [{"genre": "Sci-Fi", "raw_count": 2, "recent_count": 1, "weighted_score": 2.75}],
            "top_titles": [{"title": "Show Alpha", "play_count": 3, "media_type": "tv"}],
            "recent_momentum": [{"title": "Show Alpha", "play_count": 2, "media_type": "tv"}],
            "repeat_titles": [{"title": "Show Alpha", "play_count": 3, "media_type": "tv"}],
        },
        enrichment={"adjacent_genres": ["Mystery"], "adjacent_themes": ["closed-circle tension"]},
        existing_payload={
            "adjacent_genres": ["Animation", "Adventure"],
            "adjacent_themes": ["school rivalry"],
        },
    )

    assert payload["adjacent_genres"] == ["Mystery"]
    assert payload["adjacent_themes"] == ["closed-circle tension"]


def test_compose_profile_payload_blends_seer_similar_tmdb_and_llm_layers(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(enabled=False),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    peer_payload = VanguarrService._normalize_saved_profile_payload(
        "bob",
        {
            "history_count": 8,
            "unique_titles": 4,
            "primary_genres": ["Crime", "Drama"],
            "secondary_genres": ["Mystery"],
            "recent_genres": ["Crime"],
            "top_genres": ["Crime", "Drama", "Mystery"],
            "ranked_genres": [
                {"genre": "Crime", "raw_count": 4, "recent_count": 2, "weighted_score": 5.5},
                {"genre": "Drama", "raw_count": 3, "recent_count": 1, "weighted_score": 4.0},
            ],
            "top_titles": [
                {"title": "Case Brief", "play_count": 4, "media_type": "tv"},
                {"title": "State of Evidence", "play_count": 2, "media_type": "tv"},
            ],
            "repeat_titles": [{"title": "Case Brief", "play_count": 4, "media_type": "tv"}],
            "recent_momentum": [{"title": "State of Evidence", "play_count": 2, "media_type": "tv"}],
            "adjacent_genres": ["Political Thriller"],
            "discovery_lanes": ["Procedural"],
            "format_preference": {"preferred": "tv", "movie_plays": 1, "tv_plays": 7},
            "release_year_preference": {"bias": "balanced", "average_year": 2020},
            "top_keywords": ["courtroom"],
        },
    )
    service.profile_store.write_payload("bob", peer_payload)

    existing_payload = {
        "explicit_feedback": {
            "liked_titles": ["Case Brief"],
            "disliked_titles": ["Anime Loop"],
            "liked_genres": ["Crime"],
            "disliked_genres": ["Anime"],
        },
        "profile_exclusions": ["reality tv"],
        "operator_notes": "Keep this grounded.",
        "adjacent_themes": ["institutional pressure"],
    }

    history = [
        {
            "Name": "Episode 1",
            "SeriesName": "Case Brief",
            "Type": "Episode",
            "Genres": ["Crime", "Drama"],
            "CommunityRating": 8.4,
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Episode 2",
            "SeriesName": "Case Brief",
            "Type": "Episode",
            "Genres": ["Crime", "Drama"],
            "CommunityRating": 8.4,
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-07T10:00:00Z"},
        },
        {
            "Name": "Bench Trial",
            "Type": "Movie",
            "Genres": ["Crime", "Drama"],
            "CommunityRating": 8.0,
            "ProviderIds": {"Tmdb": "202"},
            "UserData": {"LastPlayedDate": "2026-04-06T10:00:00Z"},
        },
    ]

    async def fake_seer(summary: dict, *, recommendation_seeds: list[dict]) -> dict:
        assert summary["explicit_feedback"]["disliked_genres"] == ["Anime"]
        assert recommendation_seeds
        return {
            **summary,
            "seer_adjacent_titles": ["State of Evidence"],
            "seer_adjacent_genres": ["Legal Thriller"],
        }

    async def fake_tmdb(summary: dict, *, recommendation_seeds: list[dict]) -> dict:
        return {
            **summary,
            "top_keywords": ["courtroom"],
            "favorite_people": ["Actor Prime"],
        }

    async def fake_llm(
        username: str,
        history_summary: dict,
        *,
        existing_payload: dict | None = None,
    ) -> dict:
        assert username == "alice"
        assert history_summary["similar_users"] == ["bob"]
        assert history_summary["top_keywords"] == ["courtroom"]
        assert existing_payload is not None
        return {
            "adjacent_genres": ["Political Thriller"],
            "adjacent_themes": ["institutional pressure"],
        }

    service._enrich_profile_summary_with_seer = fake_seer  # type: ignore[method-assign]
    service._enrich_profile_summary_with_tmdb = fake_tmdb  # type: ignore[method-assign]
    service._suggest_profile_enrichment = fake_llm  # type: ignore[method-assign]

    profile_payload, recommendation_seeds = asyncio.run(
        service._compose_profile_payload("alice", history, existing_payload=existing_payload)
    )

    assert recommendation_seeds
    assert profile_payload["seer_adjacent_titles"] == ["State of Evidence"]
    assert profile_payload["similar_users"] == ["bob"]
    assert profile_payload["similar_user_titles"] == ["State of Evidence"]
    assert profile_payload["top_keywords"] == ["courtroom"]
    assert profile_payload["adjacent_genres"][:2] == ["Political Thriller", "Legal Thriller"]
    assert profile_payload["adjacent_themes"] == ["institutional pressure"]
    assert profile_payload["explicit_feedback"]["disliked_genres"] == ["Anime"]
    assert "Seer recommendation neighborhoods keep clustering around State of Evidence." in profile_payload["summary_block"]
    assert "Local overlap with profiles like bob" in profile_payload["summary_block"]


def test_profile_store_writes_json_and_summary(tmp_path) -> None:
    store = ProfileStore(tmp_path)
    payload = VanguarrService._normalize_saved_profile_payload(
        "alice",
        {
            "history_count": 2,
            "unique_titles": 1,
            "primary_genres": ["Sci-Fi"],
            "top_genres": ["Sci-Fi"],
            "ranked_genres": [{"genre": "Sci-Fi", "raw_count": 2, "recent_count": 1, "weighted_score": 2.75}],
            "top_titles": [{"title": "Show Alpha", "play_count": 2, "media_type": "tv"}],
        },
    )

    json_path, summary_path = store.write_payload("alice", payload)

    assert json_path.exists()
    assert summary_path.exists()
    assert "\"profile_version\": \"v5\"" in json_path.read_text(encoding="utf-8")
    assert "[VANGUARR_PROFILE_SUMMARY_V1]" in summary_path.read_text(encoding="utf-8")


def test_diversify_candidates_caps_one_lane_before_backfill() -> None:
    candidates = []
    for idx in range(5):
        candidates.append(
            {
                "media_type": "tv",
                "media_id": 500 + idx,
                "title": f"Candidate {idx}",
                "genres": ["Sci-Fi"],
                "sources": ["recommended:Show Alpha"],
                "recommendation_features": {
                    "deterministic_score": 0.9 - (idx * 0.05),
                    "lane_tags": ["because_you_watched"],
                    "dominant_genre": "Sci-Fi",
                },
            }
        )

    diversified = VanguarrService._diversify_candidates(candidates, limit=3)

    assert len(diversified) == 3


def test_blend_confidences_uses_ai_weight_slider() -> None:
    mostly_code = VanguarrService._blend_confidences(
        deterministic_score=0.40,
        llm_confidence=0.80,
        llm_vote="REQUEST",
        llm_weight_percent=25,
    )
    mostly_ai = VanguarrService._blend_confidences(
        deterministic_score=0.40,
        llm_confidence=0.80,
        llm_vote="REQUEST",
        llm_weight_percent=75,
    )
    ignore_vote = VanguarrService._blend_confidences(
        deterministic_score=0.70,
        llm_confidence=0.80,
        llm_vote="IGNORE",
        llm_weight_percent=50,
    )

    assert mostly_code == 0.525
    assert mostly_ai == 0.775
    assert ignore_vote == 0.4


def test_select_suggestion_ai_candidates_uses_threshold_and_limit() -> None:
    candidates = [
        {"title": "Top Pick", "recommendation_features": {"deterministic_score": 0.91}},
        {"title": "Strong Pick", "recommendation_features": {"deterministic_score": 0.79}},
        {"title": "Borderline Pick", "recommendation_features": {"deterministic_score": 0.58}},
        {"title": "Low Pick", "recommendation_features": {"deterministic_score": 0.31}},
    ]

    selected = VanguarrService._select_suggestion_ai_candidates(
        candidates,
        threshold=0.58,
        limit=2,
    )

    assert [candidate["title"] for candidate in selected] == ["Top Pick", "Strong Pick"]


def test_filter_suggestion_candidates_for_display_uses_final_score_threshold() -> None:
    candidates = [
        {"title": "High Final", "recommendation_features": {"deterministic_score": 0.81, "hybrid_score": 0.84}},
        {"title": "Borderline Final", "recommendation_features": {"deterministic_score": 0.58, "hybrid_score": 0.58}},
        {"title": "Low Final", "recommendation_features": {"deterministic_score": 0.71, "hybrid_score": 0.41}},
        {"title": "Low Deterministic", "recommendation_features": {"deterministic_score": 0.33}},
    ]

    eligible = VanguarrService._filter_suggestion_candidates_for_display(
        candidates,
        threshold=0.58,
    )

    assert [candidate["title"] for candidate in eligible] == ["High Final", "Borderline Final"]


def test_suggestion_exclusion_context_filters_recent_repeat_and_in_progress_titles() -> None:
    history = [
        {
            "Name": "Movie Fresh",
            "Type": "Movie",
            "ProviderIds": {"Tmdb": "101"},
            "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
        },
        {
            "Name": "Movie Old",
            "Type": "Movie",
            "ProviderIds": {"Tmdb": "202"},
            "UserData": {"LastPlayedDate": "2026-02-01T10:00:00Z"},
        },
        {
            "Name": "Show Loop",
            "SeriesName": "Show Loop",
            "Type": "Episode",
            "ProviderIds": {"Tmdb": "303"},
            "UserData": {"LastPlayedDate": "2026-03-20T10:00:00Z"},
        },
        {
            "Name": "Show Loop",
            "SeriesName": "Show Loop",
            "Type": "Episode",
            "ProviderIds": {"Tmdb": "303"},
            "UserData": {"LastPlayedDate": "2026-03-18T10:00:00Z"},
        },
    ]
    in_progress = [
        {
            "Name": "Episode 4",
            "SeriesName": "Show Active",
            "Type": "Episode",
            "ProviderIds": {"Tmdb": "404"},
            "UserData": {"PlaybackPositionTicks": 12345},
        }
    ]

    context = VanguarrService._build_suggestion_exclusion_context(
        history,
        in_progress,
        recent_cooldown_days=14,
        repeat_watch_cutoff=2,
    )

    assert (
        VanguarrService._suggestion_exclusion_reason(
            {"media_type": "movie", "title": "Movie Fresh", "external_ids": {"tmdb": "101"}},
            context,
        )
        == "recently_watched"
    )
    assert (
        VanguarrService._suggestion_exclusion_reason(
            {"media_type": "tv", "title": "Show Loop", "external_ids": {"tmdb": "303"}},
            context,
        )
        == "repeat_watch"
    )
    assert (
        VanguarrService._suggestion_exclusion_reason(
            {"media_type": "tv", "title": "Show Active", "external_ids": {"tmdb": "404"}},
            context,
        )
        == "in_progress"
    )
    assert (
        VanguarrService._suggestion_exclusion_reason(
            {"media_type": "movie", "title": "Movie Old", "external_ids": {"tmdb": "202"}},
            context,
        )
        == "already_watched"
    )


def test_compose_decision_reasoning_uses_final_score_wording_and_threshold_context() -> None:
    reasoning = VanguarrService._compose_decision_reasoning(
        {
            "recommendation_features": {
                "analysis_summary": "Matches top genres Drama.",
                "score_breakdown": {
                    "source_affinity": 0.04,
                    "genre_affinity": 0.30,
                    "format_fit": 0.08,
                    "freshness_fit": 0.07,
                    "quality": 0.07,
                    "tmdb_themes": 0.00,
                    "tmdb_people": 0.00,
                    "tmdb_brands": 0.00,
                },
            }
        },
        deterministic_score=0.61,
        hybrid_confidence=0.67,
        decision="IGNORE",
        request_threshold=0.72,
        llm_vote="REQUEST",
        llm_reasoning="The title fits the user's current TV preferences.",
    )

    assert reasoning.startswith("Final score 0.67. Code score 0.61.")
    assert "stayed below the request threshold of 0.72" in reasoning
    assert "LLM vote: REQUEST." in reasoning


def test_normalize_jellyfin_user_id_compacts_guid_values() -> None:
    assert normalize_jellyfin_user_id("66456a3a-4cd3-46e3-83ce-254e99d4b09a") == "66456a3a4cd346e383ce254e99d4b09a"
    assert normalize_jellyfin_user_id("66456a3a4cd346e383ce254e99d4b09a") == "66456a3a4cd346e383ce254e99d4b09a"


def test_get_suggestions_matches_hyphenated_jellyfin_user_id(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    with session_factory() as session:
        session.add(
            SuggestedMedia(
                jellyfin_user_id="66456a3a4cd346e383ce254e99d4b09a",
                username="admin",
                rank=1,
                media_type="movie",
                title="Arrival",
                overview="First contact drama.",
                production_year=2016,
                score=0.91,
                reasoning="Matches sci-fi preference.",
                state="available",
                tmdb_id=329865,
                imdb_id="tt2543164",
            )
        )
        session.commit()

    results = service.get_suggestions(
        jellyfin_user_id="66456a3a-4cd3-46e3-83ce-254e99d4b09a",
        limit=5,
    )

    assert len(results) == 1
    assert results[0].title == "Arrival"


def test_get_log_feed_supports_filter_sort_and_paging(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
        decision_page_size=2,
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    with session_factory() as session:
        session.add_all(
            [
                DecisionLog(
                    engine="decision_engine",
                    username="alice",
                    media_type="movie",
                    media_id=1,
                    media_title="Arrival",
                    source="recommended",
                    decision="REQUEST",
                    confidence=0.92,
                    threshold=0.72,
                    requested=True,
                    reasoning="Strong fit.",
                    payload_json="{}",
                ),
                DecisionLog(
                    engine="suggested_for_you",
                    username="bob",
                    media_type="tv",
                    media_id=2,
                    media_title="Severance",
                    source="library:indexed",
                    decision="SUGGEST",
                    confidence=0.88,
                    threshold=0.58,
                    requested=False,
                    reasoning="Great adjacent match.",
                    payload_json="{}",
                ),
                DecisionLog(
                    engine="decision_engine",
                    username="carol",
                    media_type="movie",
                    media_id=3,
                    media_title="Dune",
                    source="trending",
                    decision="IGNORE",
                    confidence=0.41,
                    threshold=0.72,
                    requested=False,
                    reasoning="Too broad.",
                    payload_json="{}",
                    error="LLM timeout",
                ),
            ]
        )
        session.commit()

    feed = service.get_log_feed(view="suggestions", sort_by="media_title", sort_direction="asc", page=1, limit=5)

    assert feed["view"] == "suggestions"
    assert feed["total_rows"] == 1
    assert feed["view_counts"]["all"] == 3
    assert feed["view_counts"]["requests"] == 2
    assert feed["view_counts"]["suggestions"] == 1
    assert feed["rows"][0]["media_title"] == "Severance"
    assert feed["rows"][0]["engine_label"] == "Suggested For You"

    paged = service.get_log_feed(view="all", sort_by="created_at", sort_direction="desc", page=2, limit=2)

    assert paged["page"] == 2
    assert paged["total_pages"] == 2
    assert len(paged["rows"]) == 1


def test_library_sync_payload_includes_content_fingerprint() -> None:
    payload = VanguarrService._library_item_to_sync_payload(
        {
            "Id": "item-1",
            "Name": "Arrival",
            "SortName": "Arrival",
            "Type": "Movie",
            "Overview": "First contact drama.",
            "Genres": ["Sci-Fi", "Drama"],
            "CommunityRating": 8.1,
            "ProviderIds": {"Tmdb": "329865"},
            "PremiereDate": "2016-11-11T00:00:00.0000000Z",
            "ProductionYear": 2016,
        }
    )

    assert payload is not None
    assert payload["content_fingerprint"]


def test_resolve_tv_seed_media_ids_uses_library_index_series_tmdb(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    with session_factory() as session:
        session.add(
            LibraryMedia(
                source_provider="jellyfin",
                media_server_id="series-1",
                media_type="tv",
                title="Show Alpha",
                sort_title="Show Alpha",
                overview="Series overview.",
                production_year=2024,
                release_date="2024-01-01",
                community_rating=8.5,
                genres_json='["Sci-Fi","Drama"]',
                state="available",
                tmdb_id=101,
                tvdb_id=555,
                imdb_id="ttshowalpha",
                payload_json="{}",
            )
        )
        session.commit()

    resolved = service._resolve_tv_seed_media_ids_from_library_index(
        [
            {
                "media_type": "tv",
                "media_id": 1_932_395,
                "title": "Show Alpha",
                "play_count": 2,
                "seed_lanes": ["top_seed"],
            }
        ]
    )

    assert resolved[0]["media_id"] == 101
    assert resolved[0]["external_ids"]["tmdb"] == "101"
    assert resolved[0]["external_ids"]["tvdb"] == "555"


def test_run_profile_architect_resolves_tv_seeds_before_tmdb_enrichment(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    class FakeMediaServer:
        async def list_users(self) -> list[dict]:
            return [{"Id": "user-1", "Name": "alice"}]

        async def get_playback_history(self, user_id: str, limit: int) -> list[dict]:
            return [
                {
                    "Name": "Episode 1",
                    "SeriesName": "Show Alpha",
                    "Type": "Episode",
                    "Genres": ["Sci-Fi"],
                    "CommunityRating": 8.4,
                    "ProviderIds": {"Tmdb": "1932395"},
                    "UserData": {"LastPlayedDate": "2026-04-08T10:00:00Z"},
                }
            ]

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=FakeMediaServer(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    with session_factory() as session:
        session.add(
            LibraryMedia(
                source_provider="jellyfin",
                media_server_id="series-1",
                media_type="tv",
                title="Show Alpha",
                sort_title="Show Alpha",
                overview="Series overview.",
                production_year=2024,
                release_date="2024-01-01",
                community_rating=8.5,
                genres_json='["Sci-Fi"]',
                state="available",
                tmdb_id=101,
                tvdb_id=555,
                imdb_id="ttshowalpha",
                payload_json="{}",
            )
        )
        session.commit()

    async def fake_tmdb_enrichment(summary: dict, *, recommendation_seeds: list[dict]) -> dict:
        assert recommendation_seeds[0]["media_id"] == 101
        assert recommendation_seeds[0]["external_ids"]["tmdb"] == "101"
        return {**summary, "top_keywords": ["space opera"]}

    async def fake_enrichment(
        username: str,
        compact_history: dict,
        *,
        existing_payload: dict | None = None,
    ) -> dict:
        return {"adjacent_genres": ["Mystery"], "adjacent_themes": ["found family"]}

    async def fake_refresh(_user: dict, progress_callback=None) -> dict:
        return {"stored": 0, "scored": 0, "ai_scored": 0, "ai_reused": 0}

    service._enrich_profile_summary_with_tmdb = fake_tmdb_enrichment  # type: ignore[method-assign]
    service._suggest_profile_enrichment = fake_enrichment  # type: ignore[method-assign]
    service._refresh_user_suggestions = fake_refresh  # type: ignore[method-assign]

    result = asyncio.run(service.run_profile_architect("alice"))

    assert result["status"] == "success"


def test_run_profile_architect_progress_matches_layered_rebuild_flow(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    class FakeMediaServer:
        async def list_users(self) -> list[dict]:
            return [{"Id": "user-1", "Name": "alice"}]

        async def get_playback_history(self, user_id: str, limit: int) -> list[dict]:
            return []

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
        suggestions_enabled=True,
    )
    service = VanguarrService(
        settings=settings,
        media_server=FakeMediaServer(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    task_updates: list[dict] = []
    original_update_task = service._update_task

    def capture_update(*args, **kwargs):
        task_updates.append(dict(kwargs))
        return original_update_task(*args, **kwargs)

    async def fake_compose(
        username: str,
        history: list[dict],
        *,
        existing_payload: dict | None = None,
        peer_payload_overrides: dict[str, dict[str, Any]] | None = None,
        include_llm_enrichment: bool = True,
        progress_callback=None,
    ):
        assert username == "alice"
        if progress_callback is not None:
            progress_callback("Building core playback profile for alice.", 1, "profile_history")
            progress_callback("Mapping Seer neighborhoods for alice.", 2, "seer_enrichment")
            progress_callback("Blending local similar-user lift for alice.", 3, "similar_user_enrichment")
            progress_callback("Enriching TMDb metadata for alice.", 4, "tmdb_enrichment")
            progress_callback("Finalizing profile manifest for alice.", 5, "profile_finalize")
        return (
            {
                "username": "alice",
                "profile_state": "ready",
                "history_count": 1,
                "summary_block": "summary",
            },
            [],
        )

    async def fake_refresh(user: dict, progress_callback=None) -> dict:
        assert user["Name"] == "alice"
        if progress_callback is not None:
            progress_callback("Loading playback history for alice.", 0, {"phase": "history"})
            progress_callback("Loading indexed library candidates for alice.", 1, {"phase": "library"})
            progress_callback("Ranking suggestion candidates for alice.", 2, {"phase": "ranking"})
            progress_callback("Applying AI shortlist for alice.", 3, {"phase": "ai"})
            progress_callback("Writing stored suggestion snapshot for alice.", 4, {"phase": "storage"})
        return {"stored": 0, "scored": 0, "ai_scored": 0, "ai_reused": 0}

    service._update_task = capture_update  # type: ignore[method-assign]
    service._compose_profile_payload = fake_compose  # type: ignore[method-assign]
    service._refresh_user_suggestions = fake_refresh  # type: ignore[method-assign]

    result = asyncio.run(service.run_profile_architect("alice"))

    assert result["status"] == "success"
    assert task_updates
    assert task_updates[0]["progress_total"] == 11
    assert any(update.get("summary") == "Building core playback profile for alice." for update in task_updates)
    assert any(update.get("summary") == "Mapping Seer neighborhoods for alice." for update in task_updates)
    assert any(update.get("summary") == "Blending local similar-user lift for alice." for update in task_updates)
    assert any(update.get("summary") == "Enriching TMDb metadata for alice." for update in task_updates)
    assert any(update.get("summary") == "Finalizing profile manifest for alice." for update in task_updates)
    assert any(update.get("summary") == "Loading indexed library candidates for alice." for update in task_updates)
    assert any(update.get("summary") == "Applying AI shortlist for alice." for update in task_updates)
    assert task_updates[-1]["progress_total"] == 11
    assert task_updates[-1]["progress_current"] == 11


def test_library_sync_skips_suggestion_refresh_when_library_is_unchanged(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    class FakeMediaServer:
        async def list_users(self):
            return [{"Id": "66456a3a-4cd3-46e3-83ce-254e99d4b09a", "Name": "admin"}]

    class FakeJellyfinClient:
        async def get_library_folders(self):
            return []

        async def get_library_items(self, parent_id=None):
            return [
                {
                    "Id": "item-1",
                    "Name": "Arrival",
                    "SortName": "Arrival",
                    "Type": "Movie",
                    "Overview": "First contact drama.",
                    "Genres": ["Sci-Fi", "Drama"],
                    "CommunityRating": 8.1,
                    "ProviderIds": {"Tmdb": "329865"},
                    "PremiereDate": "2016-11-11T00:00:00.0000000Z",
                    "ProductionYear": 2016,
                }
            ]

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=FakeMediaServer(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(enabled=False),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    payload = VanguarrService._library_item_to_sync_payload(
        {
            "Id": "item-1",
            "Name": "Arrival",
            "SortName": "Arrival",
            "Type": "Movie",
            "Overview": "First contact drama.",
            "Genres": ["Sci-Fi", "Drama"],
            "CommunityRating": 8.1,
            "ProviderIds": {"Tmdb": "329865"},
            "PremiereDate": "2016-11-11T00:00:00.0000000Z",
            "ProductionYear": 2016,
        }
    )
    assert payload is not None

    with session_factory() as session:
        session.add(
            LibraryMedia(
                source_provider="jellyfin",
                media_server_id="item-1",
                media_type="movie",
                title="Arrival",
                sort_title="Arrival",
                overview="First contact drama.",
                production_year=2016,
                release_date="2016-11-11T00:00:00.0000000Z",
                community_rating=8.1,
                genres_json='["Sci-Fi","Drama"]',
                state="available",
                tmdb_id=329865,
                imdb_id=None,
                content_fingerprint=payload["content_fingerprint"],
                payload_json=payload["payload_json"],
            )
        )
        session.commit()

    fake_client = FakeJellyfinClient()
    service._jellyfin_client = lambda: fake_client  # type: ignore[method-assign]

    calls = {"count": 0}

    async def fake_refresh(_user, progress_callback=None):
        calls["count"] += 1
        return {"stored": 0, "scored": 0, "ai_scored": 0, "ai_reused": 0}

    service._refresh_user_suggestions = fake_refresh  # type: ignore[method-assign]

    result = asyncio.run(service.run_library_sync())

    with session_factory() as session:
        sync_logs = list(session.scalars(select(DecisionLog).where(DecisionLog.engine == "library_sync")))

    assert result["status"] == "success"
    assert result["material_changes"] == 0
    assert result["suggestion_refresh_state"] == "skipped"
    assert calls["count"] == 0
    assert len(sync_logs) == 1
    assert sync_logs[0].username == "system"
    assert sync_logs[0].decision == "SYNC"


def test_suggestion_ai_cache_reuses_existing_llm_vote(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    class FailingLLM:
        async def generate_json(self, **kwargs):
            raise AssertionError("LLM should not be called when cache matches.")

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
        suggestion_ai_threshold=0.5,
        suggestion_ai_candidate_limit=10,
        decision_ai_weight_percent=25,
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(enabled=False),
        llm=FailingLLM(),
        session_factory=session_factory,
    )

    candidate = {
        "media_type": "movie",
        "media_id": 329865,
        "title": "Arrival",
        "overview": "First contact drama.",
        "genres": ["Sci-Fi", "Drama"],
        "rating": 8.1,
        "release_date": "2016-11-11",
        "sources": ["library:indexed"],
        "source_lanes": ["available_library"],
        "media_info": {"status": "available"},
        "external_ids": {"tmdb": "329865"},
        "tmdb_details": {"keywords": ["first contact"], "featured_people": ["Amy Adams"]},
        "recommendation_features": {
            "deterministic_score": 0.82,
            "analysis_summary": "Strong sci-fi match.",
            "score_breakdown": {},
            "lane_tags": ["available_library"],
        },
    }
    profile_payload = {
        "summary_block": "[VANGUARR_PROFILE_SUMMARY_V1]\nUser: admin",
        "primary_genres": ["Sci-Fi"],
        "secondary_genres": ["Drama"],
        "recent_genres": ["Sci-Fi"],
        "adjacent_genres": [],
        "adjacent_themes": [],
        "repeat_titles": [],
        "recent_momentum": [],
        "format_preference": {"preferred": "movie"},
        "release_year_preference": {"bias": "balanced"},
    }
    viewing_history = {
        "recent_plays": [],
        "top_titles": [],
        "recent_momentum": [],
        "repeat_titles": [],
        "primary_genres": ["Sci-Fi"],
        "top_keywords": [],
        "favorite_people": [],
        "preferred_brands": [],
        "favorite_collections": [],
    }
    ranked_candidate = VanguarrService._rank_candidate_pool([candidate], profile_summary=profile_payload)[0]
    cache_key = VanguarrService._build_suggestion_ai_cache_key(
        ranked_candidate,
        profile_payload=profile_payload,
        viewing_history=viewing_history,
    )

    scored, ai_scored, ai_reused = asyncio.run(
        service._score_suggestion_candidates_with_ai(
            [candidate],
            username="admin",
            profile_payload=profile_payload,
            viewing_history=viewing_history,
            cached_llm_votes={
                cache_key: {
                    "llm_vote": "RECOMMEND",
                    "llm_confidence": 0.74,
                    "llm_reasoning": "Still a strong adjacent match.",
                }
            },
        )
    )

    features = scored[0]["recommendation_features"]

    assert ai_scored == 0
    assert ai_reused == 1
    assert features["llm_vote"] == "RECOMMEND"
    assert features["llm_reasoning"] == "Still a strong adjacent match."
    assert features["hybrid_score"] > features["deterministic_score"]


def test_run_decision_engine_does_not_mark_noop_tv_request_as_requested(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
        request_threshold=0.7,
        tmdb_candidate_enrichment_limit=0,
    )

    class FakeMediaServer:
        async def list_users(self) -> list[dict]:
            return [{"Id": "user-1", "Name": "alice"}]

        async def get_playback_history(self, user_id: str, limit: int) -> list[dict]:
            return []

    class FakeSeer:
        async def discover_candidates(self, *args, **kwargs) -> list[dict]:
            return [
                {
                    "media_type": "tv",
                    "media_id": 1932395,
                    "title": "Test Show",
                    "overview": "A test series.",
                    "genres": ["Drama"],
                    "sources": ["recommended:Seed Show"],
                    "source_lanes": ["top_seed"],
                    "media_info": {},
                    "external_ids": {"tmdb": "1932395", "tvdb": "12345"},
                }
            ]

        async def request_media(self, media_type: str, media_id: int, *, tvdb_id: int | None = None) -> SeerRequestResult:
            assert media_type == "tv"
            assert media_id == 1932395
            assert tvdb_id == 12345
            return SeerRequestResult(
                created=False,
                request_id=None,
                status_code=202,
                message="No seasons available to request",
                payload={"message": "No seasons available to request"},
            )

    class FakeLLM:
        async def generate_json(self, *args, **kwargs) -> dict:
            return {
                "decision": "REQUEST",
                "confidence": 0.95,
                "reasoning": "Strong fit for the user.",
            }

    service = VanguarrService(
        settings=settings,
        media_server=FakeMediaServer(),
        seer=FakeSeer(),
        tmdb=SimpleNamespace(),
        llm=FakeLLM(),
        session_factory=session_factory,
    )

    async def passthrough_profile_summary(summary: dict, *, recommendation_seeds: list[dict]) -> dict:
        return summary

    async def passthrough_candidates(candidates: list[dict], *, limit: int) -> list[dict]:
        return candidates

    def ranked_candidates(candidates: list[dict], *, profile_summary: dict) -> list[dict]:
        ranked: list[dict] = []
        for candidate in candidates:
            enriched = dict(candidate)
            enriched["recommendation_features"] = {
                "deterministic_score": 0.91,
                "analysis_summary": "Strong drama match.",
                "score_breakdown": {},
                "lane_tags": ["because_you_watched"],
            }
            ranked.append(enriched)
        return ranked

    service._enrich_profile_summary_with_tmdb = passthrough_profile_summary  # type: ignore[method-assign]
    service._enrich_candidate_pool_with_tmdb = passthrough_candidates  # type: ignore[method-assign]
    service._rank_candidate_pool = ranked_candidates  # type: ignore[method-assign]
    service._diversify_candidates = lambda candidates, limit: list(candidates)  # type: ignore[method-assign]

    result = asyncio.run(service.run_decision_engine("alice"))

    with session_factory() as session:
        logs = list(session.scalars(select(DecisionLog)))
        requests = list(session.scalars(select(RequestedMedia)))

    assert result["status"] == "success"
    assert result["requested"] == 0
    assert len(requests) == 0
    assert len(logs) == 1
    assert logs[0].decision == "REQUEST"
    assert logs[0].requested is False
    assert logs[0].request_id is None
    assert "Request outcome: No seasons available to request" in logs[0].reasoning


def test_run_profile_architect_writes_operation_log(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    class FakeMediaServer:
        async def list_users(self) -> list[dict]:
            return [{"Id": "user-1", "Name": "alice"}]

        async def get_playback_history(self, user_id: str, limit: int) -> list[dict]:
            return []

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=FakeMediaServer(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )

    async def passthrough_profile_summary(summary: dict, *, recommendation_seeds: list[dict]) -> dict:
        return summary

    async def fake_enrichment(
        username: str,
        compact_history: dict,
        *,
        existing_payload: dict | None = None,
    ) -> dict:
        return {"adjacent_genres": [], "adjacent_themes": []}

    async def fake_refresh(_user: dict, progress_callback=None) -> dict:
        return {"stored": 0, "scored": 0, "ai_scored": 0, "ai_reused": 0}

    service._build_profile_history_context = lambda history, top_limit, recent_limit, recent_weight_percent=75: {  # type: ignore[method-assign]
        "history_count": 0,
        "top_titles": [],
        "recent_momentum": [],
    }
    service._build_recommendation_seed_pool = lambda history, profile_summary, limit: []  # type: ignore[method-assign]
    service._enrich_profile_summary_with_tmdb = passthrough_profile_summary  # type: ignore[method-assign]
    service._suggest_profile_enrichment = fake_enrichment  # type: ignore[method-assign]
    service._build_profile_payload = lambda current_username, compact_history, enrichment, existing_payload: {  # type: ignore[method-assign]
        "username": current_username,
        "profile_state": "ready",
        "history_count": 0,
        "summary_block": "summary",
    }
    service._refresh_user_suggestions = fake_refresh  # type: ignore[method-assign]

    result = asyncio.run(service.run_profile_architect("alice"))

    with session_factory() as session:
        logs = list(session.scalars(select(DecisionLog).where(DecisionLog.engine == "profile_architect")))

    assert result["status"] == "success"
    assert len(logs) == 2
    assert any(log.username == "alice" and log.decision == "REBUILD" for log in logs)
    assert any(log.decision == "RUN" and log.source == "manual" and "Updated 1 profile(s)" in log.reasoning for log in logs)


def test_build_candidate_features_respects_blocked_titles_and_request_outcomes() -> None:
    blocked_candidate = {
        "media_type": "tv",
        "media_id": 1,
        "title": "Anime Trap",
        "genres": ["Anime", "Animation"],
        "sources": ["recommended:Loop Show"],
        "source_lanes": ["top_seed"],
        "tmdb_details": {},
    }
    positive_candidate = {
        "media_type": "movie",
        "media_id": 2,
        "title": "Courtroom Return",
        "genres": ["Drama", "Crime"],
        "sources": ["recommended:Courtroom One"],
        "source_lanes": ["top_seed"],
        "tmdb_details": {},
    }
    profile_summary = {
        "primary_genres": ["Drama"],
        "secondary_genres": ["Crime"],
        "recent_genres": ["Drama"],
        "discovery_lanes": ["Mystery"],
        "ranked_genres": [{"genre": "Drama", "weighted_score": 3.4}],
        "format_preference": {"preferred": "balanced"},
        "release_year_preference": {"bias": "balanced", "average_year": 2020},
        "explicit_feedback": {"liked_titles": [], "disliked_titles": [], "liked_genres": [], "disliked_genres": []},
        "blocked_titles": ["Anime Trap"],
        "request_outcome_insights": {
            "positive_titles": [],
            "negative_titles": ["Anime Trap"],
            "positive_genres": ["Drama"],
            "negative_genres": ["Anime"],
        },
    }

    blocked_features = VanguarrService._build_candidate_features(blocked_candidate, profile_summary=profile_summary)
    positive_features = VanguarrService._build_candidate_features(positive_candidate, profile_summary=profile_summary)

    assert blocked_features["feedback_block_reason"] == "blocked_title"
    assert blocked_features["score_breakdown"]["outcome_fit"] < 0
    assert positive_features["score_breakdown"]["outcome_fit"] > 0
    assert "Past request outcomes leaned positive" in positive_features["analysis_summary"]


def test_record_request_outcome_updates_request_history_and_live_profile_context(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )
    service.profile_store.write_payload(
        "alice",
        {
            "username": "alice",
            "profile_state": "ready",
            "history_count": 18,
            "unique_titles": 9,
            "primary_genres": ["Drama"],
            "recent_momentum": [{"title": "Courtroom One", "play_count": 2}],
            "summary_block": "summary",
        },
    )

    with session_factory() as session:
        session.add(
            RequestedMedia(
                id=7,
                username="alice",
                media_type="movie",
                media_id=101,
                media_title="Arrival",
                source="recommended:Interstellar",
                seer_request_id=77,
            )
        )
        session.add(
            DecisionLog(
                username="alice",
                media_type="movie",
                media_id=101,
                media_title="Arrival",
                source="recommended:Interstellar",
                decision="REQUEST",
                confidence=0.91,
                threshold=0.58,
                requested=True,
                request_id=77,
                reasoning="Strong fit.",
                payload_json=json.dumps({"genres": ["Sci-Fi", "Drama"]}),
                error=None,
            )
        )
        session.commit()

    result = service.record_request_outcome(username="alice", requested_media_id=7, outcome="watched", source="test")
    history = service.get_request_history("alice")
    live_payload = service.get_profile_payload_with_live_context("alice")

    assert result["outcome"] == "watched"
    assert history[0]["latest_outcome"] == "watched"
    assert live_payload["request_outcome_insights"]["counts"]["watched"] == 1
    assert "Past request outcomes are being tracked" in " ".join(live_payload["profile_review"]["strengths"])


def test_sync_watched_request_outcomes_from_history_infers_only_new_post_request_watches(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=SimpleNamespace(),
        seer=SimpleNamespace(),
        tmdb=SimpleNamespace(),
        llm=SimpleNamespace(),
        session_factory=session_factory,
    )
    service.profile_store.write_payload(
        "alice",
        {
            "username": "alice",
            "profile_state": "ready",
            "history_count": 12,
            "unique_titles": 8,
            "primary_genres": ["Sci-Fi"],
            "recent_momentum": [{"title": "Arrival", "play_count": 1}],
            "summary_block": "summary",
        },
    )

    request_time = datetime.utcnow() - timedelta(days=2)
    old_request_time = datetime.utcnow() - timedelta(hours=2)
    with session_factory() as session:
        session.add(
            RequestedMedia(
                id=1,
                created_at=request_time,
                username="alice",
                media_type="movie",
                media_id=329865,
                media_title="Arrival",
                source="recommended:Interstellar",
                seer_request_id=90,
            )
        )
        session.add(
            RequestedMedia(
                id=2,
                created_at=old_request_time,
                username="alice",
                media_type="movie",
                media_id=157336,
                media_title="Interstellar",
                source="recommended:Arrival",
                seer_request_id=91,
            )
        )
        session.commit()

    history = [
        {
            "Name": "Arrival",
            "Type": "Movie",
            "Genres": ["Sci-Fi", "Drama"],
            "ProviderIds": {"Tmdb": "329865"},
            "UserData": {"LastPlayedDate": (datetime.utcnow() - timedelta(days=1)).replace(microsecond=0).isoformat() + "Z"},
        },
        {
            "Name": "Interstellar",
            "Type": "Movie",
            "Genres": ["Sci-Fi"],
            "ProviderIds": {"Tmdb": "157336"},
            "UserData": {"LastPlayedDate": (datetime.utcnow() - timedelta(days=10)).replace(microsecond=0).isoformat() + "Z"},
        },
    ]

    first_sync = service.sync_watched_request_outcomes_from_history(username="alice", history=history, source="profile_architect")
    second_sync = service.sync_watched_request_outcomes_from_history(username="alice", history=history, source="profile_architect")

    with session_factory() as session:
        outcomes = list(session.scalars(select(RequestOutcomeEvent).order_by(RequestOutcomeEvent.id.asc())))

    assert first_sync["count"] == 1
    assert first_sync["titles"] == ["Arrival"]
    assert second_sync["count"] == 0
    assert len(outcomes) == 1
    assert outcomes[0].media_title == "Arrival"
    assert outcomes[0].outcome == "watched"
    assert outcomes[0].source == "profile_architect"


def test_preview_decision_candidates_returns_review_cards(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)

    class FakeMediaServer:
        async def list_users(self) -> list[dict]:
            return [{"Id": "user-1", "Name": "alice"}]

        async def get_playback_history(self, user_id: str, limit: int) -> list[dict]:
            return [
                {
                    "Name": "Courtroom One",
                    "Type": "Movie",
                    "Genres": ["Drama", "Crime"],
                    "CommunityRating": 8.1,
                    "ProviderIds": {"Tmdb": "11"},
                    "UserData": {"LastPlayedDate": "2026-04-10T10:00:00Z"},
                }
            ]

    class FakeSeer:
        async def discover_candidates(self, *args, **kwargs) -> list[dict]:
            return [
                {
                    "media_type": "movie",
                    "media_id": 202,
                    "title": "Courtroom Return",
                    "overview": "Another legal drama.",
                    "genres": ["Drama", "Crime"],
                    "rating": 8.3,
                    "vote_count": 540,
                    "popularity": 88,
                    "release_date": "2025-01-01",
                    "sources": ["recommended:Courtroom One"],
                    "source_lanes": ["top_seed"],
                    "tmdb_details": {},
                }
            ]

    class FakeLLM:
        async def generate_json(self, *args, **kwargs) -> dict:
            return {"decision": "REQUEST", "confidence": 0.9, "reasoning": "Excellent profile fit."}

    settings = Settings(
        data_dir=tmp_path / "data",
        profiles_dir=tmp_path / "profiles",
        logs_dir=tmp_path / "logs",
        log_file=tmp_path / "logs" / "vanguarr.log",
    )
    service = VanguarrService(
        settings=settings,
        media_server=FakeMediaServer(),
        seer=FakeSeer(),
        tmdb=SimpleNamespace(),
        llm=FakeLLM(),
        session_factory=session_factory,
    )

    async def passthrough_profile_summary(summary: dict, *, recommendation_seeds: list[dict]) -> dict:
        return summary

    service._enrich_profile_summary_with_seer = passthrough_profile_summary  # type: ignore[method-assign]
    service._enrich_profile_summary_with_tmdb = passthrough_profile_summary  # type: ignore[method-assign]
    service._suggest_profile_enrichment = lambda *args, **kwargs: asyncio.sleep(0, result={})  # type: ignore[method-assign]
    service._enrich_candidate_pool_with_tmdb = lambda candidates, limit: asyncio.sleep(0, result=candidates)  # type: ignore[method-assign]

    preview = asyncio.run(service.preview_decision_candidates("alice", limit=4))

    assert preview["username"] == "alice"
    assert preview["candidates"][0]["title"] == "Courtroom Return"
    assert preview["candidates"][0]["decision"] == "REQUEST"
    assert preview["candidates"][0]["llm_vote"] == "REQUEST"
