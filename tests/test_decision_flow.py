from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.core.db import Base
from app.core.models import TaskRun
from app.core.prompts import build_decision_messages, build_profile_enrichment_messages
from app.core.settings import Settings
from app.core.services import ProfileStore, VanguarrService


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
    assert any(item["genre"] == "Sci-Fi" and item["raw_count"] == 2 for item in summary["ranked_genres"])
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
            "recent_genres": ["Mystery"],
            "top_titles": [{"title": "Show Alpha", "play_count": 4}],
            "discovery_lanes": ["Thriller"],
        },
    )

    assert "adjacent_genres" in messages[0]["content"]
    assert "Show Alpha" in messages[1]["content"]
    assert "code-derived viewing summary" in messages[1]["content"]


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
    viewing_history = VanguarrService._build_viewing_history_context(
        history,
        recommendation_seeds=[{"title": "Show Alpha", "media_type": "tv", "media_id": 101}],
        profile_summary=summary,
    )

    assert "Sci-Fi" in viewing_history["primary_genres"]
    assert any(item["genre"] == "Sci-Fi" for item in viewing_history["ranked_genres"])
    assert viewing_history["format_preference"]["preferred"] == "tv"
    assert viewing_history["recent_momentum"][0]["title"] == "Show Alpha"


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


def test_build_genre_discovery_seeds_prioritizes_primary_recent_then_adjacent() -> None:
    seeds = VanguarrService._build_genre_discovery_seeds(
        {
            "primary_genres": ["Drama", "History"],
            "recent_genres": ["Drama", "Crime"],
            "adjacent_genres": ["Mystery", "Thriller"],
            "format_preference": {"preferred": "tv"},
        }
    )

    assert [seed["genre_name"] for seed in seeds] == ["Drama", "History", "Crime", "Mystery", "Thriller"]
    assert seeds[0]["source_lanes"] == ["primary_genre_seed"]
    assert seeds[2]["source_lanes"] == ["recent_genre_seed"]
    assert seeds[3]["source_lanes"] == ["adjacent_genre_seed"]
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
