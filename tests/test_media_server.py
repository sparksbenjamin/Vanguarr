import asyncio

from app.api.jellyfin import JellyfinClient, VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL
from app.api.media_server import MediaServerClient
from app.api.plex import PlexClient
from app.core.settings import Settings


def test_media_server_client_switches_to_plex() -> None:
    settings = Settings(
        media_server_provider="plex",
        plex_base_url="http://plex:32400",
        plex_api_token="secret-token",
        plex_client_identifier="vanguarr-tests",
    )

    client = MediaServerClient(settings)

    assert client.provider_key == "plex"
    assert client.provider_label == "Plex"
    assert client.plex.base_url == "http://plex:32400"
    assert client.plex.headers["X-Plex-Token"] == "secret-token"
    assert client.plex.headers["X-Plex-Client-Identifier"] == "vanguarr-tests"


def test_plex_history_normalization_preserves_expected_shape() -> None:
    item = {
        "title": "My Episode",
        "type": "episode",
        "viewedAt": 1_713_156_000,
        "grandparentTitle": "My Show",
        "ratingKey": "1234",
    }
    metadata = {
        "type": "episode",
        "title": "My Episode",
        "grandparentTitle": "My Show",
        "Genre": [{"tag": "Drama"}, {"tag": "Mystery"}],
        "rating": 8.4,
        "year": 2024,
        "originallyAvailableAt": "2024-04-01",
        "Guid": [
            {"id": "tmdb://9876"},
            {"id": "imdb://tt1234567"},
        ],
    }

    normalized = PlexClient._normalize_history_item(item, metadata)

    assert normalized is not None
    assert normalized["Type"] == "Episode"
    assert normalized["Name"] == "My Episode"
    assert normalized["SeriesName"] == "My Show"
    assert normalized["Genres"] == ["Drama", "Mystery"]
    assert normalized["CommunityRating"] == 8.4
    assert normalized["ProductionYear"] == 2024
    assert normalized["PremiereDate"] == "2024-04-01"
    assert normalized["ProviderIds"]["Tmdb"] == "9876"
    assert normalized["ProviderIds"]["Imdb"] == "tt1234567"
    assert normalized["UserData"]["LastPlayedDate"].endswith("Z")


def test_jellyfin_repository_upsert_adds_missing_repo() -> None:
    repositories, added, enabled, changed = JellyfinClient._upsert_repository(
        [],
        name="Vanguarr",
        url=VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
    )

    assert added is True
    assert enabled is False
    assert changed is True
    assert repositories == [
        {
            "Name": "Vanguarr",
            "Url": VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
            "Enabled": True,
        }
    ]


def test_jellyfin_repository_upsert_enables_existing_repo() -> None:
    repositories, added, enabled, changed = JellyfinClient._upsert_repository(
        [
            {
                "Name": "Vanguarr",
                "Url": VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
                "Enabled": False,
            }
        ],
        name="Vanguarr",
        url=VANGUARR_JELLYFIN_PLUGIN_REPOSITORY_URL,
    )

    assert added is False
    assert enabled is True
    assert changed is True
    assert repositories[0]["Enabled"] is True


def test_jellyfin_playback_history_fetches_all_pages_when_full_history_enabled() -> None:
    settings = Settings(
        jellyfin_base_url="http://jellyfin:8096",
        jellyfin_api_key="secret-token",
        profile_history_limit=2,
        profile_use_full_history=True,
    )
    client = JellyfinClient(settings)
    calls: list[dict[str, object]] = []

    async def fake_request(method: str, path: str, params=None, **kwargs):
        calls.append(dict(params or {}))
        start_index = int((params or {}).get("startIndex") or 0)
        if start_index == 0:
            return {"Items": [{"Id": "one"}, {"Id": "two"}], "TotalRecordCount": 3}
        return {"Items": [{"Id": "three"}], "TotalRecordCount": 3}

    client._request = fake_request  # type: ignore[method-assign]

    history = asyncio.run(client.get_playback_history("user-1"))

    assert [item["Id"] for item in history] == ["one", "two", "three"]
    assert len(calls) == 2
    assert calls[0]["startIndex"] == 0
    assert calls[1]["startIndex"] == 2


def test_plex_history_items_fetch_all_pages_when_full_history_enabled() -> None:
    settings = Settings(
        media_server_provider="plex",
        plex_base_url="http://plex:32400",
        plex_api_token="secret-token",
        profile_history_limit=2,
        profile_use_full_history=True,
    )
    client = PlexClient(settings)
    calls: list[dict[str, object]] = []

    async def fake_request(method: str, path: str, params=None, **kwargs):
        calls.append(dict(params or {}))
        start = int((params or {}).get("X-Plex-Container-Start") or 0)
        if start == 0:
            return {"MediaContainer": {"Metadata": [{"ratingKey": "1"}, {"ratingKey": "2"}], "totalSize": 3}}
        return {"MediaContainer": {"Metadata": [{"ratingKey": "3"}], "totalSize": 3}}

    client._request = fake_request  # type: ignore[method-assign]

    history = asyncio.run(client._get_history_items(account_id="user-1", limit=None))

    assert [item["ratingKey"] for item in history] == ["1", "2", "3"]
    assert len(calls) == 2
    assert calls[0]["X-Plex-Container-Start"] == 0
    assert calls[1]["X-Plex-Container-Start"] == 2
