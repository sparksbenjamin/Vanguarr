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
