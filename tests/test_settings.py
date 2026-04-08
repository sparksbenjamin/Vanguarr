from app.core.settings import Settings


def test_blank_seer_request_user_id_is_none() -> None:
    settings = Settings(SEER_REQUEST_USER_ID="")

    assert settings.seer_request_user_id is None
