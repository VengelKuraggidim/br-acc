from unittest.mock import MagicMock

from bracc.config import settings
from bracc.middleware.rate_limit import _get_rate_limit_key, limiter
from bracc.services.auth_service import create_access_token


def _make_request(
    auth_header: str | None = None,
    client_ip: str = "127.0.0.1",
    cookies: dict[str, str] | None = None,
) -> MagicMock:
    request = MagicMock()
    headers: dict[str, str] = {}
    if auth_header:
        headers["authorization"] = auth_header
    request.headers = headers
    request.cookies = cookies or {}
    request.client = MagicMock()
    request.client.host = client_ip
    return request


def test_key_func_extracts_user_from_jwt() -> None:
    token = create_access_token("user-123")
    request = _make_request(auth_header=f"Bearer {token}")
    key = _get_rate_limit_key(request)
    assert key == "user:user-123"


def test_key_func_fallback_to_ip() -> None:
    request = _make_request(client_ip="192.168.1.1")
    key = _get_rate_limit_key(request)
    assert key == "192.168.1.1"


def test_key_func_invalid_token_fallback() -> None:
    request = _make_request(auth_header="Bearer invalid-token", client_ip="10.0.0.1")
    key = _get_rate_limit_key(request)
    assert key == "10.0.0.1"


def test_key_func_reads_session_cookie() -> None:
    token = create_access_token("cookie-user")
    request = _make_request(cookies={settings.auth_cookie_name: token})
    assert _get_rate_limit_key(request) == "user:cookie-user"


def test_key_func_invalid_cookie_falls_back_to_ip() -> None:
    request = _make_request(
        cookies={settings.auth_cookie_name: "not-a-jwt"},
        client_ip="10.1.2.3",
    )
    assert _get_rate_limit_key(request) == "10.1.2.3"


def test_key_func_empty_cookie_falls_back_to_ip() -> None:
    request = _make_request(
        cookies={settings.auth_cookie_name: "   "},
        client_ip="10.1.2.4",
    )
    assert _get_rate_limit_key(request) == "10.1.2.4"


def test_key_func_bearer_wins_over_cookie() -> None:
    bearer_token = create_access_token("bearer-user")
    cookie_token = create_access_token("cookie-user")
    request = _make_request(
        auth_header=f"Bearer {bearer_token}",
        cookies={settings.auth_cookie_name: cookie_token},
    )
    assert _get_rate_limit_key(request) == "user:bearer-user"


def test_key_func_bearer_prefix_without_token_falls_through() -> None:
    # "Bearer " (with trailing space but no token) must not be treated as
    # an authenticated user; it should fall through to cookie/IP logic.
    request = _make_request(auth_header="Bearer ", client_ip="10.5.5.5")
    assert _get_rate_limit_key(request) == "10.5.5.5"


def test_limiter_instance_exists() -> None:
    assert limiter is not None
