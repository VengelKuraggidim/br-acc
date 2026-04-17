"""Tests for bracc.dependencies pure helpers.

Full FastAPI DI integration is covered by router-level tests; this
file pins the specific small helpers that aren't otherwise exercised
directly:
- _resolve_token: Bearer token vs cookie fallback precedence
- get_driver: 503 when app.state.neo4j_driver is missing
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from bracc.config import settings
from bracc.dependencies import _resolve_token, get_driver


def _make_request(cookies: dict[str, str] | None = None) -> MagicMock:
    request = MagicMock()
    request.cookies = cookies or {}
    return request


class TestResolveToken:
    def test_explicit_token_wins(self) -> None:
        # If the OAuth2 scheme already extracted a token, return it
        # verbatim — don't touch cookies.
        request = _make_request(cookies={settings.auth_cookie_name: "cookie-value"})
        assert _resolve_token("bearer-value", request) == "bearer-value"

    def test_cookie_fallback_when_no_token(self) -> None:
        request = _make_request(cookies={settings.auth_cookie_name: "cookie-jwt"})
        assert _resolve_token(None, request) == "cookie-jwt"

    def test_cookie_is_stripped(self) -> None:
        request = _make_request(
            cookies={settings.auth_cookie_name: "  spaced-jwt  "},
        )
        assert _resolve_token(None, request) == "spaced-jwt"

    def test_whitespace_only_cookie_returns_none(self) -> None:
        request = _make_request(cookies={settings.auth_cookie_name: "   "})
        assert _resolve_token(None, request) is None

    def test_no_token_and_no_cookie_returns_none(self) -> None:
        request = _make_request(cookies={})
        assert _resolve_token(None, request) is None

    def test_non_string_cookie_ignored(self) -> None:
        # Defensive: if a test harness or middleware stuffs a non-str
        # value into the cookie dict, _resolve_token should ignore it.
        request = MagicMock()
        request.cookies = {settings.auth_cookie_name: 12345}
        assert _resolve_token(None, request) is None


class TestGetDriver:
    @pytest.mark.anyio
    async def test_raises_503_when_driver_missing(self) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace()  # no neo4j_driver attribute
        with pytest.raises(HTTPException) as exc:
            await get_driver(request)
        assert exc.value.status_code == 503
        assert "Database connection not available" in exc.value.detail

    @pytest.mark.anyio
    async def test_returns_driver_when_attached(self) -> None:
        fake_driver = MagicMock()
        request = MagicMock()
        request.app.state = SimpleNamespace(neo4j_driver=fake_driver)
        assert await get_driver(request) is fake_driver

    @pytest.mark.anyio
    async def test_raises_when_driver_is_explicitly_none(self) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace(neo4j_driver=None)
        with pytest.raises(HTTPException) as exc:
            await get_driver(request)
        assert exc.value.status_code == 503
