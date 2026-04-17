from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from bracc.middleware.security_headers import SecurityHeadersMiddleware


def _make_app(app_env: str = "dev") -> Starlette:
    async def homepage(_request: object) -> PlainTextResponse:
        return PlainTextResponse("ok")

    async def api_endpoint(_request: object) -> PlainTextResponse:
        return PlainTextResponse("api")

    async def health(_request: object) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(
        routes=[
            Route("/", homepage),
            Route("/api/v1/foo", api_endpoint),
            Route("/health", health),
            Route("/static/x.css", homepage),
        ],
    )
    app.add_middleware(SecurityHeadersMiddleware, app_env=app_env)
    return app


@pytest.fixture
def client() -> TestClient:
    return TestClient(_make_app())


class TestBaselineHeaders:
    def test_x_content_type_options(self, client: TestClient) -> None:
        response = client.get("/")
        assert response.headers["x-content-type-options"] == "nosniff"

    def test_x_frame_options(self, client: TestClient) -> None:
        response = client.get("/")
        assert response.headers["x-frame-options"] == "DENY"

    def test_referrer_policy(self, client: TestClient) -> None:
        response = client.get("/")
        assert response.headers["referrer-policy"] == "no-referrer"

    def test_permissions_policy(self, client: TestClient) -> None:
        response = client.get("/")
        policy = response.headers["permissions-policy"]
        for feature in ("camera=()", "geolocation=()", "microphone=()"):
            assert feature in policy

    def test_headers_apply_to_all_paths(self, client: TestClient) -> None:
        for path in ("/", "/api/v1/foo", "/health", "/static/x.css"):
            response = client.get(path)
            assert response.headers["x-frame-options"] == "DENY"


class TestContentSecurityPolicy:
    def test_csp_set_for_health(self, client: TestClient) -> None:
        response = client.get("/health")
        assert "default-src 'none'" in response.headers["content-security-policy"]
        assert "frame-ancestors 'none'" in response.headers["content-security-policy"]

    def test_csp_set_for_api_paths(self, client: TestClient) -> None:
        response = client.get("/api/v1/foo")
        assert "default-src 'none'" in response.headers["content-security-policy"]

    def test_csp_absent_for_non_api_paths(self, client: TestClient) -> None:
        response = client.get("/")
        assert "content-security-policy" not in response.headers

    def test_csp_absent_for_static_assets(self, client: TestClient) -> None:
        response = client.get("/static/x.css")
        assert "content-security-policy" not in response.headers


class TestHsts:
    def test_no_hsts_in_dev_over_http(self, client: TestClient) -> None:
        # TestClient defaults to http://testserver.
        response = client.get("/")
        assert "strict-transport-security" not in response.headers

    def test_no_hsts_in_prod_over_http(self) -> None:
        app = _make_app(app_env="prod")
        with TestClient(app, base_url="http://testserver") as client:
            response = client.get("/")
        assert "strict-transport-security" not in response.headers

    def test_hsts_in_prod_over_https(self) -> None:
        app = _make_app(app_env="prod")
        with TestClient(app, base_url="https://testserver") as client:
            response = client.get("/")
        hsts = response.headers["strict-transport-security"]
        assert "max-age=31536000" in hsts
        assert "includeSubDomains" in hsts

    def test_no_hsts_in_dev_over_https(self) -> None:
        app = _make_app(app_env="dev")
        with TestClient(app, base_url="https://testserver") as client:
            response = client.get("/")
        assert "strict-transport-security" not in response.headers

    def test_app_env_case_insensitive(self) -> None:
        app = _make_app(app_env="PROD")
        with TestClient(app, base_url="https://testserver") as client:
            response = client.get("/")
        # "PROD" lowercased to "prod" in __init__, so HSTS should be set.
        assert "strict-transport-security" in response.headers


class TestNonHttpScope:
    def test_existing_header_not_overridden(self) -> None:
        # setdefault semantics: if an upstream layer already set the header,
        # the middleware must not overwrite it.
        async def app_sets_csp(_request: object) -> PlainTextResponse:
            return PlainTextResponse(
                "x",
                headers={"Content-Security-Policy": "custom"},
            )

        starlette = Starlette(routes=[Route("/api/v1/custom", app_sets_csp)])
        starlette.add_middleware(SecurityHeadersMiddleware, app_env="dev")
        with TestClient(starlette) as client:
            response = client.get("/api/v1/custom")
        assert response.headers["content-security-policy"] == "custom"
