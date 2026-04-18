import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from bracc.config import settings
from bracc.dependencies import close_driver, init_driver
from bracc.middleware.cpf_masking import CPFMaskingMiddleware
from bracc.middleware.rate_limit import limiter
from bracc.middleware.security_headers import SecurityHeadersMiddleware
from bracc.routers import (
    auth,
    baseline,
    emendas,
    entity,
    go,
    graph,
    investigation,
    meta,
    pwa_parity,
    search,
)
from bracc.services.neo4j_service import ensure_schema

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    weak_or_default_jwt = (
        settings.jwt_secret_key == "change-me-in-production"
        or len(settings.jwt_secret_key) < 32
    )
    if weak_or_default_jwt:
        msg = "JWT secret is weak or default — set JWT_SECRET_KEY env var (>= 32 chars)"
        app_env = settings.app_env.strip().lower()
        if app_env in {"dev", "test"}:
            _logger.warning("%s [allowed in %s]", msg, app_env)
        else:
            _logger.critical(msg)
            raise RuntimeError(msg)
    app_env = settings.app_env.strip().lower()
    if app_env not in {"dev", "test"} and settings.neo4j_password == "changeme":
        msg = "Neo4j default password not allowed in production — set NEO4J_PASSWORD"
        _logger.critical(msg)
        raise RuntimeError(msg)
    driver = await init_driver()
    app.state.neo4j_driver = driver
    await ensure_schema(driver)
    yield
    await close_driver()


app = FastAPI(
    title="BR-ACC API",
    description="Brazilian public data graph analysis tool",
    version="0.1.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SecurityHeadersMiddleware, app_env=settings.app_env)
app.add_middleware(CPFMaskingMiddleware)

app.include_router(meta.router)
app.include_router(auth.router)
app.include_router(entity.router)
app.include_router(search.router)
app.include_router(graph.router)
app.include_router(baseline.router)
app.include_router(investigation.router)
app.include_router(investigation.shared_router)
app.include_router(emendas.router)
app.include_router(go.router)
# PWA parity facade (root paths /status and /buscar-tudo). Mirrors the
# shapes expected by pwa/index.html today so the client-side migration
# can be staged. See api/src/bracc/routers/pwa_parity.py.
app.include_router(pwa_parity.router)

# Gated federal-scope routers. Default OFF — Fiscal Cidadao serves Goias
# only. Set ENABLE_FEDERAL_ROUTES=true to mount the preserved federal
# endpoints from api/src/bracc/_federal/. See docs/_federal_gating.md.
if os.getenv("ENABLE_FEDERAL_ROUTES", "false").strip().lower() == "true":
    from bracc._federal.routers import patterns as _federal_patterns  # noqa: E402
    from bracc._federal.routers import public as _federal_public  # noqa: E402

    app.include_router(_federal_patterns.router)
    app.include_router(_federal_public.router)
    _logger.info("ENABLE_FEDERAL_ROUTES=true: mounted _federal/ routers")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
