from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from bracc.config import settings
from bracc.main import app
from bracc.services.auth_service import create_access_token

# main.py's boot-time check enforces >=32 bytes in prod, but tests skip boot,
# so the 23-byte default triggered InsecureKeyLengthWarning on every auth test.
settings.jwt_secret_key = "test-secret-key-at-least-32-bytes-for-hs256"


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    # Mock Neo4j driver so tests don't need a running database
    mock_driver = MagicMock()
    mock_driver.verify_connectivity = AsyncMock()
    mock_driver.close = AsyncMock()
    mock_session = AsyncMock()
    mock_driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_driver.session.return_value.__aexit__ = AsyncMock(return_value=None)
    app.state.neo4j_driver = mock_driver

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def auth_headers() -> dict[str, str]:
    token = create_access_token("test-user-id")
    return {"Authorization": f"Bearer {token}"}
