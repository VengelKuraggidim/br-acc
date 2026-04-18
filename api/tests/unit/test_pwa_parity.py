"""Unit tests for the PWA parity router.

Covers both happy paths and the empty-data edge cases so the landing
page still renders when Neo4j is unreachable or the graph is freshly
bootstrapped and has no ingested politicians yet.
"""

from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_status_happy_path(client: AsyncClient) -> None:
    record = {
        "total_nos": 1_000,
        "total_relacionamentos": 2_000,
        "deputados_federais": 16,
        "deputados_estaduais": 40,
        "senadores": 3,
        "vereadores_goiania": 35,
        "servidores_estaduais": 75_000,
        "cargos_comissionados": 8_000,
        "municipios_go": 246,
        "licitacoes_go": 1_234,
        "nomeacoes_go": 567,
    }

    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=record,
    ):
        response = await client.get("/status")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "online"
    assert body["bracc_conectado"] is True
    assert body["total_nos"] == 1_000
    assert body["total_relacionamentos"] == 2_000
    assert body["deputados_federais"] == 16
    assert body["deputados_estaduais"] == 40
    assert body["senadores"] == 3
    assert body["vereadores_goiania"] == 35
    assert body["servidores_estaduais"] == 75_000
    assert body["cargos_comissionados"] == 8_000
    assert body["municipios_go"] == 246
    assert body["licitacoes_go"] == 1_234
    assert body["nomeacoes_go"] == 567


@pytest.mark.anyio
async def test_status_returns_zeros_when_graph_empty(client: AsyncClient) -> None:
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=None,
    ):
        response = await client.get("/status")

    assert response.status_code == 200
    body = response.json()
    assert body["bracc_conectado"] is True
    assert body["total_nos"] == 0
    assert body["deputados_federais"] == 0
    assert body["vereadores_goiania"] == 0


@pytest.mark.anyio
async def test_status_reports_disconnected_on_driver_error(client: AsyncClient) -> None:
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        side_effect=RuntimeError("neo4j down"),
    ):
        response = await client.get("/status")

    assert response.status_code == 200
    body = response.json()
    assert body["bracc_conectado"] is False
    assert body["total_nos"] == 0
