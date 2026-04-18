"""Unit tests for the PWA ``/politico/{entity_id}`` endpoint.

Covers:
* happy path — FederalLegislator + CEAP agregado + ProvenanceBlock com
  ``snapshot_url``;
* 404 — deputado não ingerido no grafo;
* CEAP vazio — mesmo deputado, sem despesas (ainda retorna 200);
* provenance ausente — nó sem carimbo (legados) não quebra o shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import pytest

if TYPE_CHECKING:
    from httpx import AsyncClient


def _legislator_node(
    *,
    id_camara: str = "1001",
    partido: str = "XYZ",
    include_provenance: bool = True,
) -> dict[str, Any]:
    """Node props shape that ``pwa_politico`` consumes."""
    base: dict[str, Any] = {
        "id_camara": id_camara,
        "legislator_id": f"camara_{id_camara}",
        "name": "DEPUTADO EXEMPLO",
        "cpf": "***.***.*33-44",
        "partido": partido,
        "uf": "GO",
        "email": "ex@camara.leg.br",
        "url_foto": "https://example.gov.br/foto.jpg",
        "situacao": "Exercicio",
        "legislatura_atual": 57,
        "scope": "federal",
        "source": "camara_deputados",
    }
    if include_provenance:
        base.update(
            {
                "source_id": "camara_deputados",
                "source_record_id": id_camara,
                "source_url": (
                    f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id_camara}"
                ),
                "ingested_at": "2026-04-18T00:00:00+00:00",
                "run_id": "camara_deputados_20260418000000",
                "source_snapshot_uri": "camara_deputados/2026-04/aabbccddeeff.json",
            },
        )
    return base


@pytest.mark.anyio
async def test_politico_happy_path(client: AsyncClient) -> None:
    record = {
        "legislator": _legislator_node(),
        "element_id": "4:abc:123",
        "despesas": [
            {"ano": 2024, "mes": 3, "valor": 500.0},
            {"ano": 2024, "mes": 4, "valor": 1500.0},
            {"ano": 2023, "mes": 2, "valor": 100.0},
        ],
    }
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=record,
    ):
        response = await client.get("/politico/1001")

    assert response.status_code == 200
    body = response.json()
    # Shape básico
    assert body["politico"]["id_camara"] == "1001"
    assert body["politico"]["legislator_id"] == "camara_1001"
    assert body["politico"]["nome"] == "DEPUTADO EXEMPLO"
    assert body["politico"]["uf"] == "GO"
    assert body["politico"]["scope"] == "federal"
    assert body["politico"]["partido"] == "XYZ"
    assert body["politico"]["foto_url"].startswith("https://")
    # CPF mascarado não quebra nada
    assert body["politico"]["cpf"].startswith("***")
    # CEAP agregado descrescente por ano
    assert [d["ano"] for d in body["despesas_ceap"]] == [2024, 2023]
    assert body["despesas_ceap"][0]["valor_total"] == 2000.0
    assert body["despesas_ceap"][0]["n_despesas"] == 2
    assert body["despesas_ceap"][1]["valor_total"] == 100.0
    assert body["total_ceap"] == 2100.0
    assert body["total_ceap_fmt"].startswith("R$")
    # Provenance presente com snapshot_url
    prov = body["provenance"]
    assert prov is not None
    assert prov["source_id"] == "camara_deputados"
    assert prov["source_url"].startswith("https://dadosabertos.camara.leg.br")
    assert prov["snapshot_url"] == "camara_deputados/2026-04/aabbccddeeff.json"


@pytest.mark.anyio
async def test_politico_404_when_not_found(client: AsyncClient) -> None:
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=None,
    ):
        response = await client.get("/politico/9999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Politico nao encontrado"


@pytest.mark.anyio
async def test_politico_empty_record_is_404(client: AsyncClient) -> None:
    """Cypher devolveu algo mas sem o nó do legislador."""
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value={"legislator": None, "element_id": None, "despesas": []},
    ):
        response = await client.get("/politico/1001")
    assert response.status_code == 404


@pytest.mark.anyio
async def test_politico_without_ceap(client: AsyncClient) -> None:
    """Sem CEAP ingerida, o endpoint ainda devolve perfil (200)."""
    record = {
        "legislator": _legislator_node(),
        "element_id": "4:abc:123",
        "despesas": [],
    }
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=record,
    ):
        response = await client.get("/politico/1001")
    assert response.status_code == 200
    body = response.json()
    assert body["despesas_ceap"] == []
    assert body["total_ceap"] == 0.0
    assert body["total_ceap_fmt"] == "R$ 0,00"


@pytest.mark.anyio
async def test_politico_without_provenance_returns_null(client: AsyncClient) -> None:
    """Legacy node sem provenance → ``provenance: null`` no response."""
    record = {
        "legislator": _legislator_node(include_provenance=False),
        "element_id": "4:abc:123",
        "despesas": [],
    }
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=record,
    ):
        response = await client.get("/politico/1001")
    assert response.status_code == 200
    body = response.json()
    assert body["provenance"] is None


@pytest.mark.anyio
async def test_politico_502_on_driver_error(client: AsyncClient) -> None:
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        side_effect=RuntimeError("neo4j down"),
    ):
        response = await client.get("/politico/1001")
    assert response.status_code == 502


@pytest.mark.anyio
async def test_politico_filters_invalid_ceap_rows(client: AsyncClient) -> None:
    """Rows sem ano/valor ou com valor <= 0 são descartados do agregado."""
    record = {
        "legislator": _legislator_node(),
        "element_id": "4:abc:123",
        "despesas": [
            {"ano": 2024, "mes": 3, "valor": 500.0},
            {"ano": None, "mes": 4, "valor": 100.0},
            {"ano": 2024, "mes": 5, "valor": 0.0},     # zero ignorado
            {"ano": 2024, "mes": 6, "valor": "bad"},    # bad value ignorado
            {"ano": 2024, "mes": 7, "valor": 250.0},
        ],
    }
    with patch(
        "bracc.routers.pwa_parity.execute_query_single",
        new_callable=AsyncMock,
        return_value=record,
    ):
        response = await client.get("/politico/1001")
    body = response.json()
    assert body["total_ceap"] == 750.0
    assert body["despesas_ceap"][0]["n_despesas"] == 2
