"""Unit tests for the PWA parity router.

Covers both happy paths and the empty-data edge cases so the landing
page still renders when Neo4j is unreachable or the graph is freshly
bootstrapped and has no ingested politicians yet.
"""

from typing import Any
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


# ---------------------------------------------------------------------------
# /buscar-tudo
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_buscar_tudo_rejects_short_query(client: AsyncClient) -> None:
    response = await client.get("/buscar-tudo?q=a")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_buscar_tudo_rejects_missing_query(client: AsyncClient) -> None:
    response = await client.get("/buscar-tudo")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_buscar_tudo_maps_person_result_and_filters_non_go(
    client: AsyncClient,
) -> None:
    pessoa_go = _fake_search_record(
        node_id="person:1",
        labels=["Person"],
        props={
            "name": "Fulano da Silva",
            "uf": "GO",
            "patrimonio_declarado": 1_234_567.89,
            "is_pep": True,
            "foto_url": "https://example.test/fulano.jpg",
        },
        score=12.5,
        document_id="12345678900",
    )
    pessoa_sp = _fake_search_record(
        node_id="person:2",
        labels=["Person"],
        props={"name": "Outro Silva", "uf": "SP"},
        score=10.0,
        document_id="98765432100",
    )

    async def fake_execute_query(
        session: Any, name: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if name == "search":
            if params.get("entity_type") == "person":
                return [pessoa_go, pessoa_sp]
            return []
        raise AssertionError(f"unexpected query: {name}")

    async def fake_execute_query_single(
        session: Any, name: str, params: dict[str, Any]
    ) -> dict[str, Any] | None:
        if name == "search_count":
            return {"total": 2 if params.get("entity_type") == "person" else 0}
        raise AssertionError(f"unexpected single query: {name}")

    with (
        patch(
            "bracc.routers.pwa_parity.execute_query",
            new=AsyncMock(side_effect=fake_execute_query),
        ),
        patch(
            "bracc.routers.pwa_parity.execute_query_single",
            new=AsyncMock(side_effect=fake_execute_query_single),
        ),
    ):
        response = await client.get("/buscar-tudo?q=silva")

    assert response.status_code == 200
    body = response.json()
    assert body["pagina"] == 1
    assert body["total"] == 2
    assert len(body["resultados"]) == 1
    item = body["resultados"][0]
    assert item["id"] == "person:1"
    assert item["tipo"] == "person"
    assert item["nome"] == "Fulano da Silva"
    assert item["documento"] == "12345678900"
    assert item["icone"] == "pessoa"
    assert item["is_pep"] is True
    assert item["foto_url"] == "https://example.test/fulano.jpg"
    assert "Patrimonio" in item["detalhe"]


@pytest.mark.anyio
async def test_buscar_tudo_keeps_go_typed_results(client: AsyncClient) -> None:
    servidor = _fake_search_record(
        node_id="emp:1",
        labels=["StateEmployee"],
        props={
            "name": "Servidora Exemplo",
            "role": "ANALISTA",
            "salary_gross": 12_345.0,
            "is_commissioned": False,
        },
        score=5.0,
        document_id="4:abcd:0",  # internal element id → documento must be None
    )
    licitacao = _fake_search_record(
        node_id="proc:1",
        labels=["GoProcurement"],
        props={
            "object": "Aquisicao de materiais",
            "amount_estimated": 250_000.0,
        },
        score=4.0,
        document_id="4:abcd:1",
    )

    async def fake_execute_query(
        session: Any, name: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if name == "search":
            if params.get("entity_type") == "person":
                return []
            return [servidor, licitacao]
        raise AssertionError(name)

    async def fake_execute_query_single(
        session: Any, name: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        return {"total": 2}

    with (
        patch(
            "bracc.routers.pwa_parity.execute_query",
            new=AsyncMock(side_effect=fake_execute_query),
        ),
        patch(
            "bracc.routers.pwa_parity.execute_query_single",
            new=AsyncMock(side_effect=fake_execute_query_single),
        ),
    ):
        response = await client.get("/buscar-tudo?q=teste")

    assert response.status_code == 200
    resultados = response.json()["resultados"]
    tipos = {item["tipo"]: item for item in resultados}
    assert "stateemployee" in tipos
    assert tipos["stateemployee"]["icone"] == "servidor"
    assert tipos["stateemployee"]["is_comissionado"] is False
    # Internal element id must be stripped from ``documento``
    assert tipos["stateemployee"]["documento"] is None
    assert "goprocurement" in tipos
    assert tipos["goprocurement"]["icone"] == "licitacao"
    assert "Licitacao" in tipos["goprocurement"]["detalhe"]


@pytest.mark.anyio
async def test_buscar_tudo_maps_parlamentar_labels(client: AsyncClient) -> None:
    """Covers the 3 labels added to entity_search in 2026-04-22 so the
    PWA stops showing deputados/senadores as generic ``person``.

    Also asserts the UF filter (``uf!='GO'`` → dropped), which keeps
    the Brasil-wide index scope-compatible with Goias-only usage."""

    fed_go = _fake_search_record(
        node_id="fed:1",
        labels=["FederalLegislator"],
        props={
            "name": "Adriano do Baldy",
            "uf": "GO",
            "partido": "PP",
            "foto_url": "https://example.test/adriano.jpg",
        },
        score=8.0,
        document_id="4:x:1",
    )
    fed_sp = _fake_search_record(
        node_id="fed:2",
        labels=["FederalLegislator"],
        props={"name": "Outro Deputado", "uf": "SP", "partido": "PT"},
        score=7.5,
        document_id="4:x:2",
    )
    senator_go = _fake_search_record(
        node_id="sen:1",
        labels=["Senator"],
        props={"name": "Jorge Kajuru", "uf": "GO", "partido": "PSB"},
        score=6.0,
        document_id="4:x:3",
    )
    state_leg_go = _fake_search_record(
        node_id="state:1",
        labels=["StateLegislator"],
        props={"name": "Fulano Alego", "uf": "GO", "party": "MDB"},
        score=5.0,
        document_id="4:x:4",
    )

    async def fake_execute_query(
        session: Any, name: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if name == "search":
            if params.get("entity_type") == "person":
                return []
            return [fed_go, fed_sp, senator_go, state_leg_go]
        raise AssertionError(name)

    async def fake_execute_query_single(
        session: Any, name: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        return {"total": 4}

    with (
        patch(
            "bracc.routers.pwa_parity.execute_query",
            new=AsyncMock(side_effect=fake_execute_query),
        ),
        patch(
            "bracc.routers.pwa_parity.execute_query_single",
            new=AsyncMock(side_effect=fake_execute_query_single),
        ),
    ):
        response = await client.get("/buscar-tudo?q=politico")

    assert response.status_code == 200
    resultados = response.json()["resultados"]
    tipos = {item["tipo"]: item for item in resultados}

    # Non-GO deputado filtered out.
    assert "fed:2" not in {r["id"] for r in resultados}

    assert tipos["federallegislator"]["detalhe"] == "Deputado(a) Federal - PP"
    assert tipos["federallegislator"]["icone"] == "pessoa"
    assert tipos["federallegislator"]["foto_url"] == "https://example.test/adriano.jpg"

    assert tipos["senator"]["detalhe"] == "Senador(a) - PSB"
    assert tipos["senator"]["icone"] == "pessoa"

    assert tipos["statelegislator"]["detalhe"] == "Deputado(a) Estadual - MDB"
    assert tipos["statelegislator"]["icone"] == "pessoa"


@pytest.mark.anyio
async def test_buscar_tudo_empty_results(client: AsyncClient) -> None:
    async def fake_execute_query(
        session: Any, name: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return []

    async def fake_execute_query_single(
        session: Any, name: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        return {"total": 0}

    with (
        patch(
            "bracc.routers.pwa_parity.execute_query",
            new=AsyncMock(side_effect=fake_execute_query),
        ),
        patch(
            "bracc.routers.pwa_parity.execute_query_single",
            new=AsyncMock(side_effect=fake_execute_query_single),
        ),
    ):
        response = await client.get("/buscar-tudo?q=nenhum")

    assert response.status_code == 200
    body = response.json()
    assert body == {"resultados": [], "total": 0, "pagina": 1}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _fake_search_record(
    *,
    node_id: str,
    labels: list[str],
    props: dict[str, Any],
    score: float,
    document_id: str,
) -> dict[str, Any]:
    """Build a dict that mimics the neo4j Record shape consumed by
    ``_run_search`` (which only does dictionary-style access, so a
    plain dict is sufficient for unit testing).
    """
    return {
        "node": props,
        "score": score,
        "node_labels": labels,
        "node_id": node_id,
        "document_id": document_id,
    }
