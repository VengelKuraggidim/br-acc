from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_search_rejects_short_query(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search?q=a")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_search_rejects_missing_query(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_search_rejects_invalid_page(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search?q=test&page=0")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_search_rejects_oversized_page(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search?q=test&size=200")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_search_accepts_wildcard_list_all(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search?q=*&size=1")
    assert response.status_code == 200
    body = response.json()
    assert "total" in body and "results" in body


@pytest.mark.anyio
async def test_search_accepts_snake_case_type(client: AsyncClient) -> None:
    # snake_case type names like `go_municipality` must map to PascalCase
    # labels (`GoMunicipality`) by stripping underscores before comparison.
    response = await client.get("/api/v1/search?q=*&type=go_municipality&size=1")
    assert response.status_code == 200


@pytest.mark.anyio
async def test_search_dedups_by_canonical_id(client: AsyncClient) -> None:
    # Caso ROMARIO BARBOSA POLICARPO: dois Person nodes (um com CPF, um
    # sem) compartilham o mesmo canonical_id via :CanonicalPerson; o
    # router colapsa pra 1 resultado mantendo o de maior score.
    mocked_records = [
        {
            "node": {"name": "ROMARIO BARBOSA POLICARPO", "cpf": "025.784.541-08"},
            "node_labels": ["Person"],
            "node_id": "p1",
            "score": 3.5,
            "document_id": "025.784.541-08",
            "canonical_id": "canon_cpf_02578454108",
        },
        {
            "node": {"name": "ROMARIO BARBOSA POLICARPO"},
            "node_labels": ["Person"],
            "node_id": "p2",
            "score": 3.2,
            "document_id": "p2",
            "canonical_id": "canon_cpf_02578454108",
        },
        {
            "node": {"name": "ROMARIO FERNANDES LEITE"},
            "node_labels": ["Person"],
            "node_id": "p3",
            "score": 2.5,
            "document_id": "p3",
            "canonical_id": None,
        },
    ]
    with patch(
        "bracc.routers.search.execute_query",
        new_callable=AsyncMock,
        return_value=mocked_records,
    ), patch(
        "bracc.routers.search.execute_query_single",
        new_callable=AsyncMock,
        return_value={"total": 2},
    ):
        response = await client.get("/api/v1/search?q=romario")

    assert response.status_code == 200
    payload = response.json()
    # 2 resultados: o Romário deduplicado (1) + o Romario Fernandes (1).
    assert len(payload["results"]) == 2
    ids = [r["id"] for r in payload["results"]]
    # Mantém o primeiro (maior score) do cluster.
    assert ids == ["p1", "p3"]
    # canonical_id é exposto no payload pra debug/futuro uso.
    assert payload["results"][0]["canonical_id"] == "canon_cpf_02578454108"
    assert payload["results"][1]["canonical_id"] is None


@pytest.mark.anyio
async def test_search_caps_at_size_after_dedup(client: AsyncClient) -> None:
    # Buffer fetch_limit puxa size*4 nodes pra suportar dedup; depois de
    # dedup, o router corta em ``size`` mesmo que sobrem nodes únicos.
    mocked_records = [
        {
            "node": {"name": f"Pessoa {i}"},
            "node_labels": ["Person"],
            "node_id": f"p{i}",
            "score": 5.0 - i * 0.1,
            "document_id": f"p{i}",
            "canonical_id": None,
        }
        for i in range(10)
    ]
    with patch(
        "bracc.routers.search.execute_query",
        new_callable=AsyncMock,
        return_value=mocked_records,
    ), patch(
        "bracc.routers.search.execute_query_single",
        new_callable=AsyncMock,
        return_value={"total": 10},
    ):
        response = await client.get("/api/v1/search?q=pessoa&size=3")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["results"]) == 3
    assert [r["id"] for r in payload["results"]] == ["p0", "p1", "p2"]


@pytest.mark.anyio
async def test_search_handles_record_without_canonical_id_field(
    client: AsyncClient,
) -> None:
    # Defesa contra records legados sem o campo canonical_id (ex.: testes
    # com mocks antigos). Não pode levantar KeyError.
    mocked_records = [
        {
            "node": {"name": "Sem Canonical"},
            "node_labels": ["Person"],
            "node_id": "p1",
            "score": 1.0,
            "document_id": "p1",
        },
    ]
    with patch(
        "bracc.routers.search.execute_query",
        new_callable=AsyncMock,
        return_value=mocked_records,
    ), patch(
        "bracc.routers.search.execute_query_single",
        new_callable=AsyncMock,
        return_value={"total": 1},
    ):
        response = await client.get("/api/v1/search?q=sem")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["results"]) == 1
    assert payload["results"][0]["canonical_id"] is None
