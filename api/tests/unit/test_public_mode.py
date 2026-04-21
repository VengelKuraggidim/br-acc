from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest

from bracc.config import settings

if TYPE_CHECKING:
    from httpx import AsyncClient


@pytest.mark.anyio
async def test_entity_lookup_disabled_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_entity_lookup", False)
    response = await client.get("/api/v1/entity/12345678901")
    assert response.status_code == 403
    assert "disabled in public mode" in response.json()["detail"]


@pytest.mark.anyio
async def test_person_lookup_disabled_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
    monkeypatch.setattr(settings, "public_allow_person", False)
    response = await client.get("/api/v1/entity/12345678901")
    assert response.status_code == 403
    assert "Person lookup disabled" in response.json()["detail"]


@pytest.mark.anyio
async def test_search_hides_person_nodes_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_person", False)
    mocked_records = [
        {
            "node": {"name": "Pessoa Teste", "cpf": "12345678900"},
            "node_labels": ["Person"],
            "node_id": "p1",
            "score": 3.1,
            "document_id": "12345678900",
        },
        {
            "node": {"razao_social": "Empresa Teste", "cnpj": "11.111.111/0001-11"},
            "node_labels": ["Company"],
            "node_id": "c1",
            "score": 2.9,
            "document_id": "11.111.111/0001-11",
        },
    ]
    with patch(
        "bracc.routers.search.execute_query",
        new_callable=AsyncMock,
        return_value=mocked_records,
    ):
        response = await client.get("/api/v1/search?q=teste")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["results"][0]["type"] == "company"


@pytest.mark.anyio
async def test_baseline_disabled_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_entity_lookup", False)
    response = await client.get("/api/v1/baseline/test-id")
    assert response.status_code == 403
    assert "disabled in public mode" in response.json()["detail"]


@pytest.mark.anyio
async def test_stats_hides_person_count_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_person", False)
    # Clear stats cache to ensure fresh computation
    import bracc.routers.meta as meta_mod
    monkeypatch.setattr(meta_mod, "_stats_cache", None)

    fake_record = {
        "total_nodes": 100,
        "total_relationships": 200,
        "person_count": 999,
        "company_count": 50,
        "health_count": 10,
        "finance_count": 5,
        "contract_count": 20,
        "sanction_count": 3,
        "election_count": 7,
        "amendment_count": 4,
        "embargo_count": 2,
        "education_count": 6,
        "convenio_count": 8,
        "laborstats_count": 9,
        "offshore_entity_count": 1,
        "offshore_officer_count": 2,
        "global_pep_count": 3,
        "cvm_proceeding_count": 4,
        "expense_count": 11,
        "legislative_expense_count": 111,
        "pep_record_count": 12,
        "expulsion_count": 13,
        "leniency_count": 14,
        "international_sanction_count": 15,
        "gov_card_expense_count": 16,
        "gov_travel_count": 17,
        "bid_count": 18,
        "fund_count": 19,
        "dou_act_count": 20,
        "tax_waiver_count": 21,
        "municipal_finance_count": 22,
        "declared_asset_count": 23,
        "party_membership_count": 24,
        "barred_ngo_count": 25,
        "bcb_penalty_count": 26,
        "labor_movement_count": 27,
        "legal_case_count": 28,
        "judicial_case_count": 29,
        "source_document_count": 30,
        "ingestion_run_count": 31,
        "temporal_violation_count": 32,
        "cpi_count": 33,
        "inquiry_requirement_count": 34,
        "inquiry_session_count": 35,
        "municipal_bid_count": 36,
        "municipal_contract_count": 37,
        "municipal_gazette_act_count": 38,
    }
    with patch(
        "bracc.routers.meta.execute_query_single",
        new_callable=AsyncMock,
        return_value=fake_record,
    ), patch(
        "bracc.routers.meta.load_source_registry",
        return_value=[],
    ), patch(
        "bracc.routers.meta.source_registry_summary",
        return_value={
            "universe_v1_sources": 0,
            "implemented_sources": 0,
            "loaded_sources": 0,
            "healthy_sources": 0,
            "stale_sources": 0,
            "blocked_external_sources": 0,
            "quality_fail_sources": 0,
            "discovered_uningested_sources": 0,
        },
    ):
        response = await client.get("/api/v1/meta/stats")

    assert response.status_code == 200
    payload = response.json()
    assert payload["person_count"] == 0
    assert payload["company_count"] == 50  # non-person counts preserved


@pytest.mark.anyio
async def test_timeline_sanitizes_properties_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
    mock_records = [
        {
            "lbls": ["Contract"],
            "props": {"type": "licitacao", "cpf": "12345678900", "value": 50000.0},
            "event_date": "2024-01-15",
            "id": "evt-1",
        },
    ]
    with patch(
        "bracc.routers.entity.execute_query",
        new_callable=AsyncMock,
        return_value=mock_records,
    ):
        response = await client.get("/api/v1/entity/test-id/timeline")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["events"]) == 1
    event_props = payload["events"][0]["properties"]
    assert "cpf" not in event_props
    assert event_props["value"] == 50000.0


@pytest.mark.anyio
async def test_investigations_disabled_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_investigations", False)
    response = await client.get("/api/v1/investigations/")
    assert response.status_code == 403
    assert "disabled in public mode" in response.json()["detail"]
