import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from neo4j import AsyncSession

from bracc.dependencies import get_session
from bracc.services.neo4j_service import execute_query, execute_query_single
from bracc.services.public_guard import should_hide_person_entities
from bracc.services.source_registry import load_source_registry, source_registry_summary
from bracc.services.sources_public_service import (
    build_public_sources_grouped,
    load_live_source_status,
)

router = APIRouter(prefix="/api/v1/meta", tags=["meta"])

_stats_cache: dict[str, Any] | None = None
_stats_cache_time: float = 0.0


@router.get("/health")
async def neo4j_health(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str]:
    record = await execute_query_single(session, "health_check", {})
    if record and record["ok"] == 1:
        return {"neo4j": "connected"}
    return {"neo4j": "error"}


@router.get("/person-count")
async def person_count_by_uf(
    session: Annotated[AsyncSession, Depends(get_session)],
    uf: str = "GO",
) -> dict[str, Any]:
    record = await execute_query_single(
        session, "person_counts_by_uf", {"uf": uf.upper()}
    )
    if not record:
        return {"uf": uf.upper(), "total": 0}
    return {
        "uf": uf.upper(),
        "total": record["total"],
        "deputados_federais": record["deputados_federais"],
        "deputados_estaduais": record["deputados_estaduais"],
        "vereadores": record["vereadores"],
        "prefeitos": record["prefeitos"],
        "senadores": record["senadores"],
        "governadores": record["governadores"],
    }


@router.get("/election-cargos")
async def election_cargos(
    session: Annotated[AsyncSession, Depends(get_session)],
    uf: str = "GO",
) -> list[dict[str, Any]]:
    records = await execute_query(session, "election_cargos", {"uf": uf.upper()})
    return [{"cargo": r["cargo"], "total": r["total"]} for r in records]


@router.get("/stats")
async def database_stats(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    global _stats_cache, _stats_cache_time  # noqa: PLW0603

    if _stats_cache is not None and (time.monotonic() - _stats_cache_time) < 300:
        return _stats_cache

    record = await execute_query_single(session, "meta_stats", {})
    source_entries = load_source_registry()
    source_summary = source_registry_summary(source_entries)

    result = {
        "total_nodes": record["total_nodes"] if record else 0,
        "total_relationships": record["total_relationships"] if record else 0,
        "person_count": (
            0 if should_hide_person_entities() else (record["person_count"] if record else 0)
        ),
        "company_count": record["company_count"] if record else 0,
        "health_count": record["health_count"] if record else 0,
        "finance_count": record["finance_count"] if record else 0,
        "contract_count": record["contract_count"] if record else 0,
        "sanction_count": record["sanction_count"] if record else 0,
        "election_count": record["election_count"] if record else 0,
        "amendment_count": record["amendment_count"] if record else 0,
        "embargo_count": record["embargo_count"] if record else 0,
        "education_count": record["education_count"] if record else 0,
        "convenio_count": record["convenio_count"] if record else 0,
        "laborstats_count": record["laborstats_count"] if record else 0,
        "offshore_entity_count": record["offshore_entity_count"] if record else 0,
        "offshore_officer_count": record["offshore_officer_count"] if record else 0,
        "global_pep_count": record["global_pep_count"] if record else 0,
        "cvm_proceeding_count": record["cvm_proceeding_count"] if record else 0,
        "expense_count": record["expense_count"] if record else 0,
        # LegislativeExpense e o label correto pra CEAP/verba_alego; o label
        # Expense antigo fica em zero apos a migracao dos pipelines.
        "legislative_expense_count": (
            record.get("legislative_expense_count", 0) if record else 0
        ),
        "pep_record_count": record["pep_record_count"] if record else 0,
        "expulsion_count": record["expulsion_count"] if record else 0,
        "leniency_count": record["leniency_count"] if record else 0,
        "international_sanction_count": record["international_sanction_count"] if record else 0,
        "gov_card_expense_count": record["gov_card_expense_count"] if record else 0,
        "gov_travel_count": record["gov_travel_count"] if record else 0,
        "bid_count": record["bid_count"] if record else 0,
        "fund_count": record["fund_count"] if record else 0,
        "dou_act_count": record["dou_act_count"] if record else 0,
        "tax_waiver_count": record["tax_waiver_count"] if record else 0,
        "municipal_finance_count": record["municipal_finance_count"] if record else 0,
        "declared_asset_count": record["declared_asset_count"] if record else 0,
        "party_membership_count": record["party_membership_count"] if record else 0,
        "barred_ngo_count": record["barred_ngo_count"] if record else 0,
        "bcb_penalty_count": record["bcb_penalty_count"] if record else 0,
        "labor_movement_count": record["labor_movement_count"] if record else 0,
        "legal_case_count": record["legal_case_count"] if record else 0,
        "judicial_case_count": record["judicial_case_count"] if record else 0,
        "source_document_count": record.get("source_document_count", 0) if record else 0,
        "ingestion_run_count": record.get("ingestion_run_count", 0) if record else 0,
        "temporal_violation_count": record.get("temporal_violation_count", 0) if record else 0,
        "cpi_count": record["cpi_count"] if record else 0,
        "inquiry_requirement_count": record["inquiry_requirement_count"] if record else 0,
        "inquiry_session_count": record["inquiry_session_count"] if record else 0,
        "municipal_bid_count": record["municipal_bid_count"] if record else 0,
        "municipal_contract_count": record["municipal_contract_count"] if record else 0,
        "municipal_gazette_act_count": record["municipal_gazette_act_count"] if record else 0,
        "data_sources": source_summary["universe_v1_sources"],
        "implemented_sources": source_summary["implemented_sources"],
        "loaded_sources": source_summary["loaded_sources"],
        "healthy_sources": source_summary["healthy_sources"],
        "stale_sources": source_summary["stale_sources"],
        "blocked_external_sources": source_summary["blocked_external_sources"],
        "quality_fail_sources": source_summary["quality_fail_sources"],
        "discovered_uningested_sources": source_summary["discovered_uningested_sources"],
    }

    _stats_cache = result
    _stats_cache_time = time.monotonic()
    return result


@router.get("/sources")
async def list_sources() -> dict[str, list[dict[str, Any]]]:
    sources = [entry.to_public_dict() for entry in load_source_registry() if entry.in_universe_v1]
    return {"sources": sources}


@router.get("/sources/publico")
async def list_public_sources(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, list[dict[str, Any]]]:
    """Lista fontes com copy pedagógico pt-BR, agrupadas por categoria.

    Hidratada com status live do grafo: cada fonte ganha um bloco ``live``
    com ``badge`` (com_dados/parcial/falhou/sem_dados), ``last_run_at``,
    ``rows_loaded`` e ``runs``. O badge é derivado de IngestionRun agregado
    no Neo4j — atualiza sozinho conforme pipelines rodam (cache 5min).

    Consumido pela aba 'Fontes' na PWA. Exclui pipelines de enriquecimento
    interno (entity_resolution, propagacao_fotos) — são derivações, não
    fontes externas.
    """
    live = await load_live_source_status(session)
    return {"grupos": build_public_sources_grouped(live)}
