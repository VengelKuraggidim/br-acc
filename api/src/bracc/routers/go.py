from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from neo4j import AsyncSession

from bracc.dependencies import get_session
from bracc.models.entity import SourceAttribution
from bracc.models.search import SearchResponse, SearchResult
from bracc.services.neo4j_service import execute_query, sanitize_props
from bracc.services.public_guard import sanitize_public_properties

router = APIRouter(prefix="/api/v1/go", tags=["goias"])


def _node_to_result(record: Any, node_key: str, type_label: str) -> SearchResult:
    node = record[node_key]
    props = dict(node)
    source_val = props.pop("source", None)
    sources: list[SourceAttribution] = []
    if isinstance(source_val, str):
        sources = [SourceAttribution(database=source_val)]
    elif isinstance(source_val, list):
        sources = [SourceAttribution(database=s) for s in source_val]

    return SearchResult(
        id=record["node_id"],
        type=type_label,
        name=str(props.get("name", props.get("agency_name", props.get("object", "")))),
        score=0.0,
        properties=sanitize_public_properties(sanitize_props(props)),
        sources=sources,
    )


@router.get("/municipalities", response_model=SearchResponse)
async def list_go_municipalities(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SearchResponse:
    """List all Goias municipalities with aggregated fiscal totals."""
    records = await execute_query(session, "list_go_municipalities", {})

    results: list[SearchResult] = []
    for record in records:
        node = record["m"]
        props = dict(node)
        source_val = props.pop("source", None)
        sources: list[SourceAttribution] = []
        if isinstance(source_val, str):
            sources = [SourceAttribution(database=source_val)]
        elif isinstance(source_val, list):
            sources = [SourceAttribution(database=s) for s in source_val]

        total_revenue = record["total_revenue"] or 0.0
        total_expenditure = record["total_expenditure"] or 0.0
        props["total_revenue"] = float(total_revenue)
        props["total_expenditure"] = float(total_expenditure)

        results.append(SearchResult(
            id=record["node_id"],
            type="gomunicipality",
            name=str(props.get("name", "")),
            score=0.0,
            properties=sanitize_public_properties(sanitize_props(props)),
            sources=sources,
        ))

    return SearchResponse(
        results=results,
        total=len(results),
        page=1,
        size=len(results),
    )


@router.get("/procurements", response_model=SearchResponse)
async def search_go_procurements(
    session: Annotated[AsyncSession, Depends(get_session)],
    q: Annotated[str, Query(max_length=200)] = "",
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> SearchResponse:
    """Search Goias procurements (licitacoes) by object, agency or municipality."""
    records = await execute_query(
        session,
        "search_go_procurements",
        {"query": q, "limit": limit},
    )
    results = [_node_to_result(r, "p", "goprocurement") for r in records]
    return SearchResponse(
        results=results,
        total=len(results),
        page=1,
        size=len(results),
    )


@router.get("/employees", response_model=SearchResponse)
async def search_go_employees(
    session: Annotated[AsyncSession, Depends(get_session)],
    q: Annotated[str, Query(max_length=200)] = "",
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> SearchResponse:
    """Search Goias state employees by name."""
    records = await execute_query(
        session,
        "search_go_employees",
        {"query": q, "limit": limit},
    )
    results = [_node_to_result(r, "e", "stateemployee") for r in records]
    return SearchResponse(
        results=results,
        total=len(results),
        page=1,
        size=len(results),
    )
