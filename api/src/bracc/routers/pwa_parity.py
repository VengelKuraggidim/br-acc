"""PWA parity endpoints.

These routes reproduce the shapes emitted by the legacy Flask backend
(``backend/app.py``) so the existing PWA (``pwa/index.html``) can be
pointed at the FastAPI service with no client-side change.

Paths are intentionally mounted at the root (``/status``,
``/buscar-tudo``) because the PWA calls ``${API}/status`` — i.e. the
raw service root — whereas the rest of the FastAPI surface lives
under ``/api/v1``. Keeping the parity routes out of the ``/api/v1``
tree preserves a clean boundary: the ``/api/v1`` contract stays
graph-native, and this router is a thin PWA-facing facade that can be
removed once the PWA is updated to call ``/api/v1`` directly.
"""

import re
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from neo4j import AsyncSession

from bracc.dependencies import get_session
from bracc.models.entity import ProvenanceBlock
from bracc.models.pwa_parity import (
    BuscarTudoItem,
    BuscarTudoResponse,
    CeapAnoBreakdown,
    PoliticoResponse,
    PoliticoResumo,
    StatusResponse,
)
from bracc.services.neo4j_service import execute_query, execute_query_single
from bracc.services.public_guard import should_hide_person_entities

router = APIRouter(tags=["pwa-parity"])

UF_FILTRO = "GO"

# Mirrors the dedup + filter taxonomy used by the legacy Flask
# ``/buscar-tudo`` handler. We accept both the historical
# ``state_employee``/``go_procurement`` spellings and the
# lower-cased-no-underscore spellings emitted by ``/api/v1/search``
# today so the endpoint returns results regardless of which label
# normalizer upstream happens to apply.
_GO_TYPES = {
    "state_employee",
    "stateemployee",
    "go_procurement",
    "goprocurement",
    "go_appointment",
    "goappointment",
    "go_vereador",
    "govereador",
}

_LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}[\]^"~*?:\\/])')


_POLITICO_PROVENANCE_FIELDS = (
    "source_id",
    "source_record_id",
    "source_url",
    "ingested_at",
    "run_id",
    "source_snapshot_uri",
)


def _pop_provenance(props: dict[str, Any]) -> ProvenanceBlock | None:
    """Extract the provenance block from a node's property dict.

    Returns ``None`` when any required field is missing (legacy rows)
    so clients see ``provenance: null`` instead of an error.
    """
    popped = {field: props.pop(field, None) for field in _POLITICO_PROVENANCE_FIELDS}
    for required in ("source_id", "source_url", "ingested_at", "run_id"):
        if not popped.get(required):
            return None
    snapshot_uri = popped.get("source_snapshot_uri")
    return ProvenanceBlock(
        source_id=str(popped["source_id"]),
        source_record_id=(
            str(popped["source_record_id"])
            if popped.get("source_record_id")
            else None
        ),
        source_url=str(popped["source_url"]),
        ingested_at=str(popped["ingested_at"]),
        run_id=str(popped["run_id"]),
        snapshot_url=str(snapshot_uri) if snapshot_uri else None,
    )


def _to_lucene_query(query: str) -> str:
    """Lucene-escape the user query; ``*`` keeps its match-all semantics."""
    if query.strip() == "*":
        return "*:*"
    return _LUCENE_SPECIAL.sub(r"\\\1", query)


def _fmt_brl(valor: float | int | None) -> str:
    """Reimplements ``backend/app.py::fmt_brl`` 1:1 for PWA parity."""
    if not valor:
        return "R$ 0,00"
    value = float(valor)
    if value >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:.2f} bi"
    if value >= 1_000_000:
        return f"R$ {value / 1_000_000:.2f} mi"
    if value >= 1_000:
        return f"R$ {value / 1_000:.1f} mil"
    formatted = f"R$ {value:,.2f}"
    # Swap the US ``1,234.56`` → pt-BR ``1.234,56`` through an X pivot.
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


def _is_person_go(props: dict[str, Any]) -> bool:
    uf = str(props.get("uf") or "").upper()
    return uf == UF_FILTRO


def _format_item(result: dict[str, Any]) -> BuscarTudoItem | None:
    """Translate a ``/api/v1/search`` result into the PWA shape.

    Returns ``None`` when the entity is out of the Goias scope (either
    a non-GO politician or a non-GO label). The Flask backend already
    applies this filter; we preserve the same behaviour here so
    consumers see identical result lists.
    """
    props = result.get("properties") or {}
    tipo_raw = str(result.get("type") or "").lower()

    if tipo_raw == "person":
        if not _is_person_go(props):
            return None
    elif tipo_raw not in _GO_TYPES:
        return None

    item = BuscarTudoItem(
        id=str(result["id"]),
        tipo=tipo_raw,
        nome=str(result.get("name") or ""),
        documento=result.get("document"),
        score=float(result.get("score") or 0.0),
        icone="outro",
        detalhe=tipo_raw.capitalize(),
    )

    if tipo_raw == "person":
        item.icone = "pessoa"
        patrimonio = props.get("patrimonio_declarado")
        item.detalhe = (
            f"Patrimonio: {_fmt_brl(patrimonio)}" if patrimonio else "Pessoa publica"
        )
        item.is_pep = bool(props.get("is_pep", False))
    elif tipo_raw in {"state_employee", "stateemployee"}:
        item.icone = "servidor"
        salario = props.get("salary_gross")
        cargo = str(props.get("role") or "")
        if salario:
            item.detalhe = f"{cargo} - {_fmt_brl(salario)}/mes"
        else:
            item.detalhe = cargo or "Servidor estadual"
        item.is_comissionado = bool(props.get("is_commissioned", False))
    elif tipo_raw in {"go_procurement", "goprocurement"}:
        item.icone = "licitacao"
        valor = props.get("amount_estimated") or 0
        item.detalhe = (
            f"Licitacao: {_fmt_brl(valor)}" if valor else str(props.get("object") or "Licitacao")
        )
    elif tipo_raw in {"go_appointment", "goappointment"}:
        item.icone = "nomeacao"
        tipo_apt = str(props.get("appointment_type") or "Nomeacao").title()
        role = str(props.get("role") or "")
        item.detalhe = f"{tipo_apt}: {role}"
    elif tipo_raw in {"go_vereador", "govereador"}:
        item.icone = "vereador"
        item.detalhe = f"Vereador(a) - {props.get('party', '')}"
    elif tipo_raw == "company":
        item.icone = "empresa"
        item.detalhe = str(props.get("razao_social") or "")
    elif tipo_raw == "contract":
        item.icone = "contrato"
        valor = props.get("value") or 0
        item.detalhe = _fmt_brl(valor) if valor else "Contrato publico"
    elif tipo_raw == "amendment":
        item.icone = "emenda"
        valor = props.get("value_paid") or props.get("value_committed") or 0
        item.detalhe = f"Emenda: {_fmt_brl(valor)}" if valor else "Emenda parlamentar"

    return item


async def _run_search(
    session: AsyncSession,
    q: str,
    page: int,
    size: int,
    *,
    entity_type: str | None,
) -> tuple[list[dict[str, Any]], int]:
    """Thin wrapper around ``search``/``search_count`` that returns
    result dicts + total, matching what the Flask handler consumed
    from ``/api/v1/search``.
    """
    skip = (page - 1) * size
    hide_person_entities = should_hide_person_entities()
    lucene_query = _to_lucene_query(q)

    records = await execute_query(
        session,
        "search",
        {
            "query": lucene_query,
            "entity_type": entity_type,
            "skip": skip,
            "limit": size,
            "hide_person_entities": hide_person_entities,
        },
    )
    total_record = await execute_query_single(
        session,
        "search_count",
        {
            "query": lucene_query,
            "entity_type": entity_type,
            "hide_person_entities": hide_person_entities,
        },
    )
    total = 0
    if total_record and total_record["total"] is not None:
        total = int(total_record["total"])

    results: list[dict[str, Any]] = []
    for record in records:
        node = record["node"]
        props = dict(node)
        labels = record["node_labels"] or []
        doc_id = record["document_id"]
        document = (
            str(doc_id) if doc_id and not str(doc_id).startswith("4:") else None
        )
        results.append(
            {
                "id": record["node_id"],
                "type": labels[0].lower() if labels else "unknown",
                "name": props.get(
                    "name",
                    props.get("razao_social", props.get("object", "")),
                ),
                "score": float(record["score"] or 0.0),
                "document": document,
                "properties": props,
            }
        )
    return results, total


@router.get("/status", response_model=StatusResponse)
async def pwa_status(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> StatusResponse:
    """PWA landing-page counters (aggregated in one Cypher round-trip)."""
    try:
        record = await execute_query_single(
            session, "pwa_status", {"uf": UF_FILTRO}
        )
        bracc_conectado = True
    except Exception:  # noqa: BLE001 — mirror Flask: degrade on DB errors
        record = None
        bracc_conectado = False

    if record is None:
        return StatusResponse(
            status="online",
            bracc_conectado=bracc_conectado,
            total_nos=0,
            total_relacionamentos=0,
            deputados_federais=0,
            deputados_estaduais=0,
            senadores=0,
            servidores_estaduais=0,
            cargos_comissionados=0,
            municipios_go=0,
            licitacoes_go=0,
            nomeacoes_go=0,
            vereadores_goiania=0,
        )

    return StatusResponse(
        status="online",
        bracc_conectado=bracc_conectado,
        total_nos=int(record["total_nos"] or 0),
        total_relacionamentos=int(record["total_relacionamentos"] or 0),
        deputados_federais=int(record["deputados_federais"] or 0),
        deputados_estaduais=int(record["deputados_estaduais"] or 0),
        senadores=int(record["senadores"] or 0),
        vereadores_goiania=int(record["vereadores_goiania"] or 0),
        servidores_estaduais=int(record["servidores_estaduais"] or 0),
        cargos_comissionados=int(record["cargos_comissionados"] or 0),
        municipios_go=int(record["municipios_go"] or 0),
        licitacoes_go=int(record["licitacoes_go"] or 0),
        nomeacoes_go=int(record["nomeacoes_go"] or 0),
    )


@router.get("/buscar-tudo", response_model=BuscarTudoResponse)
async def pwa_buscar_tudo(
    session: Annotated[AsyncSession, Depends(get_session)],
    q: Annotated[str, Query(min_length=2, max_length=200)],
    page: Annotated[int, Query(ge=1)] = 1,
) -> BuscarTudoResponse:
    """Unified search for the PWA result list.

    Runs the same two-pass search the Flask handler performs — one
    call filtered to ``type=person`` so politicians are not buried by
    high-scoring company matches, and one unfiltered pass — then
    dedups by id keeping the highest score, applies the Goias scope
    filter, and maps each result to the PWA ``BuscarTudoItem`` shape.
    """
    try:
        pessoas, pessoas_total = await _run_search(
            session, q, page, 30, entity_type="person"
        )
        outros, outros_total = await _run_search(
            session, q, page, 20, entity_type=None
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Erro: {exc}") from exc

    combined: dict[str, dict[str, Any]] = {}
    for r in (*pessoas, *outros):
        existing = combined.get(r["id"])
        if not existing or r.get("score", 0) > existing.get("score", 0):
            combined[r["id"]] = r

    total = max(pessoas_total, outros_total)

    items: list[BuscarTudoItem] = []
    for r in sorted(combined.values(), key=lambda x: -x.get("score", 0)):
        item = _format_item(r)
        if item is not None:
            items.append(item)

    return BuscarTudoResponse(resultados=items, total=total, pagina=page)


def _aggregate_ceap(
    despesas: list[dict[str, Any]] | None,
) -> tuple[list[CeapAnoBreakdown], float]:
    """Collapse per-expense rows into (per-year breakdown, total)."""
    if not despesas:
        return [], 0.0
    per_year: dict[int, tuple[float, int]] = {}
    total = 0.0
    for item in despesas:
        if not isinstance(item, dict):
            continue
        ano_raw = item.get("ano")
        valor_raw = item.get("valor")
        if ano_raw is None or valor_raw is None:
            continue
        try:
            ano = int(ano_raw)
            valor = float(valor_raw)
        except (TypeError, ValueError):
            continue
        if valor <= 0:
            continue
        prev_total, prev_count = per_year.get(ano, (0.0, 0))
        per_year[ano] = (prev_total + valor, prev_count + 1)
        total += valor
    breakdown = [
        CeapAnoBreakdown(ano=ano, valor_total=vals[0], n_despesas=vals[1])
        for ano, vals in sorted(per_year.items(), reverse=True)
    ]
    return breakdown, total


@router.get("/politico/{entity_id}", response_model=PoliticoResponse)
async def pwa_politico(
    session: Annotated[AsyncSession, Depends(get_session)],
    entity_id: Annotated[str, Path(min_length=1, max_length=200)],
) -> PoliticoResponse:
    """Perfil de um político federal GO lido do grafo.

    Substitui o ``/politico/{entity_id}`` do Flask, que fazia live-call
    direto pra API da Câmara a cada request. Os dados aqui vêm do
    pipeline ``camara_politicos_go`` (ver ``etl/src/bracc_etl/pipelines/
    camara_politicos_go.py``), com ``ProvenanceBlock`` completo incluindo
    ``snapshot_url`` quando o archival capturou a resposta bruta.
    """
    try:
        record = await execute_query_single(
            session, "pwa_politico", {"entity_id": entity_id},
        )
    except Exception as exc:  # noqa: BLE001 — degrade, don't leak 500
        raise HTTPException(status_code=502, detail=f"Erro: {exc}") from exc

    if record is None or record.get("legislator") is None:
        raise HTTPException(status_code=404, detail="Politico nao encontrado")

    node = record["legislator"]
    props = dict(node)
    provenance = _pop_provenance(props)

    id_camara = str(props.get("id_camara") or "")
    legislator_id = str(
        props.get("legislator_id") or (f"camara_{id_camara}" if id_camara else ""),
    )
    legislatura = props.get("legislatura_atual")
    try:
        legislatura_int = int(legislatura) if legislatura is not None else None
    except (TypeError, ValueError):
        legislatura_int = None

    resumo = PoliticoResumo(
        id_camara=id_camara,
        legislator_id=legislator_id,
        nome=str(props.get("name") or props.get("nome") or ""),
        cpf=str(props.get("cpf")) if props.get("cpf") else None,
        partido=str(props.get("partido")) if props.get("partido") else None,
        uf=str(props.get("uf") or UF_FILTRO),
        email=str(props.get("email")) if props.get("email") else None,
        foto_url=str(props.get("url_foto")) if props.get("url_foto") else None,
        situacao=str(props.get("situacao")) if props.get("situacao") else None,
        legislatura_atual=legislatura_int,
        scope=str(props.get("scope") or "federal"),
    )

    despesas_ceap, total = _aggregate_ceap(record.get("despesas"))

    return PoliticoResponse(
        politico=resumo,
        despesas_ceap=despesas_ceap,
        total_ceap=total,
        total_ceap_fmt=_fmt_brl(total),
        provenance=provenance,
    )
