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
from neo4j import AsyncDriver, AsyncSession

from bracc.dependencies import get_driver, get_session
from bracc.models.perfil import PerfilPolitico
from bracc.models.pwa_parity import (
    BuscarTudoItem,
    BuscarTudoResponse,
    StatusResponse,
)
from bracc.services import perfil_service
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
    # Parlamentares — entraram no fulltext ``entity_search`` em 2026-04-22.
    # Precisam de filtro de UF (same-uf-as-GO) porque o indice cobre o
    # Brasil inteiro — ``_format_item`` aplica ``_is_person_go`` pra eles.
    "federal_legislator",
    "federallegislator",
    "state_legislator",
    "statelegislator",
    "senator",
}

_LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}[\]^"~*?:\\/])')


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

    # Tipos cuja escopo é nacional no fulltext precisam filtrar por
    # ``uf=='GO'`` pra respeitar o escopo do app. ``Senator`` entra aqui
    # (Caiado é GO, mas o indice cobre todos os 81 senadores).
    _PARLAMENTAR_BR_TYPES = {
        "federal_legislator", "federallegislator",
        "state_legislator", "statelegislator",
        "senator",
    }

    if tipo_raw == "person" or tipo_raw in _PARLAMENTAR_BR_TYPES:
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
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
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
    elif tipo_raw in {"federal_legislator", "federallegislator"}:
        item.icone = "pessoa"
        partido = str(props.get("partido") or "").strip()
        item.detalhe = (
            f"Deputado(a) Federal - {partido}" if partido else "Deputado(a) Federal"
        )
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
    elif tipo_raw in {"state_legislator", "statelegislator"}:
        item.icone = "pessoa"
        partido = str(props.get("party") or props.get("partido") or "").strip()
        item.detalhe = (
            f"Deputado(a) Estadual - {partido}" if partido else "Deputado(a) Estadual"
        )
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
    elif tipo_raw == "senator":
        item.icone = "pessoa"
        partido = str(props.get("partido") or "").strip()
        item.detalhe = (
            f"Senador(a) - {partido}" if partido else "Senador(a)"
        )
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
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
        # Person search uses a wider fulltext window (500 vs 30) so the
        # post-query UF=GO filter in ``_format_item`` has enough
        # candidates to draw from. With only 30, popular names in other
        # states (SP/MG/PE) saturate the top-ranked slice and almost no
        # GO rows survive the filter, even when the graph has hundreds
        # of matching GO persons. 500 keeps the round-trip under ~100ms
        # and restores parity with the prod result volume.
        pessoas, pessoas_total = await _run_search(
            session, q, page, 500, entity_type="person"
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


@router.get("/politico/{entity_id}", response_model=PerfilPolitico)
async def pwa_politico(
    driver: Annotated[AsyncDriver, Depends(get_driver)],
    entity_id: Annotated[str, Path(min_length=1, max_length=200)],
) -> PerfilPolitico:
    """Perfil completo do político — orquestração via ``PerfilService``.

    Shape completo (22 campos top-level, ver :class:`PerfilPolitico`)
    reproduz o endpoint do Flask ``backend/app.py::perfil_politico``:
    conexões classificadas em 7 categorias, emendas, CEAP agregado por
    tipo, comparação com cidadão comum, alertas determinísticos,
    validação TSE e ``ProvenanceBlock`` no topo.

    Zero live-call — todos os dados vêm do grafo ingerido pelos
    pipelines ``camara_politicos_go``, ``emendas_parlamentares_go`` e
    cross-refs TSE/CGU. Fase 04.F da consolidação FastAPI (ver
    ``todo-list-prompts/very_high_priority/frontend/04F-perfil-service-
    endpoint.md``).
    """
    try:
        return await perfil_service.obter_perfil(driver, entity_id)
    except perfil_service.EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except perfil_service.DriverError as exc:
        raise HTTPException(status_code=502, detail=f"Erro: {exc}") from exc
