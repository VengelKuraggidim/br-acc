"""Service do endpoint ``GET /custo-mandato/{cargo}``.

Função pura async que lê o nó ``:CustoMandato`` + componentes ligados do
grafo (ingerido pelo pipeline ``custo_mandato_br``) e devolve o payload
no shape :class:`~bracc.models.custo_mandato.CustoMandato`.

Cargos suportados são limitados no router (path validation) — aqui o
service propaga ``None`` quando o nó não existe (o router converte em
HTTPException 404).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from bracc.models.custo_mandato import CustoComponente, CustoMandato
from bracc.models.entity import ProvenanceBlock
from bracc.services.formatacao_service import fmt_brl
from bracc.services.neo4j_service import execute_query_single

if TYPE_CHECKING:
    from neo4j import AsyncSession

logger = logging.getLogger(__name__)

# Cargos suportados. O router valida path contra esse frozenset (404 se
# fora). Manter alinhado com
# ``etl/src/bracc_etl/pipelines/custo_mandato_br.py`` (federal + estadual)
# e ``custo_mandato_municipal_go.py::_GO_MUNICIPIOS`` (municipal GO,
# top-10 cidades por população — Censo IBGE 2022). Quando uma cidade for
# adicionada/removida do pipeline, ajustar aqui em paralelo.
_MUNICIPIOS_GO: tuple[str, ...] = (
    "goiania",
    "aparecida_de_goiania",
    "anapolis",
    "rio_verde",
    "aguas_lindas_de_goias",
    "luziania",
    "valparaiso_de_goias",
    "trindade",
    "formosa",
    "senador_canedo",
)
CARGOS_SUPORTADOS: frozenset[str] = frozenset(
    {"dep_federal", "senador", "dep_estadual_go", "governador_go"}
    | {f"prefeito_{m}" for m in _MUNICIPIOS_GO}
    | {f"vereador_{m}" for m in _MUNICIPIOS_GO},
)


class CargoNaoEncontradoError(Exception):
    """Cargo válido mas sem nó ``:CustoMandato`` no grafo (pipeline não rodou).

    Diferenciado do path-invalid (router rejeita antes) pra dar mensagem
    operacional clara: "rode o pipeline custo_mandato_br pra popular".
    """


def _provenance_from_node(node: dict[str, Any]) -> ProvenanceBlock | None:
    """Monta ProvenanceBlock se o nó tem os campos required, senão ``None``."""
    required = ("source_id", "source_url", "ingested_at", "run_id")
    if not all(node.get(f) for f in required):
        return None
    return ProvenanceBlock(
        source_id=str(node["source_id"]),
        source_record_id=(
            str(node["source_record_id"])
            if node.get("source_record_id")
            else None
        ),
        source_url=str(node["source_url"]),
        ingested_at=str(node["ingested_at"]),
        run_id=str(node["run_id"]),
        snapshot_url=(
            str(node["source_snapshot_uri"])
            if node.get("source_snapshot_uri")
            else None
        ),
    )


def _componente_from_node(node: dict[str, Any]) -> CustoComponente:
    valor = node.get("valor_mensal")
    valor_float: float | None
    if valor is None:
        valor_float = None
        valor_fmt = None
    else:
        valor_float = float(valor)
        valor_fmt = fmt_brl(valor_float)
    return CustoComponente(
        componente_id=str(node["componente_id"]),
        rotulo=str(node.get("rotulo") or ""),
        valor_mensal=valor_float,
        valor_mensal_fmt=valor_fmt,
        valor_observacao=str(node.get("valor_observacao") or ""),
        fonte_legal=str(node.get("fonte_legal") or "—"),
        fonte_url=str(node.get("fonte_url") or ""),
        incluir_no_total=bool(node.get("incluir_no_total", True)),
        ordem=int(node.get("ordem") or 0),
        provenance=_provenance_from_node(node),
    )


async def obter_custo_mandato(
    session: AsyncSession,
    cargo: str,
) -> CustoMandato:
    """Lê ``:CustoMandato`` + componentes do grafo, monta resposta tipada.

    Levanta :class:`CargoNaoEncontradoError` quando o cargo é válido (no
    MVP) mas o nó não existe (pipeline ``custo_mandato_br`` não foi
    executado ainda). O router converte em HTTPException 404 com mensagem
    operacional.
    """
    record = await execute_query_single(
        session, "custo_mandato", {"cargo": cargo},
    )
    if record is None or record["mandato"] is None:
        msg = (
            f"Cargo '{cargo}' não encontrado no grafo. "
            f"Rode o pipeline custo_mandato_br pra popular."
        )
        raise CargoNaoEncontradoError(msg)

    mandato_node = dict(record["mandato"])
    componentes_nodes = [
        dict(c) for c in (record["componentes"] or []) if c is not None
    ]

    componentes = [_componente_from_node(c) for c in componentes_nodes]

    custo_mensal = float(mandato_node.get("custo_mensal_individual") or 0.0)
    custo_anual = float(mandato_node.get("custo_anual_total") or 0.0)

    return CustoMandato(
        cargo=str(mandato_node["cargo"]),
        rotulo_humano=str(mandato_node.get("rotulo_humano") or ""),
        esfera=str(mandato_node.get("esfera") or ""),
        uf=(
            str(mandato_node["uf"])
            if mandato_node.get("uf")
            else None
        ),
        municipio=(
            str(mandato_node["municipio"])
            if mandato_node.get("municipio")
            else None
        ),
        n_titulares=int(mandato_node.get("n_titulares") or 0),
        custo_mensal_individual=custo_mensal,
        custo_mensal_individual_fmt=fmt_brl(custo_mensal),
        custo_anual_total=custo_anual,
        custo_anual_total_fmt=fmt_brl(custo_anual),
        equivalente_trabalhadores_min=int(
            mandato_node.get("equivalente_trabalhadores_min") or 0,
        ),
        salario_minimo_referencia=float(
            mandato_node.get("salario_minimo_referencia") or 0.0,
        ),
        salario_minimo_fonte=(
            str(mandato_node["salario_minimo_fonte"])
            if mandato_node.get("salario_minimo_fonte")
            else None
        ),
        componentes=componentes,
        provenance=_provenance_from_node(mandato_node),
    )
