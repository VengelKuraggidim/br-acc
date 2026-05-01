"""IrregularidadesService — cards detalhados de sancoes/TCE-GO/TCM/embargos.

Le 4 queries cluster-aware (cluster-walk via :CanonicalPerson igual ao
``historico_eleitoral_service``):

- ``perfil_sancoes`` — :Sanction via :SANCIONADA
- ``perfil_tce_go_irregulares`` — :TceGoIrregularAccount via
  :IMPEDIDO_TCE_GO
- ``perfil_tcm_impedidos`` — :TcmGoImpedido por nome (CPF mascarado
  upstream impede match exato)
- ``perfil_embargos`` — :Embargo direto via :EMBARGADA

Cada funcao devolve lista vazia quando o cluster nao tem nenhum
registro — ``obter_perfil`` propaga e o PWA omite o card respectivo.

Substitui parcialmente alertas agregados de :Sanction/:TcmGoImpedido em
``alertas_service`` — quando o card detalhado existir, o alerta agregado
fica redundante (TODO 09 do high_priority/variados).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bracc.config import settings
from bracc.models.perfil import (
    EmbargoCard,
    SancaoCard,
    TceGoIrregularCard,
    TcmGoImpedidoCard,
)
from bracc.services.neo4j_service import execute_query_single

if TYPE_CHECKING:
    from neo4j import AsyncDriver

_QUERY_TIMEOUT = 15.0


def _str(row: dict[str, Any], key: str) -> str:
    val = row.get(key)
    if val is None:
        return ""
    return str(val).strip()


async def obter_sancoes(
    driver: AsyncDriver,
    entity_id: str,
) -> list[SancaoCard]:
    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_sancoes",
            {"entity_id": entity_id},
            timeout=_QUERY_TIMEOUT,
        )
    if record is None:
        return []
    rows = record.get("sancoes") or []
    cards: list[SancaoCard] = []
    for row in rows:
        sanction_id_raw = row.get("sanction_id")
        cards.append(
            SancaoCard(
                sanction_id=(
                    str(sanction_id_raw).strip()
                    if sanction_id_raw is not None
                    else None
                ),
                tipo=_str(row, "tipo"),
                motivo=_str(row, "motivo"),
                fonte=_str(row, "fonte"),
                data_inicio=_str(row, "data_inicio"),
                data_fim=_str(row, "data_fim"),
            ),
        )
    return cards


async def obter_tce_go_irregulares(
    driver: AsyncDriver,
    entity_id: str,
) -> list[TceGoIrregularCard]:
    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_tce_go_irregulares",
            {"entity_id": entity_id},
            timeout=_QUERY_TIMEOUT,
        )
    if record is None:
        return []
    rows = record.get("contas") or []
    cards: list[TceGoIrregularCard] = []
    for row in rows:
        account_id = _str(row, "account_id")
        if not account_id:
            continue
        cards.append(
            TceGoIrregularCard(
                account_id=account_id,
                ano=_str(row, "ano"),
                cargo=_str(row, "cargo"),
                processo=_str(row, "processo"),
                julgamento=_str(row, "julgamento"),
                motivo=_str(row, "motivo"),
                uf=_str(row, "uf"),
                pdf_url=_str(row, "pdf_url"),
                fonte_url=_str(row, "fonte_url"),
            ),
        )
    return cards


async def obter_tcm_go_impedidos(
    driver: AsyncDriver,
    entity_id: str,
) -> list[TcmGoImpedidoCard]:
    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_tcm_impedidos",
            {"entity_id": entity_id},
            timeout=_QUERY_TIMEOUT,
        )
    if record is None:
        return []
    rows = record.get("impedidos") or []
    cards: list[TcmGoImpedidoCard] = []
    for row in rows:
        impedido_id = _str(row, "impedido_id")
        if not impedido_id:
            continue
        cards.append(
            TcmGoImpedidoCard(
                impedido_id=impedido_id,
                processo=_str(row, "processo"),
                motivo=_str(row, "motivo"),
                data_inicio=_str(row, "data_inicio"),
                data_fim=_str(row, "data_fim"),
                fonte_url=_str(row, "fonte_url"),
            ),
        )
    return cards


async def obter_embargos(
    driver: AsyncDriver,
    entity_id: str,
) -> list[EmbargoCard]:
    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_embargos",
            {"entity_id": entity_id},
            timeout=_QUERY_TIMEOUT,
        )
    if record is None:
        return []
    rows = record.get("embargos") or []
    cards: list[EmbargoCard] = []
    for row in rows:
        embargo_id = _str(row, "embargo_id")
        if not embargo_id:
            continue
        area_raw = row.get("area_ha")
        try:
            area = float(area_raw) if area_raw is not None else None
        except (TypeError, ValueError):
            area = None
        cards.append(
            EmbargoCard(
                embargo_id=embargo_id,
                infracao=_str(row, "infracao"),
                auto_infracao=_str(row, "auto_infracao"),
                data=_str(row, "data"),
                municipio=_str(row, "municipio"),
                uf=_str(row, "uf"),
                biome=_str(row, "biome"),
                area_ha=area,
                processo=_str(row, "processo"),
                fonte=_str(row, "fonte"),
            ),
        )
    return cards
