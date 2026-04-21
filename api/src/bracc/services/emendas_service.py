"""EmendasService — lê emendas parlamentares do grafo BRACC.

Substitui o live-call que o Flask (`backend/app.py` -> `apis_externas
.buscar_emendas_transparencia`) faz no Portal da Transparência. A
ingestão agora é responsabilidade do pipeline
``emendas_parlamentares_go`` (ver ``etl/src/bracc_etl/pipelines/
emendas_parlamentares_go.py``), que arquiva cada página e carimba
proveniência completa em ``:Amendment`` / ``(:FederalLegislator)-[:PROPOS]->``.

Este módulo é **puramente de leitura**: zero rede, zero chamada fora
do Neo4j. Se o grafo ainda não tem emendas para o deputado (pipeline
não rodou, ou deputado sem emenda), devolve lista vazia.

Tradução e formatação seguem o padrão da fase 04.A:
:func:`bracc.services.formatacao_service.fmt_brl`,
:func:`bracc.services.traducao_service.traduzir_funcao_emenda`, e
:func:`bracc.services.traducao_service.traduzir_tipo_emenda`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from bracc.config import settings
from bracc.models.perfil import Emenda
from bracc.services.formatacao_service import fmt_brl
from bracc.services.neo4j_service import execute_query
from bracc.services.traducao_service import (
    traduzir_funcao_emenda,
    traduzir_tipo_emenda,
)

if TYPE_CHECKING:
    from neo4j import AsyncDriver, Record

logger = logging.getLogger(__name__)

_QUERY_NAME = "perfil_emendas_deputado"


def _record_to_emenda(record: Record) -> Emenda:
    """Mapeia um record Neo4j -> Emenda com tradução e formatação aplicadas."""
    amendment_id = str(record["id"] or "")
    tipo_raw = record["tipo"]
    funcao_raw = record["funcao"]
    municipio_raw = record["municipio"]
    uf_raw = record["uf"]
    valor_empenhado = float(record["valor_empenhado"] or 0)
    valor_pago = float(record["valor_pago"] or 0)
    ano_raw = record.get("ano")
    try:
        ano: int | None = int(ano_raw) if ano_raw is not None else None
    except (TypeError, ValueError):
        ano = None
    beneficiario_cnpj_raw = record.get("beneficiario_cnpj")
    beneficiario_nome_raw = record.get("beneficiario_nome")
    beneficiario_data_abertura_raw = record.get("beneficiario_data_abertura")

    return Emenda(
        id=amendment_id,
        tipo=traduzir_tipo_emenda(
            str(tipo_raw) if tipo_raw is not None else None,
        ),
        funcao=traduzir_funcao_emenda(
            str(funcao_raw) if funcao_raw is not None else None,
        ),
        municipio=(
            str(municipio_raw).strip() or None
            if municipio_raw is not None
            else None
        ),
        uf=(
            str(uf_raw).strip().upper() or None
            if uf_raw is not None
            else None
        ),
        ano=ano,
        valor_empenhado=valor_empenhado,
        valor_empenhado_fmt=fmt_brl(valor_empenhado),
        valor_pago=valor_pago,
        valor_pago_fmt=fmt_brl(valor_pago),
        beneficiario_cnpj=(
            str(beneficiario_cnpj_raw).strip() or None
            if beneficiario_cnpj_raw is not None
            else None
        ),
        beneficiario_nome=(
            str(beneficiario_nome_raw).strip() or None
            if beneficiario_nome_raw is not None
            else None
        ),
        beneficiario_data_abertura=(
            str(beneficiario_data_abertura_raw).strip() or None
            if beneficiario_data_abertura_raw is not None
            else None
        ),
    )


async def obter_emendas_deputado(
    driver: AsyncDriver,
    id_camara: int,
) -> list[Emenda]:
    """Devolve as emendas propostas por um deputado federal GO.

    Parameters
    ----------
    driver:
        Driver Neo4j async já configurado pela aplicação.
    id_camara:
        ``id_camara`` do deputado federal (mesmo id usado pela API da
        Câmara / pipeline ``camara_politicos_go``).

    Returns
    -------
    list[Emenda]
        Lista ordenada por valor pago (desc), com ``fmt_brl`` e
        traduções aplicadas. Lista vazia quando o deputado não tem
        emendas ingeridas.
    """
    # O pipeline ``camara_politicos_go`` grava ``id_camara`` como string
    # (``str(dep.id)``), então consultamos como string pra casar com a
    # propriedade do nó. A assinatura aceita ``int`` pra ergonomia do
    # caller (id da API da Câmara é numérico).
    async with driver.session(database=settings.neo4j_database) as session:
        records = await execute_query(
            session,
            _QUERY_NAME,
            {"id_camara": str(id_camara)},
        )
    return [_record_to_emenda(r) for r in records]
