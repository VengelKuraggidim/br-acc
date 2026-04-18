"""DespesasService — lê CEAP de deputados federais GO do grafo.

Portado do Flask (``backend/apis_externas.py::buscar_despesas_deputado``,
``agrupar_despesas_por_tipo`` e ``buscar_media_despesas_estado`` —
linhas 170-310) como parte da fase 04.C da consolidação FastAPI.

Contraste com o Flask:

* O Flask fazia **live-call** na API da Câmara dos Deputados a cada
  request — custoso, frágil e sem rastreabilidade.
* Este service lê do grafo (nós já ingeridos pelo pipeline
  ``camara_politicos_go``). Zero network em tempo de request — os
  dados são consolidados por uma cadência offline (mensal, ver o
  docstring do pipeline).

Shape esperado do grafo (``etl/src/bracc_etl/pipelines/camara_politicos_go.py``):

* Nó ``:FederalLegislator`` com prop ``id_camara`` (string) e ``uf``.
* Rel ``(:FederalLegislator)-[:INCURRED {tipo: 'CEAP'}]->(:LegislativeExpense)``
  com props ``ano``, ``mes``, ``valor_liquido``.
* Nó ``:LegislativeExpense`` com props ``tipo_despesa``, ``valor_liquido``,
  ``ano``.

Stateless — funções puras sobre o driver Neo4j. Tradução de
``tipo_despesa`` via :func:`bracc.services.traducao_service.traduzir_despesa`;
formatação BRL via :func:`bracc.services.formatacao_service.fmt_brl`.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from bracc.config import settings
from bracc.models.perfil import DespesaGabinete
from bracc.services.formatacao_service import fmt_brl
from bracc.services.neo4j_service import execute_query, execute_query_single
from bracc.services.traducao_service import traduzir_despesa

if TYPE_CHECKING:
    from neo4j import AsyncDriver

logger = logging.getLogger(__name__)

_DEFAULT_AMOSTRA = 10


def _default_anos() -> list[int]:
    """Default do Flask: ano corrente + ano anterior."""
    ano_atual = datetime.now(tz=UTC).year
    return [ano_atual, ano_atual - 1]


async def obter_ceap_deputado(
    driver: AsyncDriver,
    id_camara: int,
    anos: list[int] | None = None,
) -> list[DespesaGabinete]:
    """Lê despesas CEAP de um deputado federal do grafo, agregadas por tipo.

    Args:
        driver: Neo4j async driver (session aberta internamente).
        id_camara: ID do deputado na API da Câmara (``FederalLegislator.id_camara``).
            O pipeline armazena esse campo como string, então o cast
            acontece aqui — o caller pode passar ``int`` sem se preocupar.
        anos: Lista de anos a considerar. Se ``None``, usa ``[ano_atual,
            ano_atual - 1]`` — espelho do default do Flask.

    Returns:
        Lista de ``DespesaGabinete`` ordenada por ``total`` decrescente.
        Cada item representa um ``tipo_despesa`` agregado (com tradução
        aplicada via ``traduzir_despesa``). Lista vazia se o deputado não
        tem CEAP ingerido (ou o nó não existe); nunca levanta exceção nesse
        caso (log ``warning``).
    """
    anos_filtro = anos if anos is not None else _default_anos()

    async with driver.session(database=settings.neo4j_database) as session:
        records = await execute_query(
            session,
            "perfil_ceap_deputado",
            {"id_camara": str(id_camara), "anos": anos_filtro},
        )

    if not records:
        logger.warning(
            "[despesas_service] sem CEAP para deputado id_camara=%s anos=%s",
            id_camara, anos_filtro,
        )
        return []

    # Agrega por tipo_despesa traduzido. Preserva o raw quando a tradução
    # é identidade (dict sem match) para não quebrar comparações downstream.
    por_tipo: dict[str, float] = {}
    for record in records:
        tipo_raw = record.get("tipo_raw") or ""
        valor_raw = record.get("valor")
        if valor_raw is None:
            continue
        try:
            valor = float(valor_raw)
        except (TypeError, ValueError):
            continue
        if valor <= 0:
            continue
        tipo_traduzido = traduzir_despesa(str(tipo_raw)) if tipo_raw else "Outros"
        por_tipo[tipo_traduzido] = por_tipo.get(tipo_traduzido, 0.0) + valor

    if not por_tipo:
        logger.warning(
            "[despesas_service] CEAP de id_camara=%s sem valores válidos",
            id_camara,
        )
        return []

    return [
        DespesaGabinete(tipo=tipo, total=total, total_fmt=fmt_brl(total))
        for tipo, total in sorted(
            por_tipo.items(), key=lambda kv: -kv[1],
        )
    ]


async def obter_verba_indenizatoria_alego(
    driver: AsyncDriver,
    legislator_id: str,
    anos: list[int] | None = None,
) -> list[DespesaGabinete]:
    """Lê verba indenizatória ALEGO de um deputado estadual GO do grafo.

    A ALEGO (Assembleia Legislativa de Goiás) publica a "verba indenizatória"
    dos deputados estaduais em ``transparencia.al.go.leg.br``. O pipeline
    ``alego`` ingere os lançamentos e cria a rel
    ``(:StateLegislator)-[:GASTOU_COTA_GO]->(:LegislativeExpense)``.

    Esta função é o análogo de :func:`obter_ceap_deputado` para o escopo
    estadual — **mesmo shape de saída** (``DespesaGabinete`` agregado por
    tipo) pra que o ``PerfilService`` trate federal e estadual de forma
    transparente pro PWA.

    Args:
        driver: Neo4j async driver.
        legislator_id: ``legislator_id`` do nó ``:StateLegislator`` (hash
            estável gerado pelo pipeline ``alego`` a partir do nome).
        anos: Lista de anos a considerar. Se ``None``, usa
            ``[ano_atual, ano_atual - 1]`` (mesmo default do CEAP federal).
            Se ``[]``, traz todos os anos ingeridos.

    Returns:
        Lista de ``DespesaGabinete`` ordenada por ``total`` decrescente.
        Lista vazia se o deputado não tem verba ingerida ou o nó não existe;
        nunca levanta exceção nesse caso (log ``warning``).
    """
    anos_filtro = anos if anos is not None else _default_anos()

    async with driver.session(database=settings.neo4j_database) as session:
        records = await execute_query(
            session,
            "perfil_verba_alego",
            {"legislator_id": str(legislator_id), "anos": anos_filtro},
        )

    if not records:
        logger.warning(
            "[despesas_service] sem verba ALEGO para legislator_id=%s anos=%s",
            legislator_id, anos_filtro,
        )
        return []

    # Mesma lógica de agregação do CEAP federal (preserva total_fmt BRL).
    por_tipo: dict[str, float] = {}
    for record in records:
        tipo_raw = record.get("tipo_raw") or ""
        valor_raw = record.get("valor")
        if valor_raw is None:
            continue
        try:
            valor = float(valor_raw)
        except (TypeError, ValueError):
            continue
        if valor <= 0:
            continue
        tipo_traduzido = traduzir_despesa(str(tipo_raw)) if tipo_raw else "Outros"
        por_tipo[tipo_traduzido] = por_tipo.get(tipo_traduzido, 0.0) + valor

    if not por_tipo:
        logger.warning(
            "[despesas_service] verba ALEGO de legislator_id=%s sem valores validos",
            legislator_id,
        )
        return []

    return [
        DespesaGabinete(tipo=tipo, total=total, total_fmt=fmt_brl(total))
        for tipo, total in sorted(
            por_tipo.items(), key=lambda kv: -kv[1],
        )
    ]


async def obter_cota_vereador_goiania(
    driver: AsyncDriver,
    vereador_id: str,
    anos: list[int] | None = None,
) -> list[DespesaGabinete]:
    """Le cota/despesas de gabinete de um vereador da Camara Municipal de Goiania.

    A Camara Municipal de Goiania publica as despesas de gabinete dos
    vereadores no portal ``goiania.go.leg.br`` (endpoint ``@@transparency-json``).
    O pipeline ``camara_goiania`` ingere os lancamentos e cria a rel
    ``(:GoVereador)-[:DESPESA_GABINETE]->(:GoCouncilExpense)``.

    Esta funcao e o analogo municipal de :func:`obter_ceap_deputado` (federal)
    e :func:`obter_verba_indenizatoria_alego` (estadual) — **mesmo shape de
    saida** (``DespesaGabinete`` agregado por tipo) pra que o ``PerfilService``
    trate os 3 niveis (federal/estadual/municipal GO) de forma transparente
    pro PWA.

    Args:
        driver: Neo4j async driver.
        vereador_id: ``vereador_id`` do no ``:GoVereador`` (hash estavel
            gerado pelo pipeline ``camara_goiania`` a partir de nome+partido).
        anos: Lista de anos a considerar. Se ``None``, usa
            ``[ano_atual, ano_atual - 1]`` (mesmo default de CEAP/ALEGO).
            Se ``[]``, traz todos os anos ingeridos.

    Returns:
        Lista de ``DespesaGabinete`` ordenada por ``total`` decrescente.
        Lista vazia se o vereador nao tem despesas ingeridas ou o no nao
        existe; nunca levanta excecao nesse caso (log ``warning``).
    """
    anos_filtro = anos if anos is not None else _default_anos()

    async with driver.session(database=settings.neo4j_database) as session:
        records = await execute_query(
            session,
            "perfil_cota_vereador_goiania",
            {"vereador_id": str(vereador_id), "anos": anos_filtro},
        )

    if not records:
        logger.warning(
            "[despesas_service] sem cota vereador GYN para vereador_id=%s anos=%s",
            vereador_id, anos_filtro,
        )
        return []

    # Mesma logica de agregacao do CEAP/ALEGO (preserva total_fmt BRL).
    por_tipo: dict[str, float] = {}
    for record in records:
        tipo_raw = record.get("tipo_raw") or ""
        valor_raw = record.get("valor")
        if valor_raw is None:
            continue
        try:
            valor = float(valor_raw)
        except (TypeError, ValueError):
            continue
        if valor <= 0:
            continue
        tipo_traduzido = traduzir_despesa(str(tipo_raw)) if tipo_raw else "Outros"
        por_tipo[tipo_traduzido] = por_tipo.get(tipo_traduzido, 0.0) + valor

    if not por_tipo:
        logger.warning(
            "[despesas_service] cota vereador GYN de vereador_id=%s sem valores validos",
            vereador_id,
        )
        return []

    return [
        DespesaGabinete(tipo=tipo, total=total, total_fmt=fmt_brl(total))
        for tipo, total in sorted(
            por_tipo.items(), key=lambda kv: -kv[1],
        )
    ]


async def calcular_media_ceap_estado(
    driver: AsyncDriver,
    uf: str,
    anos: list[int] | None = None,
    amostra: int = _DEFAULT_AMOSTRA,
) -> float:
    """Calcula média de gasto CEAP dos top-N deputados de uma UF.

    Args:
        driver: Neo4j async driver (session aberta internamente).
        uf: Sigla da UF (ex: ``"GO"``). Normalizada para maiúsculas.
        anos: Lista de anos. Default: ``[ano_atual, ano_atual - 1]``.
        amostra: Top-N deputados (por total gasto). Default: 10.

    Returns:
        Média (``float``) em reais dos totais dos top-N deputados. Retorna
        ``0.0`` quando a UF não tem deputados com CEAP ingerido no grafo
        (resposta honesta — não dá pra calcular média sem amostra).
    """
    if not uf:
        return 0.0

    anos_filtro = anos if anos is not None else _default_anos()

    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_ceap_media_estado",
            {
                "uf": uf.upper(),
                "anos": anos_filtro,
                "amostra": int(amostra),
            },
        )

    if record is None:
        return 0.0

    media = record.get("media")
    if media is None:
        return 0.0
    try:
        return float(media)
    except (TypeError, ValueError):
        return 0.0
