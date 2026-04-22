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
from typing import TYPE_CHECKING, Any

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
    """Janela padrão de anos pra consulta de despesas: ano atual + anterior.

    Por que 2 anos:
        Espelha o comportamento do Flask original
        (``backend/apis_externas.py::buscar_despesas_deputado``). A
        janela de 2 anos dá cobertura completa do mandato corrente sem
        trazer dados antigos que inflam o UI sem agregar valor — no
        PWA a seção de despesas é um "resumo recente", não histórico
        longitudinal.

    Quando é chamado:
        Default para o parâmetro ``anos`` de :func:`obter_ceap_deputado`,
        :func:`obter_verba_indenizatoria_alego` e
        :func:`obter_cota_vereador_goiania`. O caller pode passar uma
        lista explícita pra override (útil em testes e em pesquisas
        históricas ad-hoc).

    Comportamento dependente de calendário:
        Usa :func:`datetime.now` com ``tz=UTC`` — o valor muda por ano
        calendário. Em 1/jan o range se desloca automaticamente (isso
        é desejado — em janeiro ainda há processamento do ano anterior
        pendente no portal da Câmara). Testes que fixam ``now`` devem
        mockar este módulo.

    Returns
    -------
    list[int]
        ``[ano_atual, ano_atual - 1]`` — ordem decrescente, preservada
        pelas queries Cypher downstream.
    """
    ano_atual = datetime.now(tz=UTC).year
    return [ano_atual, ano_atual - 1]


async def _aggregate_despesas(
    driver: AsyncDriver,
    cypher_name: str,
    params: dict[str, Any],
    *,
    contexto_log: str,
) -> list[DespesaGabinete]:
    """Executa Cypher + agrega rows por ``tipo_raw`` → ``DespesaGabinete``.

    Lógica única compartilhada pelas 3 funções públicas (CEAP federal,
    verba ALEGO, cota vereador GYN). Evita drift entre cópias da mesma
    agregação — qualquer ajuste (ex.: mudar cap ``<= 0``, trocar default
    pra ``Outros``) acontece em 1 lugar só.

    Parameters
    ----------
    driver:
        Async driver Neo4j — a session é aberta e fechada aqui dentro.
    cypher_name:
        Nome do arquivo ``.cypher`` (sem extensão) a ser resolvido por
        :func:`execute_query`.
    params:
        Parâmetros Cypher já formatados (ex.: ``{"id_camara": "...",
        "anos": [...]}``). O caller garante o contrato de cada query.
    contexto_log:
        Texto curto usado nos ``logger.warning`` — basta pra o operador
        identificar qual variante retornou vazia sem espalhar formatação
        na logic de agregação.

    Returns
    -------
    list[DespesaGabinete]
        Lista ordenada por ``total`` decrescente. Vazia (log warning)
        quando não há records ou todos os valores são inválidos/não-
        positivos — nunca levanta nesse caso (é o contrato do Flask
        original, que o PerfilService já trata como "seção omitida").
    """
    async with driver.session(database=settings.neo4j_database) as session:
        records = await execute_query(session, cypher_name, params)

    if not records:
        logger.warning("[despesas_service] sem records — %s", contexto_log)
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
            "[despesas_service] records sem valores validos — %s", contexto_log,
        )
        return []

    return [
        DespesaGabinete(tipo=tipo, total=total, total_fmt=fmt_brl(total))
        for tipo, total in sorted(
            por_tipo.items(), key=lambda kv: -kv[1],
        )
    ]


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
    return await _aggregate_despesas(
        driver,
        "perfil_ceap_deputado",
        {"id_camara": str(id_camara), "anos": anos_filtro},
        contexto_log=f"CEAP deputado id_camara={id_camara} anos={anos_filtro}",
    )


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
    return await _aggregate_despesas(
        driver,
        "perfil_verba_alego",
        {"legislator_id": str(legislator_id), "anos": anos_filtro},
        contexto_log=(
            f"verba ALEGO legislator_id={legislator_id} anos={anos_filtro}"
        ),
    )


async def obter_ceaps_senador(
    driver: AsyncDriver,
    id_senado: str,
    anos: list[int] | None = None,
) -> list[DespesaGabinete]:
    """Le CEAPS de um senador federal do grafo, agregada por tipo.

    CEAPS (Cota para o Exercicio da Atividade Parlamentar dos Senadores) e a
    cota do Senado, regulada pelo Ato da Comissao Diretora no 3/2016. Analogo
    de :func:`obter_ceap_deputado` (federal Camara) / :func:`obter_verba_
    indenizatoria_alego` (estadual GO) / :func:`obter_cota_vereador_goiania`
    (municipal GYN) — **mesmo shape de saida** (``DespesaGabinete`` agregado
    por tipo) pra que o PerfilService trate as quatro casas uniformemente.

    A ingestao e feita pelo pipeline ``senado`` (CEAPS CSV anual do portal
    do Senado) + ``senado_senadores_foto`` (roster :Senator com id_senado).
    A bridge :Senator -> :Expense passa por :Person por name-match (ver
    ``perfil_ceaps_senador.cypher`` pra detalhes).

    Args:
        driver: Neo4j async driver.
        id_senado: ``id_senado`` do no ``:Senator`` (string, mesmo formato
            que o pipeline ``senado_senadores_foto`` grava).
        anos: Lista de anos a considerar. Se ``None``, usa
            ``[ano_atual, ano_atual - 1]`` (mesmo default das outras casas).
            Se ``[]``, traz todos os anos ingeridos.

    Returns:
        Lista de ``DespesaGabinete`` ordenada por ``total`` decrescente.
        Lista vazia se o senador nao tem CEAPS ingerida ou o no nao existe;
        nunca levanta excecao nesse caso (log ``warning``).
    """
    anos_filtro = anos if anos is not None else _default_anos()
    return await _aggregate_despesas(
        driver,
        "perfil_ceaps_senador",
        {"id_senado": str(id_senado), "anos": anos_filtro},
        contexto_log=(
            f"CEAPS senador id_senado={id_senado} anos={anos_filtro}"
        ),
    )


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
    return await _aggregate_despesas(
        driver,
        "perfil_cota_vereador_goiania",
        {"vereador_id": str(vereador_id), "anos": anos_filtro},
        contexto_log=(
            f"cota vereador GYN vereador_id={vereador_id} anos={anos_filtro}"
        ),
    )


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
