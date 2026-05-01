"""PerfilService — orquestra montagem do ``PerfilPolitico`` (Fase 04.F).

Integra os sub-services das fases 04.A-E numa única função async que
reproduz fielmente o algoritmo do Flask ``backend/app.py::perfil_politico``
(linhas 559-972) — sem live-calls, só leitura do grafo.

Fluxo::

    obter_perfil(driver, entity_id)
        │
        ├─ 1. Cypher ``perfil_politico_connections`` → nó focal + connections
        ├─ 2. Valida que é pessoa (labels contém ``Person``/``FederalLegislator``)
        ├─ 3. Monta ``PoliticoResumo`` (CPF pleno vai aqui — mascaramento é
        │     feito na serialização das listas de doadores/família — compat
        │     com o Flask, que também devolve o CPF pleno no politico.cpf).
        ├─ 4. ``ConexoesService.classificar`` → 7 listas tipadas
        ├─ 5. asyncio.gather( obter_ceap_deputado, obter_emendas_deputado )
        │     (ambos keyed em ``id_camara`` — skip se não for FederalLegislator)
        ├─ 6. ``AnaliseService.gerar_resumo_politico`` + ``analisar_despesas_vs_cidadao``
        ├─ 7. ``AlertasService.gerar_alertas_completos`` + alertas CEAP/picos/media
        ├─ 8. ``ValidacaoTSEService.gerar_validacao_tse``
        └─ 9. Totais (doações, emendas, despesas) + ``ProvenanceBlock`` no topo

Erros:
    * :class:`EntityNotFoundError` — 404 (nó não existe OU labels não contém
      pessoa/legislador).
    * :class:`DriverError` — 502 (driver Neo4j levantou).

Constraints desta fase:
    * Zero live-call. Todos os dados vêm do grafo. Emendas pelo Portal
      da Transparência + despesas CEAP live agora vêm do pipeline
      ``camara_politicos_go`` (ingerido offline).
    * Shape da resposta é **exatamente** o do Flask
      :class:`~bracc.models.perfil.PerfilPolitico` (22 top-level) —
      exceto ``capital_social`` que 04.A já removeu.
    * ``ProvenanceBlock`` no topo usa o carimbo do nó focal do político
      (``source_*``, ``ingested_at``, ``run_id``, ``source_snapshot_uri``).
      Quando faltam campos obrigatórios, vira ``None`` — provenance de
      agregações (CEAP, emendas, TSE) é deferida para fase futura.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from bracc.config import settings
from bracc.models.entity import ProvenanceBlock
from bracc.models.perfil import (
    ComparacaoCidada,
    DespesaGabinete,
    Emenda,
    PerfilPolitico,
    PoliticoResumo,
)
from bracc.services.alertas_service import (
    analisar_carreira_longa,
    analisar_despesas_gabinete,
    analisar_despesas_vs_media,
    analisar_picos_mensais,
    analisar_teto_gastos,
    analisar_variacao_patrimonial,
    calcular_red_flags_summary,
    gerar_alertas_completos,
)
from bracc.services.bens_service import obter_bens_declarados
from bracc.services.historico_eleitoral_service import (
    obter_historico_eleitoral,
)
from bracc.services.analise_service import (
    analisar_despesas_vs_cidadao,
    gerar_resumo_politico,
)
from bracc.services.common_helpers import archival_url
from bracc.services.conexoes_service import classificar
from bracc.services.contas_campanha_service import gerar_comparacao_contas
from bracc.services.despesas_service import (
    calcular_media_ceap_estado,
    obter_ceap_deputado,
    obter_ceaps_senador,
    obter_cota_vereador_goiania,
    obter_verba_indenizatoria_alego,
)
from bracc.services.emendas_service import obter_emendas_deputado
from bracc.services.formatacao_service import fmt_brl
from bracc.services.neo4j_service import execute_query_single
from bracc.services.teto_service import calcular_teto
from bracc.services.traducao_service import traduzir_cargo
from bracc.services.validacao_tse_service import gerar_validacao_tse

if TYPE_CHECKING:
    from neo4j import AsyncDriver

logger = logging.getLogger(__name__)

# Labels que qualificam o nó como "político" pra fins deste endpoint.
# ``Person`` / ``FederalLegislator`` / ``StateLegislator`` / ``GoVereador``
# cobrem o escopo GO atual (deputados federais, estaduais, vereadores de
# Goiânia, servidores públicos GO). ``Senator`` e ``CityCouncilor`` ficam
# aqui por precaução caso o pipeline venha a gerar esses labels (compatível
# com busca TSE ampla).
_POLITICIAN_LABELS = {
    "Person",
    "FederalLegislator",
    "StateLegislator",
    "Senator",
    "CityCouncilor",
    "GoVereador",
}

# Campos de proveniência que o unpacker espera no dict do nó focal.
_PROVENANCE_FIELDS = (
    "source_id",
    "source_record_id",
    "source_url",
    "ingested_at",
    "run_id",
    "source_snapshot_uri",
)

# Severidade pra ordenar alertas (espelha o Flask).
_SEVERIDADE = {"grave": 0, "atencao": 1, "info": 2, "ok": 3}

# Anos eleitorais carimbados pelo pipeline TSE em ``cargo_tse_<ano>``.
# Ordem reversa: detector de "ano eleitoral mais recente" pega o primeiro
# que tem cargo setado. 2026 entra preventivamente — quando o pipeline
# TSE 2026 rodar, o detector já passa a usar.
_ANOS_TSE_RECENTES: tuple[int, ...] = (2026, 2024, 2022, 2020, 2018)


def _detectar_ano_eleitoral_recente(props: dict[str, Any]) -> int:
    """Devolve o ano TSE mais recente com ``cargo_tse_<ano>`` carimbado.

    Default ``2022`` quando o nó não tem nenhum cargo TSE — preserva o
    comportamento pré-existente pra deputado federal/estadual/senador
    cujos dados vêm dos pipelines de casa (Câmara/ALEGO/Senado), não do
    TSE direto. O ano resultante é usado como ``ano_doacao`` no
    ``classificar`` — sem isso, vereadores 2024 (que tem cargo_tse_2024
    setado mas nenhum 2022) ficam com lista de doadores vazia porque o
    filtro ano=2022 hardcoded descartava as rels TSE 2024.
    """
    for ano in _ANOS_TSE_RECENTES:
        if props.get(f"cargo_tse_{ano}"):
            return ano
    return 2022

# Timeout explícito da query principal (override do default 15s do
# ``neo4j_service.execute_query_single``). 30s combina com grafos densos
# sem comprometer o UX do PWA.
_CONNECTIONS_TIMEOUT = 30.0


class EntityNotFoundError(Exception):
    """Entity_id não existe no grafo OU não tem label de político.

    Mapeado para ``HTTP 404`` no router.
    """


class DriverError(Exception):
    """Driver Neo4j levantou durante a leitura.

    Mapeado para ``HTTP 502`` no router.
    """


def _extract_provenance(props: dict[str, Any]) -> ProvenanceBlock | None:
    """Monta ``ProvenanceBlock`` a partir de ``props`` do nó focal.

    Remove os campos de proveniência do dict (mutação in-place) pra não
    poluir o resto do uso dos props. Retorna ``None`` se qualquer campo
    obrigatório está ausente — dados legados carimbados antes do
    contrato de proveniência.
    """
    popped: dict[str, Any] = {}
    for field in _PROVENANCE_FIELDS:
        popped[field] = props.pop(field, None)

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
        snapshot_url=archival_url(str(snapshot_uri) if snapshot_uri else None),
    )


def _is_politician(labels: list[Any] | None) -> bool:
    """Valida se o nó tem pelo menos 1 label aceitável como político."""
    if not labels:
        return False
    return any(
        isinstance(label, str) and label in _POLITICIAN_LABELS
        for label in labels
    )


def _build_politico_resumo(
    entity_id: str,
    props: dict[str, Any],
) -> PoliticoResumo:
    """Monta ``PoliticoResumo`` a partir dos props do nó focal.

    Equivalente às linhas 591-603 do Flask. Nota: o Flask devolve o
    ``cpf`` pleno no campo ``politico.cpf``; a compatibilidade com o PWA
    está preservada aqui. O middleware ``bracc.middleware.cpf_masking``
    é a linha de defesa final (mascara no response HTTP).
    """
    patrimonio_raw = props.get("patrimonio_declarado")
    patrimonio: float | None
    try:
        patrimonio = (
            float(patrimonio_raw) if patrimonio_raw is not None else None
        )
    except (TypeError, ValueError):
        patrimonio = None

    # Cargo + partido: prioriza ``role``/``cargo`` (Câmara/ALEGO/Senado);
    # cai pra ``cargo_tse_<ano>``/``partido_tse_<ano>`` quando o nó é
    # vereador TSE puro (Person sem cargo carimbado pelo pipeline da
    # casa). Sem esse fallback, o header do perfil de vereador 2024
    # aparece sem cargo nem partido mesmo com a info disponível.
    cargo_raw = props.get("role") or props.get("cargo")
    if not cargo_raw:
        for ano in _ANOS_TSE_RECENTES:
            valor = props.get(f"cargo_tse_{ano}")
            if valor:
                cargo_raw = valor
                break
    cargo_txt: str | None = None
    if isinstance(cargo_raw, str) and cargo_raw:
        cargo_txt = traduzir_cargo(cargo_raw)

    cpf_raw = props.get("cpf")
    partido_raw = props.get("partido")
    if not partido_raw:
        for ano in _ANOS_TSE_RECENTES:
            valor = props.get(f"partido_tse_{ano}")
            if valor:
                partido_raw = valor
                break
    uf_raw = props.get("uf")
    foto_raw = props.get("foto_url") or props.get("url_foto")

    return PoliticoResumo(
        id=entity_id,
        nome=str(props.get("name") or ""),
        cpf=str(cpf_raw) if cpf_raw else None,
        patrimonio=patrimonio,
        patrimonio_formatado=fmt_brl(patrimonio) if patrimonio else None,
        is_pep=bool(props.get("is_pep", False)),
        partido=str(partido_raw) if partido_raw else None,
        cargo=cargo_txt,
        uf=str(uf_raw) if uf_raw else None,
        foto_url=str(foto_raw) if foto_raw else None,
    )


def _adapt_connections(
    raw_conexoes: list[dict[str, Any]],
    politico_element_id: str,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Adapta shape da query pra shape que ``conexoes_service.classificar`` espera.

    Query devolve ``{rel_type, rel_props, source_id, target_id, target_element_id,
    target_type, target_labels, target_props}``. O service espera, pra cada
    conexão, ``{source_id, target_id, relationship_type, properties}`` + um
    mapa de entidades ``{element_id: {type, properties}}``.

    Retorna uma tupla ``(conexoes_normalizadas, entidades_conectadas)``.
    Conexões sem ``rel_type`` (OPTIONAL MATCH sem match) são descartadas.
    """
    conexoes_norm: list[dict[str, Any]] = []
    entidades: dict[str, dict[str, Any]] = {}

    for conn in raw_conexoes:
        rel_type = conn.get("rel_type")
        if not rel_type:
            # OPTIONAL MATCH sem match → tupla com todos os campos None.
            continue

        source_id = conn.get("source_id")
        target_id = conn.get("target_id")
        target_element_id = conn.get("target_element_id") or target_id

        if not isinstance(source_id, str) or not isinstance(target_id, str):
            continue

        conexoes_norm.append({
            "source_id": source_id,
            "target_id": target_id,
            "relationship_type": rel_type,
            "properties": conn.get("rel_props") or {},
        })

        # Dict de entidades conectadas — a "outra ponta" da aresta.
        if (
            isinstance(target_element_id, str)
            and target_element_id != politico_element_id
        ):
            target_type = conn.get("target_type") or ""
            entidades[target_element_id] = {
                "type": str(target_type),
                "properties": conn.get("target_props") or {},
            }

    return conexoes_norm, entidades


def _despesas_ceap_to_raw_dicts(
    despesas: list[DespesaGabinete],
) -> list[dict[str, Any]]:
    """Converte ``DespesaGabinete`` → shape esperado por ``analisar_*``.

    ``analisar_despesas_gabinete`` / ``analisar_picos_mensais`` /
    ``analisar_despesas_vs_cidadao`` esperam dicts com chaves
    ``tipoDespesa``, ``valorLiquido``, ``ano``, ``mes`` (shape da API da
    Câmara, preservado por compatibilidade). Como o service 04.C já
    agrega por tipo, **perdemos o breakdown ano/mês** — logo, o alerta
    de picos mensais fica inexequível quando os dados vêm do grafo
    agregado. Para picos e média precisamos de uma leitura mais fina —
    documentado como débito em :func:`obter_perfil`.
    """
    return [
        {
            "tipoDespesa": d.tipo,
            "valorLiquido": d.total,
            # Sem ano/mes aqui: o agregado por tipo já colapsa meses.
            # ``analisar_picos_mensais`` simplesmente devolve [] nesse caso.
            "ano": None,
            "mes": None,
        }
        for d in despesas
    ]


def _emendas_to_raw_dicts(emendas: list[Emenda]) -> list[dict[str, Any]]:
    """Converte ``Emenda`` (tipada) → dicts pra ``analisar_emendas``.

    O analisador espera ``value_paid``, ``value_committed``, ``municipality``,
    ``type`` — shape raw do Portal da Transparência preservado.
    """
    return [
        {
            "value_paid": e.valor_pago,
            "value_committed": e.valor_empenhado,
            "municipality": e.municipio or "",
            "type": e.tipo,
        }
        for e in emendas
    ]


def _build_entidade_for_alertas(
    props: dict[str, Any],
) -> dict[str, Any]:
    """Adapta props do nó focal pro shape que ``gerar_alertas_completos`` consome.

    O orquestrador espera ``{"properties": {...}}`` com ``patrimonio_declarado``,
    ``role``/``cargo`` — os mesmos campos que já vêm do grafo.
    """
    return {"properties": props}


def _build_descricao_conexoes(resultado: Any) -> str:
    """Monta a descrição textual leiga das conexões do(a) político(a).

    Por que existe:
        O PWA exibe um parágrafo explicativo em português natural pra
        preparar o leitor leigo antes de mostrar as listas de conexões.
        Essa função centraliza a geração desse parágrafo — antes estava
        no Flask (``backend/app.py`` l. 876-896) e era duplicado entre
        rotas. Mantemos o mesmo fraseado pra não mudar a expectativa do
        usuário que já conhece a UI.

    Parameters
    ----------
    resultado:
        :class:`~bracc.services.conexoes_service.ConexoesClassificadas`
        — estrutura com as listas ``doadores_empresa``, ``doadores_pessoa``,
        ``socios`` e ``familia`` já classificadas. Tipagem frouxa (``Any``)
        é deliberada: a função só acessa atributos e faz ``len()``, e
        tipar como ``ConexoesClassificadas`` criaria import circular
        ``perfil_service`` ↔ ``conexoes_service``.

    Returns
    -------
    str
        Texto em português natural, montado somando categorias separadas
        por ``; `` e finalizado com a nota explicativa sobre a origem
        (TSE/Receita Federal). Exemplo:

            "Encontramos: 3 empresa(s) que doaram para a campanha;
            2 familiar(es) com ligacao politica. Esses dados vem da
            Justica Eleitoral (TSE) e da Receita Federal — sao publicos.
            …"

        Retorna string vazia quando todas as listas relevantes estão
        vazias — o PWA interpreta como "política sem conexões a mostrar".
    """
    cats: list[str] = []
    if resultado.doadores_empresa:
        cats.append(
            f"{len(resultado.doadores_empresa)} empresa(s) que doaram para a campanha",
        )
    if resultado.doadores_pessoa:
        cats.append(
            f"{len(resultado.doadores_pessoa)} pessoa(s) que doaram para a campanha",
        )
    if resultado.socios:
        cats.append(
            f"{len(resultado.socios)} empresa(s) em que o(a) politico(a) "
            "aparece como socio(a)",
        )
    if resultado.familia:
        cats.append(
            f"{len(resultado.familia)} familiar(es) com ligacao politica",
        )
    if not cats:
        return ""
    return (
        "Encontramos: " + "; ".join(cats) + ". "
        "Esses dados vem da Justica Eleitoral (TSE) e da Receita Federal — "
        "sao publicos. Aparecer aqui nao quer dizer que tem algo errado; "
        "e so pra voce saber com quem o(a) politico(a) se relaciona."
    )


def _build_aviso_despesas(
    despesas_gabinete: list[DespesaGabinete],
    *,
    is_deputado_federal: bool,
    is_estadual_go: bool,
    is_vereador_goiania: bool,
    is_senador_federal: bool,
    is_vereador_tse_outro_municipio: bool = False,
    municipio_vereador: str = "",
) -> str:
    """Aviso explicativo da fonte de despesas de gabinete.

    Cinco casos cobertos — o PWA renderiza o texto como legenda da secao:

    * Deputado federal com CEAP ingerido → fonte curta "cota CEAP".
    * Senador federal com CEAPS ingerida → fonte "cota CEAPS" do Senado.
    * Deputado estadual GO com verba ALEGO ingerida → fonte "verba ALEGO".
    * Vereador(a) da Camara Municipal de Goiania (CMG) → cota municipal
      do portal ``goiania.go.leg.br``.
    * Qualquer outro caso (outros municipios, sem dados, etc.) →
      mensagem curta "Dados de gastos parlamentares nao disponiveis".

    Quando ``despesas_gabinete`` esta vazio mas o politico tem label
    conhecida (federal/estadual GO/vereador GYN/senador), exibimos ainda
    a fonte esperada **e** um complemento que explicita a ausencia de
    lancamentos no periodo. Sem o complemento, o texto da fonte sozinho
    passa impressao de erro quando na verdade o orgao de transparencia
    nao publicou despesa pro deputado (caso documentado 2026-04-24: 10
    dos 42 StateLegislators GO nao aparecem no feed ALEGO mesmo com
    janela de 12 meses — suplentes/licenciados/sem verba).
    """
    descricao_fonte = ""
    if is_deputado_federal:
        descricao_fonte = (
            "Cota de atividade parlamentar da Camara Federal (CEAP) — "
            "inclui gastos de gabinete, telefone, combustivel e aluguel "
            "de escritorio."
        )
    elif is_senador_federal:
        descricao_fonte = (
            "Cota para o Exercicio da Atividade Parlamentar dos Senadores "
            "(CEAPS) — ressarcimento de despesas de atividade legislativa "
            "federal (passagens, telefonia, aluguel de imoveis, divulgacao, "
            "combustivel). Regulada pelo Ato da Comissao Diretora no 3/2016."
        )
    elif is_estadual_go:
        descricao_fonte = (
            "Verba indenizatoria da Assembleia Legislativa de Goias "
            "(ALEGO) — ressarcimento de despesas de atividade parlamentar."
        )
    elif is_vereador_goiania:
        descricao_fonte = (
            "Cota parlamentar da Camara Municipal de Goiania (CMG) — "
            "despesas de gabinete publicadas no portal de transparencia "
            "municipal (goiania.go.leg.br)."
        )
    elif is_vereador_tse_outro_municipio:
        # Vereador TSE 2024 fora de Goiania: nao temos pipeline de cota
        # municipal pras outras 245 cidades de GO. Mensagem explica em
        # vez de devolver tela vazia.
        municipio_txt = (
            f" de {municipio_vereador.title()}" if municipio_vereador else ""
        )
        return (
            f"Cota parlamentar da Camara Municipal{municipio_txt} ainda "
            "nao integrada — hoje so a Camara Municipal de Goiania publica "
            "no formato que conseguimos ler. As demais 245 cidades de GO "
            "ficam sem dados de despesa de gabinete por enquanto."
        )

    if descricao_fonte and not despesas_gabinete:
        # Label conhecida + lista vazia = orgao de transparencia nao
        # publicou lancamentos pro deputado no periodo. Nao e bug do
        # app — o feed simplesmente nao tem linhas pra ele.
        return (
            f"{descricao_fonte} Sem lancamentos publicados pra esse(a) "
            "parlamentar no periodo consultado — possiveis razoes: "
            "suplente sem mandato exercido, renuncia a verba ou licenca."
        )
    if descricao_fonte:
        return descricao_fonte
    if despesas_gabinete:
        # Fallback improvavel: tem despesas mas nenhum label conhecido.
        return ""
    return "Dados de gastos parlamentares nao disponiveis pra essa casa legislativa."


async def obter_perfil(
    driver: AsyncDriver,
    entity_id: str,
    *,
    limit_conexoes: int = 50,
    anos_ceap: list[int] | None = None,
) -> PerfilPolitico:
    """Orquestra montagem completa do ``PerfilPolitico`` a partir do grafo.

    Parameters
    ----------
    driver:
        Neo4j async driver (o caller abre sessões conforme precisar).
    entity_id:
        ``elementId`` do político focal. Aceita também ``id_camara`` e
        ``legislator_id`` pra compatibilidade com o PWA atual.
    limit_conexoes:
        Cap no tamanho de cada lista de conexões classificadas. Default
        50 (compatível com Flask).
    anos_ceap:
        Anos a considerar pra CEAP. Default: últimos 2 (ver
        :func:`despesas_service._default_anos`).

    Raises
    ------
    EntityNotFoundError
        ``entity_id`` não existe ou o nó não tem label de político.
    DriverError
        O driver Neo4j levantou qualquer exceção durante a leitura.
    """
    # --- 1. Leitura do nó focal + connections --------------------------------
    try:
        async with driver.session(database=settings.neo4j_database) as session:
            record = await execute_query_single(
                session,
                "perfil_politico_connections",
                {"entity_id": entity_id},
                timeout=_CONNECTIONS_TIMEOUT,
            )
    except Exception as exc:  # noqa: BLE001 — traduz p/ erro de domínio
        raise DriverError(str(exc)) from exc

    if record is None:
        raise EntityNotFoundError(f"Politico '{entity_id}' nao encontrado")

    # ``record.get`` do driver Neo4j devolve ``Any`` por design — não dá
    # pra estreitar sem cast explícito. A conversão segura acontece
    # logo abaixo via ``dict(...)`` + ``_is_politician`` que valida
    # labels. Mantemos ``Any`` local pra deixar claro que a origem é
    # o payload não-tipado do driver.
    politico_node_raw: Any = record.get("politico")
    if not politico_node_raw:
        raise EntityNotFoundError(f"Politico '{entity_id}' nao encontrado")

    # O Cypher devolve o nó como ``p {.*, element_id, labels}`` — já vem dict.
    politico_node: dict[str, Any] = dict(politico_node_raw)
    labels_raw = politico_node.pop("labels", None)
    politico_element_id_raw = politico_node.pop("element_id", None)
    politico_element_id = (
        str(politico_element_id_raw) if politico_element_id_raw else entity_id
    )

    if not _is_politician(
        labels_raw if isinstance(labels_raw, list) else None,
    ):
        raise EntityNotFoundError(
            f"Entity '{entity_id}' existe mas nao e um politico",
        )

    # props são os atributos do nó focal (com proveniência dentro).
    # ``_extract_provenance`` faz pop dos campos de provenance → o resto
    # é usado pra montar o PoliticoResumo e ValidacaoTSE.
    props: dict[str, Any] = dict(politico_node)
    provenance = _extract_provenance(props)

    politico = _build_politico_resumo(politico_element_id, props)

    # Props mergeadas (irmaos do cluster + focal) — usadas pelos services
    # TSE (validacao, contas_campanha, teto) e alertas, que dependem de
    # campos como ``total_tse_<ano>``, ``total_despesas_tse_<ano>`` e
    # ``cargo_tse_<ano>``. Esses campos vivem no ``:Person`` do TSE, mas
    # o focal pode ser ``:StateLegislator``/``:FederalLegislator``/
    # ``:Senator`` sem eles. Focal sobrescreve no merge — preserva nome
    # de gabinete, foto e party do nó oficial. Cabeçalho (``politico``,
    # ``id_camara``, ``legislator_id`` etc.) continua usando ``props``
    # do focal pra não vazar identidade de irmão.
    sibling_props_raw: Any = record.get("sibling_props") or {}
    sibling_props: dict[str, Any] = (
        dict(sibling_props_raw) if isinstance(sibling_props_raw, dict) else {}
    )
    merged_props: dict[str, Any] = {**sibling_props, **props}

    # --- 2. Classificar conexões ---------------------------------------------
    raw_conexoes: list[dict[str, Any]] = list(record.get("conexoes") or [])
    conexoes_norm, entidades_conectadas = _adapt_connections(
        raw_conexoes, politico_element_id,
    )
    # Ano da eleição usado pra filtrar doações: pega o cargo_tse_<ano>
    # mais recente do nó. Pra deputado federal/estadual/senador (cujos
    # dados vêm de pipeline de casa, não do TSE), o detector cai no
    # default 2022 — preserva exatamente o comportamento anterior.
    # Pra vereador TSE 2024 (Person sem cargo_tse_2022), o detector
    # promove pra 2024 e destrava as 200+ doações que ficavam invisíveis
    # com o filtro hardcoded.
    ano_eleicao_doacao = _detectar_ano_eleitoral_recente(merged_props)
    resultado = classificar(
        conexoes_norm,
        entidades_conectadas,
        politico_element_id,
        limit_por_categoria=limit_conexoes,
        ano_doacao=ano_eleicao_doacao,
    )

    # --- 3. Paralelo: despesas_gabinete + emendas ----------------------------
    # Roteamento por tipo de politico (3 niveis de "quanto gasta com politica"):
    #   * FederalLegislator com id_camara  → CEAP Camara + emendas RP-06/09
    #   * StateLegislator GO               → verba indenizatoria ALEGO
    #                                        (sem emendas federais, obvio)
    #   * GoVereador (municipio Goiania)   → cota da Camara Municipal de Goiania
    #                                        (sem emendas; vereador nao tem)
    #   * qualquer outro                    → despesas vazias
    id_camara_raw = props.get("id_camara")
    id_camara: int | None = None
    if id_camara_raw is not None:
        try:
            id_camara = int(str(id_camara_raw))
        except (TypeError, ValueError):
            id_camara = None

    is_deputado_federal = bool(
        labels_raw and "FederalLegislator" in labels_raw,
    )
    is_senador_federal = bool(
        labels_raw and "Senator" in labels_raw,
    )
    uf_props_raw = props.get("uf")
    uf_props = (
        str(uf_props_raw).upper() if isinstance(uf_props_raw, str) else ""
    )
    is_estadual_go = bool(
        labels_raw
        and "StateLegislator" in labels_raw
        and uf_props == "GO",
    )
    # Vereador GYN: label ``:GoVereador`` + municipality == "Goiania".
    # Guard duplo pra garantir escopo municipal-capital-only (o pipeline
    # so ingere Goiania hoje, mas o guard evita regressao se expandirmos
    # pra outros municipios no futuro sem querer).
    municipality_raw = props.get("municipality")
    municipality = (
        str(municipality_raw).strip() if isinstance(municipality_raw, str) else ""
    )
    is_vereador_goiania = bool(
        labels_raw
        and "GoVereador" in labels_raw
        and municipality.lower() == "goiania",
    )
    # Vereador TSE puro (Person + cargo_tse_<ano>='Vereador', sem label
    # de cargo). Cai aqui o vereador 2024/2020 de qualquer municipio GO
    # ingerido pelo pipeline TSE. Sem esse branch, o roteamento de despesa
    # silenciosamente devolve [] sem aviso, e o usuario nao entende por
    # que o perfil esta vazio. _build_aviso_despesas usa o flag pra
    # explicar que so Goiania tem feed integrado de cota municipal.
    cargo_tse_recente = (
        str(merged_props.get(f"cargo_tse_{ano_eleicao_doacao}") or "").upper()
    )
    is_vereador_tse_outro_municipio = bool(
        not is_vereador_goiania
        and "VEREADOR" in cargo_tse_recente,
    )

    despesas_gabinete: list[DespesaGabinete] = []
    emendas_grafo: list[Emenda] = []

    if is_deputado_federal and id_camara is not None:
        try:
            despesas_gabinete, emendas_grafo = await asyncio.gather(
                obter_ceap_deputado(driver, id_camara, anos_ceap),
                obter_emendas_deputado(driver, id_camara),
            )
        except Exception as exc:  # noqa: BLE001
            raise DriverError(str(exc)) from exc
    elif is_senador_federal:
        # ``id_senado`` e a chave que o pipeline ``senado_senadores_foto``
        # grava no no :Senator. Bridge Senator -> Person (por nome) ->
        # GASTOU -> Expense (source='senado') via ``perfil_ceaps_senador``.
        id_senado_raw = props.get("id_senado")
        if id_senado_raw:
            try:
                despesas_gabinete = await obter_ceaps_senador(
                    driver, str(id_senado_raw), anos_ceap,
                )
            except Exception as exc:  # noqa: BLE001
                raise DriverError(str(exc)) from exc
    elif is_estadual_go:
        # ``legislator_id`` e o hash estavel que o pipeline ``alego`` grava
        # no no ``:StateLegislator`` e usa como chave do rel GASTOU_COTA_GO.
        legislator_id_raw = props.get("legislator_id")
        if legislator_id_raw:
            try:
                despesas_gabinete = await obter_verba_indenizatoria_alego(
                    driver, str(legislator_id_raw), anos_ceap,
                )
            except Exception as exc:  # noqa: BLE001
                raise DriverError(str(exc)) from exc
    elif is_vereador_goiania:
        # ``vereador_id`` e o hash estavel que o pipeline ``camara_goiania``
        # grava no no ``:GoVereador`` e usa como chave do rel DESPESA_GABINETE.
        vereador_id_raw = props.get("vereador_id")
        if vereador_id_raw:
            try:
                despesas_gabinete = await obter_cota_vereador_goiania(
                    driver, str(vereador_id_raw), anos_ceap,
                )
            except Exception as exc:  # noqa: BLE001
                raise DriverError(str(exc)) from exc

    # Bens declarados (TSE) — query separada com cluster-walk pra cobrir
    # deputado federal/senador cujo Person sibling carrega DECLAROU_BEM.
    # Roda em paralelo logico com tudo acima — chama em try/except pra
    # nao bloquear o perfil quando a query falha (ex: timeout).
    try:
        bens_declarados = await obter_bens_declarados(driver, entity_id)
    except Exception:  # noqa: BLE001 — degradacao silenciosa
        bens_declarados = None

    # Historico eleitoral (TSE :Election + :CANDIDATO_EM cluster-walk).
    # Mesmo padrao de fallback silencioso — sem candidaturas conhecidas
    # o card simplesmente nao aparece no PWA.
    try:
        carreira_politica = await obter_historico_eleitoral(driver, entity_id)
    except Exception:  # noqa: BLE001 — degradacao silenciosa
        carreira_politica = None

    # Fonte de emendas: mescla as duas fontes disponíveis por id. A query
    # dedicada ``perfil_emendas_deputado`` enriquece com
    # ``beneficiario_cnpj``/``beneficiario_nome`` via OPTIONAL MATCH em
    # ``:BENEFICIOU`` — dado essencial pro cross-check
    # doador↔beneficiário. ``conexoes_service`` pega qualquer edge
    # depth-1 (``PROPOS``/``AUTOR_EMENDA``) mas não carrega o
    # beneficiário. Por isso preferimos a versão enriquecida quando o id
    # aparece nas duas; caímos na classificação pro resto (edges que a
    # dedicated query não cobre).
    emendas: list[Emenda]
    if resultado.emendas or emendas_grafo:
        por_id: dict[str, Emenda] = {e.id: e for e in resultado.emendas}
        for e in emendas_grafo:
            por_id[e.id] = e  # Sobrescreve quando dedicated query cobre.
        emendas = list(por_id.values())
        fonte_emendas: str | None = "bracc"
    else:
        emendas = []
        fonte_emendas = None

    # --- 4. Totais (doações, emendas, despesas) ------------------------------
    total_doacoes = sum(d.valor_total for d in resultado.doadores_empresa) + sum(
        d.valor_total for d in resultado.doadores_pessoa
    )
    total_emendas_valor = sum(
        e.valor_pago or e.valor_empenhado for e in emendas
    )
    total_despesas_gabinete = sum(d.total for d in despesas_gabinete)

    # --- 5. Comparações, resumo, validação TSE -------------------------------
    despesas_raw = _despesas_ceap_to_raw_dicts(despesas_gabinete)
    comparacoes_cidada: list[ComparacaoCidada] = []
    comparacao_cidada_resumo = ""
    comparacao_alertas: list[dict[str, str]] = []
    if despesas_raw:
        resultado_cidadao = analisar_despesas_vs_cidadao(despesas_raw)
        comparacao_cidada_resumo = str(resultado_cidadao.get("resumo") or "")
        comparacao_alertas = list(resultado_cidadao.get("alertas") or [])
        for comp in resultado_cidadao.get("comparacoes") or []:
            comparacoes_cidada.append(
                ComparacaoCidada(
                    categoria=comp["categoria"],
                    total_politico_fmt=comp["total_politico_fmt"],
                    media_mensal_politico_fmt=comp["media_mensal_politico_fmt"],
                    referencia_cidadao_fmt=comp["referencia_cidadao_fmt"],
                    razao=comp.get("razao"),
                    razao_texto=comp["razao_texto"],
                    classificacao=comp["classificacao"],
                ),
            )

    cargo_raw = props.get("role") or props.get("cargo")
    resumo = gerar_resumo_politico(
        nome=politico.nome,
        cargo=str(cargo_raw) if isinstance(cargo_raw, str) else None,
        patrimonio=politico.patrimonio,
        num_emendas=len(emendas),
        total_emendas=total_emendas_valor,
        num_doadores_empresa=len(resultado.doadores_empresa),
        num_doadores_pessoa=len(resultado.doadores_pessoa),
        num_socios=len(resultado.socios),
        num_familia=len(resultado.familia),
    )

    validacao_tse = gerar_validacao_tse(merged_props, total_doacoes)
    contas_campanha = gerar_comparacao_contas(merged_props, 2022)

    # Teto de gastos de campanha vs despesas declaradas (Resolução TSE
    # 23.607/2019 — MVP só cobre eleição 2022). ``total_despesas_tse_2022``
    # e ``cargo_tse_2022`` vêm do pipeline ``tse_prestacao_contas_go``.
    # Sem pipeline rodado em prod → props ausentes → calcular_teto retorna
    # None (degradação silenciosa, seção omitida no PWA).
    # TODO: parametrizar o ano quando adicionarmos ``TETOS_2026``.
    teto_ano = 2022
    total_despesas_tse_raw = merged_props.get(f"total_despesas_tse_{teto_ano}") or 0.0
    try:
        total_despesas_tse = float(total_despesas_tse_raw)
    except (TypeError, ValueError):
        total_despesas_tse = 0.0
    cargo_tse = (
        merged_props.get(f"cargo_tse_{teto_ano}")
        or props.get("role")
        or props.get("cargo")
    )
    teto_gastos = calcular_teto(
        cargo=str(cargo_tse) if cargo_tse else None,
        uf=politico.uf,
        ano_eleicao=teto_ano,
        total_despesas_declaradas=total_despesas_tse,
    )

    # --- 6. Alertas (orquestração completa) ----------------------------------
    entidade_para_alertas = _build_entidade_for_alertas(merged_props)
    emendas_raw_alertas = _emendas_to_raw_dicts(emendas)
    alertas = gerar_alertas_completos(
        entidade_para_alertas,
        conexoes_norm,
        entidades_conectadas,
        emendas_raw_alertas,
        perfil=resultado,  # Duck-typed: ConexoesClassificadas tem
                            # .doadores_empresa + .socios com campo
                            # ``situacao`` já propagado pelo grafo.
        emendas_tipadas=emendas,  # Typed list: permite cross-check
                            # doador↔beneficiário sem perder beneficiario_cnpj
                            # (que _emendas_to_raw_dicts dropa).
        politico_uf=politico.uf,  # UF da base pro flag de emendas fora
                            # do estado.
    )

    # Alerta sobre teto de gastos (grave se ultrapassou, info/atenção
    # nos buckets inferiores). Sem ``teto_gastos`` → lista vazia.
    alertas.extend(analisar_teto_gastos(teto_gastos))

    # Alerta de variacao patrimonial >100% entre eleicoes consecutivas.
    # Vereador que vai de R$50k em 2020 para R$5M em 2024 levanta
    # bandeira (atencao); >300% vira grave. Sem bens_declarados ou com
    # 1 ano so, nao gera nada.
    if bens_declarados is not None:
        alertas.extend(analisar_variacao_patrimonial(bens_declarados))

    # Alerta de carreira eleitoral longa (proxy de "casta politica" / coronel
    # com decadas de presenca em urna). Sem carreira_politica -> lista vazia.
    if carreira_politica is not None:
        alertas.extend(analisar_carreira_longa(carreira_politica))

    uf_deputado = politico.uf
    if despesas_raw:
        alertas.extend(analisar_despesas_gabinete(despesas_raw, uf_deputado))
        alertas.extend(analisar_picos_mensais(despesas_raw))
        alertas.extend(comparacao_alertas)

        if uf_deputado and total_despesas_gabinete > 0 and is_deputado_federal:
            try:
                media = await calcular_media_ceap_estado(driver, uf_deputado)
            except Exception:  # noqa: BLE001 — não bloqueia o perfil
                media = 0.0
            alerta_media = analisar_despesas_vs_media(
                total_despesas_gabinete, media, uf_deputado,
            )
            if alerta_media:
                alertas.append(alerta_media)

    # Remove alerta "ok" genérico quando já temos alertas reais.
    if len(alertas) > 1:
        alertas = [a for a in alertas if a.get("tipo") != "ok"]

    # Ordena por severidade (grave > atencao > info > ok).
    alertas.sort(key=lambda a: _SEVERIDADE.get(a.get("tipo", "info"), 2))

    # Remove alerta genérico "Avaliação indisponível" quando temos dados.
    tem_dados = bool(
        emendas
        or resultado.doadores_empresa
        or resultado.doadores_pessoa
        or resultado.socios
        or resultado.familia
        or resultado.empresas
        or resultado.contratos
        or despesas_gabinete,
    )
    if tem_dados:
        alertas = [
            a for a in alertas
            if "Avaliação indisponível" not in a.get("texto", "")
        ]

    # --- 7. Descrição de conexões + aviso de despesas ------------------------
    descricao_conexoes = _build_descricao_conexoes(resultado)
    municipio_vereador_props = (
        str(merged_props.get(f"municipio_tse_{ano_eleicao_doacao}") or "").strip()
    )
    aviso_despesas = _build_aviso_despesas(
        despesas_gabinete,
        is_deputado_federal=is_deputado_federal,
        is_estadual_go=is_estadual_go,
        is_vereador_goiania=is_vereador_goiania,
        is_senador_federal=is_senador_federal,
        is_vereador_tse_outro_municipio=is_vereador_tse_outro_municipio,
        municipio_vereador=municipio_vereador_props,
    )

    # Score consolidado de red flags (pedagógico pro PWA — agrega os
    # alertas em um único indicador baixo/medio/alto/critico).
    red_flags_summary = calcular_red_flags_summary(alertas)

    # --- 8. Monta o PerfilPolitico final -------------------------------------
    return PerfilPolitico(
        provenance=provenance,
        politico=politico,
        resumo=resumo,
        emendas=emendas,
        total_emendas_valor=total_emendas_valor,
        total_emendas_valor_fmt=fmt_brl(total_emendas_valor),
        empresas=resultado.empresas,
        contratos=resultado.contratos,
        despesas_gabinete=despesas_gabinete,
        total_despesas_gabinete=total_despesas_gabinete,
        total_despesas_gabinete_fmt=fmt_brl(total_despesas_gabinete),
        comparacao_cidada=comparacoes_cidada,
        comparacao_cidada_resumo=comparacao_cidada_resumo,
        alertas=alertas,
        fonte_emendas=fonte_emendas,
        descricao_conexoes=descricao_conexoes,
        doadores_empresa=resultado.doadores_empresa,
        doadores_pessoa=resultado.doadores_pessoa,
        total_doacoes=total_doacoes,
        total_doacoes_fmt=fmt_brl(total_doacoes),
        socios=resultado.socios,
        familia=resultado.familia,
        aviso_despesas=aviso_despesas,
        validacao_tse=validacao_tse,
        contas_campanha=contas_campanha,
        teto_gastos=teto_gastos,
        red_flags_summary=red_flags_summary,
        bens_declarados=bens_declarados,
        carreira_politica=carreira_politica,
    )
