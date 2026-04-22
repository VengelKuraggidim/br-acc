"""Classificação de conexões BRACC em 7 categorias (Fase 04.B).

Portado do Flask (``backend/app.py`` linhas 559-710) como parte da
consolidação FastAPI. Toma o shape de conexões retornado pela query
Cypher ``perfil_politico_connections.cypher`` e separa em:

- ``emendas`` — ``Amendment`` alvos (traduz função/tipo).
- ``doadores_empresa`` — agregação por CNPJ das relações ``DOOU`` com
  ``Company`` na outra ponta.
- ``doadores_pessoa`` — agregação por CPF das relações ``DOOU`` com
  ``Person`` na outra ponta. **CPF pleno nunca sai daqui** — passa por
  ``mascarar_cpf`` antes de montar o ``DoadorPessoa``.
- ``socios`` — ``SOCIO_DE`` onde a outra ponta é ``Company``.
- ``familia`` — ``CONJUGE_DE``/``PARENTE_DE`` com CPF mascarado.
- ``contratos`` — ``target_type`` em ``{contract, go_procurement}``.
- ``empresas`` — qualquer outra relação com empresa (fallback informativo,
  traduz ``rel_type`` via ``traduzir_relacao``).

Convenção: módulo com funções puras (segue o padrão dos services da 04.A:
``formatacao_service``, ``traducao_service``, ``analise_service``). Zero
I/O — Neo4j fica no router/fase 04.F.

Shape esperado em ``conexoes_raw`` (um dict por conexão)::

    {
        "source_id": str,              # elementId do source
        "target_id": str,              # elementId do target
        "relationship_type": str,      # rel_type (SOCIO_DE, DOOU, ...)
        "properties": dict,            # properties da aresta (ex: valor)
    }

Shape esperado em ``entidades_conectadas``: ``{element_id: {"type": str,
"properties": dict}}`` onde ``type`` é label em lowercase (``company``,
``person``, ``amendment``, ``contract``, ``go_procurement``, ...).

Constraints LGPD (aplicadas pelo service, testadas explicitamente):

- Nenhuma das 7 categorias publica o CPF pleno. Só o formato
  mascarado (``***.***.***-XX``) do :func:`mascarar_cpf` do
  ``FormatacaoService``.
- Empresas sem CNPJ NÃO duplicam por engano: a chave de agregação cai
  para ``element_id`` do nó alvo (prefixada com ``empresa_``).
- ``DoadorPessoa.provenance.source_record_id`` é sempre ``None`` — no TSE
  o record_id normalmente carrega o CPF pleno do doador, e surfar isso
  no chip de fonte vazaria o dado que já mascaramos em ``cpf_mascarado``.

Proveniência (Fase 05, chip de Fonte nos 7 sub-cards):

- Todas as 7 categorias do :class:`ConexoesClassificadas` carregam o
  campo opcional ``provenance: ProvenanceBlock | None``
  (``Emenda``/``DoadorEmpresa``/``DoadorPessoa``/``SocioConectado``/
  ``FamiliarConectado``/``ContratoConectado``/``EmpresaConectada`` —
  essa última ainda pendente). Construído via
  :func:`_provenance_from_props` a partir dos 5+1 campos carimbados pelo
  loader em ``attach_provenance``.
- ``DoadorEmpresa`` / ``DoadorPessoa`` são agregados: o bloco publicado é
  o da **doação mais recente por ``ingested_at``** (ISO 8601 →
  ordenação lexicográfica equivale à cronológica). Quando nenhuma
  doação agregada trouxe os 4 campos obrigatórios, ``provenance`` é
  ``None``.
- ``FamiliarConectado`` usa ``drop_record_id=True`` (analogo a
  ``DoadorPessoa``): record_id do nó :Person pode ser o CPF pleno.
- ``SocioConectado`` e ``ContratoConectado`` usam ``drop_record_id=False``
  — CNPJ e ID de contrato/licitação são dados públicos.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bracc.models.entity import ProvenanceBlock
from bracc.models.perfil import (
    ContratoConectado,
    DoacaoItem,
    DoadorEmpresa,
    DoadorPessoa,
    Emenda,
    EmpresaConectada,
    FamiliarConectado,
    SocioConectado,
)
from bracc.services.common_helpers import archival_url, as_float, as_str, norm_type
from bracc.services.formatacao_service import fmt_brl, fmt_data_br, mascarar_cpf
from bracc.services.traducao_service import (
    traduzir_funcao_emenda,
    traduzir_relacao,
    traduzir_tipo_emenda,
)

_PROV_REQUIRED = ("source_id", "source_url", "ingested_at", "run_id")


def _provenance_from_props(
    props: dict[str, Any],
    *,
    drop_record_id: bool = False,
) -> ProvenanceBlock | None:
    """Constrói ``ProvenanceBlock`` a partir dos props de um nó do grafo.

    Retorna ``None`` quando qualquer um dos 4 campos obrigatórios
    (``source_id``, ``source_url``, ``ingested_at``, ``run_id``) não está
    presente ou está vazio — comportamento consistente com
    :func:`bracc.routers.entity._extract_provenance`. Nós legados que ainda
    não foram re-ingeridos sob o contrato de proveniência silenciosamente
    devolvem ``None`` em vez de levantar.

    Parameters
    ----------
    props:
        Props do nó alvo (``target_props`` do shape normalizado). **Não é
        mutado** — :func:`classificar` precisa dos mesmos props pra
        extrair campos vizinhos (cnpj, situacao, etc.). Contrapartida do
        ``pop`` usado no router entity.
    drop_record_id:
        Quando ``True``, ``source_record_id`` é forçado a ``None``. Usado
        em ``DoadorPessoa`` porque no TSE o record_id do doador
        costumeiramente carrega o CPF pleno — surfar isso no chip de
        fonte violaria a máscara que o service já aplica no
        ``cpf_mascarado``.

    Returns
    -------
    ProvenanceBlock | None
        Bloco populado (com ``snapshot_url`` opt-in, já prefixado via
        :func:`archival_url` quando relativo) ou ``None`` pra dados
        legados.
    """
    for required in _PROV_REQUIRED:
        if not props.get(required):
            return None
    record_id_raw = props.get("source_record_id")
    snapshot_raw = props.get("source_snapshot_uri")
    return ProvenanceBlock(
        source_id=str(props["source_id"]),
        source_record_id=(
            None
            if drop_record_id or not record_id_raw
            else str(record_id_raw)
        ),
        source_url=str(props["source_url"]),
        ingested_at=str(props["ingested_at"]),
        run_id=str(props["run_id"]),
        snapshot_url=archival_url(str(snapshot_raw) if snapshot_raw else None),
    )


def _provenance_with_ingested(
    rel_props: dict[str, Any],
    target_props: dict[str, Any],
    *,
    drop_record_id: bool = False,
) -> tuple[ProvenanceBlock | None, str | None]:
    """Escolhe a proveniência da rel (preferencial) ou do nó (fallback).

    Pipelines como ``tse_prestacao_contas_go`` carimbam proveniência na
    relação ``:DOOU`` — o record_id aponta pro registro único da doação,
    e a ``ingested_at`` reflete a ingestão daquela aresta específica.
    Ler só do nó target (``:Person`` / ``:Company``) colapsa todas as
    doações do doador num único registro de proveniência: só a primeira
    ou a última vista leva o chip.

    Regra: se ``rel_props`` traz os 4 campos obrigatórios, é a fonte
    preferida (mais específica). Caso contrário cai pra ``target_props``.
    O timestamp ``ingested_at`` retornado vem sempre da **mesma fonte**
    que o bloco — misturar (bloco da rel + timestamp do nó) quebraria a
    agregação por "doação mais recente" em ``classificar``.

    Parameters
    ----------
    rel_props:
        Props da aresta (``conn["properties"]``).
    target_props:
        Props do nó alvo.
    drop_record_id:
        Forwarded para :func:`_provenance_from_props` — LGPD pra PF.

    Returns
    -------
    tuple[ProvenanceBlock | None, str | None]
        ``(bloco, ingested_at)`` — ``(None, None)`` se nenhum dos dois
        lados tem os 4 campos obrigatórios.
    """
    rel_block = _provenance_from_props(rel_props, drop_record_id=drop_record_id)
    if rel_block is not None:
        return rel_block, as_str(rel_props, "ingested_at")
    node_block = _provenance_from_props(
        target_props,
        drop_record_id=drop_record_id,
    )
    if node_block is not None:
        return node_block, as_str(target_props, "ingested_at")
    return None, None


@dataclass
class ConexoesClassificadas:
    """Resultado da classificação: 7 listas tipadas, sem overlap."""

    emendas: list[Emenda] = field(default_factory=list)
    doadores_empresa: list[DoadorEmpresa] = field(default_factory=list)
    doadores_pessoa: list[DoadorPessoa] = field(default_factory=list)
    socios: list[SocioConectado] = field(default_factory=list)
    familia: list[FamiliarConectado] = field(default_factory=list)
    contratos: list[ContratoConectado] = field(default_factory=list)
    empresas: list[EmpresaConectada] = field(default_factory=list)


@dataclass
class _DoacaoEmpresaAcc:
    """Acumulador interno de doações por empresa (antes de virar ``DoadorEmpresa``).

    ``situacao_*`` replicam o ultimo valor visto nos ``target_props`` da
    empresa — se varias arestas DOOU apontam pro mesmo :Company, todas
    veem a mesma situacao cadastral (a do no), entao preservar o primeiro
    e suficiente.

    ``provenance`` + ``provenance_ingested_at`` guardam a proveniência da
    doação **mais recente por ``ingested_at``** vista até agora — o
    agregado publicado vai carregar a última ingestão disponível do TSE
    (razoável porque é o mesmo doador com doações que podem ter vindo de
    batches diferentes).

    ``data_primeira_iso``/``data_ultima_iso`` acompanham o min/max do
    ``donated_at`` (ISO YYYY-MM-DD) carimbado pelos pipelines TSE nas rels
    ``:DOOU``. Comparação lexicográfica ISO equivale a cronológica.
    ``None`` quando nenhuma das rels agregadas trouxe ``donated_at``
    (legado pré-DT_RECEITA).
    """

    nome: str
    cnpj: str | None
    total: float = 0.0
    n: int = 0
    situacao: str | None = None
    situacao_verified_at: str | None = None
    provenance: ProvenanceBlock | None = None
    provenance_ingested_at: str | None = None
    data_primeira_iso: str | None = None
    data_ultima_iso: str | None = None
    doacoes: list[DoacaoItem] = field(default_factory=list)
    # Classificacao deterministica vinda do grafo (todo 07 Fase 1).
    # Quando ``tipo_entidade='comite_campanha'`` o PWA classifica sem cair
    # no regex do nome — cobre comites com razao social atipica (ex.:
    # MAGDA MOFATTO, HUMBERTO TEOFILO).
    tipo_entidade: str | None = None
    cnae_principal: str | None = None


@dataclass
class _DoacaoPessoaAcc:
    """Acumulador interno de doações por pessoa (CPF já mascarado).

    Mesma regra de agregação de proveniência de
    :class:`_DoacaoEmpresaAcc`: mantém a da doação mais recente.
    """

    nome: str
    cpf_mascarado: str | None
    total: float = 0.0
    n: int = 0
    provenance: ProvenanceBlock | None = None
    provenance_ingested_at: str | None = None
    data_primeira_iso: str | None = None
    data_ultima_iso: str | None = None
    doacoes: list[DoacaoItem] = field(default_factory=list)


def _valor_doacao(rel_props: dict[str, Any] | None) -> float:
    """Extrai o valor da doação da aresta. Prefere ``valor``, cai pra ``amount``."""
    if not rel_props:
        return 0.0
    raw = rel_props.get("valor")
    if raw is None:
        raw = rel_props.get("amount")
    return as_float(raw)


def _donated_at_iso(rel_props: dict[str, Any]) -> str | None:
    """Extrai ``donated_at`` da rel :DOOU. ``None`` quando vazio/ausente.

    Carimbado pelos pipelines TSE com ``parse_date`` → ISO ``YYYY-MM-DD``
    ou string vazia. Rels legadas (pré-DT_RECEITA) não têm o campo — o
    service aceita silenciosamente e o agregado fica ``None``.
    """
    raw = as_str(rel_props, "donated_at")
    return raw if raw else None


def _nome_empresa(props: dict[str, Any]) -> str:
    """Nome preferido da empresa: razão social > name > ''."""
    return as_str(props, "razao_social") or as_str(props, "name") or ""


# Tradução leiga pras 5 situações cadastrais RFB. Exibida direto no card
# de doadores/sócios; `situacao_fmt` vai pro UI, `situacao` bruto fica
# pras buscas/filtros. Constantes centralizadas em `rfb_status`.
from bracc.services.rfb_status import LABEL_LEIGA as _SITUACAO_LEIGA  # noqa: E402


def _situacao_from_props(
    props: dict[str, Any],
) -> tuple[str | None, str | None, str | None]:
    """Extrai (situacao, situacao_fmt, situacao_verified_at) de ``props``.

    ``situacao`` só é considerada quando for uma das 5 categorias válidas
    da Receita (``_SITUACAO_LEIGA``) — evita propagar string lixo pro UI.
    ``None`` em todos os 3 campos quando a empresa ainda não foi
    verificada pelo pipeline ``brasilapi_cnpj_status``.
    """
    raw = as_str(props, "situacao_cadastral")
    if raw is None:
        return None, None, None
    upper = raw.upper()
    if upper not in _SITUACAO_LEIGA:
        return None, None, None
    verified_at = as_str(props, "situacao_verified_at")
    return upper, _SITUACAO_LEIGA[upper], verified_at


def classificar(
    conexoes_raw: list[dict[str, Any]],
    entidades_conectadas: dict[str, dict[str, Any]],
    politico_entity_id: str,
    *,
    limit_por_categoria: int = 50,
    ano_doacao: int | None = None,
) -> ConexoesClassificadas:
    """Classifica ``conexoes_raw`` em 7 categorias tipadas.

    Parameters
    ----------
    conexoes_raw:
        Lista de conexões vinda do Cypher (shape documentado no módulo).
    entidades_conectadas:
        Mapa ``element_id -> {"type": str, "properties": dict}`` das
        entidades envolvidas (a outra ponta de cada conexão).
    politico_entity_id:
        ``elementId`` do político focal (pra detectar direção da aresta).
    limit_por_categoria:
        Cap no tamanho de cada lista devolvida. Default 50 (compatível com
        Flask). Passe maior pra expandir sem mudar a API pública.
    ano_doacao:
        Quando definido, descarta rels ``:DOOU`` cuja ``rel_props.ano``
        está carimbada e diverge do valor fornecido. Rels sem ``ano``
        (``None``) são **mantidas** — doadores PJ/PF vindos de pipelines
        que não carimbam `ano` (Company/Person → Person) continuariam
        ausentes se fossem filtrados, e o ganho do filtro é evitar o
        double-count de rels TSE com ``ano`` em múltiplas eleições
        (2014/2018/2022). Usado pra alinhar ``total_doacoes`` com
        ``total_tse_{ano}`` declarado no Person (pipeline
        ``tse_prestacao_contas_go`` carimba ``ano`` em cada rel
        ``:DOOU``). ``None`` (default) preserva o comportamento
        pré-existente — agrega todos os anos sem filtrar.

    Returns
    -------
    ConexoesClassificadas
        7 listas independentes, cada uma capada a ``limit_por_categoria``.
    """

    emendas: list[Emenda] = []
    empresas: list[EmpresaConectada] = []
    contratos: list[ContratoConectado] = []
    socios: list[SocioConectado] = []
    familia: list[FamiliarConectado] = []

    # Agregação de doadores por documento (CNPJ/CPF) — 1 doador pode fazer
    # várias doações.
    doacoes_empresa: dict[str, _DoacaoEmpresaAcc] = {}
    doacoes_pessoa: dict[str, _DoacaoPessoaAcc] = {}

    for conn in conexoes_raw:
        source_id = conn.get("source_id")
        target_id_raw = conn.get("target_id")
        if not isinstance(source_id, str) or not isinstance(target_id_raw, str):
            continue

        # Detecta qual ponta é o político → a "outra" ponta vira target.
        if source_id == politico_entity_id:
            target_id = target_id_raw
            politico_is_source = True
        elif target_id_raw == politico_entity_id:
            target_id = source_id
            politico_is_source = False
        else:
            # Conexão espúria que não toca o político focal — ignora.
            continue

        target = entidades_conectadas.get(target_id, {})
        target_type = norm_type(target.get("type"))
        target_props_raw = target.get("properties") or {}
        target_props: dict[str, Any] = (
            target_props_raw if isinstance(target_props_raw, dict) else {}
        )
        rel_type_raw = conn.get("relationship_type")
        rel_type = rel_type_raw if isinstance(rel_type_raw, str) else ""
        rel_props_raw = conn.get("properties") or {}
        rel_props: dict[str, Any] = (
            rel_props_raw if isinstance(rel_props_raw, dict) else {}
        )

        # --- 1. Emendas ---------------------------------------------------
        if target_type == "amendment":
            val_committed = as_float(target_props.get("value_committed"))
            val_paid = as_float(target_props.get("value_paid"))
            amendment_id = as_str(target_props, "amendment_id") or target_id
            emendas.append(
                Emenda(
                    id=amendment_id,
                    tipo=traduzir_tipo_emenda(as_str(target_props, "type") or ""),
                    funcao=traduzir_funcao_emenda(
                        as_str(target_props, "function") or "",
                    ),
                    municipio=as_str(target_props, "municipality"),
                    uf=as_str(target_props, "uf"),
                    valor_empenhado=val_committed,
                    valor_empenhado_fmt=fmt_brl(val_committed),
                    valor_pago=val_paid,
                    valor_pago_fmt=fmt_brl(val_paid),
                    provenance=_provenance_from_props(target_props),
                ),
            )
            continue

        # --- 2. Doadores (DOOU inbound; político = target) ----------------
        if rel_type == "DOOU" and not politico_is_source:
            # Filtro por ano — evita somar doações de 2014/2018 com 2022
            # num mesmo ``valor_total``. Sem o filtro, ``total_doacoes``
            # acaba > ``total_tse_{ano}`` quando o candidato tem
            # múltiplas eleições ingeridas (o CSV TSE por ano gera uma
            # rel ``:DOOU`` por linha, com ``ano`` carimbado).
            #
            # Rels sem ``ano`` carimbada (legacy: Company/Person → Person
            # de pipelines não-TSE) passam — descartá-las zera doadores
            # PJ/PF do candidato. Débito: backfill de ``ano`` em todos
            # pipelines que criam :DOOU
            # (todo-list-prompts/high_priority/debitos/backfill-ano-doou-rels.md).
            if ano_doacao is not None:
                rel_ano_raw = rel_props.get("ano")
                try:
                    rel_ano = (
                        int(rel_ano_raw) if rel_ano_raw is not None else None
                    )
                except (TypeError, ValueError):
                    rel_ano = None
                if rel_ano is not None and rel_ano != ano_doacao:
                    continue
            valor = _valor_doacao(rel_props)
            donated_at = _donated_at_iso(rel_props)
            # Proveniência da DOAÇÃO: preferida na rel :DOOU (onde o
            # pipeline TSE carimba 1 registro por doação), com fallback
            # nos ``target_props`` do nó doador pra legados.
            if target_type == "company":
                cnpj = as_str(target_props, "cnpj")
                # Gotcha do audit: CNPJ ausente → usa element_id como chave
                # pra evitar colapsar empresas diferentes em 1 só.
                chave = cnpj or f"empresa_{target_id}"
                situacao, _fmt, verified_at = _situacao_from_props(
                    target_props,
                )
                # Proveniência da DOAÇÃO individual: ler da rel :DOOU
                # (pipeline TSE carimba 1 registro por doação na aresta).
                # Fallback pro nó apenas quando a rel estiver sem os
                # campos obrigatórios — mantém legados fluindo.
                prov_block, prov_ingested = _provenance_with_ingested(
                    rel_props, target_props,
                )
                emp_acc = doacoes_empresa.setdefault(
                    chave,
                    _DoacaoEmpresaAcc(
                        nome=_nome_empresa(target_props),
                        cnpj=cnpj,
                        situacao=situacao,
                        situacao_verified_at=verified_at,
                        provenance=prov_block,
                        provenance_ingested_at=prov_ingested,
                        tipo_entidade=as_str(target_props, "tipo_entidade") or None,
                        cnae_principal=(
                            as_str(target_props, "cnae_principal") or None
                        ),
                    ),
                )
                # Se a primeira doação vista não trazia situacao (props
                # antigos) mas uma próxima traz, adota — o no é o mesmo.
                if emp_acc.situacao is None and situacao is not None:
                    emp_acc.situacao = situacao
                    emp_acc.situacao_verified_at = verified_at
                # Agregação de proveniência: fica com a doação mais
                # recente por ``ingested_at`` (ISO 8601 → ordenação
                # lexicográfica = ordenação cronológica).
                if prov_block is not None and (
                    emp_acc.provenance is None
                    or (
                        prov_ingested is not None
                        and (
                            emp_acc.provenance_ingested_at is None
                            or prov_ingested > emp_acc.provenance_ingested_at
                        )
                    )
                ):
                    emp_acc.provenance = prov_block
                    emp_acc.provenance_ingested_at = prov_ingested
                if donated_at is not None:
                    if (
                        emp_acc.data_primeira_iso is None
                        or donated_at < emp_acc.data_primeira_iso
                    ):
                        emp_acc.data_primeira_iso = donated_at
                    if (
                        emp_acc.data_ultima_iso is None
                        or donated_at > emp_acc.data_ultima_iso
                    ):
                        emp_acc.data_ultima_iso = donated_at
                emp_acc.doacoes.append(
                    DoacaoItem(
                        valor=valor,
                        valor_fmt=fmt_brl(valor),
                        data_doacao=donated_at,
                        data_doacao_fmt=fmt_data_br(donated_at),
                        provenance=prov_block,
                    ),
                )
                emp_acc.total += valor
                emp_acc.n += 1
                continue
            if target_type == "person":
                cpf_pleno = as_str(target_props, "cpf")
                # LGPD: máscara APLICADA AQUI — o dict intermediário e o
                # DoadorPessoa só carregam o formato mascarado.
                cpf_mascarado = mascarar_cpf(cpf_pleno)
                # Chave de agregação: CPF pleno (só usado como chave interna,
                # descartada depois) se existir; senão o mascarado; senão o
                # element_id pra preservar identidade do nó.
                chave = cpf_pleno or cpf_mascarado or f"pessoa_{target_id}"
                # Proveniência da DOAÇÃO individual: idem empresa, ler da
                # rel :DOOU primeiro (mais específica) e cair no nó só
                # quando a rel estiver sem os 4 campos obrigatórios.
                # LGPD: drop_record_id=True — no TSE o source_record_id do
                # doador PF pode ser o próprio CPF. Surfar isso no chip
                # vazaria o que já mascaramos em ``cpf_mascarado``.
                prov_block, prov_ingested = _provenance_with_ingested(
                    rel_props, target_props, drop_record_id=True,
                )
                pes_acc = doacoes_pessoa.setdefault(
                    chave,
                    _DoacaoPessoaAcc(
                        nome=as_str(target_props, "name") or "",
                        cpf_mascarado=cpf_mascarado,
                        provenance=prov_block,
                        provenance_ingested_at=prov_ingested,
                    ),
                )
                if prov_block is not None and (
                    pes_acc.provenance is None
                    or (
                        prov_ingested is not None
                        and (
                            pes_acc.provenance_ingested_at is None
                            or prov_ingested > pes_acc.provenance_ingested_at
                        )
                    )
                ):
                    pes_acc.provenance = prov_block
                    pes_acc.provenance_ingested_at = prov_ingested
                if donated_at is not None:
                    if (
                        pes_acc.data_primeira_iso is None
                        or donated_at < pes_acc.data_primeira_iso
                    ):
                        pes_acc.data_primeira_iso = donated_at
                    if (
                        pes_acc.data_ultima_iso is None
                        or donated_at > pes_acc.data_ultima_iso
                    ):
                        pes_acc.data_ultima_iso = donated_at
                pes_acc.doacoes.append(
                    DoacaoItem(
                        valor=valor,
                        valor_fmt=fmt_brl(valor),
                        data_doacao=donated_at,
                        data_doacao_fmt=fmt_data_br(donated_at),
                        # ``prov_block`` já vem de ``_provenance_with_ingested``
                        # com ``drop_record_id=True`` — LGPD preservada por
                        # doação individual também.
                        provenance=prov_block,
                    ),
                )
                pes_acc.total += valor
                pes_acc.n += 1
                continue
            # DOOU mas target inesperado: ignora (não cai em "empresas"
            # fallback abaixo).
            continue

        # --- 3. Sócio de empresa ------------------------------------------
        if rel_type == "SOCIO_DE" and target_type == "company":
            situacao, situacao_fmt, verified_at = _situacao_from_props(
                target_props,
            )
            socios.append(
                SocioConectado(
                    nome=_nome_empresa(target_props),
                    cnpj=as_str(target_props, "cnpj"),
                    situacao=situacao,
                    situacao_fmt=situacao_fmt,
                    situacao_verified_at=verified_at,
                    # CNPJ é público — preserva source_record_id.
                    provenance=_provenance_from_props(target_props),
                ),
            )
            continue

        # --- 4. Familia (cônjuge / parente) --------------------------------
        if rel_type in ("CONJUGE_DE", "PARENTE_DE"):
            if target_type != "person":
                # Rel familiar apontando pra non-Person é dado sujo — ignora.
                continue
            familia.append(
                FamiliarConectado(
                    nome=as_str(target_props, "name") or "",
                    # LGPD: mascara AQUI antes de construir o model.
                    cpf_mascarado=mascarar_cpf(as_str(target_props, "cpf")),
                    relacao="Cônjuge" if rel_type == "CONJUGE_DE" else "Parente",
                    # LGPD: drop_record_id=True — analogo a DoadorPessoa. O
                    # record_id do nó Person pode ser o CPF pleno; surfar
                    # isso violaria a máscara que aplicamos acima.
                    provenance=_provenance_from_props(
                        target_props, drop_record_id=True,
                    ),
                ),
            )
            continue

        # --- 5. Contratos (federal e GO) -----------------------------------
        if target_type == "contract":
            valor = as_float(target_props.get("value"))
            contratos.append(
                ContratoConectado(
                    objeto=as_str(target_props, "object") or "Nao informado",
                    valor=valor,
                    valor_fmt=fmt_brl(valor),
                    orgao=as_str(target_props, "contracting_org"),
                    data=as_str(target_props, "date"),
                    # Identificador de contrato é público.
                    provenance=_provenance_from_props(target_props),
                ),
            )
            continue
        if target_type == "go_procurement":
            valor = as_float(target_props.get("amount_estimated"))
            contratos.append(
                ContratoConectado(
                    objeto=(
                        as_str(target_props, "object")
                        or "Licitacao estadual/municipal"
                    ),
                    valor=valor,
                    valor_fmt=fmt_brl(valor),
                    orgao=as_str(target_props, "agency_name"),
                    data=as_str(target_props, "published_at"),
                    # Identificador de licitação é público.
                    provenance=_provenance_from_props(target_props),
                ),
            )
            continue

        # --- 6. Empresas conectadas (fallback) -----------------------------
        if target_type == "company":
            situacao, situacao_fmt, verified_at = _situacao_from_props(
                target_props,
            )
            empresas.append(
                EmpresaConectada(
                    nome=_nome_empresa(target_props),
                    cnpj=as_str(target_props, "cnpj"),
                    relacao=traduzir_relacao(rel_type),
                    situacao=situacao,
                    situacao_fmt=situacao_fmt,
                    situacao_verified_at=verified_at,
                ),
            )
            continue

        # --- 7. Órgão estadual (lotação) → reaproveita EmpresaConectada ----
        if target_type == "state_agency":
            empresas.append(
                EmpresaConectada(
                    nome=as_str(target_props, "name") or "",
                    cnpj=None,
                    relacao="Lotado em (orgao estadual)",
                ),
            )
            continue

        # Outros target_types (election, go_gazette_act, person sem rel
        # familiar/doação) são informativos e ignorados — comportamento
        # espelha o Flask.

    # --- Materialização de doadores (após agregação) ----------------------
    # Ordena doações por data (rels sem ``donated_at`` ficam no fim).
    def _ordenar_doacoes(itens: list[DoacaoItem]) -> list[DoacaoItem]:
        return sorted(
            itens,
            key=lambda it: (it.data_doacao is None, it.data_doacao or ""),
        )

    doadores_empresa: list[DoadorEmpresa] = [
        DoadorEmpresa(
            nome=acc.nome,
            cnpj=acc.cnpj,
            valor_total=acc.total,
            valor_total_fmt=fmt_brl(acc.total),
            n_doacoes=acc.n,
            situacao=acc.situacao,
            situacao_fmt=(
                _SITUACAO_LEIGA.get(acc.situacao)
                if acc.situacao is not None
                else None
            ),
            situacao_verified_at=acc.situacao_verified_at,
            data_primeira_doacao=acc.data_primeira_iso,
            data_primeira_doacao_fmt=fmt_data_br(acc.data_primeira_iso),
            data_ultima_doacao=acc.data_ultima_iso,
            data_ultima_doacao_fmt=fmt_data_br(acc.data_ultima_iso),
            doacoes=_ordenar_doacoes(acc.doacoes),
            provenance=acc.provenance,
            tipo_entidade=acc.tipo_entidade,
            cnae_principal=acc.cnae_principal,
        )
        for acc in doacoes_empresa.values()
    ]
    doadores_empresa.sort(key=lambda d: -d.valor_total)

    doadores_pessoa: list[DoadorPessoa] = [
        DoadorPessoa(
            nome=acc.nome,
            # Já mascarado na agregação — NUNCA volta pro CPF pleno aqui.
            cpf_mascarado=acc.cpf_mascarado,
            valor_total=acc.total,
            valor_total_fmt=fmt_brl(acc.total),
            n_doacoes=acc.n,
            data_primeira_doacao=acc.data_primeira_iso,
            data_primeira_doacao_fmt=fmt_data_br(acc.data_primeira_iso),
            data_ultima_doacao=acc.data_ultima_iso,
            data_ultima_doacao_fmt=fmt_data_br(acc.data_ultima_iso),
            doacoes=_ordenar_doacoes(acc.doacoes),
            provenance=acc.provenance,
        )
        for acc in doacoes_pessoa.values()
    ]
    doadores_pessoa.sort(key=lambda d: -d.valor_total)

    # Ordenação determinística das listas não-agregadas.
    empresas.sort(key=lambda e: (e.nome or "").lower())
    socios.sort(key=lambda s: (s.nome or "").lower())
    familia.sort(key=lambda f: (f.nome or "").lower())
    contratos.sort(key=lambda c: -c.valor)
    emendas.sort(key=lambda e: -(e.valor_pago or e.valor_empenhado))

    return ConexoesClassificadas(
        emendas=emendas[:limit_por_categoria],
        doadores_empresa=doadores_empresa[:limit_por_categoria],
        doadores_pessoa=doadores_pessoa[:limit_por_categoria],
        socios=socios[:limit_por_categoria],
        familia=familia[:limit_por_categoria],
        contratos=contratos[:limit_por_categoria],
        empresas=empresas[:limit_por_categoria],
    )
