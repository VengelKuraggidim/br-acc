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
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bracc.models.perfil import (
    ContratoConectado,
    DoadorEmpresa,
    DoadorPessoa,
    Emenda,
    EmpresaConectada,
    FamiliarConectado,
    SocioConectado,
)
from bracc.services.common_helpers import as_float, as_str, norm_type
from bracc.services.formatacao_service import fmt_brl, mascarar_cpf
from bracc.services.traducao_service import (
    traduzir_funcao_emenda,
    traduzir_relacao,
    traduzir_tipo_emenda,
)


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
    """

    nome: str
    cnpj: str | None
    total: float = 0.0
    n: int = 0
    situacao: str | None = None
    situacao_verified_at: str | None = None


@dataclass
class _DoacaoPessoaAcc:
    """Acumulador interno de doações por pessoa (CPF já mascarado)."""

    nome: str
    cpf_mascarado: str | None
    total: float = 0.0
    n: int = 0


def _valor_doacao(rel_props: dict[str, Any] | None) -> float:
    """Extrai o valor da doação da aresta. Prefere ``valor``, cai pra ``amount``."""
    if not rel_props:
        return 0.0
    raw = rel_props.get("valor")
    if raw is None:
        raw = rel_props.get("amount")
    return as_float(raw)


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
                ),
            )
            continue

        # --- 2. Doadores (DOOU inbound; político = target) ----------------
        if rel_type == "DOOU" and not politico_is_source:
            valor = _valor_doacao(rel_props)
            if target_type == "company":
                cnpj = as_str(target_props, "cnpj")
                # Gotcha do audit: CNPJ ausente → usa element_id como chave
                # pra evitar colapsar empresas diferentes em 1 só.
                chave = cnpj or f"empresa_{target_id}"
                situacao, _fmt, verified_at = _situacao_from_props(
                    target_props,
                )
                emp_acc = doacoes_empresa.setdefault(
                    chave,
                    _DoacaoEmpresaAcc(
                        nome=_nome_empresa(target_props),
                        cnpj=cnpj,
                        situacao=situacao,
                        situacao_verified_at=verified_at,
                    ),
                )
                # Se a primeira doação vista não trazia situacao (props
                # antigos) mas uma próxima traz, adota — o no é o mesmo.
                if emp_acc.situacao is None and situacao is not None:
                    emp_acc.situacao = situacao
                    emp_acc.situacao_verified_at = verified_at
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
                pes_acc = doacoes_pessoa.setdefault(
                    chave,
                    _DoacaoPessoaAcc(
                        nome=as_str(target_props, "name") or "",
                        cpf_mascarado=cpf_mascarado,
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
