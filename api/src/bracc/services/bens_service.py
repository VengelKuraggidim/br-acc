"""BensService — leitura de :DeclaredAsset (TSE) por entity_id e agregacao.

Le da query ``perfil_bens_declarados.cypher``, que faz cluster-walk via
:CanonicalPerson pra cobrir deputados/senadores cujo Person sibling carrega
o DECLAROU_BEM (pipeline tse_bens_go grava no CPF do Person, nao no
id_camara/id_senado/legislator_id do nó cargo).

Responsabilidades:

* Mapear cada row da query num :class:`BemDeclarado`.
* Agregar por ``ano`` em :class:`PatrimonioAno` ordenado crescente.
* Calcular ``variacao_pct`` ano-a-ano (None no ano mais antigo).
* Montar o ``resumo`` textual pro PWA quando >=2 anos com dados.

Retorna ``None`` quando o entity_id nao tem nenhum :DeclaredAsset — o
perfil omite a secao toda no PWA.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bracc.config import settings
from bracc.models.entity import ProvenanceBlock
from bracc.models.perfil import BemDeclarado, BensDeclarados, PatrimonioAno
from bracc.services.common_helpers import archival_url
from bracc.services.formatacao_service import fmt_brl
from bracc.services.neo4j_service import execute_query_single

if TYPE_CHECKING:
    from neo4j import AsyncDriver

_QUERY_TIMEOUT = 15.0


def _provenance_from_row(row: dict[str, Any]) -> ProvenanceBlock | None:
    """Monta ``ProvenanceBlock`` a partir do dict que a query devolve.

    A query carimba sempre os 4 campos obrigatorios — pipeline tse_bens_go
    e novo. Se algum vier ausente (legado raro), retorna None pra nao
    quebrar a serializacao Pydantic.
    """
    required = ("source_id", "source_url", "ingested_at", "run_id")
    if any(not row.get(field) for field in required):
        return None
    snapshot = row.get("source_snapshot_uri")
    return ProvenanceBlock(
        source_id=str(row["source_id"]),
        source_record_id=(
            str(row["source_record_id"]) if row.get("source_record_id") else None
        ),
        source_url=str(row["source_url"]),
        ingested_at=str(row["ingested_at"]),
        run_id=str(row["run_id"]),
        snapshot_url=archival_url(str(snapshot) if snapshot else None),
    )


def _agregar_por_ano(bens: list[BemDeclarado]) -> list[PatrimonioAno]:
    """Agrupa bens por ``ano`` e calcula ``variacao_pct`` ano-a-ano.

    Ordem do retorno: crescente (2018 -> 2024) pra o PWA desenhar
    timeline esquerda-direita. ``variacao_pct`` no primeiro ano e None.
    Quando o ano anterior tem total 0 (defensivo), variacao fica None
    em vez de divisao por zero.
    """
    totais: dict[int, dict[str, float | int]] = {}
    for b in bens:
        bucket = totais.setdefault(b.ano, {"total": 0.0, "n": 0})
        bucket["total"] = float(bucket["total"]) + b.valor
        bucket["n"] = int(bucket["n"]) + 1

    anos_ordenados = sorted(totais.keys())
    saida: list[PatrimonioAno] = []
    anterior_total: float | None = None
    for ano in anos_ordenados:
        total = float(totais[ano]["total"])
        n = int(totais[ano]["n"])
        if anterior_total is None or anterior_total <= 0:
            variacao = None
        else:
            variacao = round(
                ((total - anterior_total) / anterior_total) * 100.0, 1,
            )
        saida.append(
            PatrimonioAno(
                ano=ano,
                total=total,
                total_fmt=fmt_brl(total),
                variacao_pct=variacao,
                num_bens=n,
            ),
        )
        anterior_total = total
    return saida


def _build_resumo(por_ano: list[PatrimonioAno]) -> str:
    """Frase em pt-BR resumindo a evolucao patrimonial.

    Usada como subtitulo da secao "Bens declarados" no PWA. So gera texto
    quando ha 2+ eleicoes com dados — caso contrario nao tem comparacao
    pra fazer.
    """
    if len(por_ano) < 2:
        return ""
    primeiro = por_ano[0]
    ultimo = por_ano[-1]
    if primeiro.total <= 0:
        return (
            f"Patrimonio passou de zero em {primeiro.ano} para "
            f"{ultimo.total_fmt} em {ultimo.ano}."
        )
    delta = ((ultimo.total - primeiro.total) / primeiro.total) * 100.0
    if abs(delta) < 5:
        return (
            f"Patrimonio praticamente estavel entre {primeiro.ano} e "
            f"{ultimo.ano} (variacao de {round(delta, 1)}%)."
        )
    direcao = "cresceu" if delta > 0 else "caiu"
    return (
        f"Patrimonio {direcao} {round(abs(delta), 1)}% entre "
        f"{primeiro.ano} e {ultimo.ano} (de {primeiro.total_fmt} "
        f"para {ultimo.total_fmt})."
    )


async def obter_bens_declarados(
    driver: AsyncDriver,
    entity_id: str,
) -> BensDeclarados | None:
    """Le bens TSE do entity_id e retorna agregado pronto pro PWA.

    Devolve ``None`` quando nao ha nenhum :DeclaredAsset ligado a nenhum
    no do cluster canonico — o ``obter_perfil`` propaga isso pro
    PerfilPolitico.bens_declarados=None, e o PWA omite a secao toda.
    """
    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_bens_declarados",
            {"entity_id": entity_id},
            timeout=_QUERY_TIMEOUT,
        )

    if record is None:
        return None
    rows = record.get("bens") or []
    if not rows:
        return None

    bens: list[BemDeclarado] = []
    for row in rows:
        ano_raw = row.get("ano")
        try:
            ano = int(ano_raw) if ano_raw is not None else None
        except (TypeError, ValueError):
            ano = None
        if ano is None:
            # Sem ano nao da pra agregar nem mostrar — pula silenciosamente.
            continue
        valor_raw = row.get("valor") or 0
        try:
            valor = float(valor_raw)
        except (TypeError, ValueError):
            valor = 0.0
        bens.append(
            BemDeclarado(
                ano=ano,
                tipo=str(row.get("tipo") or ""),
                descricao=str(row.get("descricao") or ""),
                valor=valor,
                valor_fmt=fmt_brl(valor),
                provenance=_provenance_from_row(row),
            ),
        )

    if not bens:
        return None

    por_ano = _agregar_por_ano(bens)
    ultimo = por_ano[-1] if por_ano else None
    return BensDeclarados(
        por_ano=por_ano,
        bens=bens,
        total_geral_ultimo_ano=ultimo.total if ultimo else 0.0,
        total_geral_ultimo_ano_fmt=ultimo.total_fmt if ultimo else fmt_brl(0),
        resumo=_build_resumo(por_ano),
    )
