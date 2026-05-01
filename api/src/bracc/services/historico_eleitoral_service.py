"""HistoricoEleitoralService — leitura de :Election (TSE) por entity_id.

Le da query ``perfil_historico_eleitoral.cypher``, que faz cluster-walk via
:CanonicalPerson pra cobrir cargos cujo Person sibling carrega o
:CANDIDATO_EM (TSE grava no CPF do Person, nao no id_camara/id_senado/
legislator_id do no de cargo).

Importante: TSE so registra *candidatura*, nao mandato exercido. A
relacao :CANDIDATO_EM nao guarda se foi eleita. Pra "tempo efetivo em
cargo" precisaria cruzar com nos de mandato, mas hoje so temos snapshot
da legislatura atual no grafo (17 FedLeg + 45 StateLeg + 3 Senator). O
agregado aqui mede *presenca em eleicoes*, nao mandato.

Retorna ``None`` quando o entity_id nao tem nenhuma candidatura no
cluster — o perfil omite a secao no PWA.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from bracc.config import settings
from bracc.models.perfil import CandidaturaTSE, CarreiraPolitica
from bracc.services.neo4j_service import execute_query_single
from bracc.services.traducao_service import traduzir_cargo

if TYPE_CHECKING:
    from neo4j import AsyncDriver

_QUERY_TIMEOUT = 15.0


def _build_resumo(
    num: int,
    primeira: int | None,
    ultima: int | None,
    anos: int,
    cargos: list[str],
) -> str:
    if num == 0 or primeira is None or ultima is None:
        return ""
    if num == 1:
        cargo = cargos[0] if cargos else "cargo eletivo"
        return (
            f"Disputou 1 eleicao em {primeira} para {cargo}."
        )
    if anos == 0:
        return f"Disputou {num} eleicoes em {primeira}."
    return (
        f"Disputou {num} eleicoes entre {primeira} e {ultima} "
        f"({anos} anos de presenca eleitoral)."
    )


async def obter_historico_eleitoral(
    driver: AsyncDriver,
    entity_id: str,
) -> CarreiraPolitica | None:
    """Le candidaturas TSE do entity_id e retorna agregado pro PWA.

    Devolve ``None`` quando nao ha nenhuma candidatura ligada ao cluster
    canonico — ``obter_perfil`` propaga isso pro PerfilPolitico.
    carreira_politica=None, e o PWA omite a secao.
    """
    async with driver.session(database=settings.neo4j_database) as session:
        record = await execute_query_single(
            session,
            "perfil_historico_eleitoral",
            {"entity_id": entity_id},
            timeout=_QUERY_TIMEOUT,
        )

    if record is None:
        return None
    rows = record.get("eleicoes") or []
    if not rows:
        return None

    candidaturas: list[CandidaturaTSE] = []
    anos_set: set[int] = set()
    cargos_set: set[str] = set()
    for row in rows:
        ano_raw = row.get("ano")
        try:
            ano = int(ano_raw) if ano_raw is not None else None
        except (TypeError, ValueError):
            ano = None
        if ano is None:
            continue
        cargo = str(row.get("cargo") or "").strip()
        uf = str(row.get("uf") or "").strip().upper()
        municipio_raw = row.get("municipio")
        municipio = (
            str(municipio_raw).strip()
            if isinstance(municipio_raw, str) and municipio_raw.strip()
            else None
        )
        candidaturas.append(
            CandidaturaTSE(
                ano=ano,
                cargo=cargo,
                uf=uf,
                municipio=municipio,
            ),
        )
        anos_set.add(ano)
        if cargo:
            cargos_set.add(traduzir_cargo(cargo))

    if not candidaturas:
        return None

    primeira = min(anos_set) if anos_set else None
    ultima = max(anos_set) if anos_set else None
    anos_carreira = (ultima - primeira) if (primeira and ultima) else 0
    cargos_distintos = sorted(cargos_set)
    resumo = _build_resumo(
        num=len(candidaturas),
        primeira=primeira,
        ultima=ultima,
        anos=anos_carreira,
        cargos=cargos_distintos,
    )
    return CarreiraPolitica(
        num_candidaturas=len(candidaturas),
        primeira_eleicao=primeira,
        ultima_eleicao=ultima,
        anos_carreira=anos_carreira,
        cargos_distintos=cargos_distintos,
        candidaturas=candidaturas,
        resumo=resumo,
    )
