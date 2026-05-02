"""PWA parity endpoints.

These routes reproduce the shapes emitted by the legacy Flask backend
(``backend/app.py``) so the existing PWA (``pwa/index.html``) can be
pointed at the FastAPI service with no client-side change.

Paths are intentionally mounted at the root (``/status``,
``/buscar-tudo``) because the PWA calls ``${API}/status`` — i.e. the
raw service root — whereas the rest of the FastAPI surface lives
under ``/api/v1``. Keeping the parity routes out of the ``/api/v1``
tree preserves a clean boundary: the ``/api/v1`` contract stays
graph-native, and this router is a thin PWA-facing facade that can be
removed once the PWA is updated to call ``/api/v1`` directly.
"""

import re
import unicodedata
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from neo4j import AsyncDriver, AsyncSession

from bracc.dependencies import get_driver, get_session
from bracc.models.perfil import PerfilPolitico
from bracc.models.pwa_parity import (
    BuscarTudoItem,
    BuscarTudoResponse,
    StatusResponse,
)
from bracc.services import perfil_service
from bracc.services.neo4j_service import execute_query, execute_query_single
from bracc.services.public_guard import should_hide_person_entities

router = APIRouter(tags=["pwa-parity"])

UF_FILTRO = "GO"

# Mirrors the dedup + filter taxonomy used by the legacy Flask
# ``/buscar-tudo`` handler. We accept both the historical
# ``state_employee``/``go_procurement`` spellings and the
# lower-cased-no-underscore spellings emitted by ``/api/v1/search``
# today so the endpoint returns results regardless of which label
# normalizer upstream happens to apply.
_GO_TYPES = {
    "state_employee",
    "stateemployee",
    "go_procurement",
    "goprocurement",
    "go_appointment",
    "goappointment",
    "go_vereador",
    "govereador",
    # Parlamentares — entraram no fulltext ``entity_search`` em 2026-04-22.
    # Precisam de filtro de UF (same-uf-as-GO) porque o indice cobre o
    # Brasil inteiro — ``_format_item`` aplica ``_is_person_go`` pra eles.
    "federal_legislator",
    "federallegislator",
    "state_legislator",
    "statelegislator",
    "senator",
}

_LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}[\]^"~*?:\\/])')

# Mesma hierarquia do perfil_politico_connections.cypher: quando vários
# rows do fulltext apontam pro mesmo CanonicalPerson, fica apenas o de
# label mais oficial. GoVereador entra acima de Person (cargo eletivo
# municipal), porque pode coexistir com :Person no mesmo cluster.
_CLUSTER_RANK = {
    "Senator": 0,
    "FederalLegislator": 1,
    "StateLegislator": 2,
    "GoVereador": 3,
}


def _cluster_rank(labels: list[str]) -> int:
    return min((_CLUSTER_RANK.get(lbl, 4) for lbl in labels), default=4)


_PERSON_DEDUP_TYPES = {
    "person",
    "federal_legislator", "federallegislator",
    "state_legislator", "statelegislator",
    "senator",
    "go_vereador", "govereador",
}

_DIGITS_RE = re.compile(r"\D")


def _normalize_name(nome: str) -> str:
    """Uppercase + sem acentos + collapsa espacos."""
    if not nome:
        return ""
    nfkd = unicodedata.normalize("NFKD", nome)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(sem_acento.upper().split())


def _person_cpf(item: dict[str, Any]) -> str:
    """CPF normalizado (so digitos, 11 chars) ou ''."""
    props = item.get("properties") or {}
    cpf_raw = item.get("document") or props.get("cpf") or ""
    cpf = _DIGITS_RE.sub("", str(cpf_raw))
    return cpf if len(cpf) == 11 else ""


def _person_group_key(item: dict[str, Any]) -> tuple[str, str] | None:
    """Chave grossa de agrupamento: nome normalizado + UF.

    Subdivisao por CPF acontece depois — dentro do grupo, rows com CPFs
    diferentes sao tratadas como pessoas distintas, mas rows sem CPF
    fundem com o cluster do unico CPF presente (caso Karlos: Person com
    CPF + Person sem CPF, mesmo nome+UF → mesma pessoa).
    Retorna None pra rows que nao devem dedupar (companies, contracts).
    """
    tipo = str(item.get("type") or "").lower()
    if tipo not in _PERSON_DEDUP_TYPES:
        return None
    props = item.get("properties") or {}
    nome = _normalize_name(str(item.get("name") or ""))
    uf = str(props.get("uf") or "").upper()
    if not nome or not uf:
        return None
    return (nome, uf)


def _to_lucene_query(query: str) -> str:
    """Lucene-escape the user query; ``*`` keeps its match-all semantics."""
    if query.strip() == "*":
        return "*:*"
    return _LUCENE_SPECIAL.sub(r"\\\1", query)


def _edit_distance_le1(a: str, b: str) -> bool:
    """True quando Levenshtein(a, b) <= 1 (cobre CESAR↔CEZAR)."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    i = 0
    while i < min(la, lb) and a[i] == b[i]:
        i += 1
    if la == lb:
        return a[i + 1 :] == b[i + 1 :]
    if la > lb:
        return a[i + 1 :] == b[i:]
    return a[i:] == b[i + 1 :]


def _token_match(qt: str, nt: str) -> bool:
    """Fuzzy: igual exato ou edit-distance <=1 quando ambos têm >=4 chars."""
    if qt == nt:
        return True
    return len(qt) >= 4 and len(nt) >= 4 and _edit_distance_le1(qt, nt)


def _local_relevance(query: str, name: str) -> tuple[int, int, int]:
    """Tupla de ranking — menor é melhor.

    Classes (lexicográficas):
      0 = nome igual à query (normalizado).
      1 = nome começa com a query (prefix).
      2 = todos os tokens da query aparecem no nome, na mesma ordem.
      3 = todos os tokens da query aparecem no nome (ordem livre).
      4 = só parte dos tokens aparece.
      9 = nada bate além do que o Lucene já viu.

    Match de token tolera 1 char de diferença (CESAR↔CEZAR) — sem isso,
    grafias variantes do mesmo nome ficam atrás de homônimos com tokens
    em ordem trocada.
    """
    q = _normalize_name(query)
    n = _normalize_name(name)
    if not q or not n:
        return (9, 0, 999)
    if n == q:
        return (0, 0, 0)
    if n.startswith(q + " "):
        return (1, 0, 0)
    q_tokens = q.split()
    n_tokens = n.split()
    if not q_tokens or not n_tokens:
        return (9, 0, 999)

    matches = 0
    last_pos = -1
    in_order = True
    first_pos = -1
    for qt in q_tokens:
        found = -1
        for idx in range(last_pos + 1, len(n_tokens)):
            if _token_match(qt, n_tokens[idx]):
                found = idx
                break
        if found >= 0:
            matches += 1
            last_pos = found
            if first_pos == -1:
                first_pos = found
            continue
        for idx, nt in enumerate(n_tokens):
            if _token_match(qt, nt):
                matches += 1
                in_order = False
                if first_pos == -1:
                    first_pos = idx
                break

    if matches == len(q_tokens) and in_order:
        return (2, -matches, first_pos)
    if matches == len(q_tokens):
        return (3, -matches, first_pos)
    if matches > 0:
        return (4, -matches, first_pos if first_pos >= 0 else 999)
    return (9, 0, 999)


def _fmt_brl(valor: float | int | None) -> str:
    """Reimplements ``backend/app.py::fmt_brl`` 1:1 for PWA parity."""
    if not valor:
        return "R$ 0,00"
    value = float(valor)
    if value >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:.2f} bi"
    if value >= 1_000_000:
        return f"R$ {value / 1_000_000:.2f} mi"
    if value >= 1_000:
        return f"R$ {value / 1_000:.1f} mil"
    formatted = f"R$ {value:,.2f}"
    # Swap the US ``1,234.56`` → pt-BR ``1.234,56`` through an X pivot.
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


def _is_person_go(props: dict[str, Any]) -> bool:
    uf = str(props.get("uf") or "").upper()
    return uf == UF_FILTRO


def _format_item(result: dict[str, Any]) -> BuscarTudoItem | None:
    """Translate a ``/api/v1/search`` result into the PWA shape.

    Returns ``None`` when the entity is out of the Goias scope (either
    a non-GO politician or a non-GO label). The Flask backend already
    applies this filter; we preserve the same behaviour here so
    consumers see identical result lists.
    """
    props = result.get("properties") or {}
    tipo_raw = str(result.get("type") or "").lower()

    # Tipos cuja escopo é nacional no fulltext precisam filtrar por
    # ``uf=='GO'`` pra respeitar o escopo do app. ``Senator`` entra aqui
    # (Caiado é GO, mas o indice cobre todos os 81 senadores).
    parlamentar_br_types = {
        "federal_legislator", "federallegislator",
        "state_legislator", "statelegislator",
        "senator",
    }

    if tipo_raw == "person" or tipo_raw in parlamentar_br_types:
        if not _is_person_go(props):
            return None
    elif tipo_raw not in _GO_TYPES:
        return None

    item = BuscarTudoItem(
        id=str(result["id"]),
        tipo=tipo_raw,
        nome=str(result.get("name") or ""),
        documento=result.get("document"),
        score=float(result.get("score") or 0.0),
        icone="outro",
        detalhe=tipo_raw.capitalize(),
    )

    if tipo_raw == "person":
        # Promove a icone/detalhe de cargo eletivo TSE quando o Person
        # carrega ``cargo_tse_<ano>`` mais recente — sem isso, vereador
        # 2024 aparece como "Pessoa publica" generica e o usuario nao
        # entende o que clica. Olha do mais recente pro mais antigo.
        cargo_tse_recente = ""
        partido_tse_recente = ""
        municipio_tse_recente = ""
        for ano in (2026, 2024, 2022, 2020, 2018):
            valor = props.get(f"cargo_tse_{ano}")
            if valor:
                cargo_tse_recente = str(valor).strip()
                partido_tse_recente = str(props.get(f"partido_tse_{ano}") or "").strip()
                municipio_tse_recente = str(props.get(f"municipio_tse_{ano}") or "").strip()
                break

        if cargo_tse_recente.upper() == "VEREADOR":
            item.icone = "vereador"
            partes = ["Vereador(a)"]
            if municipio_tse_recente:
                partes.append(municipio_tse_recente.title())
            if partido_tse_recente:
                partes.append(partido_tse_recente)
            item.detalhe = " - ".join(partes)
        elif cargo_tse_recente:
            item.icone = "pessoa"
            partes = [cargo_tse_recente.title()]
            if partido_tse_recente:
                partes.append(partido_tse_recente)
            item.detalhe = " - ".join(partes)
        else:
            item.icone = "pessoa"
            patrimonio = props.get("patrimonio_declarado")
            item.detalhe = (
                f"Patrimonio: {_fmt_brl(patrimonio)}" if patrimonio else "Pessoa publica"
            )
        item.is_pep = bool(props.get("is_pep", False))
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
    elif tipo_raw in {"state_employee", "stateemployee"}:
        item.icone = "servidor"
        salario = props.get("salary_gross")
        cargo = str(props.get("role") or "")
        if salario:
            item.detalhe = f"{cargo} - {_fmt_brl(salario)}/mes"
        else:
            item.detalhe = cargo or "Servidor estadual"
        item.is_comissionado = bool(props.get("is_commissioned", False))
    elif tipo_raw in {"go_procurement", "goprocurement"}:
        item.icone = "licitacao"
        valor = props.get("amount_estimated") or 0
        item.detalhe = (
            f"Licitacao: {_fmt_brl(valor)}" if valor else str(props.get("object") or "Licitacao")
        )
    elif tipo_raw in {"go_appointment", "goappointment"}:
        item.icone = "nomeacao"
        tipo_apt = str(props.get("appointment_type") or "Nomeacao").title()
        role = str(props.get("role") or "")
        item.detalhe = f"{tipo_apt}: {role}"
    elif tipo_raw in {"go_vereador", "govereador"}:
        item.icone = "vereador"
        item.detalhe = f"Vereador(a) - {props.get('party', '')}"
    elif tipo_raw in {"federal_legislator", "federallegislator"}:
        item.icone = "pessoa"
        partido = str(props.get("partido") or "").strip()
        item.detalhe = (
            f"Deputado(a) Federal - {partido}" if partido else "Deputado(a) Federal"
        )
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
    elif tipo_raw in {"state_legislator", "statelegislator"}:
        item.icone = "pessoa"
        partido = str(props.get("party") or props.get("partido") or "").strip()
        item.detalhe = (
            f"Deputado(a) Estadual - {partido}" if partido else "Deputado(a) Estadual"
        )
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
    elif tipo_raw == "senator":
        item.icone = "pessoa"
        partido = str(props.get("partido") or "").strip()
        item.detalhe = (
            f"Senador(a) - {partido}" if partido else "Senador(a)"
        )
        foto_raw = props.get("foto_url") or props.get("url_foto")
        if foto_raw:
            item.foto_url = str(foto_raw)
    elif tipo_raw == "company":
        item.icone = "empresa"
        item.detalhe = str(props.get("razao_social") or "")
    elif tipo_raw == "contract":
        item.icone = "contrato"
        valor = props.get("value") or 0
        item.detalhe = _fmt_brl(valor) if valor else "Contrato publico"
    elif tipo_raw == "amendment":
        item.icone = "emenda"
        valor = props.get("value_paid") or props.get("value_committed") or 0
        item.detalhe = f"Emenda: {_fmt_brl(valor)}" if valor else "Emenda parlamentar"

    return item


async def _run_search(
    session: AsyncSession,
    q: str,
    page: int,
    size: int,
    *,
    entity_type: str | None,
) -> tuple[list[dict[str, Any]], int]:
    """Thin wrapper around ``search``/``search_count`` that returns
    result dicts + total, matching what the Flask handler consumed
    from ``/api/v1/search``.
    """
    skip = (page - 1) * size
    hide_person_entities = should_hide_person_entities()
    lucene_query = _to_lucene_query(q)

    records = await execute_query(
        session,
        "search",
        {
            "query": lucene_query,
            "entity_type": entity_type,
            "skip": skip,
            "limit": size,
            "hide_person_entities": hide_person_entities,
        },
    )
    total_record = await execute_query_single(
        session,
        "search_count",
        {
            "query": lucene_query,
            "entity_type": entity_type,
            "hide_person_entities": hide_person_entities,
        },
    )
    total = 0
    if total_record and total_record["total"] is not None:
        total = int(total_record["total"])

    results: list[dict[str, Any]] = []
    for record in records:
        node = record["node"]
        props = dict(node)
        labels = record["node_labels"] or []
        doc_id = record["document_id"]
        document = (
            str(doc_id) if doc_id and not str(doc_id).startswith("4:") else None
        )
        results.append(
            {
                "id": record["node_id"],
                "type": labels[0].lower() if labels else "unknown",
                "name": props.get(
                    "name",
                    props.get("razao_social", props.get("object", "")),
                ),
                "score": float(record["score"] or 0.0),
                "document": document,
                "properties": props,
                "labels": labels,
                "canonical_id": record.get("canonical_id"),
            }
        )
    return results, total


@router.get("/status", response_model=StatusResponse)
async def pwa_status(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> StatusResponse:
    """PWA landing-page counters (aggregated in one Cypher round-trip)."""
    try:
        record = await execute_query_single(
            session, "pwa_status", {"uf": UF_FILTRO}
        )
        bracc_conectado = True
    except Exception:  # noqa: BLE001 — mirror Flask: degrade on DB errors
        record = None
        bracc_conectado = False

    if record is None:
        return StatusResponse(
            status="online",
            bracc_conectado=bracc_conectado,
            total_nos=0,
            total_relacionamentos=0,
            deputados_federais=0,
            deputados_estaduais=0,
            senadores=0,
            servidores_estaduais=0,
            cargos_comissionados=0,
            municipios_go=0,
            licitacoes_go=0,
            nomeacoes_go=0,
            vereadores_goiania=0,
        )

    return StatusResponse(
        status="online",
        bracc_conectado=bracc_conectado,
        total_nos=int(record["total_nos"] or 0),
        total_relacionamentos=int(record["total_relacionamentos"] or 0),
        deputados_federais=int(record["deputados_federais"] or 0),
        deputados_estaduais=int(record["deputados_estaduais"] or 0),
        senadores=int(record["senadores"] or 0),
        vereadores_goiania=int(record["vereadores_goiania"] or 0),
        servidores_estaduais=int(record["servidores_estaduais"] or 0),
        cargos_comissionados=int(record["cargos_comissionados"] or 0),
        municipios_go=int(record["municipios_go"] or 0),
        licitacoes_go=int(record["licitacoes_go"] or 0),
        nomeacoes_go=int(record["nomeacoes_go"] or 0),
    )


@router.get("/buscar-tudo", response_model=BuscarTudoResponse)
async def pwa_buscar_tudo(
    session: Annotated[AsyncSession, Depends(get_session)],
    q: Annotated[str, Query(min_length=2, max_length=200)],
    page: Annotated[int, Query(ge=1)] = 1,
) -> BuscarTudoResponse:
    """Unified search for the PWA result list.

    Runs the same two-pass search the Flask handler performs — one
    call filtered to ``type=person`` so politicians are not buried by
    high-scoring company matches, and one unfiltered pass — then
    dedups by id keeping the highest score, applies the Goias scope
    filter, and maps each result to the PWA ``BuscarTudoItem`` shape.
    """
    try:
        # Person search uses a wider fulltext window (500 vs 30) so the
        # post-query UF=GO filter in ``_format_item`` has enough
        # candidates to draw from. With only 30, popular names in other
        # states (SP/MG/PE) saturate the top-ranked slice and almost no
        # GO rows survive the filter, even when the graph has hundreds
        # of matching GO persons. 500 keeps the round-trip under ~100ms
        # and restores parity with the prod result volume.
        pessoas, pessoas_total = await _run_search(
            session, q, page, 500, entity_type="person"
        )
        outros, outros_total = await _run_search(
            session, q, page, 20, entity_type=None
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Erro: {exc}") from exc

    combined: dict[str, dict[str, Any]] = {}
    for r in (*pessoas, *outros):
        existing = combined.get(r["id"])
        if not existing or r.get("score", 0) > existing.get("score", 0):
            combined[r["id"]] = r

    total = max(pessoas_total, outros_total)

    # Segundo passo de dedup: agrupa rows do mesmo CanonicalPerson
    # (Person + cargo + Person TSE no mesmo cluster). Eleicao do
    # vencedor + acumulo de cargos_relacionados acontece via
    # _merge_group abaixo (mesma logica da fase 3 pra coerencia).
    by_cluster: dict[str, list[dict[str, Any]]] = {}
    deduped: list[dict[str, Any]] = []
    for r in combined.values():
        canonical = r.get("canonical_id")
        if not canonical:
            deduped.append(r)
            continue
        by_cluster.setdefault(canonical, []).append(r)

    # Fase 3 de dedup: colapsa pessoa-fisica que ficou em mais de um no
    # do grafo apesar de ER nao ter unificado. Tres mecanismos:
    # (a) Mesmo CPF puro (mesmo se nomes divergem — ex: "KARLOS CABRAL"
    #     StateLeg + "KARLOS MARCIO VIEIRA CABRAL" Person — ambos com
    #     CPF 831.869.051-68 sao a mesma pessoa).
    # (b) Mesmo nome+UF (Person+Person sem CPF ou com CPF unico).
    # (c) Subgrupo dentro de (b) por CPF: homonimos com CPFs distintos
    #     ficam separados.
    # NAO cobre nomes diferentes SEM CPF compartilhado — caso assim
    # precisa de fix no entity resolution upstream.

    def _latest_cargo_year(row: dict[str, Any]) -> int:
        props = row.get("properties") or {}
        anos = [
            int(k.rsplit("_", 1)[1])
            for k in props
            if k.startswith("cargo_tse_") and props.get(k) and k.rsplit("_", 1)[1].isdigit()
        ]
        return max(anos) if anos else 0

    def _rank(row: dict[str, Any]) -> tuple[int, int, int, float, str]:
        # Vencedor preferido: cargo mais recente (TSE eleicao mais nova) >
        # tem CPF (perfil mais rico no grafo) > label mais oficial >
        # maior score > id menor. Priorizar ano cobre o pedido da usuaria
        # de "deixar apenas o perfil mais recente" (ex: Prefeito 2024 ganha
        # de Deputado 2022 mesmo se a row Deputado tiver CPF).
        has_cpf = 0 if _person_cpf(row) else 1
        return (
            -_latest_cargo_year(row),
            has_cpf,
            _cluster_rank(row.get("labels") or []),
            -float(row.get("score") or 0.0),
            str(row.get("id")),
        )

    def _merge_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
        rows.sort(key=_rank)
        winner = rows[0]
        for loser in rows[1:]:
            winner.setdefault("cargos_relacionados", [])
            loser_format = _format_item(loser)
            if loser_format is not None and loser_format.detalhe:
                detalhe = loser_format.detalhe
                if detalhe not in winner["cargos_relacionados"]:
                    winner["cargos_relacionados"].append(detalhe)
            for cargo in loser.get("cargos_relacionados") or []:
                if cargo not in winner["cargos_relacionados"]:
                    winner["cargos_relacionados"].append(cargo)
        return winner

    # Aplica _merge_group nos clusters da fase 2 (CanonicalPerson) pra
    # acumular cargos_relacionados — sem isso, ALEGO StateLeg + Person
    # TSE no mesmo canonical reduzem pra 1 row mas perdem o cargo extra.
    for cluster_rows in by_cluster.values():
        deduped.append(_merge_group(cluster_rows))

    # (a) Pre-pass por CPF puro: rows com mesmo CPF colapsam mesmo
    # quando o nome diverge (StateLeg "X" + Person "X Y Z W").
    cpf_index: dict[str, list[dict[str, Any]]] = {}
    sem_cpf: list[dict[str, Any]] = []
    final_results: list[dict[str, Any]] = []
    for r in deduped:
        if _person_group_key(r) is None:
            final_results.append(r)
            continue
        cpf = _person_cpf(r)
        if cpf:
            cpf_index.setdefault(cpf, []).append(r)
        else:
            sem_cpf.append(r)

    cpf_winners: list[dict[str, Any]] = []
    for rows in cpf_index.values():
        cpf_winners.append(_merge_group(rows))

    # (b) Agrupamento por nome+UF dos vencedores de CPF + rows sem CPF.
    person_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in (*cpf_winners, *sem_cpf):
        key = _person_group_key(r)
        if key is None:
            final_results.append(r)
            continue
        person_groups.setdefault(key, []).append(r)

    for rows in person_groups.values():
        # Subgrupos por CPF: depois do pre-pass, rows com mesmo CPF ja
        # foram colapsadas. Aqui rows com CPFs distintos sao homonimos
        # reais — ficam separadas. Rows sem CPF se juntam ao unico
        # cluster com CPF se houver exatamente um.
        cpf_buckets: dict[str, list[dict[str, Any]]] = {}
        no_cpf: list[dict[str, Any]] = []
        for row in rows:
            cpf = _person_cpf(row)
            if cpf:
                cpf_buckets.setdefault(cpf, []).append(row)
            else:
                no_cpf.append(row)
        if len(cpf_buckets) == 1 and no_cpf:
            (only_cpf,) = cpf_buckets.keys()
            cpf_buckets[only_cpf].extend(no_cpf)
        elif not cpf_buckets:
            cpf_buckets[""] = no_cpf
        elif no_cpf:
            cpf_buckets["__none__"] = no_cpf

        for bucket_rows in cpf_buckets.values():
            if bucket_rows:
                final_results.append(_merge_group(bucket_rows))

    # Reranking final: classe de match (exato > prefix > tokens em ordem >
    # tokens fora de ordem > parcial) primeiro; só entao desempata pelo
    # score Lucene. Sem isso, "CESAR HENRIQUE ..." (2 tokens em ordem
    # trocada) empatava no Lucene com "HENRIQUE CEZAR PEREIRA" (3 tokens
    # na ordem certa, com CESAR↔CEZAR a 1 char) e o alvo correto caía
    # pra 7° lugar quando a usuaria buscava "Henrique César Pereira".
    def _final_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        relevance = _local_relevance(q, str(row.get("name") or ""))
        return (*relevance, -float(row.get("score") or 0.0), str(row.get("id")))

    items: list[BuscarTudoItem] = []
    for r in sorted(final_results, key=_final_sort_key):
        item = _format_item(r)
        if item is not None:
            cargos_extras = r.get("cargos_relacionados")
            if cargos_extras:
                item.cargos_relacionados = cargos_extras
            items.append(item)

    return BuscarTudoResponse(resultados=items, total=total, pagina=page)


@router.get("/politico/{entity_id}", response_model=PerfilPolitico)
async def pwa_politico(
    driver: Annotated[AsyncDriver, Depends(get_driver)],
    entity_id: Annotated[str, Path(min_length=1, max_length=200)],
) -> PerfilPolitico:
    """Perfil completo do político — orquestração via ``PerfilService``.

    Shape completo (22 campos top-level, ver :class:`PerfilPolitico`)
    reproduz o endpoint do Flask ``backend/app.py::perfil_politico``:
    conexões classificadas em 7 categorias, emendas, CEAP agregado por
    tipo, comparação com cidadão comum, alertas determinísticos,
    validação TSE e ``ProvenanceBlock`` no topo.

    Zero live-call — todos os dados vêm do grafo ingerido pelos
    pipelines ``camara_politicos_go``, ``emendas_parlamentares_go`` e
    cross-refs TSE/CGU. Fase 04.F da consolidação FastAPI (ver
    ``todo-list-prompts/very_high_priority/frontend/04F-perfil-service-
    endpoint.md``).
    """
    try:
        return await perfil_service.obter_perfil(driver, entity_id)
    except perfil_service.EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except perfil_service.DriverError as exc:
        raise HTTPException(status_code=502, detail=f"Erro: {exc}") from exc
