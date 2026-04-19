"""Entity resolution de políticos GO — cargo ↔ Person cross-label.

Problema
--------
O grafo acumula múltiplos nós pra mesma pessoa física. Exemplo real
medido em 2026-04-18: Jorge Kajuru aparece como 2 ``:Person`` (um com
CPF e sq_candidato, outro apenas com ``name``) + 1 ``:Senator``. A busca
PWA retorna só o primeiro ``:Person`` (partido antigo, sem foto) porque
nada liga os 3. Problema análogo atinge 17 ``:FederalLegislator`` e, quando
o pipeline ``alego`` rodar, ``:StateLegislator``. Ver a investigação em
``docs/entity_resolution.md``.

Estratégia
----------
Estratégia **C** (CanonicalPerson layer): este pipeline cria nós
``:CanonicalPerson`` representando a "pessoa real" e arestas ``:REPRESENTS``
apontando pros nós-fonte preservados. Proveniência por pipeline continua
intacta (nada é mergeado/deletado), e queries novas no grafo passam a
pivotar pela layer canônica.

Regras de matching (ordem decrescente de confiança; só a primeira que
resolve sem ambiguidade vence):

1. **cpf_exact** — ``:Person.cpf == :StateLegislator.cpf`` (dígitos,
   normalizados). Conf 1.00. ``:Senator`` e ``:FederalLegislator`` não
   entram aqui porque no grafo atual o primeiro não tem CPF e o segundo
   traz CPF mascarado vindo da Câmara.
2. **name_exact** — cargo.name normalizado (upper + sem acento, espaço
   colapsado) == Person.name normalizado, dentro do escopo ``uf='GO'``
   do Person. Ambiguidade (>1 Person GO com mesmo nome) vira audit-log
   e skip. Conf 0.95.
3. **name_stripped** — mesmo de (2) aplicado após tirar prefixos
   honoríficos (``DR. `` / ``DRA. `` / ``CEL. `` / ``DEP. `` / ``SEN. ``)
   e sufixos patronímicos (``JUNIOR`` / ``FILHO`` / ``NETO``) de qualquer
   ponta. Cobre "DR. ISMAEL ALEXANDRINO" ↔ "ISMAEL ALEXANDRINO JUNIOR".
   Conf 0.85.

Pós-resolução de cargos, tentamos **shadow attach**: ``:Person`` sem CPF,
sem UF (nós bare "só name" originados de referências em outros pipelines,
ex.: autores de inquéritos) com nome normalizado batendo exatamente com
UM dos nomes já presentes no cluster canônico → REPRESENTS adicional com
método ``shadow_name_exact`` (conf 0.80). Ambiguidade = skip+log.

Stop on ambiguidade é política do projeto (CLAUDE.md §3). Audit log em
``data/entity_resolution_politicos_go/audit_{run_id}.jsonl`` lista todos
os casos puláveis pra revisão humana.

Saída no grafo
--------------
Nó ``:CanonicalPerson`` com ``canonical_id`` estável por cluster. Prioridade
do canonical_id:

1. ``canon_senado_{id_senado}``
2. ``canon_camara_{id_camara}``
3. ``canon_alego_{legislator_id_digits}`` (pipeline ``alego``)
4. ``canon_cpf_{cpf_digits}`` (Person com CPF mas sem cargo ativo)

Props no nó (além de proveniência):

* ``display_name``: nome do cargo mais oficial (Senator > Fed > State >
  Person com CPF).
* ``cargo_ativo``: ``"senador"`` / ``"deputado_federal"`` /
  ``"deputado_estadual"`` / ``None``.
* ``uf``: sempre ``"GO"`` (escopo do pipeline).
* ``partido``: do cargo ativo (mais recente).
* ``num_sources``: tamanho do cluster.
* ``confidence_min``: menor confidence entre os REPRESENTS do cluster —
  útil pro frontend sinalizar "match com dúvida".

Arestas ``:REPRESENTS`` (1 por nó-fonte), direcionadas
``(:CanonicalPerson)-[:REPRESENTS]->(sourceNode)``. Props:

* ``method``: ``"cpf_exact" | "name_exact" | "name_stripped" |
  "shadow_name_exact" | "cargo_root"``.
* ``confidence``: float [0, 1].
* Proveniência do próprio pipeline (source_id, run_id, source_url,
  ingested_at, source_record_id).

Idempotência
------------
``MERGE`` em ``canonical_id`` + ``MERGE`` em ``(canonical)-[r:REPRESENTS]->
(source)``. Re-runs atualizam props (``SET r.method = ...``) mas não
duplicam. Pipelines-fonte (tse, senado_senadores_foto, camara_politicos_go,
alego) são **desacoplados**: rodar este pipeline não altera os nós-fonte.

Sem archival: este pipeline não busca dados externos. ``source_url`` é
o próprio código do pipeline no repo público — honesto: a "fonte"
desta derivação *é* a lógica de resolução versionada em git.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


_SOURCE_ID = "entity_resolution_politicos_go"

# Escopo GO-only (alinha com o produto Fiscal Cidadão). Cargos Senate,
# Fed, State: os 3 labels que este pipeline liga a Person.
_TARGET_UF = "GO"

# Prefixos honoríficos/de ocupação que aparecem em nomes de campanha
# ("DR. ISMAEL ALEXANDRINO") mas não constam do registro TSE canônico
# ("ISMAEL ALEXANDRINO JUNIOR"). Removidos na fase ``name_stripped``.
# Inclui ponto opcional e variações masculino/feminino.
_HONORIFIC_PREFIXES = frozenset({
    "DR", "DRA", "DR.", "DRA.",
    "PROF", "PROFA", "PROF.", "PROFA.",
    "CEL", "CEL.", "GEN", "GEN.", "SGT", "SGT.",
    "DEP", "DEP.", "SEN", "SEN.", "VER", "VER.",
    "PASTOR", "PADRE", "IRMAO", "IRMÃO", "DELEGADO", "DELEGADA",
})
# Sufixos patronímicos (cargo registro TSE "MARCONI PERILLO JUNIOR"
# x label social "MARCONI PERILLO"). Replica _HONORIFIC_SUFFIXES do
# pipeline wikidata_politicos_foto; fonte unificada ficaria boa como
# follow-up.
_HONORIFIC_SUFFIXES = frozenset({
    "JUNIOR", "JR", "FILHO", "NETO", "SOBRINHO", "SEGUNDO",
})

_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")
_MULTI_SPACE = re.compile(r"\s+")

# Cargo ranking — quem "ganha" como display_name quando um cluster tem
# múltiplas fontes. Mais oficial primeiro.
_CARGO_RANK: dict[str, int] = {
    "Senator": 0,
    "FederalLegislator": 1,
    "StateLegislator": 2,
    "Person": 3,
}

_CARGO_ATIVO_LABEL: dict[str, str] = {
    "Senator": "senador",
    "FederalLegislator": "deputado_federal",
    "StateLegislator": "deputado_estadual",
}


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_name(raw: str | None) -> str:
    """Upper + sem acento + sem pontuação + whitespace colapsado."""
    if not raw:
        return ""
    base = _strip_accents(str(raw)).upper()
    base = _NON_ALNUM.sub(" ", base)
    return _MULTI_SPACE.sub(" ", base).strip()


def _strip_honorifics(normalized: str) -> str:
    """Remove honoríficos/sufixos das pontas (já com ``_normalize_name``).

    - Prefixos: "DR", "DRA", "PROF", "CEL", "DEP", "SEN", "VER", "PASTOR",
      etc. — só da primeira palavra (evita tirar "DEP" do meio do nome).
    - Sufixos: "JUNIOR", "JR", "FILHO", "NETO" — só da última palavra.

    Os conjuntos cobrem os 2 deputados federais GO cujo nome de campanha
    diverge do TSE ("DR. ISMAEL ALEXANDRINO" → "ISMAEL ALEXANDRINO";
    pareado via ``name_stripped`` contra "ISMAEL ALEXANDRINO JUNIOR"
    depois que este também perde o "JUNIOR").
    """
    if not normalized:
        return ""
    parts = normalized.split(" ")
    # Prefix strip — até 2 tokens honoríficos encadeados ("DR CEL ...").
    while parts and parts[0] in _HONORIFIC_PREFIXES:
        parts.pop(0)
    # Suffix strip — última palavra.
    while parts and parts[-1] in _HONORIFIC_SUFFIXES:
        parts.pop()
    return " ".join(parts)


def _digits_only(raw: str | None) -> str:
    if not raw:
        return ""
    return "".join(ch for ch in str(raw) if ch.isdigit())


def _is_masked_cpf(raw: str | None) -> bool:
    """Retorna True se o CPF tem ``*`` — format de mascaramento LGPD.

    ``camara_deputados`` grava CPF mascarado (`***.***.*31-53`). Não dá
    pra comparar com CPFs plenos do TSE — pulamos esses cases no path
    ``cpf_exact`` (vão pro ``name_*`` depois).
    """
    return bool(raw) and "*" in str(raw)


# Cypher: puxa todos os nós candidatos pro ER — os 3 cargos GO +
# Persons GO (com UF=GO) + shadow Persons (UF IS NULL, CPF IS NULL,
# só ``name``).  Formato flat pra simplificar parsing no Python.
_DISCOVERY_QUERY = """
CALL {
    MATCH (n:Senator)
    WHERE coalesce(n.uf, 'GO') = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.senator_id AS stable_key,
           n.id_senado AS id_senado,
           NULL AS id_camara,
           NULL AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           coalesce(n.uf, $target_uf) AS uf
UNION ALL
    MATCH (n:FederalLegislator)
    WHERE coalesce(n.uf, 'GO') = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.legislator_id AS stable_key,
           NULL AS id_senado,
           n.id_camara AS id_camara,
           n.legislator_id AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           coalesce(n.uf, $target_uf) AS uf
UNION ALL
    MATCH (n:StateLegislator)
    WHERE coalesce(n.uf, 'GO') = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.legislator_id AS stable_key,
           NULL AS id_senado,
           NULL AS id_camara,
           n.legislator_id AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           coalesce(n.uf, $target_uf) AS uf
UNION ALL
    MATCH (n:Person)
    WHERE n.uf = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           coalesce(n.cpf, n.name) AS stable_key,
           NULL AS id_senado,
           NULL AS id_camara,
           NULL AS legislator_id,
           n.sq_candidato AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           n.uf AS uf
UNION ALL
    MATCH (n:Person)
    WHERE n.uf IS NULL AND n.cpf IS NULL AND coalesce(n.name, '') <> ''
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.name AS stable_key,
           NULL AS id_senado,
           NULL AS id_camara,
           NULL AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           NULL AS cpf,
           NULL AS partido,
           NULL AS uf
}
RETURN labels, element_id, stable_key, id_senado, id_camara,
       legislator_id, sq_candidato, name, cpf, partido, uf
"""


def _primary_label(labels: list[str]) -> str:
    """Pega a label "mais específica" (menor rank no ``_CARGO_RANK``)."""
    known = [label for label in labels if label in _CARGO_RANK]
    if not known:
        return labels[0] if labels else "Person"
    return min(known, key=lambda lbl: _CARGO_RANK[lbl])


def _display_source_label(canonical: dict[str, Any]) -> str:
    """Label equivalente ao ``cargo_ativo`` atual do canonical node.

    Usado pra decidir se um novo nó-fonte desbanca o display_name
    corrente: Senator bate qualquer cargo; Fed bate State/Person; etc.
    """
    reverse = {v: k for k, v in _CARGO_ATIVO_LABEL.items()}
    cargo = canonical.get("cargo_ativo")
    if cargo and cargo in reverse:
        return reverse[cargo]
    return "Person"


def _canonical_id_for(primary_label: str, node: dict[str, Any]) -> str:
    """Deriva ``canonical_id`` estável a partir do nó âncora do cluster.

    Ordem:
    1. Senator → ``canon_senado_{id_senado}``.
    2. FederalLegislator → ``canon_camara_{id_camara}``.
    3. StateLegislator → ``canon_alego_{digits(legislator_id) or legislator_id}``.
    4. Person com CPF pleno → ``canon_cpf_{digits(cpf)}``.

    Person shadow (só name) nunca vira âncora — é sempre anexado a um
    cluster existente via shadow attach. Se escaparmos aqui, fica no
    audit-log como "shadow sem cluster".
    """
    if primary_label == "Senator" and node.get("id_senado"):
        return f"canon_senado_{node['id_senado']}"
    if primary_label == "FederalLegislator" and node.get("id_camara"):
        return f"canon_camara_{node['id_camara']}"
    if primary_label == "StateLegislator" and node.get("legislator_id"):
        leg_id = str(node["legislator_id"])
        digits = _digits_only(leg_id) or leg_id.replace(" ", "_")
        return f"canon_alego_{digits}"
    if primary_label == "Person":
        cpf_digits = _digits_only(node.get("cpf"))
        if cpf_digits and cpf_digits != "00000000000":
            return f"canon_cpf_{cpf_digits}"
    # Fallback defensivo (nunca deveria ocorrer dado o filtro de
    # ``extract``; se ocorrer, o pipeline levanta no transform pra não
    # criar canonical_id instável).
    raise ValueError(
        f"no stable canonical_id for label={primary_label} node={node}",
    )


class EntityResolutionPoliticosGoPipeline(Pipeline):
    """Liga ``:Senator`` / ``:FederalLegislator`` / ``:StateLegislator`` ↔ ``:Person``.

    Lê o grafo uma vez, aplica regras determinísticas de matching,
    grava ``:CanonicalPerson`` + ``:REPRESENTS``. Sem fetch externo.

    Cadência recomendada: diária ou sempre que um pipeline de cargo
    rodar (esquecer é de baixo risco — os nós-fonte continuam no grafo;
    só a camada canônica fica desatualizada até a próxima run).
    """

    name = "entity_resolution_politicos_go"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        # Nós-fonte lidos do grafo, separados por primary label.
        self._nodes_by_label: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # Canonical clusters finais:
        # canonical_id → {"canonical": {...}, "edges": [rel_row, ...]}
        self._clusters: dict[str, dict[str, Any]] = {}
        self._audit_entries: list[dict[str, Any]] = []
        self.canonical_rows: list[dict[str, Any]] = []
        self.represents_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # extract — lê nós do grafo
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Puxa cargos + Persons GO + shadow Persons do grafo."""
        with self.driver.session(database=self.neo4j_database) as session:
            result = session.run(_DISCOVERY_QUERY, {"target_uf": _TARGET_UF})
            rows = [dict(record) for record in result]

        for row in rows:
            labels = list(row.get("labels") or [])
            primary = _primary_label(labels)
            node = {
                "labels": labels,
                "primary_label": primary,
                "element_id": row.get("element_id"),
                "stable_key": row.get("stable_key"),
                "id_senado": row.get("id_senado"),
                "id_camara": row.get("id_camara"),
                "legislator_id": row.get("legislator_id"),
                "sq_candidato": row.get("sq_candidato"),
                "name": row.get("name"),
                "cpf": row.get("cpf"),
                "partido": row.get("partido"),
                "uf": row.get("uf"),
                "name_normalized": _normalize_name(row.get("name")),
            }
            node["name_stripped"] = _strip_honorifics(
                str(node["name_normalized"] or ""),
            )
            self._nodes_by_label[primary].append(node)

        self.rows_in = sum(len(v) for v in self._nodes_by_label.values())
        logger.info(
            "[%s] extracted: %d senators, %d federal, %d state, %d persons GO, %d shadow",
            self.name,
            len(self._nodes_by_label.get("Senator", [])),
            len(self._nodes_by_label.get("FederalLegislator", [])),
            len(self._nodes_by_label.get("StateLegislator", [])),
            sum(1 for n in self._nodes_by_label.get("Person", []) if n["uf"] == _TARGET_UF),
            sum(1 for n in self._nodes_by_label.get("Person", []) if not n["uf"]),
        )

    # ------------------------------------------------------------------
    # transform — aplica regras de matching e monta clusters
    # ------------------------------------------------------------------

    def transform(self) -> None:
        persons_go = [
            n for n in self._nodes_by_label.get("Person", []) if n["uf"] == _TARGET_UF
        ]
        persons_shadow = [
            n for n in self._nodes_by_label.get("Person", []) if not n["uf"]
        ]

        # Índices pra lookup eficiente.
        persons_by_cpf: dict[str, list[dict[str, Any]]] = defaultdict(list)
        persons_by_name_norm: dict[str, list[dict[str, Any]]] = defaultdict(list)
        persons_by_name_stripped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        name_norm_counts: Counter[str] = Counter()
        for person in persons_go:
            cpf_digits = _digits_only(person["cpf"])
            if cpf_digits and cpf_digits != "00000000000":
                persons_by_cpf[cpf_digits].append(person)
            if person["name_normalized"]:
                persons_by_name_norm[person["name_normalized"]].append(person)
                name_norm_counts[person["name_normalized"]] += 1
            if person["name_stripped"]:
                persons_by_name_stripped[person["name_stripped"]].append(person)

        # Rastreia quais Persons já foram anexados a algum cluster.
        person_elt_ids_in_cluster: set[str] = set()

        # ---- Fase 1: cada cargo vira um cluster; tenta anexar Person ----
        for cargo_label in ("Senator", "FederalLegislator", "StateLegislator"):
            for cargo in self._nodes_by_label.get(cargo_label, []):
                self._resolve_cargo(
                    cargo,
                    persons_by_cpf=persons_by_cpf,
                    persons_by_name_norm=persons_by_name_norm,
                    persons_by_name_stripped=persons_by_name_stripped,
                    name_norm_counts=name_norm_counts,
                    claimed=person_elt_ids_in_cluster,
                )

        # ---- Fase 2: shadow attach ----
        # Pra cada shadow, tenta anexar a um cluster existente por nome normalizado.
        cluster_names: dict[str, list[str]] = defaultdict(list)
        for canonical_id, cluster in self._clusters.items():
            for edge in cluster["edges"]:
                src_name = edge.get("_source_name_norm") or ""
                if src_name:
                    cluster_names[src_name].append(canonical_id)

        for shadow in persons_shadow:
            name_norm = shadow["name_normalized"]
            if not name_norm:
                continue
            candidate_ids = cluster_names.get(name_norm, [])
            # Dedup — mesma canonical pode ter >1 source com nome igual.
            unique_canonicals = sorted(set(candidate_ids))
            if len(unique_canonicals) == 1:
                self._attach_source(
                    canonical_id=unique_canonicals[0],
                    node=shadow,
                    method="shadow_name_exact",
                    confidence=0.80,
                )
            elif len(unique_canonicals) > 1:
                self._audit_entries.append({
                    "type": "shadow_ambiguous",
                    "shadow_element_id": shadow["element_id"],
                    "shadow_name": shadow["name"],
                    "candidate_canonicals": unique_canonicals,
                })
            # else: shadow sem match — não vira cluster próprio (só name é
            # pouco pra criar entidade canônica nova). Cai pro audit.
            else:
                self._audit_entries.append({
                    "type": "shadow_no_match",
                    "shadow_element_id": shadow["element_id"],
                    "shadow_name": shadow["name"],
                })

        # ---- Finaliza: materializa rows pra Neo4jBatchLoader ----
        for cluster in self._clusters.values():
            self.canonical_rows.append(cluster["canonical"])
            self.represents_rels.extend(cluster["edges"])

        # Drop campos de trabalho que não vão pro grafo.
        for edge in self.represents_rels:
            edge.pop("_source_name_norm", None)

        self.rows_loaded = len(self.canonical_rows) + len(self.represents_rels)
        logger.info(
            "[%s] transformed: %d canonical clusters, %d REPRESENTS edges, %d audit entries",
            self.name,
            len(self.canonical_rows),
            len(self.represents_rels),
            len(self._audit_entries),
        )

    def _resolve_cargo(
        self,
        cargo: dict[str, Any],
        *,
        persons_by_cpf: dict[str, list[dict[str, Any]]],
        persons_by_name_norm: dict[str, list[dict[str, Any]]],
        persons_by_name_stripped: dict[str, list[dict[str, Any]]],
        name_norm_counts: Counter[str],
        claimed: set[str],
    ) -> None:
        """Cria cluster pro cargo e anexa Person(GO) se match conservador existir."""
        primary_label = cargo["primary_label"]
        try:
            canonical_id = _canonical_id_for(primary_label, cargo)
        except ValueError as exc:
            # Cargo sem stable key — carga parcial do pipeline-fonte. Skip
            # e registra no audit pra o operador olhar.
            self._audit_entries.append({
                "type": "cargo_no_stable_key",
                "element_id": cargo["element_id"],
                "label": primary_label,
                "reason": str(exc),
            })
            return

        # Cluster vazio inicial (canonical + 1 edge pro cargo).
        canonical = self._build_canonical_row(canonical_id, cargo)
        self._clusters[canonical_id] = {"canonical": canonical, "edges": []}
        self._attach_source(
            canonical_id=canonical_id,
            node=cargo,
            method="cargo_root",
            confidence=1.00,
        )

        # Tenta anexar Person via cpf_exact / name_exact / name_stripped.
        matched_person: dict[str, Any] | None = None
        method: str | None = None
        confidence: float | None = None

        cargo_cpf_digits = (
            "" if _is_masked_cpf(cargo.get("cpf"))
            else _digits_only(cargo.get("cpf"))
        )
        if cargo_cpf_digits and cargo_cpf_digits != "00000000000":
            hits = [
                p for p in persons_by_cpf.get(cargo_cpf_digits, [])
                if p["element_id"] not in claimed
            ]
            if len(hits) == 1:
                matched_person, method, confidence = hits[0], "cpf_exact", 1.00
            elif len(hits) > 1:
                self._audit_entries.append({
                    "type": "cargo_cpf_ambiguous",
                    "cargo_element_id": cargo["element_id"],
                    "cargo_label": primary_label,
                    "cargo_name": cargo["name"],
                    "cpf_digits": cargo_cpf_digits,
                    "person_candidates": [p["element_id"] for p in hits],
                })

        if matched_person is None:
            name_norm = cargo["name_normalized"]
            if name_norm:
                hits = [
                    p for p in persons_by_name_norm.get(name_norm, [])
                    if p["element_id"] not in claimed
                ]
                if len(hits) == 1:
                    matched_person, method, confidence = hits[0], "name_exact", 0.95
                elif len(hits) > 1:
                    disambiguated = self._disambiguate_by_partido(cargo, hits)
                    if disambiguated is not None:
                        matched_person = disambiguated
                        method, confidence = "name_exact_partido", 0.90
                    else:
                        self._audit_entries.append({
                            "type": "cargo_name_ambiguous",
                            "cargo_element_id": cargo["element_id"],
                            "cargo_label": primary_label,
                            "cargo_name": cargo["name"],
                            "candidates": [p["element_id"] for p in hits],
                        })

        if matched_person is None:
            stripped = cargo["name_stripped"]
            if stripped and stripped != cargo["name_normalized"]:
                # Matching cruzado: stripped(cargo) vs stripped(person).
                hits = [
                    p for p in persons_by_name_stripped.get(stripped, [])
                    if p["element_id"] not in claimed
                ]
                if len(hits) == 1:
                    matched_person, method, confidence = hits[0], "name_stripped", 0.85
                elif len(hits) > 1:
                    self._audit_entries.append({
                        "type": "cargo_stripped_ambiguous",
                        "cargo_element_id": cargo["element_id"],
                        "cargo_label": primary_label,
                        "cargo_name": cargo["name"],
                        "cargo_stripped": stripped,
                        "candidates": [p["element_id"] for p in hits],
                    })

        if matched_person is None:
            # Cargo sem Person pareável — cluster fica com 1 só source
            # (ainda útil: enfileira foto, partido atual, etc.). Loga
            # pra auditoria saber que não achamos histórico TSE.
            self._audit_entries.append({
                "type": "cargo_without_person",
                "cargo_element_id": cargo["element_id"],
                "cargo_label": primary_label,
                "cargo_name": cargo["name"],
            })
            return

        assert method is not None and confidence is not None  # noqa: S101
        self._attach_source(
            canonical_id=canonical_id,
            node=matched_person,
            method=method,
            confidence=confidence,
        )
        claimed.add(matched_person["element_id"])

    def _disambiguate_by_partido(
        self,
        cargo: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Reduz candidates por partido do cargo; retorna único vencedor ou None."""
        cargo_partido = str(cargo.get("partido") or "").strip().upper()
        if not cargo_partido:
            return None
        matching = [
            p for p in candidates
            if str(p.get("partido") or "").strip().upper() == cargo_partido
        ]
        if len(matching) == 1:
            return matching[0]
        return None

    def _build_canonical_row(
        self,
        canonical_id: str,
        cargo: dict[str, Any],
    ) -> dict[str, Any]:
        """Monta o row do ``:CanonicalPerson`` a partir do cargo âncora."""
        primary_label = cargo["primary_label"]
        cargo_ativo = _CARGO_ATIVO_LABEL.get(primary_label)
        record_url = self._get_primary_url()
        return self.attach_provenance(
            {
                "canonical_id": canonical_id,
                "display_name": str(cargo.get("name") or ""),
                "uf": _TARGET_UF,
                "partido": (
                    str(cargo.get("partido")) if cargo.get("partido") else None
                ),
                "cargo_ativo": cargo_ativo,
                "num_sources": 0,  # atualizado em _attach_source
                "confidence_min": 1.0,
            },
            record_id=canonical_id,
            record_url=record_url,
        )

    def _attach_source(
        self,
        *,
        canonical_id: str,
        node: dict[str, Any],
        method: str,
        confidence: float,
    ) -> None:
        """Anexa um nó-fonte ao cluster via REPRESENTS + atualiza canonical."""
        cluster = self._clusters.get(canonical_id)
        if cluster is None:
            raise KeyError(f"cluster {canonical_id} não existe — chamar _resolve_cargo antes")

        element_id = node["element_id"]
        target_label = node["primary_label"]
        source_name_norm = node["name_normalized"]
        record_url = self._get_primary_url()

        record_id = f"{canonical_id}|{element_id}"
        edge = self.attach_provenance(
            {
                "source_key": canonical_id,  # canonical lado A
                # target_key só preservado pro enforce_provenance do
                # loader; o Cypher custom usa target_element_id.
                "target_key": element_id,
                "target_label": target_label,
                "target_element_id": element_id,
                "method": method,
                "confidence": float(confidence),
                "_source_name_norm": source_name_norm,  # só p/ fase 2
            },
            record_id=record_id,
            record_url=record_url,
        )
        cluster["edges"].append(edge)

        # Atualiza props agregadas do canonical.
        canonical = cluster["canonical"]
        canonical["num_sources"] = len(cluster["edges"])
        # min() ignora o campo inicial 1.0 do cargo root.
        canonical["confidence_min"] = min(
            canonical.get("confidence_min", 1.0), float(confidence),
        )
        # Display name: escolhe o da label mais oficial que entrou.
        if _CARGO_RANK.get(target_label, 99) < _CARGO_RANK.get(
            _display_source_label(canonical), 99,
        ):
            canonical["display_name"] = str(node.get("name") or canonical.get("display_name"))
            if node.get("partido"):
                canonical["partido"] = str(node["partido"])

    # ------------------------------------------------------------------
    # load — persiste no grafo + grava audit log
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.canonical_rows:
            logger.warning("[%s] nothing to load", self.name)
            self._write_audit_log()
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes(
            "CanonicalPerson",
            self.canonical_rows,
            key_field="canonical_id",
        )
        if self.represents_rels:
            # Target é dinâmico (Senator/Fed/State/Person) e
            # ``:Person`` não tem chave de propriedade estável universal
            # — usar elementId é o único caminho uniforme. O loader
            # genérico só aceita ``{prop: v}`` no MATCH, então montamos
            # o Cypher aqui direto.
            loader.run_query_with_retry(
                _REPRESENTS_MERGE_QUERY,
                self.represents_rels,
            )
        self._write_audit_log()

    def _write_audit_log(self) -> None:
        """Grava ``data/entity_resolution_politicos_go/audit_{run_id}.jsonl``."""
        audit_dir = Path(self.data_dir) / _SOURCE_ID
        audit_dir.mkdir(parents=True, exist_ok=True)
        path = audit_dir / f"audit_{self.run_id}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for entry in self._audit_entries:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info(
            "[%s] wrote %d audit entries to %s",
            self.name, len(self._audit_entries), path,
        )


# Cypher que cria/atualiza ``(:CanonicalPerson)-[:REPRESENTS]->(source)``
# em lote. Match do source é por ``elementId`` porque é a única chave
# uniformemente presente (:Person não tem senator_id nem legislator_id
# e CPF pode estar ausente/mascarado). O loader genérico
# ``load_relationships`` só suporta ``{prop: v}`` no MATCH, então este
# query fica inline.
_REPRESENTS_MERGE_QUERY = """
UNWIND $rows AS row
MATCH (cp:CanonicalPerson {canonical_id: row.source_key})
MATCH (src) WHERE elementId(src) = row.target_element_id
MERGE (cp)-[r:REPRESENTS]->(src)
SET r.method = row.method,
    r.confidence = row.confidence,
    r.source_id = row.source_id,
    r.source_record_id = row.source_record_id,
    r.source_url = row.source_url,
    r.ingested_at = row.ingested_at,
    r.run_id = row.run_id,
    r.target_label = row.target_label
"""
