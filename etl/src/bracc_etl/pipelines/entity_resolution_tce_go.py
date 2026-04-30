"""Entity resolution dos Person stubs do TCE-GO contra :CanonicalPerson.

Phase 3 do scraper TCE-GO (ver `todo-list-prompts/.../tce-go-qlik-scraper.md`
e `.../11-tce-go-irregulares-link-canonicalperson.md`).

Phase 2 do scraper cria ~120 ``:Person`` stubs com ``source='tce_go_irregulares'``
+ ``cpf`` (XXX.XXX.XXX-XX) + ``name`` a partir dos PDFs de Contas Julgadas
Irregulares — cada um já tem ``IMPEDIDO_TCE_GO`` ligando-o ao
``TceGoIrregularAccount`` correspondente. Mas os stubs ficam **desconectados
do grafo político existente**: o perfil de um deputado/vereador que ALÉM
de político também teve conta julgada irregular como servidor estadual
não vê esse fato porque o ``:CanonicalPerson`` dele não tem aresta pra
``TceGoIrregularAccount``; só o ``:Person`` stub do TCE-GO tem.

Este pipeline resolve isso criando arestas
``(:CanonicalPerson)-[:REPRESENTS]->(:Person {source:'tce_go_irregulares'})``,
seguindo o padrão de ``entity_resolution_politicos_go``. Queries de perfil
que pivotam pelo cluster canônico passam a hop pro Person stub e descobrem
o ``IMPEDIDO_TCE_GO`` → ``TceGoIrregularAccount``.

Estratégia
----------

1. **cpf_exact** (default ON, conf 1.0) — CPF do stub (dígitos
   normalizados) bate com CPF de algum nó-fonte de um cluster canônico
   existente (Senator/FederalLegislator/StateLegislator/Person GO). Match
   1-pra-1 — múltiplos clusters com mesmo CPF é estado inválido do grafo
   (entity_resolution_politicos_go já garantiria 1 cluster por CPF), mas
   defendemos com audit + skip se ocorrer.

2. **name_exact** (opt-in via ``enable_name_tier=True``, conf 0.7) — pra
   stubs sem match Tier 1, casa nome normalizado do stub contra
   ``display_name`` normalizado de algum cluster. Skip em ambiguidade
   (>1 cluster com mesmo nome). Default OFF porque homonímia no Brasil
   é alta — Tier 2 exige curadoria humana antes de ser ligado.

3. **cpf_masked** — não tentar resolver. Por design upstream, stubs com
   CPF mascarado nunca são criados (ver ``pipelines/tce_go.py`` _transform_irregular,
   ``elif cpf_fmt:`` só dispara quando o CPF tem 11 dígitos). A chave
   parcial dos PDFs (mascaramento LGPD pós-2022) deixa o ``TceGoIrregularAccount``
   sem aresta saindo de Person — fica isolado, documentado.

Idempotência
------------

``MERGE`` em ``(canonical)-[r:REPRESENTS]->(stub)`` por ``elementId(stub)``.
Re-runs não duplicam arestas. Provenance fields (``source_id``,
``source_record_id``, ``source_url``, ``ingested_at``, ``run_id``) são
re-escritos a cada run, mantendo o último run_id como o canônico.

Sem fetch externo. ``source_url`` aponta pro próprio código deste pipeline
no GitHub — a "fonte" desta derivação É a lógica versionada em git.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


_SOURCE_ID = "entity_resolution_tce_go"
_TARGET_UF = "GO"

_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")
_MULTI_SPACE = re.compile(r"\s+")


def _digits_only(raw: str | None) -> str:
    if not raw:
        return ""
    return "".join(ch for ch in str(raw) if ch.isdigit())


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_name(raw: str | None) -> str:
    """Upper + sem acento + sem pontuação + whitespace colapsado.

    Mesma normalização de ``entity_resolution_politicos_go`` — copia ao
    invés de importar pra manter os 2 pipelines desacoplados (um pode
    evoluir sem alterar o outro).
    """
    if not raw:
        return ""
    base = _strip_accents(str(raw)).upper()
    base = _NON_ALNUM.sub(" ", base)
    return _MULTI_SPACE.sub(" ", base).strip()


# Lê os Person stubs do TCE-GO + o índice ``cpf_digits → canonical_id``
# derivado dos clusters existentes. ``elementId(stub)`` é a chave de
# escrita no MERGE final (Person stub não tem chave de propriedade
# universal — ``cpf`` está presente mas o MATCH genérico do
# ``load_relationships`` só aceita ``{prop: v}``, então usamos elementId
# direto como em ``entity_resolution_politicos_go``).
_DISCOVERY_QUERY = """
CALL () {
    MATCH (n:Person {source: 'tce_go_irregulares'})
    WHERE n.cpf IS NOT NULL AND n.cpf <> ''
    RETURN 'stub' AS kind,
           elementId(n) AS element_id,
           n.cpf AS cpf,
           n.name AS name,
           NULL AS canonical_id,
           NULL AS display_name
UNION ALL
    MATCH (cp:CanonicalPerson)-[:REPRESENTS]->(src)
    WHERE src.cpf IS NOT NULL AND src.cpf <> ''
      AND coalesce(cp.uf, $target_uf) = $target_uf
    RETURN 'cluster_cpf' AS kind,
           elementId(src) AS element_id,
           src.cpf AS cpf,
           cp.display_name AS name,
           cp.canonical_id AS canonical_id,
           cp.display_name AS display_name
UNION ALL
    MATCH (cp:CanonicalPerson)
    WHERE coalesce(cp.uf, $target_uf) = $target_uf
      AND cp.display_name IS NOT NULL AND cp.display_name <> ''
    RETURN 'cluster_name' AS kind,
           NULL AS element_id,
           NULL AS cpf,
           cp.display_name AS name,
           cp.canonical_id AS canonical_id,
           cp.display_name AS display_name
}
RETURN kind, element_id, cpf, name, canonical_id, display_name
"""


# Cypher de escrita: liga ``CanonicalPerson`` ao Person stub via
# elementId. Espelha ``_REPRESENTS_MERGE_QUERY`` de
# ``entity_resolution_politicos_go`` — formato unificado pra que o
# frontend possa traversar REPRESENTS sem se importar com qual pipeline
# escreveu a aresta.
_REPRESENTS_MERGE_QUERY = """
UNWIND $rows AS row
MATCH (cp:CanonicalPerson {canonical_id: row.canonical_id})
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


class EntityResolutionTceGoPipeline(Pipeline):
    """Linka :Person stubs do TCE-GO (Phase 2) a :CanonicalPerson existentes.

    Tier 1 (CPF) é default. Tier 2 (nome) é opt-in via
    ``enable_name_tier=True`` — homonímia no Brasil exige curadoria
    humana antes de promover matches por nome a evidência de identidade.

    Cadência recomendada: depois de ``tce_go`` (que cria os stubs) E
    de ``entity_resolution_politicos_go`` (que cria os clusters
    canônicos). Re-rodar com qualquer frequência: idempotente.
    """

    name = "entity_resolution_tce_go"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        enable_name_tier: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        self.enable_name_tier = enable_name_tier
        self._stubs: list[dict[str, Any]] = []
        # cpf_digits → canonical_id (1-pra-1; múltiplos = audit).
        self._cluster_by_cpf: dict[str, str] = {}
        self._cluster_cpf_collisions: set[str] = set()
        # name_normalized → set(canonical_id) (skip em ambiguidade).
        self._cluster_by_name: dict[str, set[str]] = {}
        self.represents_rels: list[dict[str, Any]] = []
        self._audit_entries: list[dict[str, Any]] = []
        self._stats = {
            "stubs_total": 0,
            "matched_cpf": 0,
            "matched_name": 0,
            "unmatched": 0,
            "cpf_collisions": 0,
            "name_ambiguous": 0,
        }

    # ------------------------------------------------------------------
    # extract — lê stubs + índice de clusters do grafo
    # ------------------------------------------------------------------

    def extract(self) -> None:
        with self.driver.session(database=self.neo4j_database) as session:
            result = session.run(_DISCOVERY_QUERY, {"target_uf": _TARGET_UF})
            rows = [dict(record) for record in result]

        for row in rows:
            kind = row.get("kind")
            if kind == "stub":
                self._stubs.append({
                    "element_id": row["element_id"],
                    "cpf": row.get("cpf") or "",
                    "name": row.get("name") or "",
                })
            elif kind == "cluster_cpf":
                cpf_digits = _digits_only(row.get("cpf"))
                cid = row.get("canonical_id")
                if not cpf_digits or not cid:
                    continue
                existing = self._cluster_by_cpf.get(cpf_digits)
                if existing and existing != cid:
                    # 2 clusters reivindicando o mesmo CPF — estado
                    # inválido upstream. Marca pra skip + audit; não
                    # tenta adivinhar qual é o "certo".
                    self._cluster_cpf_collisions.add(cpf_digits)
                else:
                    self._cluster_by_cpf[cpf_digits] = cid
            elif kind == "cluster_name":
                name_norm = _normalize_name(row.get("display_name"))
                cid = row.get("canonical_id")
                if not name_norm or not cid:
                    continue
                self._cluster_by_name.setdefault(name_norm, set()).add(cid)

        self.rows_in = len(self._stubs)
        self._stats["stubs_total"] = len(self._stubs)
        logger.info(
            "[%s] extracted: %d stubs, %d cpf-indexed clusters, %d name-indexed clusters",
            self.name,
            len(self._stubs),
            len(self._cluster_by_cpf),
            len(self._cluster_by_name),
        )

    # ------------------------------------------------------------------
    # transform — aplica Tier 1 e (opcional) Tier 2
    # ------------------------------------------------------------------

    def transform(self) -> None:
        for stub in self._stubs:
            cpf_digits = _digits_only(stub["cpf"])
            if cpf_digits and cpf_digits in self._cluster_cpf_collisions:
                self._audit_entries.append({
                    "type": "cluster_cpf_collision",
                    "stub_element_id": stub["element_id"],
                    "stub_name": stub["name"],
                    "cpf_digits": cpf_digits,
                })
                self._stats["cpf_collisions"] += 1
                self._stats["unmatched"] += 1
                continue

            cid = self._cluster_by_cpf.get(cpf_digits) if cpf_digits else None
            if cid:
                self._emit_edge(stub, cid, method="tce_go_cpf_exact", confidence=1.0)
                self._stats["matched_cpf"] += 1
                continue

            if self.enable_name_tier:
                name_norm = _normalize_name(stub["name"])
                candidates = self._cluster_by_name.get(name_norm) or set()
                if len(candidates) == 1:
                    self._emit_edge(
                        stub,
                        next(iter(candidates)),
                        method="tce_go_name_exact",
                        confidence=0.7,
                    )
                    self._stats["matched_name"] += 1
                    continue
                if len(candidates) > 1:
                    self._audit_entries.append({
                        "type": "name_ambiguous",
                        "stub_element_id": stub["element_id"],
                        "stub_name": stub["name"],
                        "candidate_canonical_ids": sorted(candidates),
                    })
                    self._stats["name_ambiguous"] += 1
                    self._stats["unmatched"] += 1
                    continue

            self._audit_entries.append({
                "type": "no_match",
                "stub_element_id": stub["element_id"],
                "stub_name": stub["name"],
                "stub_cpf_digits": cpf_digits,
            })
            self._stats["unmatched"] += 1

    def _emit_edge(
        self,
        stub: dict[str, Any],
        canonical_id: str,
        *,
        method: str,
        confidence: float,
    ) -> None:
        # ``record_id`` é o cpf do stub (chave natural do Person stub no
        # pipeline ``tce_go``) — mantém a cadeia de proveniência
        # rastreável pra deep-link no GitHub.
        edge = self.attach_provenance(
            {
                "canonical_id": canonical_id,
                "target_element_id": stub["element_id"],
                "method": method,
                "confidence": float(confidence),
                "target_label": "Person",
            },
            record_id=stub.get("cpf") or stub["element_id"],
        )
        self.represents_rels.append(edge)

    # ------------------------------------------------------------------
    # load — escreve REPRESENTS + audit log
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.represents_rels:
            logger.info(
                "[%s] no edges to write (stubs=%d, matched_cpf=%d, matched_name=%d)",
                self.name,
                self._stats["stubs_total"],
                self._stats["matched_cpf"],
                self._stats["matched_name"],
            )
            self._write_audit_log()
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.run_query_with_retry(
            _REPRESENTS_MERGE_QUERY,
            self.represents_rels,
        )
        self.rows_loaded = len(self.represents_rels)
        self._write_audit_log()
        logger.info(
            "[%s] loaded %d REPRESENTS edges (cpf=%d, name=%d, unmatched=%d)",
            self.name,
            self.rows_loaded,
            self._stats["matched_cpf"],
            self._stats["matched_name"],
            self._stats["unmatched"],
        )

    def _write_audit_log(self) -> None:
        """Grava ``data/entity_resolution_tce_go/audit_{run_id}.jsonl``."""
        audit_dir = Path(self.data_dir) / _SOURCE_ID
        audit_dir.mkdir(parents=True, exist_ok=True)
        path = audit_dir / f"audit_{self.run_id}.jsonl"
        # Sempre grava (mesmo que entries vazias) — manter o arquivo é
        # útil pra confirmar que o pipeline rodou e pra auditoria.
        with path.open("w", encoding="utf-8") as fh:
            summary = {"type": "summary", **self._stats}
            fh.write(json.dumps(summary, ensure_ascii=False) + "\n")
            for entry in self._audit_entries:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info(
            "[%s] wrote %d audit entries to %s",
            self.name, len(self._audit_entries), path,
        )
