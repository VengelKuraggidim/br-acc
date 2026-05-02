"""Dry-run ER pra coletar audit entries da fase 5.6 shadow_first_last_match.

Roda extract+transform com enable_first_last_match=True e first_last_audit_only=True.
NÃO chama load(): grafo fica intacto. Só dumpa as entries do tipo
shadow_first_last_match_audit / shadow_first_last_ambiguous + expande os
canonical_ids com display_name + lista de sources pra spot-check humano.
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bracc_etl.pipelines.entity_resolution_politicos_go import (  # noqa: E402
    EntityResolutionPoliticosGoPipeline,
)


def main() -> None:
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        pipeline = EntityResolutionPoliticosGoPipeline(
            driver=driver,
            data_dir="./data",
            enable_first_last_match=True,
            first_last_audit_only=True,
        )
        print(f"[audit] extract + transform (no load) — uri={uri}", flush=True)
        pipeline.extract()
        pipeline.transform()

        audit = pipeline._audit_entries  # type: ignore[attr-defined]
        match_audit = [e for e in audit if e.get("type") == "shadow_first_last_match_audit"]
        ambiguous = [e for e in audit if e.get("type") == "shadow_first_last_ambiguous"]
        print(f"[audit] total entries: {len(audit)}")
        print(f"[audit] shadow_first_last_match_audit (1 cluster, attach candidato): {len(match_audit)}")
        print(f"[audit] shadow_first_last_ambiguous (>1 cluster, skip): {len(ambiguous)}")

        nodes_by_label = pipeline._nodes_by_label  # type: ignore[attr-defined]
        node_by_eid: dict[str, dict] = {}
        for label_nodes in nodes_by_label.values():
            for node in label_nodes:
                node_by_eid[node["element_id"]] = node

        clusters = pipeline._clusters  # type: ignore[attr-defined]
        canon_summary: dict[str, dict] = {}
        for cid, cluster in clusters.items():
            sources = []
            for edge in cluster["edges"]:
                eid = edge.get("target_element_id")
                src = node_by_eid.get(eid, {})
                sources.append({
                    "label": src.get("primary_label"),
                    "name": src.get("name"),
                    "cpf": src.get("cpf"),
                    "uf": src.get("uf"),
                    "partido": src.get("partido"),
                    "method": edge.get("method"),
                    "confidence": edge.get("confidence"),
                })
            canon_summary[cid] = {
                "display_name": cluster["canonical"].get("display_name"),
                "uf": cluster["canonical"].get("uf"),
                "partido": cluster["canonical"].get("partido"),
                "n_sources": len(sources),
                "sources": sources,
            }

        out_path = Path("./data/entity_resolution_politicos_go/spotcheck_first_last.json")
        report = {
            "summary": {
                "total_audit_entries": len(audit),
                "match_candidates": len(match_audit),
                "ambiguous_skips": len(ambiguous),
            },
            "match_candidates": [],
            "ambiguous_skips": [],
        }
        for entry in match_audit:
            cid = entry.get("canonical_id")
            shadow_eid = entry.get("shadow_element_id")
            shadow_node = node_by_eid.get(shadow_eid, {})
            report["match_candidates"].append({
                "shadow_name": entry.get("shadow_name"),
                "shadow_uf": shadow_node.get("uf"),
                "shadow_element_id": shadow_eid,
                "canonical_id": cid,
                "canonical": canon_summary.get(cid, {}),
            })
        for entry in ambiguous:
            cids = entry.get("candidate_canonicals", [])
            shadow_eid = entry.get("shadow_element_id")
            shadow_node = node_by_eid.get(shadow_eid, {})
            report["ambiguous_skips"].append({
                "shadow_name": entry.get("shadow_name"),
                "shadow_uf": shadow_node.get("uf"),
                "shadow_element_id": shadow_eid,
                "candidate_canonicals": [
                    {"canonical_id": c, "canonical": canon_summary.get(c, {})}
                    for c in cids
                ],
            })

        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"[audit] spot-check report → {out_path}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
