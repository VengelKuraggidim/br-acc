"""Promove 6 matches da fase 5.6 shadow_first_last_match aprovados em spot-check.

Lista veio de etl/scripts/audit_first_last_match.py + decisão humana
2026-05-02 (opção C: 3 raros + 3 com sinal de partido_municipal_vereador).
Cria :REPRESENTS método=shadow_first_last_match_manual conf=0.65.
Idempotente — MERGE não duplica.
"""
from __future__ import annotations

import os

from neo4j import GraphDatabase

APPROVED = [
    # (shadow_element_id, canonical_id, shadow_name, canonical_name)
    ("4:da0ec56f-cb5d-454a-b730-78a989eacdb6:1950428", "canon_cpf_96986042191",
     "JOAO MAGALHAES", "JOAO FERREIRA MAGALHAES"),
    ("4:da0ec56f-cb5d-454a-b730-78a989eacdb6:1950512", "canon_cpf_85662933120",
     "MANOEL JUNIOR", "MANOEL CORREIA PONTES JUNIOR"),
    ("4:da0ec56f-cb5d-454a-b730-78a989eacdb6:1950718", "canon_senado_5070",
     "WILDER MORAIS", "WILDER PEDRO DE MORAIS"),
    ("4:da0ec56f-cb5d-454a-b730-78a989eacdb6:1951252", "canon_camara_204419",
     "GLAUSTIN FOKUS", "GLAUSTIN DA FOKUS"),
    ("4:da0ec56f-cb5d-454a-b730-78a989eacdb6:1951427", "canon_senado_5899",
     "VANDERLAN CARDOSO", "VANDERLAN VIEIRA CARDOSO"),
    ("4:da0ec56f-cb5d-454a-b730-78a989eacdb6:1951633", "canon_cpf_98805630187",
     "MAURICIO CARVALHO", "MAURICIO DE PAULA CARVALHO"),
]

CYPHER = """
MATCH (c:CanonicalPerson {canonical_id: $canonical_id})
MATCH (p:Person) WHERE elementId(p) = $shadow_eid
MERGE (c)-[r:REPRESENTS]->(p)
ON CREATE SET r.method = 'shadow_first_last_match_manual',
              r.confidence = 0.65,
              r.added_at = datetime(),
              r.added_by = 'spot-check 2026-05-02'
RETURN c.canonical_id AS cid, elementId(p) AS pid,
       r.method AS method, r.confidence AS conf
"""


def main() -> None:
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            for shadow_eid, cid, sname, cname in APPROVED:
                result = session.run(
                    CYPHER,
                    canonical_id=cid,
                    shadow_eid=shadow_eid,
                ).single()
                if result is None:
                    print(f"[FAIL] {sname} → {cname}: nenhum match (cid={cid}, eid={shadow_eid})")
                else:
                    print(
                        f"[OK]   {sname} → {cname}: "
                        f"method={result['method']} conf={result['conf']}"
                    )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
