"""Apaga rels :DOOU legadas duplicadas contra rels granulares (donation_id).

Defesa em profundidade pra o fix de ``conexoes_service`` (commits ``4e7bf0a``
PF e ``313cd78`` PJ): o pipeline ``tse.py`` legado e o
``tse_prestacao_contas_go.py`` novo escrevem rels :DOOU paralelas com MERGE
keys diferentes (``{year}`` vs ``{donation_id}``). O service já dedupa em
runtime, mas o grafo continua com as duplicatas — ~497k rels :Person no
local + dupes :Company. Esse script apaga as legadas (sem ``donation_id``)
quando há granular cobrindo o mesmo doador-ano.

**Idempotente**: re-runs não fazem nada se as duplicatas já foram apagadas.

**Critério de delete** (ambos PF e PJ):

1. Existe rel granular ``[r2:DOOU {donation_id <> NULL, ano = X}]`` do mesmo
   doador (CampaignDonor por ``last4(doador_id)`` ou Company por ``cnpj``)
   pro mesmo target ``Person``.
2. A rel a apagar é ``[r:DOOU]`` SEM ``donation_id``, com ``ano = X``.

Sem (1), não delete (preserva rels que vieram só da legacy — ainda válidas).

**Não tocá**:
- Rels :CampaignDonor (sempre carimbam ``donation_id``).
- Rels com ``ano IS NULL`` (cobertas pelo backfill ``SET r.ano = r.year``
  em 2026-04-22 / 30 — se ainda houver, executar ele primeiro).

Modo dry-run (``--dry-run``) só conta as duplicatas; sem flag, deleta via
``apoc.periodic.iterate`` em batches.
"""
from __future__ import annotations

import argparse
import os

from neo4j import GraphDatabase

# Conta + amostra: quantas rels legacy PF têm um par granular (last4_cpf, ano).
COUNT_PF_DUPES = """
MATCH (donor_legacy:Person)-[r:DOOU]->(target:Person)
WHERE r.donation_id IS NULL AND r.ano IS NOT NULL
  AND donor_legacy.cpf IS NOT NULL
WITH donor_legacy, target, r,
     right(replace(donor_legacy.cpf, '-', ''), 4) AS last4,
     toInteger(r.ano) AS ano
WHERE EXISTS {
    MATCH (granular:CampaignDonor)-[r2:DOOU]->(target)
    WHERE r2.donation_id IS NOT NULL
      AND toInteger(r2.ano) = ano
      AND right(replace(replace(granular.doador_id, '-', ''), '*', ''), 4) = last4
}
RETURN count(r) AS dupes
"""

DELETE_PF_DUPES = """
CALL apoc.periodic.iterate(
  "
  MATCH (donor_legacy:Person)-[r:DOOU]->(target:Person)
  WHERE r.donation_id IS NULL AND r.ano IS NOT NULL
    AND donor_legacy.cpf IS NOT NULL
  WITH donor_legacy, target, r,
       right(replace(donor_legacy.cpf, '-', ''), 4) AS last4,
       toInteger(r.ano) AS ano
  WHERE EXISTS {
      MATCH (granular:CampaignDonor)-[r2:DOOU]->(target)
      WHERE r2.donation_id IS NOT NULL
        AND toInteger(r2.ano) = ano
        AND right(replace(replace(granular.doador_id, '-', ''), '*', ''), 4) = last4
  }
  RETURN r
  ",
  "DELETE r",
  {batchSize: 5000, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
"""

# Conta + amostra: quantas rels legacy PJ (Company) têm par granular (cnpj, ano).
COUNT_PJ_DUPES = """
MATCH (donor_legacy:Company)-[r:DOOU]->(target:Person)
WHERE r.donation_id IS NULL AND r.ano IS NOT NULL
  AND donor_legacy.cnpj IS NOT NULL
WITH donor_legacy, target, r, toInteger(r.ano) AS ano
WHERE EXISTS {
    MATCH (donor_legacy)-[r2:DOOU]->(target)
    WHERE r2.donation_id IS NOT NULL
      AND toInteger(r2.ano) = ano
}
RETURN count(r) AS dupes
"""

DELETE_PJ_DUPES = """
CALL apoc.periodic.iterate(
  "
  MATCH (donor_legacy:Company)-[r:DOOU]->(target:Person)
  WHERE r.donation_id IS NULL AND r.ano IS NOT NULL
    AND donor_legacy.cnpj IS NOT NULL
  WITH donor_legacy, target, r, toInteger(r.ano) AS ano
  WHERE EXISTS {
      MATCH (donor_legacy)-[r2:DOOU]->(target)
      WHERE r2.donation_id IS NOT NULL
        AND toInteger(r2.ano) = ano
  }
  RETURN r
  ",
  "DELETE r",
  {batchSize: 5000, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Só conta as duplicatas; não deleta.",
    )
    parser.add_argument(
        "--skip-pf",
        action="store_true",
        help="Pula a fase PF (Person legacy → CampaignDonor granular).",
    )
    parser.add_argument(
        "--skip-pj",
        action="store_true",
        help="Pula a fase PJ (Company legacy vs granular do mesmo Company).",
    )
    args = parser.parse_args()

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            if not args.skip_pf:
                count = session.run(COUNT_PF_DUPES).single()
                pf_dupes = count["dupes"] if count else 0
                print(f"[PF] {pf_dupes} rels :Person→:Person DOOU duplicadas (sem donation_id, com granular twin).")
                if pf_dupes and not args.dry_run:
                    result = session.run(DELETE_PF_DUPES).single()
                    print(
                        f"[PF] deletadas em {result['batches']} batches; "
                        f"total={result['total']}; erros={result['errorMessages']}"
                    )

            if not args.skip_pj:
                count = session.run(COUNT_PJ_DUPES).single()
                pj_dupes = count["dupes"] if count else 0
                print(f"[PJ] {pj_dupes} rels :Company→:Person DOOU duplicadas (sem donation_id, com granular twin).")
                if pj_dupes and not args.dry_run:
                    result = session.run(DELETE_PJ_DUPES).single()
                    print(
                        f"[PJ] deletadas em {result['batches']} batches; "
                        f"total={result['total']}; erros={result['errorMessages']}"
                    )

        if args.dry_run:
            print("\nDry-run: nenhuma rel foi apagada. Re-executar sem --dry-run pra deletar.")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
