#!/usr/bin/env python3
"""Re-aponta DOOU dos :CampaignDonor stubs (criados com chave donation_id)
pros doadores reais (Company por CNPJ, ou stub agregado por doador_id).

Contexto
--------
Até 2026-04-29, ``etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py``
fazia ``MERGE (p)<-[r:DOOU {donation_id}]-(d) ON CREATE SET d:CampaignDonor``
sem chave no nó ``d`` — então cada donation_id criava um :CampaignDonor
órfão. Mesmo doador (CNPJ pleno + ano) aparecia como N stubs em vez de
mergir num :Company existente. Resultado: o card "Confere com o TSE" do
PWA mostrava R$ 0 ingerido em vez do total real.

Diagnóstico no Neo4j local (2026-04-29):

    | tipo         | rels    | doador_ids únicos |
    |--------------|--------:|------------------:|
    | pj           | 11.557  |               180 |
    | pf mascarado | 12.193  |             1.424 |
    | desconhecido |    274  |               274 |

Estratégia
----------
1. **PJ** — pra cada stub PJ, MERGE :Company {cnpj: format(stub.doador_id)}
   e move a aresta DOOU pra ele (preservando todas as props da rel).
   Deleta o stub no fim.
2. **PF mascarado** — MERGE :CampaignDonor {doador_id: <CPF mascarado>}
   (chave durável agora) e move a aresta. Stubs antigos (chave-fantasma
   sem doador_id) são deletados; o stub novo carrega N rels.
3. **Desconhecido** — fica como está. Já é 1:1 (sem chave pra agregar).

Move-aresta = cria nova rel idêntica e deleta a antiga (Cypher não tem
``MOVE``). ``apoc.refactor.from`` faz isso atomicamente.

Idempotente: re-runs encontram 0 stubs PJ/PF mascarados e saem cedo.
Requer APOC no Neo4j.

Uso
---

    NEO4J_PASSWORD=changeme python3 scripts/backfill_doou_campaign_donor_stubs.py \\
        --uri bolt://localhost:7687 --user neo4j --database neo4j

    # dry-run (só conta)
    python3 scripts/backfill_doou_campaign_donor_stubs.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_doou_stubs")


COUNT_QUERY = """
MATCH (d:CampaignDonor)
RETURN d.doador_tipo AS tipo, count(d) AS n_stubs
ORDER BY tipo
"""


# --- PJ: re-aponta DOOU pro :Company existente -------------------------------
#
# format(doador_id) — doador_id em PJ é CNPJ digits crus (14 chars). O
# resto do grafo usa formato pontuado XX.XXX.XXX/XXXX-XX. Aplicamos a
# regex direto no Cypher pra não depender de import Python no batch.
#
# apoc.refactor.from move o rel pra outra origem preservando props +
# direção. Stub é detach-deletado no fim do iterate.
PJ_BACKFILL_QUERY = """
CALL apoc.periodic.iterate(
  "MATCH (d:CampaignDonor)-[r:DOOU]->(p:Person)
   WHERE d.doador_tipo = 'pj'
     AND d.doador_id =~ '^\\\\d{14}$'
   WITH d, r, p,
        d.doador_id AS digits
   WITH d, r, p,
        substring(digits,0,2) + '.' +
        substring(digits,2,3) + '.' +
        substring(digits,5,3) + '/' +
        substring(digits,8,4) + '-' +
        substring(digits,12,2) AS cnpj_fmt
   RETURN d, r, p, cnpj_fmt",
  "MERGE (c:Company {cnpj: cnpj_fmt})
     ON CREATE SET c.razao_social = coalesce(d.doador_nome, '')
   WITH d, r, p, c
   CALL apoc.refactor.from(r, c) YIELD input, output
   WITH d
   WHERE NOT (d)--()
   DELETE d",
  {batchSize: $batch_size, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
"""


# --- PF: agrega stubs duplicados por doador_id -------------------------------
#
# Pra cada CPF mascarado distinto, pega 1 stub canônico (qualquer um) e
# move as DOOU dos OUTROS stubs pra ele. Depois detach-delete dos
# duplicados (que ficaram sem rel).
PF_BACKFILL_QUERY = """
CALL apoc.periodic.iterate(
  "MATCH (d:CampaignDonor)
   WHERE d.doador_tipo = 'pf' AND d.doador_id <> ''
   WITH d.doador_id AS doador_id, collect(d) AS stubs
   WHERE size(stubs) > 1
   WITH doador_id, stubs[0] AS canonical, stubs[1..] AS dups
   RETURN canonical, dups",
  "UNWIND dups AS dup
   MATCH (dup)-[r:DOOU]->(p:Person)
   CALL apoc.refactor.from(r, canonical) YIELD input, output
   WITH dup
   WHERE NOT (dup)--()
   DELETE dup",
  {batchSize: 100, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
"""


# --- Sanity: re-conta após o backfill ----------------------------------------
SANITY_QUERY = """
MATCH (d:CampaignDonor)-[r:DOOU]->(:Person)
WITH d.doador_tipo AS tipo,
     count(r) AS n_rels,
     count(distinct d) AS n_stubs,
     count(distinct d.doador_id) AS n_distinct_ids
RETURN tipo, n_rels, n_stubs, n_distinct_ids
ORDER BY tipo
"""


def _load_password() -> str:
    pw = os.environ.get("NEO4J_PASSWORD")
    if pw:
        return pw
    log.error("NEO4J_PASSWORD não setado no ambiente")
    sys.exit(2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--dry-run", action="store_true", help="Só conta. Não escreve.",
    )
    args = parser.parse_args()

    try:
        import neo4j
    except ImportError:
        log.error("neo4j driver não instalado; rode com `uv run` ou ative .venv")
        return 2

    pw = _load_password()

    with neo4j.GraphDatabase.driver(args.uri, auth=(args.user, pw)) as drv:
        with drv.session(database=args.database) as s:
            log.info("Pre-backfill: distribuição de :CampaignDonor por doador_tipo")
            for row in s.run(COUNT_QUERY):
                log.info("  %-15s %d stubs", row["tipo"], row["n_stubs"])
            if args.dry_run:
                log.info("--dry-run: não mutando.")
                return 0

            log.info("Fase 1: PJ → :Company (move rel + delete stub)")
            result = s.run(PJ_BACKFILL_QUERY, batch_size=args.batch_size).single()
            errors = result["errorMessages"] or {}
            if errors:
                log.error("Erros no backfill PJ: %s", errors)
                return 1
            log.info(
                "  PJ: %d batches, %d operações completadas",
                result["batches"], result["total"],
            )

            log.info("Fase 2: PF mascarado → agrega stubs por doador_id")
            result = s.run(PF_BACKFILL_QUERY).single()
            errors = result["errorMessages"] or {}
            if errors:
                log.error("Erros no backfill PF: %s", errors)
                return 1
            log.info(
                "  PF: %d batches, %d operações completadas",
                result["batches"], result["total"],
            )

            log.info("Pós-backfill: distribuição")
            for row in s.run(SANITY_QUERY):
                log.info(
                    "  %-15s %d rels, %d stubs, %d ids únicos",
                    row["tipo"], row["n_rels"], row["n_stubs"], row["n_distinct_ids"],
                )
    return 0


if __name__ == "__main__":
    sys.exit(main())
