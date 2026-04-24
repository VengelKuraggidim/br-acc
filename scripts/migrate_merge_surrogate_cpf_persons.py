#!/usr/bin/env python3
"""Colapsa pares duplicados ``:Person`` originados do bug ``cpf="sq:<X>"``.

Contexto
--------
Até 2026-04-23, o loader ``tse_prestacao_contas_go`` escrevia a dict key
de ``by_cpf`` direto pro node ``:Person.cpf`` quando o CPF vinha mascarado
(TSE 2024+). A key era ``"sq:<sq_candidato>"``, então o MERGE criava um
``:Person {cpf: "sq:<X>"}`` paralelo ao ``:Person {sq_candidato: <X>,
cpf: null}`` já materializado pelo pipeline ``tse_bens``. Diagnosticado
em 2026-04-23 via busca PWA por "IGOR RECELLY FRANCO DE FREITAS" que
retornava 3 linhas (Dep.Fed. 2022 com CPF real + os 2 gêmeos 2024).

Este script colapsa os pares. O surrogate ``a`` não tem rels (verificado
no local: 0 rels em todos os 19.228 nós), então o merge é puramente de
propriedades — mais barato que ``apoc.refactor.mergeNodes``.

Estratégia
----------
1. Match ``a:Person`` com ``a.cpf STARTS WITH 'sq:'`` + sibling
   ``b:Person {sq_candidato: substring(a.cpf, 3)}`` onde ``b.cpf`` é nulo.
2. Copia pra ``b`` todas as keys de ``a`` (exceto ``cpf``) que ``b`` ainda
   não tem. Preserva provenance de ``b`` (tse_bens foi mais cedo) e ganha
   campos TSE-prestacao-specific (patrimonio_declarado, total_tse_<ano>,
   tse_<ano>_partido/proprios/pessoa_fisica/juridica/fin_coletivo/outros,
   cargo_tse_<ano>, total_despesas_tse_<ano>, source_snapshot_uri).
3. Deleta ``a``.

Idempotente. Após rodar, próximas execuções retornam ``merged = 0``.

Uso
---
::

    NEO4J_PASSWORD=changeme python3 scripts/migrate_merge_surrogate_cpf_persons.py \\
        --uri bolt://localhost:7687 --user neo4j --database neo4j

    # dry-run (só conta, sem mutar)
    python3 scripts/migrate_merge_surrogate_cpf_persons.py --dry-run

Requer APOC no Neo4j (usado em ``apoc.periodic.iterate`` pra batchear).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("migrate_merge_surrogate_cpf")


COUNT_QUERY = """
MATCH (a:Person) WHERE a.cpf STARTS WITH 'sq:'
WITH a, substring(a.cpf, 3) AS sq
OPTIONAL MATCH (b:Person {sq_candidato: sq})
  WHERE b <> a AND (b.cpf IS NULL OR b.cpf = '')
RETURN
  count(a) AS total_surrogates,
  count(b) AS with_sibling,
  count(a) - count(b) AS orphans
"""

# Copia props de ``a`` ausentes em ``b``, depois deleta ``a``.
# ``apoc.create.setProperties`` aceita listas paralelas de (keys, values).
# Filtro ``k <> 'cpf'`` é redundante (b.cpf é null e copiaríamos "sq:X"
# que já é o problema original) mas explícito evita regressão futura.
MERGE_BATCH_QUERY = """
CALL apoc.periodic.iterate(
  "MATCH (a:Person) WHERE a.cpf STARTS WITH 'sq:'
   WITH a, substring(a.cpf, 3) AS sq
   MATCH (b:Person {sq_candidato: sq})
   WHERE b <> a AND (b.cpf IS NULL OR b.cpf = '')
   RETURN a, b",
  "WITH a, b,
        [k IN keys(a) WHERE NOT k IN keys(b) AND k <> 'cpf'] AS new_keys
   WITH a, b, new_keys, [k IN new_keys | properties(a)[k]] AS new_vals
   CALL apoc.create.setProperties(b, new_keys, new_vals) YIELD node
   WITH a
   DELETE a",
  {batchSize: $batch_size, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
"""

# Converter orfãos (surrogate sem sibling). No local esse contador é 0 mas
# cobrimos o caso pra ter paridade entre ambientes (Aura pode divergir).
CONVERT_ORPHANS_QUERY = """
CALL apoc.periodic.iterate(
  "MATCH (a:Person) WHERE a.cpf STARTS WITH 'sq:'
   WITH a, substring(a.cpf, 3) AS sq
   WHERE NOT EXISTS { MATCH (b:Person {sq_candidato: sq}) WHERE b <> a }
   RETURN a, sq",
  "SET a.sq_candidato = sq REMOVE a.cpf",
  {batchSize: $batch_size, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages
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
        "--dry-run", action="store_true",
        help="Só conta. Não escreve.",
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
            counts = s.run(COUNT_QUERY).single()
            total = counts["total_surrogates"]
            with_sibling = counts["with_sibling"]
            orphans = counts["orphans"]
            log.info(
                "surrogates: %d (%d com sibling pra merge, %d orfãos)",
                total, with_sibling, orphans,
            )
            if total == 0:
                log.info("Nada a migrar. Saindo.")
                return 0
            if args.dry_run:
                log.info("--dry-run: não mutando.")
                return 0

            log.info("Fase 1: merge de %d pares (batch=%d)...",
                     with_sibling, args.batch_size)
            result = s.run(MERGE_BATCH_QUERY, batch_size=args.batch_size).single()
            errors = result["errorMessages"] or {}
            if errors:
                log.error("Erros no merge: %s", errors)
                return 1
            log.info(
                "  %d batches, %d operações completadas",
                result["batches"], result["total"],
            )

            if orphans > 0:
                log.info("Fase 2: converter %d orfãos (batch=%d)...",
                         orphans, args.batch_size)
                result = s.run(
                    CONVERT_ORPHANS_QUERY, batch_size=args.batch_size,
                ).single()
                errors = result["errorMessages"] or {}
                if errors:
                    log.error("Erros na conversão: %s", errors)
                    return 1
                log.info(
                    "  %d batches, %d operações completadas",
                    result["batches"], result["total"],
                )

            # Sanity: re-query
            remaining = s.run(COUNT_QUERY).single()["total_surrogates"]
            if remaining > 0:
                log.error(
                    "Pós-migração ainda há %d surrogates remanescentes", remaining,
                )
                return 1
            log.info("Migração completa. 0 surrogates remanescentes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
