#!/usr/bin/env python3
"""Cria :Municipio e :CamaraMunicipal a partir das Elections municipais.

Deriva nodes e rels de primeira classe a partir do que o pipeline TSE já
carregou como ``Election {year, cargo, uf, municipio}`` + CANDIDATO_EM
com ``situacao``. Idempotente — pode rodar quantas vezes quiser.

Modelo resultante:

    (Municipio {uf, nome})-[:TEM_CAMARA]->(CamaraMunicipal {uf, municipio})

    (Person)-[:PREFEITO_DE {year, partido}]->(Municipio)
    (Person)-[:VICE_PREFEITO_DE {year, partido}]->(Municipio)
    (Person)-[:MEMBRO_DE {year, partido}]->(CamaraMunicipal)

Rel é MERGE em ``(Person, Municipio|CamaraMunicipal, year)`` — múltiplos
mandatos do mesmo político em anos diferentes coexistem.

Uso::

    NEO4J_PASSWORD=... python3 scripts/migrate_municipal_model.py [--uf GO]
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("migrate_municipal_model")


def _load_password() -> str:
    pw = os.environ.get("NEO4J_PASSWORD")
    if pw:
        return pw
    try:
        out = subprocess.run(
            [
                "gcloud", "secrets", "versions", "access", "latest",
                "--secret=fiscal-cidadao-neo4j-password",
                "--project=fiscal-cidadao-493716",
            ],
            capture_output=True, text=True, check=True, timeout=15,
        ).stdout
        return out.strip()
    except Exception as exc:
        raise RuntimeError(
            "NEO4J_PASSWORD não definida e falha ao buscar do GCP Secret Manager"
        ) from exc


CONSTRAINTS = [
    "CREATE CONSTRAINT municipio_uf_nome IF NOT EXISTS "
    "FOR (m:Municipio) REQUIRE (m.uf, m.nome) IS UNIQUE",
    "CREATE CONSTRAINT camara_uf_municipio IF NOT EXISTS "
    "FOR (c:CamaraMunicipal) REQUIRE (c.uf, c.municipio) IS UNIQUE",
]

CREATE_MUNICIPIOS = """
MATCH (e:Election)
WHERE e.uf IS NOT NULL AND e.municipio IS NOT NULL
  AND e.municipio <> '' AND e.cargo IN ['PREFEITO', 'VICE-PREFEITO', 'VEREADOR']
WITH DISTINCT toUpper(e.uf) AS uf, e.municipio AS nome
MERGE (m:Municipio {uf: uf, nome: nome})
RETURN count(m) AS municipios
"""

CREATE_CAMARAS = """
MATCH (e:Election)
WHERE e.cargo = 'VEREADOR' AND e.uf IS NOT NULL AND e.municipio IS NOT NULL
  AND e.municipio <> ''
WITH DISTINCT toUpper(e.uf) AS uf, e.municipio AS municipio
MERGE (m:Municipio {uf: uf, nome: municipio})
MERGE (c:CamaraMunicipal {uf: uf, municipio: municipio})
MERGE (m)-[:TEM_CAMARA]->(c)
RETURN count(c) AS camaras
"""

LINK_PREFEITOS = """
MATCH (p:Person)-[r:CANDIDATO_EM]->(e:Election)
WHERE e.cargo = 'PREFEITO'
  AND r.situacao STARTS WITH 'ELEITO'
  AND ($uf IS NULL OR toUpper(e.uf) = $uf)
MATCH (m:Municipio {uf: toUpper(e.uf), nome: e.municipio})
MERGE (p)-[rel:PREFEITO_DE {year: e.year}]->(m)
SET rel.partido = p.partido
RETURN count(rel) AS prefeitos
"""

LINK_VICE = """
MATCH (p:Person)-[r:CANDIDATO_EM]->(e:Election)
WHERE e.cargo = 'VICE-PREFEITO'
  AND r.situacao STARTS WITH 'ELEITO'
  AND ($uf IS NULL OR toUpper(e.uf) = $uf)
MATCH (m:Municipio {uf: toUpper(e.uf), nome: e.municipio})
MERGE (p)-[rel:VICE_PREFEITO_DE {year: e.year}]->(m)
SET rel.partido = p.partido
RETURN count(rel) AS vices
"""

LINK_VEREADORES = """
MATCH (p:Person)-[r:CANDIDATO_EM]->(e:Election)
WHERE e.cargo = 'VEREADOR'
  AND r.situacao STARTS WITH 'ELEITO'
  AND ($uf IS NULL OR toUpper(e.uf) = $uf)
MATCH (c:CamaraMunicipal {uf: toUpper(e.uf), municipio: e.municipio})
MERGE (p)-[rel:MEMBRO_DE {year: e.year}]->(c)
SET rel.partido = p.partido, rel.situacao_eleicao = r.situacao
RETURN count(rel) AS vereadores
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="neo4j+s://5cb9f76f.databases.neo4j.io")
    parser.add_argument("--user", default="5cb9f76f")
    parser.add_argument("--database", default="5cb9f76f")
    parser.add_argument("--uf", default=None, help="Restringir ligação a 1 UF (ex: GO). Omitir = nacional.")
    args = parser.parse_args()

    try:
        import neo4j
    except ImportError:
        log.error("neo4j driver não instalado; rode com `uv run` dentro do api/ ou etl/.")
        return 2

    pw = _load_password()
    uf_filter = args.uf.upper() if args.uf else None

    with neo4j.GraphDatabase.driver(args.uri, auth=(args.user, pw)) as drv:
        with drv.session(database=args.database) as s:
            for c in CONSTRAINTS:
                log.info("constraint: %s", c.split("FOR")[0].strip())
                s.run(c)

            log.info("criando :Municipio ...")
            n = s.run(CREATE_MUNICIPIOS).single()["municipios"]
            log.info("  municipios processados: %d", n)

            log.info("criando :CamaraMunicipal + :TEM_CAMARA ...")
            n = s.run(CREATE_CAMARAS).single()["camaras"]
            log.info("  camaras processadas: %d", n)

            log.info("ligando prefeitos (filtro UF=%s) ...", uf_filter or "nacional")
            n = s.run(LINK_PREFEITOS, uf=uf_filter).single()["prefeitos"]
            log.info("  prefeitos ligados: %d", n)

            log.info("ligando vice-prefeitos ...")
            n = s.run(LINK_VICE, uf=uf_filter).single()["vices"]
            log.info("  vices ligados: %d", n)

            log.info("ligando vereadores ...")
            n = s.run(LINK_VEREADORES, uf=uf_filter).single()["vereadores"]
            log.info("  vereadores ligados: %d", n)

            # Sanity report
            stats = s.run(
                "MATCH (m:Municipio) "
                "WITH count(m) AS total_mun "
                "MATCH (c:CamaraMunicipal) "
                "WITH total_mun, count(c) AS total_cam "
                "MATCH ()-[r:MEMBRO_DE]->() "
                "WITH total_mun, total_cam, count(r) AS total_mem "
                "MATCH ()-[r2:PREFEITO_DE]->() "
                "WITH total_mun, total_cam, total_mem, count(r2) AS total_pref "
                "RETURN total_mun, total_cam, total_mem, total_pref"
            ).single()
            log.info(
                "grafo final: %d :Municipio, %d :CamaraMunicipal, %d :MEMBRO_DE, %d :PREFEITO_DE",
                stats["total_mun"], stats["total_cam"], stats["total_mem"], stats["total_pref"],
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
