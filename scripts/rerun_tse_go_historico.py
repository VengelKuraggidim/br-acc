"""Re-roda tse_prestacao_contas_go para anos eleitorais GO históricos.

Uso dentro do container etl:
    uv run python /workspace/scripts/rerun_tse_go_historico.py
"""
from __future__ import annotations

import os
import sys
import time

from neo4j import GraphDatabase

from bracc_etl.pipelines.tse_prestacao_contas_go import TsePrestacaoContasGoPipeline

YEARS = [2018, 2014, 2010, 2006]


def main() -> int:
    password = os.environ.get("NEO4J_PASSWORD")
    if not password:
        print("ERRO: NEO4J_PASSWORD ausente no env", file=sys.stderr)
        return 1

    driver = GraphDatabase.driver(
        "bolt://neo4j:7687",
        auth=("neo4j", password),
    )
    try:
        for year in YEARS:
            print(f"\n{'='*60}\n=== ANO {year} ===\n{'='*60}", flush=True)
            t0 = time.monotonic()
            try:
                pipeline = TsePrestacaoContasGoPipeline(
                    driver=driver,
                    data_dir="/workspace/data",
                    year=year,
                )
                pipeline.run()
                elapsed = time.monotonic() - t0
                print(f"=== ANO {year} OK em {elapsed:.1f}s ===", flush=True)
            except Exception as exc:
                elapsed = time.monotonic() - t0
                print(
                    f"=== ANO {year} FALHOU após {elapsed:.1f}s: "
                    f"{type(exc).__name__}: {exc} ===",
                    flush=True,
                    file=sys.stderr,
                )
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
