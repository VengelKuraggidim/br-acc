#!/usr/bin/env python3
"""Roda os pipelines de foto de politicos GO em sequencia.

Ordem canonica:
    1. senado_senadores_foto   (cria/MERGE :Senator)
    2. alego_deputados_foto    (MERGE em :StateLegislator existente)
    3. wikidata_politicos_foto (enriquece nodes existentes)
    4. tse_candidatos_foto     (enriquece :Person GO existente)
    5. propagacao_fotos_person (costura foto_url cross-label pro :Person)

Os 2 primeiros podem CRIAR nodes; os seguintes so atualizam. Por isso
a ordem importa — wikidata/tse so funcionam depois que os nodes existem.
O passo final (``propagacao_fotos_person``) copia foto_url de labels
de cargo (:FederalLegislator, :StateLegislator, :Senator) pro :Person
homonimo pra que a busca da PWA (que so le :Person via fulltext
``entity_search``) tambem exiba a foto nos cards de resultado.

Cauda do TSE
------------
O ``tse_candidatos_foto`` processa por batch (default 500/run).
Pra cobrir os ~4k candidatos GO na cauda, use ``--tse-iterations N``
(re-roda o pipeline N vezes — cada iter pega um batch novo, ja que
o discovery filtra por ``foto_url`` ainda vazia).

Lendo Neo4j via .env (NEO4J_URI/USER/PASSWORD/DATABASE). Senha local
canonica vive no .env (gitignored); se sumir, recupere com
``docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2``
(ver CLAUDE.md secao 2).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ETL_DIR = REPO_ROOT / "etl"

# Ordem de execucao matters — primeiros 2 criam nodes, 3-4 enriquecem,
# o ultimo costura cross-label pro :Person (onde a busca da PWA le).
PHOTO_PIPELINES: tuple[str, ...] = (
    "senado_senadores_foto",
    "alego_deputados_foto",
    "wikidata_politicos_foto",
    "tse_candidatos_foto",
    "propagacao_fotos_person",
)


def parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolve_neo4j_password() -> str | None:
    pw = os.environ.get("NEO4J_PASSWORD")
    if pw:
        return pw
    dotenv = parse_dotenv(REPO_ROOT / ".env")
    pw = dotenv.get("NEO4J_PASSWORD")
    if pw:
        return pw
    # Fallback: tenta extrair do container local
    try:
        out = subprocess.run(
            ["docker", "exec", "fiscal-neo4j", "env"],
            capture_output=True, text=True, check=True, timeout=5,
        ).stdout
        for line in out.splitlines():
            if line.startswith("NEO4J_AUTH="):
                return line.split("/", 1)[1] if "/" in line else None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def run_pipeline(source: str, neo4j_args: list[str], limit: int | None, dry_run: bool, *, tag: str | None = None) -> int:
    cmd = ["uv", "run", "bracc-etl", "run", "--source", source, *neo4j_args]
    if limit is not None and source == "tse_candidatos_foto":
        # tse e' o unico que pode ser pesado (4k+ candidatos)
        cmd += ["--limit", str(limit)]
    safe_cmd = []
    skip_next = False
    for tok in cmd:
        if skip_next:
            safe_cmd.append("***")
            skip_next = False
            continue
        if tok == "--neo4j-password":
            skip_next = True
        safe_cmd.append(tok)
    header = tag or source
    print(f"\n{'='*70}\n>>> {header}\n{'='*70}", flush=True)
    print(f"$ {' '.join(safe_cmd)}", flush=True)
    if dry_run:
        print("(dry-run, nao executando)", flush=True)
        return 0
    start = time.time()
    rc = subprocess.run(cmd, cwd=ETL_DIR).returncode
    print(f"<<< {header} exit={rc} ({time.time()-start:.1f}s)", flush=True)
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", help=f"CSV de pipelines pra rodar (default: todos os {len(PHOTO_PIPELINES)})")
    ap.add_argument("--skip", help="CSV de pipelines pra pular")
    ap.add_argument("--limit", type=int, help="Limite de candidatos pro tse_candidatos_foto (ignored pelos outros)")
    ap.add_argument("--tse-iterations", type=int, default=1, help="Quantas vezes re-rodar tse_candidatos_foto pra cobrir a cauda (default 1, cada iter pega batch_size=500 novos)")
    ap.add_argument("--dry-run", action="store_true", help="Imprime comandos sem executar")
    ap.add_argument("--continue-on-error", action="store_true", help="Nao para se um pipeline falha")
    args = ap.parse_args()

    env = parse_dotenv(REPO_ROOT / ".env")
    neo4j_args: list[str] = []
    for env_key, cli_flag in (
        ("NEO4J_URI", "--neo4j-uri"),
        ("NEO4J_USER", "--neo4j-user"),
        ("NEO4J_DATABASE", "--neo4j-database"),
    ):
        val = os.environ.get(env_key) or env.get(env_key)
        if val:
            neo4j_args += [cli_flag, val]
    pw = resolve_neo4j_password()
    if pw:
        neo4j_args += ["--neo4j-password", pw]
    else:
        print("WARNING: NEO4J_PASSWORD nao encontrada. bracc-etl vai tentar GCP Secret Manager.", file=sys.stderr)

    selected = list(PHOTO_PIPELINES)
    if args.only:
        only = {s.strip() for s in args.only.split(",") if s.strip()}
        unknown = only - set(PHOTO_PIPELINES)
        if unknown:
            print(f"ERROR: --only inclui pipelines desconhecidos: {sorted(unknown)}", file=sys.stderr)
            return 2
        selected = [s for s in PHOTO_PIPELINES if s in only]
    if args.skip:
        skip = {s.strip() for s in args.skip.split(",") if s.strip()}
        selected = [s for s in selected if s not in skip]

    if not selected:
        print("Nada pra rodar (--only/--skip filtraram tudo).", file=sys.stderr)
        return 2

    print(f"Vai rodar {len(selected)} pipeline(s) em ordem: {', '.join(selected)}")
    if args.limit:
        print(f"Limit pro tse_candidatos_foto: {args.limit}")

    tse_iterations = max(1, args.tse_iterations)
    if tse_iterations > 1:
        print(f"TSE vai rodar {tse_iterations}x (cauda de ex-candidatos)")

    failures: list[tuple[str, int]] = []
    for source in selected:
        runs = tse_iterations if source == "tse_candidatos_foto" else 1
        for iter_idx in range(runs):
            tag = f"{source} (iter {iter_idx + 1}/{runs})" if runs > 1 else source
            rc = run_pipeline(source, neo4j_args, args.limit, args.dry_run, tag=tag)
            if rc != 0:
                failures.append((tag, rc))
                if not args.continue_on_error:
                    print(f"\nABORTADO em {tag} (exit {rc}). Use --continue-on-error pra ignorar.", file=sys.stderr)
                    break
        else:
            continue
        break

    print(f"\n{'='*70}\nResumo: {len(selected) - len(failures)}/{len(selected)} pipelines OK")
    if failures:
        for src, rc in failures:
            print(f"  FAIL {src} (exit {rc})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
