#!/usr/bin/env python3
"""Valida que pipelines GO usam archive_fetch da camada de archival.

Evita regressão: pipelines novos que ingerem dado externo precisam preservar
cópia bruta (proveniência + archival). Ver docs/archival.md e docs/provenance.md.

Exit 0 on pass, 1 on any violation.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# source_ids isentos (pipelines file-only / operator-fed / derived, sem HTTP).
# Ver docs/archival.md — adicione aqui ao introduzir pipeline GO file-only.
# entity_resolution_politicos_go é derived (lê puro do grafo, zero fetch).
EXEMPT: frozenset[str] = frozenset({"tce_go", "entity_resolution_politicos_go"})

_SOURCE_ID_RE = re.compile(r'source_id\s*=\s*"([a-z0-9_]+)"')
_ARCHIVE_IMPORT_RE = re.compile(r"from\s+bracc_etl\.archival\s+import[^\n]*archive_fetch")
_ARCHIVE_CALL_RE = re.compile(r"\barchive_fetch\s*\(")
_SNAPSHOT_URI_RE = re.compile(r"snapshot_uri")


def _source_id(path: Path, source: str) -> str:
    match = _SOURCE_ID_RE.search(source)
    return match.group(1) if match else path.stem


def check_pipeline_file(path: Path) -> tuple[list[str], list[str]]:
    """Return (violations, warnings) for a single ``*_go.py`` pipeline file."""
    source = path.read_text(encoding="utf-8")
    source_id = _source_id(path, source)
    if source_id in EXEMPT:
        return [], []
    has_import = bool(_ARCHIVE_IMPORT_RE.search(source))
    has_call = bool(_ARCHIVE_CALL_RE.search(source))
    if not (has_import or has_call):
        return [
            f"{path}: GO pipeline '{source_id}' não usa archive_fetch. "
            "Importe 'from bracc_etl.archival import archive_fetch' e chame em "
            "cada HTTP fetch (ver docs/archival.md). Se file-only/sem HTTP, "
            f"adicione '{source_id}' em EXEMPT.",
        ], []
    warnings: list[str] = []
    if has_import and not has_call:
        warnings.append(f"{path}: importa archive_fetch mas não chama.")
    if not _SNAPSHOT_URI_RE.search(source):
        warnings.append(
            f"{path}: archive_fetch presente mas sem 'snapshot_uri' — "
            "confirme attach_provenance(snapshot_uri=uri).",
        )
    return [], warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pipelines-dir",
        default="etl/src/bracc_etl/pipelines",
        help="Directory containing pipeline .py files",
    )
    args = parser.parse_args()

    pipelines_dir = Path(args.pipelines_dir)
    if not pipelines_dir.is_dir():
        print(f"error: {pipelines_dir} is not a directory", file=sys.stderr)
        return 2

    violations: list[str] = []
    warnings: list[str] = []
    checked = 0
    exempt_count = 0
    for path in sorted(pipelines_dir.glob("*_go.py")):
        checked += 1
        if _source_id(path, path.read_text(encoding="utf-8")) in EXEMPT:
            exempt_count += 1
        v, w = check_pipeline_file(path)
        violations.extend(v)
        warnings.extend(w)
    for warning in warnings:
        print(f"warn: {warning}", file=sys.stderr)
    if violations:
        print("archival usage violations:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1
    print(
        f"ok: {checked - exempt_count}/{checked} GO pipelines usam "
        f"archive_fetch ({exempt_count} exempt: {sorted(EXEMPT)}) — "
        "ver docs/archival.md"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
