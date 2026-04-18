#!/usr/bin/env python3
"""Ensure GO pipelines call Pipeline.attach_provenance() for node/rel dicts.

This is a lightweight CI gate: it confirms each GO-scope pipeline file
contains at least one ``self.attach_provenance(`` call. The gate is a
necessary-but-not-sufficient check — a pipeline can still forget the
helper in some call sites and pass this check — so it's paired with
runtime enforcement in ``Neo4jBatchLoader`` under
``BRACC_PROVENANCE_MODE=strict``.

See ``docs/provenance.md`` for the full contract.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Pipelines under the current Fiscal Cidadão product scope (Goiás).
# Keep in sync with ``config/bootstrap_go_contract.yml``.
GO_PIPELINES: frozenset[str] = frozenset({
    "alego",
    "camara_goiania",
    "folha_go",
    "pncp_go",
    "querido_diario_go",
    "ssp_go",
    "state_portal_go",
    "tce_go",
    "tcm_go",
    "tcmgo_sancoes",
})

_SOURCE_ID_RE = re.compile(r'source_id\s*=\s*"([a-z0-9_]+)"')
_ATTACH_CALL_RE = re.compile(r'self\.attach_provenance\s*\(')


def check_pipeline_file(path: Path) -> list[str]:
    """Return a list of human-readable violations for a single pipeline file."""
    source = path.read_text(encoding="utf-8")
    match = _SOURCE_ID_RE.search(source)
    if not match:
        return []
    source_id = match.group(1)
    if source_id not in GO_PIPELINES:
        return []
    if not _ATTACH_CALL_RE.search(source):
        return [
            f"{path.name}: GO pipeline '{source_id}' must call "
            f"self.attach_provenance() — see docs/provenance.md",
        ]
    return []


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
    covered_pipelines: set[str] = set()
    for path in sorted(pipelines_dir.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        match = _SOURCE_ID_RE.search(source)
        if match and match.group(1) in GO_PIPELINES:
            covered_pipelines.add(match.group(1))
        violations.extend(check_pipeline_file(path))

    missing_in_tree = GO_PIPELINES - covered_pipelines
    if missing_in_tree:
        print(
            "error: expected GO pipelines not found in "
            f"{pipelines_dir}: {sorted(missing_in_tree)}",
            file=sys.stderr,
        )
        return 2

    if violations:
        print("provenance contract violations:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1

    print(
        f"ok: all {len(GO_PIPELINES)} GO pipelines call "
        "self.attach_provenance() (see docs/provenance.md)",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
