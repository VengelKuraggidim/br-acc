#!/usr/bin/env python3
"""Validate config/bootstrap_all_contract.yml against the source registry.

Runs the same contract ⇆ registry parity check that run_bootstrap_all.py
does at the start of a bootstrap run, but isolated so CI can catch
drift before a scheduled bootstrap-all-audit trips on it at 03:41 UTC.

Exits 0 on PASS, 1 on any mismatch (printed to stderr).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _load_contract(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_implemented_registry_ids(path: Path) -> set[str]:
    rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
    implemented = {
        (row.get("pipeline_id") or "").strip()
        for row in rows
        if _parse_bool(row.get("in_universe_v1", ""))
        and (row.get("implementation_state") or "").strip() == "implemented"
    }
    implemented.discard("")
    return implemented


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate bootstrap-all contract")
    parser.add_argument("--contract-path", default="config/bootstrap_all_contract.yml")
    parser.add_argument("--registry-path", default="docs/source_registry_br_v1.csv")
    args = parser.parse_args()

    contract = _load_contract(Path(args.contract_path))
    sources = contract.get("sources")
    if not isinstance(sources, list):
        print("FAIL: contract.sources must be a list", file=sys.stderr)
        return 1

    contract_ids = {str(s.get("pipeline_id", "")).strip() for s in sources}
    contract_ids.discard("")

    expected = int(contract.get("expected_implemented_count", len(contract_ids)))
    registry_ids = _parse_implemented_registry_ids(Path(args.registry_path))
    contract_mode = str(contract.get("contract_mode", "full")).strip().lower()

    errors: list[str] = []
    if len(contract_ids) != expected:
        errors.append(
            f"contract sources={len(contract_ids)} but expected_implemented_count={expected}",
        )

    extra_in_contract = sorted(contract_ids - registry_ids)
    if extra_in_contract:
        errors.append(f"in contract but not implemented in registry: {extra_in_contract}")

    if contract_mode != "subset":
        missing_from_contract = sorted(registry_ids - contract_ids)
        if missing_from_contract:
            errors.append(f"in registry but missing from contract: {missing_from_contract}")

    if errors:
        print("FAIL")
        for err in errors:
            print(f"- {err}", file=sys.stderr)
        return 1

    mode_note = "subset" if contract_mode == "subset" else "full"
    print(
        f"PASS: contract/registry parity OK ({len(contract_ids)} sources, mode={mode_note})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
