#!/usr/bin/env python3
"""Run QSA history-specific gates for CNPJ historical coverage."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime

from neo4j import GraphDatabase


@dataclass(frozen=True)
class NumericGate:
    name: str
    query: str
    operator: str
    expected: int


NUMERIC_GATES: list[NumericGate] = [
    NumericGate(
        name="qsa_history_rows_loaded",
        query="MATCH ()-[r:SOCIO_DE_SNAPSHOT]->() RETURN count(r) AS value",
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="qsa_temporal_invalid_count",
        query=(
            "MATCH ()-[r:SOCIO_DE_SNAPSHOT]->() "
            "WHERE r.temporal_status = 'invalid' "
            "   OR (coalesce(r.snapshot_date, '') <> '' "
            "       AND coalesce(r.data_entrada, '') <> '' "
            "       AND r.data_entrada > r.snapshot_date) "
            "RETURN count(r) AS value"
        ),
        operator="eq",
        expected=0,
    ),
]

SNAPSHOT_MONTHS_QUERY = (
    "MATCH ()-[r:SOCIO_DE_SNAPSHOT]->() "
    "WHERE r.snapshot_date =~ '\\d{4}-\\d{2}-\\d{2}' "
    "RETURN collect(DISTINCT substring(r.snapshot_date, 0, 7)) AS months, "
    "       max(r.snapshot_date) AS max_snapshot_date"
)


def _passes(operator: str, value: int, expected: int) -> bool:
    if operator == "eq":
        return value == expected
    if operator == "gt":
        return value > expected
    if operator == "gte":
        return value >= expected
    if operator == "lt":
        return value < expected
    if operator == "lte":
        return value <= expected
    raise ValueError(f"Unsupported operator: {operator}")


def _months_between(start_ym: str, end_ym: str) -> list[str]:
    start = datetime.strptime(f"{start_ym}-01", "%Y-%m-%d").date()
    end = datetime.strptime(f"{end_ym}-01", "%Y-%m-%d").date()
    months: list[str] = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = date(year, month, 1)
    return months


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True, help="Neo4j bolt URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--database", default="neo4j", help="Neo4j database")
    parser.add_argument(
        "--password-env",
        default="NEO4J_PASSWORD",
        help="Environment variable containing Neo4j password",
    )
    parser.add_argument(
        "--max-lag-days",
        type=int,
        default=45,
        help="Max allowed lag (in days) from latest snapshot_date to today",
    )
    args = parser.parse_args()

    password = os.getenv(args.password_env, "")
    if not password:
        print(f"[ERROR] Missing password in env var: {args.password_env}")
        return 2

    failed = 0
    driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
    try:
        with driver.session(database=args.database) as session:
            for gate in NUMERIC_GATES:
                value = int(session.run(gate.query).single()["value"])
                ok = _passes(gate.operator, value, gate.expected)
                status = "PASS" if ok else "FAIL"
                expected_desc = (
                    f"== {gate.expected}" if gate.operator == "eq" else f"> {gate.expected}"
                )
                print(f"[{status}] {gate.name}: value={value} expected {expected_desc}")
                if not ok:
                    failed += 1

            record = session.run(SNAPSHOT_MONTHS_QUERY).single()
            months = sorted(record["months"] or [])
            max_snapshot_date = record["max_snapshot_date"] or ""
            qsa_snapshot_max_month = max_snapshot_date[:7] if max_snapshot_date else ""

            missing_months_count = 0
            if months:
                expected_months = _months_between(months[0], months[-1])
                missing_months_count = len(set(expected_months) - set(months))
            print(
                f"[{'PASS' if missing_months_count == 0 else 'FAIL'}] "
                f"qsa_missing_months_count: value={missing_months_count} expected == 0"
            )
            if missing_months_count != 0:
                failed += 1

            lag_days = 10_000
            if max_snapshot_date:
                lag_days = (datetime.now(UTC).date() - datetime.strptime(
                    max_snapshot_date, "%Y-%m-%d",
                ).date()).days
            print(f"[INFO] qsa_snapshot_max_month: value={qsa_snapshot_max_month or 'N/A'}")
            lag_ok = lag_days <= args.max_lag_days
            print(
                f"[{'PASS' if lag_ok else 'FAIL'}] qsa_latest_projection_lag_days: "
                f"value={lag_days} expected <= {args.max_lag_days}"
            )
            if not lag_ok:
                failed += 1
    finally:
        driver.close()

    if failed:
        print(f"[SUMMARY] {failed} QSA history gate(s) failed.")
        return 1
    print("[SUMMARY] All QSA history gates passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
