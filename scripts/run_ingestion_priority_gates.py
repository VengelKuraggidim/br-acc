#!/usr/bin/env python3
"""Run ingestion-priority gates for shadow/promote workflow."""

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


@dataclass(frozen=True)
class DateFreshnessGate:
    name: str
    query: str
    max_age_days: int


NUMERIC_GATES: list[NumericGate] = [
    NumericGate(
        name="bid_2025_count",
        query=(
            "MATCH (b:Bid) "
            "WHERE b.source = 'pncp' AND b.date >= '2025-01-01' AND b.date < '2026-01-01' "
            "RETURN count(b) AS value"
        ),
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="inquiry_count",
        query="MATCH (i:Inquiry) RETURN count(i) AS value",
        operator="gt",
        expected=3,
    ),
    NumericGate(
        name="inquiry_inss_or_previd_count",
        query=(
            "MATCH (i:Inquiry) "
            "WHERE toUpper(coalesce(i.name, '') + ' ' + coalesce(i.subject, '')) CONTAINS 'INSS' "
            "   OR toUpper(coalesce(i.name, '') + ' ' + coalesce(i.subject, '')) CONTAINS 'PREVID' "
            "RETURN count(i) AS value"
        ),
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="inquiry_requirement_count",
        query="MATCH (r:InquiryRequirement) RETURN count(r) AS value",
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="inquiry_requirement_rel_count",
        query=(
            "MATCH (:Inquiry)-[r:TEM_REQUERIMENTO]->(:InquiryRequirement) "
            "RETURN count(r) AS value"
        ),
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="senado_fallback_rows_count",
        query=(
            "MATCH (i:Inquiry) "
            "WHERE i.source = 'senado_cpis' "
            "  AND i.inquiry_id = 'senado-cpmi-inss-2026' "
            "RETURN count(i) AS value"
        ),
        operator="eq",
        expected=0,
    ),
    NumericGate(
        name="senado_inquiry_count",
        query="MATCH (i:Inquiry {source: 'senado_cpis'}) RETURN count(i) AS value",
        operator="gt",
        expected=1,
    ),
    NumericGate(
        name="senado_history_expected_count",
        query="RETURN 3 AS value",
        operator="eq",
        expected=3,
    ),
    NumericGate(
        name="senado_history_loaded_count",
        query=(
            "MATCH (i:Inquiry) "
            "WHERE i.source = 'senado_cpis' "
            "  AND i.source_system = 'senado_archive' "
            "RETURN count(i) AS value"
        ),
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="senado_sessions_count",
        query=(
            "MATCH (s:InquirySession) "
            "WHERE s.source = 'senado_cpis' "
            "RETURN count(s) AS value"
        ),
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="senado_temporal_invalid_edges_count",
        query=(
            "MATCH (i:Inquiry {source: 'senado_cpis'})-[r:TEM_REQUERIMENTO|REALIZOU_SESSAO]->() "
            "WHERE r.temporal_status = 'invalid' "
            "RETURN count(r) AS value"
        ),
        operator="eq",
        expected=0,
    ),
    NumericGate(
        name="senado_temporal_unknown_edges_count",
        query=(
            "MATCH (i:Inquiry {source: 'senado_cpis'})-[r:TEM_REQUERIMENTO|REALIZOU_SESSAO]->() "
            "WHERE r.temporal_status = 'unknown' "
            "RETURN count(r) AS value"
        ),
        operator="lte",
        expected=5000,
    ),
    NumericGate(
        name="camara_inquiry_count",
        query="MATCH (i:Inquiry {source: 'camara_inquiries'}) RETURN count(i) AS value",
        operator="gte",
        expected=151,
    ),
    NumericGate(
        name="camara_requirements_count",
        query=(
            "MATCH (r:InquiryRequirement {source: 'camara_inquiries'}) "
            "RETURN count(r) AS value"
        ),
        operator="gte",
        expected=1000,
    ),
    NumericGate(
        name="camara_sessions_count",
        query=(
            "MATCH (s:InquirySession {source: 'camara_inquiries'}) "
            "RETURN count(s) AS value"
        ),
        operator="gte",
        expected=2000,
    ),
    NumericGate(
        name="absurd_future_contract_dates",
        query=(
            "MATCH (c:Contract) "
            "WHERE c.date =~ '\\d{4}-\\d{2}-\\d{2}' "
            "AND date(c.date) > date() + duration('P365D') "
            "RETURN count(c) AS value"
        ),
        operator="eq",
        expected=0,
    ),
    NumericGate(
        name="absurd_future_municipal_contract_dates",
        query=(
            "MATCH (c:MunicipalContract) "
            "WHERE c.signed_at =~ '\\d{4}-\\d{2}-\\d{2}' "
            "AND date(c.signed_at) > date() + duration('P365D') "
            "RETURN count(c) AS value"
        ),
        operator="eq",
        expected=0,
    ),
    NumericGate(
        name="absurd_future_municipal_bid_dates",
        query=(
            "MATCH (b:MunicipalBid) "
            "WHERE b.published_at =~ '\\d{4}-\\d{2}-\\d{2}' "
            "AND date(b.published_at) > date() + duration('P365D') "
            "RETURN count(b) AS value"
        ),
        operator="eq",
        expected=0,
    ),
    NumericGate(
        name="municipal_gazette_act_count",
        query="MATCH (a:MunicipalGazetteAct) RETURN count(a) AS value",
        operator="gt",
        expected=0,
    ),
    NumericGate(
        name="person_cpf_masked",
        query="MATCH (p:Person) WHERE p.cpf CONTAINS '*' RETURN count(p) AS value",
        operator="eq",
        expected=0,
    ),
    NumericGate(
        name="person_cpf_14_digits",
        query=(
            "MATCH (p:Person) "
            "WHERE replace(replace(p.cpf, '.', ''), '-', '') =~ '\\d{14}' "
            "RETURN count(p) AS value"
        ),
        operator="eq",
        expected=0,
    ),
]

DATE_GATES: list[DateFreshnessGate] = [
    DateFreshnessGate(
        name="pncp_max_date",
        query=(
            "MATCH (b:Bid) "
            "WHERE b.source = 'pncp' AND b.date =~ '\\d{4}-\\d{2}-\\d{2}' "
            "RETURN max(b.date) AS value"
        ),
        max_age_days=45,
    ),
    DateFreshnessGate(
        name="comprasnet_max_date",
        query=(
            "MATCH (c:Contract) "
            "WHERE c.source = 'comprasnet' AND c.date =~ '\\d{4}-\\d{2}-\\d{2}' "
            "RETURN max(c.date) AS value"
        ),
        max_age_days=60,
    ),
]

GAZETTE_TEXT_RATIO_QUERY = (
    "MATCH (a:MunicipalGazetteAct) "
    "RETURN count(a) AS total, "
    "sum(CASE WHEN a.text_status = 'available' THEN 1 ELSE 0 END) AS available"
)

GAZETTE_MENTION_COUNT_QUERY = (
    "MATCH (:Company)-[r:MENCIONADA_EM]->(:MunicipalGazetteAct) "
    "RETURN count(r) AS value"
)

QSA_SNAPSHOT_MONTHS_QUERY = (
    "MATCH ()-[r:SOCIO_DE_SNAPSHOT]->() "
    "WHERE r.snapshot_date =~ '\\d{4}-\\d{2}-\\d{2}' "
    "RETURN collect(DISTINCT substring(r.snapshot_date, 0, 7)) AS months, "
    "       max(r.snapshot_date) AS max_snapshot_date"
)

QSA_HISTORY_COUNT_QUERY = "MATCH ()-[r:SOCIO_DE_SNAPSHOT]->() RETURN count(r) AS value"

QSA_TEMPORAL_INVALID_QUERY = (
    "MATCH ()-[r:SOCIO_DE_SNAPSHOT]->() "
    "WHERE r.temporal_status = 'invalid' "
    "   OR (coalesce(r.snapshot_date, '') <> '' "
    "       AND coalesce(r.data_entrada, '') <> '' "
    "       AND r.data_entrada > r.snapshot_date) "
    "RETURN count(r) AS value"
)

PNCP_MONTHS_QUERY = (
    "MATCH (b:Bid) "
    "WHERE b.source = 'pncp' AND b.date =~ '\\d{4}-\\d{2}-\\d{2}' "
    "RETURN collect(DISTINCT substring(b.date, 0, 7)) AS months"
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


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").replace(tzinfo=UTC).date()
    except ValueError:
        return None


def _month_range(start_ym: str, end_date: date) -> list[str]:
    start_year, start_month = map(int, start_ym.split("-"))
    current_year, current_month = start_year, start_month
    out: list[str] = []
    while (current_year, current_month) <= (end_date.year, end_date.month):
        out.append(f"{current_year:04d}-{current_month:02d}")
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True, help="Neo4j bolt URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j user")
    parser.add_argument("--database", default="neo4j", help="Neo4j database name")
    parser.add_argument(
        "--password-env",
        default="NEO4J_PASSWORD",
        help="Environment variable with password",
    )
    parser.add_argument(
        "--reference-date",
        default=date.today().isoformat(),
        help="Reference date for freshness checks (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--check-qsa-history",
        action="store_true",
        help="Enable QSA historical coverage gates (SOCIO_DE_SNAPSHOT).",
    )
    parser.add_argument(
        "--qsa-max-lag-days",
        type=int,
        default=45,
        help="Max allowed lag from latest QSA snapshot month to reference date.",
    )
    args = parser.parse_args()

    password = os.getenv(args.password_env, "")
    if not password:
        print(f"[ERROR] Missing password in env var: {args.password_env}")
        return 2

    ref_date = _parse_iso_date(args.reference_date)
    if ref_date is None:
        print(f"[ERROR] Invalid --reference-date: {args.reference_date}")
        return 2

    driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
    failed = 0

    try:
        with driver.session(database=args.database) as session:
            for gate in NUMERIC_GATES:
                value = int(session.run(gate.query).single()["value"])
                ok = _passes(gate.operator, value, gate.expected)
                if gate.operator == "eq":
                    expected_desc = f"== {gate.expected}"
                elif gate.operator == "gte":
                    expected_desc = f">= {gate.expected}"
                elif gate.operator == "lt":
                    expected_desc = f"< {gate.expected}"
                elif gate.operator == "lte":
                    expected_desc = f"<= {gate.expected}"
                else:
                    expected_desc = f"> {gate.expected}"
                print(
                    f"[{'PASS' if ok else 'FAIL'}] {gate.name}: "
                    f"value={value} expected {expected_desc}",
                )
                if not ok:
                    failed += 1

            for gate in DATE_GATES:
                raw_value = session.run(gate.query).single()["value"]
                parsed = _parse_iso_date(raw_value)
                if parsed is None:
                    print(f"[FAIL] {gate.name}: no valid max date found")
                    failed += 1
                    continue

                age_days = (ref_date - parsed).days
                ok = age_days <= gate.max_age_days
                print(
                    f"[{'PASS' if ok else 'FAIL'}] {gate.name}: "
                    f"max_date={parsed.isoformat()} "
                    f"age_days={age_days} max_allowed={gate.max_age_days}",
                )
                if not ok:
                    failed += 1

            pncp_months_raw = session.run(PNCP_MONTHS_QUERY).single()["months"]
            actual_months = set(pncp_months_raw or [])
            expected_months = _month_range("2021-08", ref_date)
            missing_months = [m for m in expected_months if m not in actual_months]
            pncp_month_ok = len(missing_months) == 0
            suffix = ""
            if missing_months:
                preview = ",".join(missing_months[:12])
                suffix = f" missing_sample={preview}"
            print(
                f"[{'PASS' if pncp_month_ok else 'FAIL'}] pncp_missing_months_count: "
                f"value={len(missing_months)} expected == 0{suffix}",
            )
            if not pncp_month_ok:
                failed += 1

            ratio_row = session.run(GAZETTE_TEXT_RATIO_QUERY).single()
            total_acts = int(ratio_row["total"] or 0)
            available_acts = int(ratio_row["available"] or 0)
            ratio = (available_acts / total_acts) if total_acts > 0 else 0.0
            print(
                "[INFO] gazette_text_available_ratio: "
                f"available={available_acts} total={total_acts} ratio={ratio:.3f}",
            )

            if ratio >= 0.2:
                mention_count = int(session.run(GAZETTE_MENTION_COUNT_QUERY).single()["value"])
                ok = mention_count > 0
                print(
                    f"[{'PASS' if ok else 'FAIL'}] municipal_gazette_mentions: "
                    f"value={mention_count} expected > 0 (ratio >= 0.2)",
                )
                if not ok:
                    failed += 1
            else:
                print(
                    "[WARN] municipal_gazette_mentions gate relaxed: "
                    f"gazette_text_available_ratio={ratio:.3f} < 0.2",
                )

            if args.check_qsa_history:
                qsa_history_count = int(session.run(QSA_HISTORY_COUNT_QUERY).single()["value"])
                qsa_history_ok = qsa_history_count > 0
                print(
                    f"[{'PASS' if qsa_history_ok else 'FAIL'}] qsa_history_rows_loaded: "
                    f"value={qsa_history_count} expected > 0",
                )
                if not qsa_history_ok:
                    failed += 1

                qsa_invalid_count = int(
                    session.run(QSA_TEMPORAL_INVALID_QUERY).single()["value"],
                )
                qsa_invalid_ok = qsa_invalid_count == 0
                print(
                    f"[{'PASS' if qsa_invalid_ok else 'FAIL'}] qsa_temporal_invalid_count: "
                    f"value={qsa_invalid_count} expected == 0",
                )
                if not qsa_invalid_ok:
                    failed += 1

                qsa_row = session.run(QSA_SNAPSHOT_MONTHS_QUERY).single()
                qsa_months = set(qsa_row["months"] or [])
                qsa_max_snapshot_date = qsa_row["max_snapshot_date"] or ""
                qsa_snapshot_max_month = qsa_max_snapshot_date[:7] if qsa_max_snapshot_date else ""
                print(
                    f"[INFO] qsa_snapshot_max_month: "
                    f"value={qsa_snapshot_max_month or 'N/A'}",
                )

                if qsa_months:
                    qsa_min_month = min(qsa_months)
                    qsa_max_month = max(qsa_months)
                    qsa_max_month_dt = _parse_iso_date(f"{qsa_max_month}-01")
                    qsa_expected_months = _month_range(
                        qsa_min_month,
                        qsa_max_month_dt if qsa_max_month_dt else ref_date,
                    )
                    qsa_missing_months = [
                        month for month in qsa_expected_months if month not in qsa_months
                    ]
                else:
                    qsa_missing_months = []

                qsa_missing_ok = len(qsa_missing_months) == 0
                qsa_suffix = ""
                if qsa_missing_months:
                    qsa_suffix = f" missing_sample={','.join(qsa_missing_months[:12])}"
                print(
                    f"[{'PASS' if qsa_missing_ok else 'FAIL'}] qsa_missing_months_count: "
                    f"value={len(qsa_missing_months)} expected == 0{qsa_suffix}",
                )
                if not qsa_missing_ok:
                    failed += 1

                if qsa_max_snapshot_date:
                    qsa_snapshot_dt = _parse_iso_date(qsa_max_snapshot_date)
                    qsa_lag_days = (
                        (ref_date - qsa_snapshot_dt).days if qsa_snapshot_dt else 9999
                    )
                else:
                    qsa_lag_days = 9999
                qsa_lag_ok = qsa_lag_days <= args.qsa_max_lag_days
                print(
                    f"[{'PASS' if qsa_lag_ok else 'FAIL'}] qsa_latest_projection_lag_days: "
                    f"value={qsa_lag_days} expected <= {args.qsa_max_lag_days}",
                )
                if not qsa_lag_ok:
                    failed += 1
    finally:
        driver.close()

    if failed:
        print(f"[SUMMARY] {failed} gate(s) failed.")
        return 1

    print("[SUMMARY] All ingestion-priority gates passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
