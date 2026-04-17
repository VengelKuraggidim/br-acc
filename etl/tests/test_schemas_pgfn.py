"""Contract tests for bracc_etl.schemas.pgfn.

These tests pin the column-level constraints so renames / prefix changes
in pgfn transform() output surface immediately instead of silently
loading malformed records into Neo4j.
"""

from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pytest

from bracc_etl.schemas.pgfn import deve_relationship_schema, finances_schema


def _valid_finance_row() -> dict[str, object]:
    return {
        "finance_id": "pgfn_123456789",
        "type": "divida_ativa",
        "inscription_number": "123456789",
        "value": 1000.0,
        "date": "2024-01-15",
        "situation": "ativa",
        "revenue_type": "simples",
        "court_action": "none",
        "source": "pgfn",
    }


def _valid_deve_row() -> dict[str, object]:
    return {
        "source_key": "12.345.678/0001-95",
        "target_key": "pgfn_123456789",
        "value": 1000.0,
        "date": "2024-01-15",
        "company_name": "ACME LTDA",
    }


class TestFinancesSchema:
    def test_valid_row_passes(self) -> None:
        df = pd.DataFrame([_valid_finance_row()])
        finances_schema.validate(df)

    def test_finance_id_must_have_prefix(self) -> None:
        row = _valid_finance_row()
        row["finance_id"] = "notpgfn_123"
        with pytest.raises(pa_errors.SchemaError):
            finances_schema.validate(pd.DataFrame([row]))

    def test_type_enum_enforced(self) -> None:
        row = _valid_finance_row()
        row["type"] = "other_debt"
        with pytest.raises(pa_errors.SchemaError):
            finances_schema.validate(pd.DataFrame([row]))

    def test_source_enum_enforced(self) -> None:
        row = _valid_finance_row()
        row["source"] = "not_pgfn"
        with pytest.raises(pa_errors.SchemaError):
            finances_schema.validate(pd.DataFrame([row]))

    def test_negative_value_rejected(self) -> None:
        row = _valid_finance_row()
        row["value"] = -1.0
        with pytest.raises(pa_errors.SchemaError):
            finances_schema.validate(pd.DataFrame([row]))

    def test_empty_inscription_rejected(self) -> None:
        row = _valid_finance_row()
        row["inscription_number"] = ""
        with pytest.raises(pa_errors.SchemaError):
            finances_schema.validate(pd.DataFrame([row]))

    def test_optional_date_allows_empty(self) -> None:
        row = _valid_finance_row()
        row["date"] = ""
        finances_schema.validate(pd.DataFrame([row]))

    def test_non_strict_allows_extra_columns(self) -> None:
        # strict=False is important — transform() may add fields later and
        # the schema shouldn't break on forward-compatible additions.
        row = _valid_finance_row()
        row["extra_future_field"] = "ok"
        finances_schema.validate(pd.DataFrame([row]))


class TestDeveRelationshipSchema:
    def test_valid_row_passes(self) -> None:
        df = pd.DataFrame([_valid_deve_row()])
        deve_relationship_schema.validate(df)

    def test_accepts_raw_14_digit_cnpj(self) -> None:
        row = _valid_deve_row()
        row["source_key"] = "12345678000195"
        deve_relationship_schema.validate(pd.DataFrame([row]))

    def test_rejects_unformatted_cnpj(self) -> None:
        row = _valid_deve_row()
        row["source_key"] = "12345.678.0001-95"  # wrong format
        with pytest.raises(pa_errors.SchemaError):
            deve_relationship_schema.validate(pd.DataFrame([row]))

    def test_rejects_cpf_in_source_key(self) -> None:
        row = _valid_deve_row()
        row["source_key"] = "123.456.789-01"  # CPF shape
        with pytest.raises(pa_errors.SchemaError):
            deve_relationship_schema.validate(pd.DataFrame([row]))

    def test_target_key_must_have_pgfn_prefix(self) -> None:
        row = _valid_deve_row()
        row["target_key"] = "finance_123"
        with pytest.raises(pa_errors.SchemaError):
            deve_relationship_schema.validate(pd.DataFrame([row]))

    def test_negative_value_rejected(self) -> None:
        row = _valid_deve_row()
        row["value"] = -100.0
        with pytest.raises(pa_errors.SchemaError):
            deve_relationship_schema.validate(pd.DataFrame([row]))

    def test_nullable_value_and_date(self) -> None:
        row = _valid_deve_row()
        row["value"] = None
        row["date"] = None
        deve_relationship_schema.validate(pd.DataFrame([row]))
