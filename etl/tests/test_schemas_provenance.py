"""Contract tests for bracc_etl.schemas.provenance."""

from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pandera.pandas as pa
import pytest

from bracc_etl.schemas.provenance import (
    PROVENANCE_COLUMNS,
    PROVENANCE_FIELDS,
    with_provenance,
)


def _valid_row() -> dict[str, object]:
    return {
        "source_id": "folha_go",
        "source_record_id": "servidores_2025-12.csv#row=1",
        "source_url": "https://dadosabertos.go.gov.br/dataset/folha",
        "ingested_at": "2026-04-17T12:34:56+00:00",
        "run_id": "folha_go_20260417123456",
    }


_PROVENANCE_SCHEMA = pa.DataFrameSchema(columns=PROVENANCE_COLUMNS)


class TestProvenanceColumns:
    def test_five_fields_defined(self) -> None:
        assert set(PROVENANCE_FIELDS) == {
            "source_id",
            "source_record_id",
            "source_url",
            "ingested_at",
            "run_id",
        }

    def test_valid_row_passes(self) -> None:
        _PROVENANCE_SCHEMA.validate(pd.DataFrame([_valid_row()]))

    @pytest.mark.parametrize(
        "field", ["source_id", "source_url", "ingested_at", "run_id"]
    )
    def test_non_nullable_fields_reject_empty(self, field: str) -> None:
        row = _valid_row()
        row[field] = ""
        with pytest.raises(pa_errors.SchemaError):
            _PROVENANCE_SCHEMA.validate(pd.DataFrame([row]))

    def test_source_record_id_accepts_empty(self) -> None:
        row = _valid_row()
        row["source_record_id"] = ""
        _PROVENANCE_SCHEMA.validate(pd.DataFrame([row]))

    def test_source_url_rejects_non_http(self) -> None:
        row = _valid_row()
        row["source_url"] = "ftp://example.com/file"
        with pytest.raises(pa_errors.SchemaError):
            _PROVENANCE_SCHEMA.validate(pd.DataFrame([row]))

    def test_source_url_accepts_https(self) -> None:
        row = _valid_row()
        row["source_url"] = "https://example.com/path"
        _PROVENANCE_SCHEMA.validate(pd.DataFrame([row]))

    def test_ingested_at_rejects_non_iso(self) -> None:
        row = _valid_row()
        row["ingested_at"] = "17/04/2026 12:34"
        with pytest.raises(pa_errors.SchemaError):
            _PROVENANCE_SCHEMA.validate(pd.DataFrame([row]))


class TestWithProvenance:
    def test_merges_business_columns(self) -> None:
        business = pa.DataFrameSchema(
            columns={"cpf": pa.Column(str, nullable=False, coerce=True)},
            strict=False,
        )
        merged = with_provenance(business)
        assert "cpf" in merged.columns
        for field in PROVENANCE_FIELDS:
            assert field in merged.columns

    def test_preserves_coerce_and_strict(self) -> None:
        business = pa.DataFrameSchema(
            columns={"x": pa.Column(str, coerce=True)},
            coerce=True,
            strict=True,
        )
        merged = with_provenance(business)
        assert merged.coerce is True
        assert merged.strict is True

    def test_end_to_end_validation(self) -> None:
        business = pa.DataFrameSchema(
            columns={"cpf": pa.Column(str, nullable=False, coerce=True)},
        )
        merged = with_provenance(business)
        row = {**_valid_row(), "cpf": "12345678900"}
        merged.validate(pd.DataFrame([row]))

    def test_missing_provenance_column_fails(self) -> None:
        business = pa.DataFrameSchema(columns={"cpf": pa.Column(str)})
        merged = with_provenance(business)
        incomplete = {k: v for k, v in _valid_row().items() if k != "source_id"}
        incomplete["cpf"] = "12345678900"
        with pytest.raises(pa_errors.SchemaError):
            merged.validate(pd.DataFrame([incomplete]))

    def test_business_wins_on_collision(self) -> None:
        """Business-level column def takes precedence if name collides."""
        business = pa.DataFrameSchema(
            columns={"source_id": pa.Column(str, nullable=True, coerce=True)},
        )
        merged = with_provenance(business)
        row = {
            **_valid_row(),
            "source_id": None,
        }
        merged.validate(pd.DataFrame([row]))
