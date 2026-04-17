from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.tesouro_emendas import (
    TesouroEmendasPipeline,
    _parse_excel_date,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> TesouroEmendasPipeline:
    return TesouroEmendasPipeline(
        driver=MagicMock(),
        data_dir=str(data_dir or FIXTURES),
    )


class TestMetadata:
    def test_name(self) -> None:
        assert TesouroEmendasPipeline.name == "tesouro_emendas"

    def test_source_id(self) -> None:
        assert TesouroEmendasPipeline.source_id == "tesouro_emendas"


class TestHelpers:
    def test_parse_excel_date_serial(self) -> None:
        # Excel serial 45658 = 2025-01-01 (origin 1899-12-30 + 45658 days)
        assert _parse_excel_date("45658") == "2025-01-01"

    def test_parse_excel_date_passthrough_for_iso_string(self) -> None:
        # Non-digit strings are returned unchanged (already ISO-formatted).
        assert _parse_excel_date("2024-02-15") == "2024-02-15"

    def test_parse_excel_date_passthrough_for_empty(self) -> None:
        assert _parse_excel_date("") == ""

class TestExtract:
    def test_raises_when_csv_missing(self, tmp_path: Path) -> None:
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        with pytest.raises(FileNotFoundError):
            pipeline.extract()

    def test_reads_fixture_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        # Fixture has 5 rows (one is a deliberate duplicate of row 1).
        assert len(pipeline._raw) == 5


class TestTransform:
    def test_transfer_count_dedupes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 4 unique OBs (the duplicate of 900001 collapses).
        assert len(pipeline.transfers) == 4

    def test_company_count_excludes_rows_without_cnpj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 3 distinct CNPJs; one row has no CNPJ so no company.
        assert len(pipeline.companies) == 3

    def test_transfer_rels_count_matches_companies_only(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # Rels are built one per row that had a CNPJ — so 4 total including
        # the duplicate, because rel dedup isn't applied to transfer_rels.
        assert len(pipeline.transfer_rels) == 4

    def test_transfer_id_is_stable(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        ids = {t["transfer_id"] for t in pipeline.transfers}
        assert "transfer_tesouro_900001" in ids
        assert "transfer_tesouro_900002" in ids

    def test_value_parsed_as_float(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        by_ob = {t["ob"]: t for t in pipeline.transfers}
        assert by_ob["900001"]["value"] == 1000.50
        assert by_ob["900002"]["value"] == 250000.00
        assert by_ob["900003"]["value"] == 0.0

    def test_excel_date_converted_in_transfer(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        by_ob = {t["ob"]: t for t in pipeline.transfers}
        # Excel serial 45658 -> 2025-01-01
        assert by_ob["900001"]["date"] == "2025-01-01"
        # Iso-like date passes through unchanged.
        assert by_ob["900003"]["date"] == "2024-02-15"

    def test_source_field_set(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert all(t["source"] == "tesouro_emendas" for t in pipeline.transfers)

    def test_cnpj_zero_padded(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {c["cnpj"] for c in pipeline.companies}
        # 15-digit column value "111222333000144" stays 15 chars and is
        # filtered out (only 14-char CNPJs land in companies); fixture
        # includes a 15-digit row to exercise the guard.
        assert all(len(c) == 14 for c in cnpjs)

    def test_respects_limit(self) -> None:
        driver = MagicMock()
        pipeline = TesouroEmendasPipeline(
            driver=driver, data_dir=str(FIXTURES), limit=2,
        )
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.transfers) == 2


class TestLoad:
    def test_load_no_data_does_not_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.transfers = []
        pipeline.companies = []
        pipeline.transfer_rels = []
        pipeline.load()  # no exception

    def test_load_runs_without_error_on_populated_state(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()  # MagicMock driver accepts any cypher calls
