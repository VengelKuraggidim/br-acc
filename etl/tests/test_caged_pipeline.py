from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.caged import (
    CagedPipeline,
    _build_movement_date,
    _generate_stats_id,
    _parse_salary,
)
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CagedPipeline:
    driver = MagicMock()
    return CagedPipeline(driver=driver, data_dir=str(FIXTURES))


def _load_fixture_df() -> pd.DataFrame:
    return pd.read_csv(
        FIXTURES / "caged" / "caged_2023.csv",
        dtype=str,
        keep_default_na=False,
    )


class TestCagedPipelineMetadata:
    def test_name(self) -> None:
        assert CagedPipeline.name == "caged"

    def test_source_id(self) -> None:
        assert CagedPipeline.source_id == "caged"


class TestGenerateStatsId:
    def test_deterministic(self) -> None:
        id1 = _generate_stats_id(
            "2023",
            "06",
            "SP",
            "3550308",
            "1011201",
            "411005",
            "admissao",
        )
        id2 = _generate_stats_id(
            "2023",
            "06",
            "SP",
            "3550308",
            "1011201",
            "411005",
            "admissao",
        )
        assert id1 == id2

    def test_length(self) -> None:
        stats_id = _generate_stats_id(
            "2023",
            "06",
            "SP",
            "3550308",
            "1011201",
            "411005",
            "admissao",
        )
        assert len(stats_id) == 16


class TestBuildMovementDate:
    def test_pads_single_digit_month(self) -> None:
        assert _build_movement_date("2023", "6") == "2023-06"

    def test_double_digit_month(self) -> None:
        assert _build_movement_date("2023", "12") == "2023-12"


class TestParseSalary:
    def test_simple_float(self) -> None:
        assert _parse_salary("2500.00") == 2500.0

    def test_brazilian_format(self) -> None:
        assert _parse_salary("1.500,50") == 1500.5

    def test_empty_returns_none(self) -> None:
        assert _parse_salary("") is None

    def test_negative_returns_none(self) -> None:
        assert _parse_salary("-100") is None


class TestCagedExtract:
    def test_finds_csv_files(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert len(pipeline._csv_files) == 1
        assert pipeline._csv_files[0].name == "caged_2023.csv"


class TestCagedTransformChunk:
    def test_produces_aggregate_rows(self) -> None:
        pipeline = _make_pipeline()
        df = _load_fixture_df()
        rows = pipeline._transform_chunk(df)
        assert len(rows) == 5

    def test_aggregate_row_fields(self) -> None:
        pipeline = _make_pipeline()
        df = _load_fixture_df()
        row = pipeline._transform_chunk(df)[0]
        assert "stats_id" in row
        assert "year" in row
        assert "month" in row
        assert "movement_date" in row
        assert "movement_type" in row
        assert "uf" in row
        assert "municipality_code" in row
        assert "cnae_subclass" in row
        assert "cbo_code" in row
        assert "total_movements" in row
        assert "admissions" in row
        assert "dismissals" in row
        assert "net_balance" in row
        assert row["source"] == "caged"
        assert row["identity_quality"] == "aggregate"

    def test_total_admissions_and_dismissals(self) -> None:
        pipeline = _make_pipeline()
        df = _load_fixture_df()
        rows = pipeline._transform_chunk(df)
        admissions = sum(int(r["admissions"]) for r in rows)
        dismissals = sum(int(r["dismissals"]) for r in rows)
        assert admissions == 3
        assert dismissals == 2


class TestCagedLoad:
    def test_load_creates_laborstats_nodes_only(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        laborstats_calls = [call for call in run_calls if "MERGE (n:LaborStats" in str(call)]
        assert len(laborstats_calls) >= 1

        assert all("MOVIMENTOU" not in str(call) for call in run_calls)
        assert all("EMPREGADO_EM" not in str(call) for call in run_calls)
        assert all("MERGE (n:Person" not in str(call) for call in run_calls)
        assert all("MERGE (n:Company" not in str(call) for call in run_calls)
