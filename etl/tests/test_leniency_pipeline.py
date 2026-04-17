from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.leniency import LeniencyPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> LeniencyPipeline:
    driver = MagicMock()
    pipeline = LeniencyPipeline(driver=driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _load_fixture_data(pipeline: LeniencyPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "leniency" / "leniencia.csv",
        dtype=str,
        keep_default_na=False,
    )


class TestLeniencyPipelineMetadata:
    def test_name(self) -> None:
        assert LeniencyPipeline.name == "leniency"

    def test_source_id(self) -> None:
        assert LeniencyPipeline.source_id == "cgu_leniencia"


class TestLeniencyTransform:
    def test_produces_agreements(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 2 valid CNPJs out of 4 rows (1 bad CNPJ, 1 empty CNPJ)
        assert len(pipeline.agreements) == 2

    def test_produces_company_rels(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.company_rels) == 2

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {a["name"] for a in pipeline.agreements}
        assert "ODEBRECHT S.A." in names
        assert "BRASKEM S.A." in names

    def test_formats_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cnpjs = {a["cnpj"] for a in pipeline.agreements}
        assert "33.000.167/0001-01" in cnpjs
        assert "60.746.948/0001-12" in cnpjs

    def test_skips_invalid_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cnpj_digits = {
            a["cnpj"].replace(".", "").replace("/", "").replace("-", "")
            for a in pipeline.agreements
        }
        assert "1234" not in cnpj_digits

    def test_skips_empty_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {a["name"] for a in pipeline.agreements}
        assert "SEM CNPJ LTDA" not in names

    def test_parses_dates(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        start_dates = {a["start_date"] for a in pipeline.agreements}
        assert "2016-12-01" in start_dates
        assert "2017-06-15" in start_dates

    def test_empty_end_date(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # Braskem has empty data_fim -> stored as None
        end_dates = [a["end_date"] for a in pipeline.agreements]
        assert None in end_dates

    def test_agreement_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        a = pipeline.agreements[0]
        assert "leniency_id" in a
        assert "cnpj" in a
        assert "name" in a
        assert "start_date" in a
        assert "end_date" in a
        assert "status" in a
        assert "responsible_agency" in a
        assert "proceedings_count" in a
        assert "source" in a
        assert a["source"] == "cgu_leniencia"

    def test_leniency_id_format(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for a in pipeline.agreements:
            assert a["leniency_id"].startswith("leniencia_")

    def test_proceedings_count_parsed(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        counts = {a["proceedings_count"] for a in pipeline.agreements}
        assert 12 in counts
        assert 5 in counts


class TestLeniencyLoad:
    def test_load_creates_agreement_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        agreement_calls = [
            call for call in run_calls
            if "MERGE (n:LeniencyAgreement" in str(call)
        ]
        assert len(agreement_calls) >= 1

    def test_load_creates_company_nodes_with_razao_social(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        company_calls = [
            call for call in run_calls
            if "MERGE (n:Company" in str(call)
        ]
        assert len(company_calls) >= 1

        for call in company_calls:
            rows = (
                call[1]["rows"]
                if "rows" in call[1]
                else call[0][1]["rows"]
            )
            for row in rows:
                assert "razao_social" in row
                assert row["razao_social"] == row["name"]

    def test_load_creates_firmou_leniencia_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "FIRMOU_LENIENCIA" in str(call)
        ]
        assert len(rel_calls) >= 1
