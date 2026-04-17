from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.bcb import BcbPipeline, _generate_penalty_id
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> BcbPipeline:
    driver = MagicMock()
    pipeline = BcbPipeline(driver=driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _load_fixture_data(pipeline: BcbPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "bcb" / "penalidades.csv",
        sep=";",
        dtype=str,
        keep_default_na=False,
    )


class TestBcbPipelineMetadata:
    def test_name(self) -> None:
        assert BcbPipeline.name == "bcb"

    def test_source_id(self) -> None:
        assert BcbPipeline.source_id == "bcb"


class TestGeneratePenaltyId:
    def test_deterministic(self) -> None:
        id1 = _generate_penalty_id("12345678000195", "12345/2023", "Multa")
        id2 = _generate_penalty_id("12345678000195", "12345/2023", "Multa")
        assert id1 == id2

    def test_different_inputs_different_ids(self) -> None:
        id1 = _generate_penalty_id("12345678000195", "12345/2023", "Multa")
        id2 = _generate_penalty_id("98765432000110", "67890/2022", "Advertência")
        assert id1 != id2

    def test_length(self) -> None:
        penalty_id = _generate_penalty_id("12345678000195", "12345/2023", "Multa")
        assert len(penalty_id) == 16


class TestBcbTransform:
    def test_produces_penalties(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 3 valid CNPJs out of 5 rows (1 bad CNPJ, 1 empty CNPJ)
        assert len(pipeline.penalties) == 3

    def test_produces_company_rels(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.company_rels) == 3

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {p["institution_name"] for p in pipeline.penalties}
        assert "BANCO EXEMPLO S.A." in names
        assert "FINANCEIRA TESTE LTDA" in names

    def test_formats_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cnpjs = {p["cnpj"] for p in pipeline.penalties}
        assert "12.345.678/0001-95" in cnpjs
        assert "98.765.432/0001-10" in cnpjs

    def test_skips_invalid_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {p["institution_name"] for p in pipeline.penalties}
        assert "NOME COM CNPJ INVALIDO" not in names

    def test_skips_empty_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {p["institution_name"] for p in pipeline.penalties}
        assert "INSTITUICAO SEM CNPJ" not in names

    def test_penalty_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        penalty = pipeline.penalties[0]
        assert "penalty_id" in penalty
        assert "cnpj" in penalty
        assert "institution_name" in penalty
        assert "penalty_type" in penalty
        assert "process_number" in penalty
        assert "decision_date" in penalty
        assert "source" in penalty
        assert penalty["source"] == "bcb"

    def test_penalty_id_is_deterministic_hash(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for penalty in pipeline.penalties:
            assert len(penalty["penalty_id"]) == 16

    def test_parses_value(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        penalties_with_value = [p for p in pipeline.penalties if "penalty_value" in p]
        assert len(penalties_with_value) >= 1

        multa = next(p for p in pipeline.penalties if p["penalty_type"] == "Multa")
        assert multa["penalty_value"] == 500_000.0

        inab = next(
            p for p in pipeline.penalties if p["penalty_type"] == "Inabilitação"
        )
        assert inab["penalty_value"] == 1_000_000.5

    def test_company_rels_link_cnpj_to_penalty_id(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for rel in pipeline.company_rels:
            assert "source_key" in rel  # CNPJ
            assert "target_key" in rel  # penalty_id
            assert "." in rel["source_key"]  # formatted CNPJ
            assert len(rel["target_key"]) == 16  # hash ID


class TestBcbLoad:
    def test_load_creates_bcb_penalty_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        penalty_calls = [
            call for call in run_calls
            if "MERGE (n:BCBPenalty" in str(call)
        ]
        assert len(penalty_calls) >= 1

    def test_load_creates_company_nodes(self) -> None:
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

    def test_load_creates_bcb_penalizada_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "BCB_PENALIZADA" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_skips_when_empty(self) -> None:
        pipeline = _make_pipeline()
        # Don't load fixture data — penalties and company_rels remain empty
        pipeline.load()

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0
