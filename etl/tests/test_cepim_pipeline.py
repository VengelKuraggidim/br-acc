from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.cepim import CepimPipeline, _generate_ngo_id
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CepimPipeline:
    driver = MagicMock()
    pipeline = CepimPipeline(driver=driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _load_fixture_data(pipeline: CepimPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "cepim" / "cepim.csv",
        sep=";",
        dtype=str,
        keep_default_na=False,
    )


class TestCepimPipelineMetadata:
    def test_name(self) -> None:
        assert CepimPipeline.name == "cepim"

    def test_source_id(self) -> None:
        assert CepimPipeline.source_id == "cepim"


class TestGenerateNgoId:
    def test_deterministic(self) -> None:
        id1 = _generate_ngo_id("12345678000195", "CONV-2020-001")
        id2 = _generate_ngo_id("12345678000195", "CONV-2020-001")
        assert id1 == id2

    def test_different_inputs_different_ids(self) -> None:
        id1 = _generate_ngo_id("12345678000195", "CONV-2020-001")
        id2 = _generate_ngo_id("98765432000110", "CONV-2021-042")
        assert id1 != id2

    def test_length(self) -> None:
        ngo_id = _generate_ngo_id("12345678000195", "CONV-2020-001")
        assert len(ngo_id) == 16


class TestCepimTransform:
    def test_produces_ngos(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 3 valid CNPJs out of 5 rows (1 bad CNPJ, 1 empty CNPJ)
        assert len(pipeline.ngos) == 3

    def test_produces_company_rels(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.company_rels) == 3

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {n["name"] for n in pipeline.ngos}
        assert "ASSOCIACAO BENEFICENTE DO NORTE" in names
        assert "INSTITUTO CULTURAL DO SUL" in names
        assert "FUNDACAO SOCIAL DO LESTE" in names

    def test_formats_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cnpjs = {n["cnpj"] for n in pipeline.ngos}
        assert "12.345.678/0001-95" in cnpjs
        assert "98.765.432/0001-10" in cnpjs
        assert "55.667.788/0001-33" in cnpjs

    def test_skips_invalid_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {n["name"] for n in pipeline.ngos}
        assert "NOME COM CNPJ INVALIDO" not in names

    def test_skips_empty_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {n["name"] for n in pipeline.ngos}
        assert "ENTIDADE SEM CNPJ" not in names

    def test_ngo_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        ngo = pipeline.ngos[0]
        assert "ngo_id" in ngo
        assert "cnpj" in ngo
        assert "name" in ngo
        assert "reason" in ngo
        assert "agreement_number" in ngo
        assert "agency" in ngo
        assert "source" in ngo
        assert ngo["source"] == "cepim"

    def test_ngo_id_is_deterministic_hash(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for ngo in pipeline.ngos:
            assert len(ngo["ngo_id"]) == 16

    def test_agreement_numbers_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        agreements = {n["agreement_number"] for n in pipeline.ngos}
        assert "CONV-2020-001" in agreements
        assert "CONV-2021-042" in agreements
        assert "CONV-2019-789" in agreements

    def test_agencies_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        agencies = {n["agency"] for n in pipeline.ngos}
        assert "Ministério da Saúde" in agencies
        assert "Ministério da Educação" in agencies
        assert "Ministério do Desenvolvimento Social" in agencies

    def test_reasons_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        reasons = {n["reason"] for n in pipeline.ngos}
        assert "Inadimplência na prestação de contas" in reasons
        assert "Omissão no dever de prestar contas" in reasons

    def test_company_rels_link_cnpj_to_ngo_id(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for rel in pipeline.company_rels:
            assert "source_key" in rel  # CNPJ
            assert "target_key" in rel  # ngo_id
            assert "." in rel["source_key"]  # formatted CNPJ
            assert len(rel["target_key"]) == 16  # hash ID


class TestCepimLoad:
    def test_load_creates_barred_ngo_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        ngo_calls = [
            call for call in run_calls
            if "MERGE (n:BarredNGO" in str(call)
        ]
        assert len(ngo_calls) >= 1

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

    def test_load_creates_impedida_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "IMPEDIDA" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_skips_when_empty(self) -> None:
        pipeline = _make_pipeline()
        # Don't load fixture data — ngos and company_rels remain empty
        pipeline.load()

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0
