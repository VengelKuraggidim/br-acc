from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bracc_etl.pipelines.tse_bens import TseBensPipeline, _make_asset_id
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TseBensPipeline:
    driver = MagicMock()
    pipeline = TseBensPipeline(driver=driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _load_fixture_data(pipeline: TseBensPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "tse_bens" / "bens.csv",
        dtype=str,
        keep_default_na=False,
    )


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestTseBensPipelineMetadata:
    def test_name(self) -> None:
        assert TseBensPipeline.name == "tse_bens"

    def test_source_id(self) -> None:
        assert TseBensPipeline.source_id == "tse_bens"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestMakeAssetId:
    def test_deterministic(self) -> None:
        id1 = _make_asset_id("12345678901", "2022", "31", "100000", "Apartamento")
        id2 = _make_asset_id("12345678901", "2022", "31", "100000", "Apartamento")
        assert id1 == id2

    def test_different_inputs_different_ids(self) -> None:
        id1 = _make_asset_id("12345678901", "2022", "31", "100000", "Apartamento")
        id2 = _make_asset_id("12345678901", "2022", "31", "200000", "Apartamento")
        assert id1 != id2

    def test_length(self) -> None:
        result = _make_asset_id("12345678901", "2022", "31", "100000", "Apartamento")
        assert len(result) == 16

    def test_hex_characters(self) -> None:
        result = _make_asset_id("12345678901", "2022", "31", "100000", "Apt")
        assert all(c in "0123456789abcdef" for c in result)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------


class TestTseBensExtract:
    def test_extract_reads_csv(self) -> None:
        pipeline = _make_pipeline()
        # Point data_dir to the fixtures parent so tse_bens/ is found
        pipeline.data_dir = str(FIXTURES)
        pipeline.extract()
        assert len(pipeline._raw) == 5

    def test_extract_with_limit(self) -> None:
        pipeline = _make_pipeline()
        pipeline.data_dir = str(FIXTURES)
        pipeline.limit = 2
        pipeline.extract()
        assert len(pipeline._raw) == 2

    def test_extract_missing_file(self) -> None:
        pipeline = _make_pipeline()
        pipeline.data_dir = "/nonexistent"
        with pytest.raises(FileNotFoundError):
            pipeline.extract()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


class TestTseBensTransform:
    def test_produces_assets(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        # 3 valid CPFs out of 5 rows (1 short CPF, 1 empty CPF)
        assert len(pipeline.assets) == 3

    def test_produces_person_rels(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        assert len(pipeline.person_rels) == 3

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        names = {a["candidate_name"] for a in pipeline.assets}
        assert "JOAO DA SILVA" in names
        assert "MARIA SOUZA" in names
        assert "PEDRO SANTOS" in names

    def test_formats_cpf(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        cpfs = {a["candidate_cpf"] for a in pipeline.assets}
        assert "529.982.247-25" in cpfs
        assert "111.444.777-35" in cpfs
        assert "987.654.321-00" in cpfs

    def test_skips_short_cpf(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        names = {a["candidate_name"] for a in pipeline.assets}
        assert "NOME INVALIDO" not in names

    def test_skips_empty_cpf(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        names = {a["candidate_name"] for a in pipeline.assets}
        assert "SEM CPF" not in names

    def test_parses_asset_value(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        values = {a["candidate_cpf"]: a["asset_value"] for a in pipeline.assets}
        assert values["529.982.247-25"] == 150000.50
        assert values["111.444.777-35"] == 850000.00
        assert values["987.654.321-00"] == 95000.75

    def test_election_year_is_int(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        for asset in pipeline.assets:
            assert isinstance(asset["election_year"], int)

    def test_asset_type_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        types = {a["asset_type"] for a in pipeline.assets}
        assert "37 - Quotas ou quinhoes de capital" in types
        assert "31 - Apartamento" in types
        assert "21 - Veiculo automotor terrestre" in types

    def test_asset_description_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        descs = {a["asset_description"] for a in pipeline.assets}
        assert "Empresa ABC Ltda" in descs
        assert "Apartamento no Leblon" in descs
        assert "Toyota Corolla 2019" in descs

    def test_uf_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        ufs = {a["uf"] for a in pipeline.assets}
        assert "SP" in ufs
        assert "RJ" in ufs
        assert "MG" in ufs

    def test_partido_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        partidos = {a["partido"] for a in pipeline.assets}
        assert "PT" in partidos
        assert "PSDB" in partidos
        assert "MDB" in partidos

    def test_source_field(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        for asset in pipeline.assets:
            assert asset["source"] == "tse_bens"

    def test_asset_id_is_hex(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        for asset in pipeline.assets:
            assert len(asset["asset_id"]) == 16
            assert all(c in "0123456789abcdef" for c in asset["asset_id"])

    def test_asset_ids_are_unique(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        ids = [a["asset_id"] for a in pipeline.assets]
        assert len(ids) == len(set(ids))

    def test_person_rel_keys_match_assets(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        asset_ids = {a["asset_id"] for a in pipeline.assets}
        asset_cpfs = {a["candidate_cpf"] for a in pipeline.assets}
        for rel in pipeline.person_rels:
            assert rel["target_key"] in asset_ids
            # CPF-keyed rels (real CPF em anos <2024) vs sq-keyed (2024+ masked).
            assert rel["source_cpf"] in asset_cpfs or rel["source_sq"] != ""

    def test_asset_fields_present(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        expected_fields = {
            "asset_id", "candidate_cpf", "candidate_name", "asset_type",
            "asset_description", "asset_value", "election_year", "uf",
            "partido", "source",
            # Provenance fields (attach_provenance stamps every row).
            "source_id", "source_record_id", "source_url",
            "ingested_at", "run_id",
        }
        for asset in pipeline.assets:
            assert set(asset.keys()) == expected_fields


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


class TestTseBensLoad:
    def test_load_creates_asset_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        asset_calls = [
            call for call in run_calls
            if "MERGE (n:DeclaredAsset" in str(call)
        ]
        assert len(asset_calls) >= 1

    def test_load_creates_person_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        person_calls = [
            call for call in run_calls
            if "MERGE (n:Person" in str(call)
        ]
        assert len(person_calls) >= 1

    def test_load_creates_declarou_bem_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "DECLAROU_BEM" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_uses_param_binding(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "DECLAROU_BEM" in str(call)
        ]
        for call in rel_calls:
            query = str(call)
            assert "row.source_cpf" in query or "row.source_sq" in query
            assert "row.target_key" in query

    def test_load_deduplicates_persons(self) -> None:
        """Ensure Person nodes are not duplicated when same CPF appears multiple times."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # Duplicate a person_rel to simulate same person with 2 assets
        pipeline.person_rels.append(pipeline.person_rels[0].copy())
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        person_calls = [
            call for call in run_calls
            if "MERGE (n:Person" in str(call)
        ]
        # Should still only have one batch call for persons (3 unique CPFs)
        assert len(person_calls) >= 1
        # Check that the batch had unique persons
        for call in person_calls:
            rows = call[1]["rows"] if len(call) > 1 and "rows" in call[1] else call[0][1]["rows"]
            cpfs = [r["cpf"] for r in rows]
            assert len(cpfs) == len(set(cpfs))

    def test_load_with_no_assets(self) -> None:
        """When transform produces no assets, load should not fail."""
        pipeline = _make_pipeline()
        pipeline.assets = []
        pipeline.person_rels = []
        pipeline.load()
        # No assertions needed beyond not raising


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------


class TestTseBensEndToEnd:
    def test_full_run(self) -> None:
        pipeline = _make_pipeline()
        pipeline.data_dir = str(FIXTURES)
        pipeline.run()
        assert len(pipeline.assets) == 3
        assert len(pipeline.person_rels) == 3
