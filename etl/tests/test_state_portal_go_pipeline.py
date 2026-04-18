"""Tests for the Goias state transparency portal pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.state_portal_go import StatePortalGoPipeline, _hash_id
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> StatePortalGoPipeline:
    return StatePortalGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert StatePortalGoPipeline.name == "state_portal_go"

    def test_source_id(self) -> None:
        assert StatePortalGoPipeline.source_id == "state_portal_go"


class TestExtract:
    def test_extract_loads_all_three_domains(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert len(pipeline._raw_contracts) == 3
        assert len(pipeline._raw_suppliers) == 4
        assert len(pipeline._raw_sanctions) == 2


class TestTransform:
    def test_transform_contract_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.contracts) == 3

    def test_transform_supplier_count_skips_non_cnpj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # Fixture has 4 rows including one invalid CNPJ that should be dropped.
        assert len(pipeline.suppliers) == 3

    def test_transform_sanction_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.sanctions) == 2

    def test_contract_cnpj_formatted(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {c["cnpj_supplier"] for c in pipeline.contracts}
        assert "12.345.678/0001-95" in cnpjs

    def test_contract_amount_parsed(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        amounts = sorted(
            c["amount"] for c in pipeline.contracts if c["amount"] is not None
        )
        assert 850750.50 in amounts
        assert 1500000.00 in amounts
        assert 3200000.00 in amounts

    def test_contract_rels_created_for_suppliers(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.contract_rels) == 3

    def test_sanction_rels_created_for_cnpj_targets(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.sanction_rels) == 2

    def test_all_uf_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for record in pipeline.contracts + pipeline.suppliers + pipeline.sanctions:
            assert record["uf"] == "GO"

    def test_source_tagged(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for record in pipeline.contracts + pipeline.suppliers + pipeline.sanctions:
            assert record["source"] == "state_portal_go"

    def test_provenance_stamped_on_contracts_and_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.contracts
        for c in pipeline.contracts:
            assert c["source_id"] == "state_portal_go"
            # numero|cnpj_fmt|published composite.
            assert c["source_record_id"].count("|") == 2
            assert c["source_url"].startswith("http")
            assert c["ingested_at"].startswith("20")
            assert c["run_id"].startswith("state_portal_go_")
        for rel in pipeline.contract_rels:
            assert rel["source_id"] == "state_portal_go"
            assert rel["source_record_id"].count("|") == 2

    def test_provenance_stamped_on_suppliers(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.suppliers
        for s in pipeline.suppliers:
            assert s["source_id"] == "state_portal_go"
            # Natural record_id is cnpj_fmt.
            assert s["source_record_id"] == s["cnpj"]
            assert s["source_url"].startswith("http")

    def test_provenance_stamped_on_sanctions_and_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.sanctions
        for s in pipeline.sanctions:
            assert s["source_id"] == "state_portal_go"
            # cnpj|tipo|processo composite.
            assert s["source_record_id"].count("|") == 2
            assert s["source_url"].startswith("http")
        for rel in pipeline.sanction_rels:
            assert rel["source_id"] == "state_portal_go"
            assert "|" in rel["source_record_id"]

    def test_hash_id_is_stable(self) -> None:
        assert _hash_id("a", "b") == _hash_id("a", "b")
        assert _hash_id("a", "b") != _hash_id("b", "a")


class TestLoad:
    def test_load_calls_session_run(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0

    def test_load_creates_contract_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        contract_calls = [
            call for call in session.run.call_args_list
            if "GoStateContract" in str(call)
        ]
        assert len(contract_calls) >= 1

    def test_load_creates_supplier_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        supplier_calls = [
            call for call in session.run.call_args_list
            if "GoStateSupplier" in str(call)
        ]
        assert len(supplier_calls) >= 1

    def test_load_creates_sanction_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        sanction_calls = [
            call for call in session.run.call_args_list
            if "GoStateSanction" in str(call)
        ]
        assert len(sanction_calls) >= 1

    def test_load_creates_contratou_estado_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        rel_calls = [
            call for call in session.run.call_args_list
            if "CONTRATOU_ESTADO_GO" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_skips_when_empty(self) -> None:
        pipeline = _make_pipeline()
        pipeline.contracts = []
        pipeline.suppliers = []
        pipeline.sanctions = []
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count == 0
