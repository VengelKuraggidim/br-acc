"""Tests for the ALEGO scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.alego import AlegoPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> AlegoPipeline:
    return AlegoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert AlegoPipeline.name == "alego"

    def test_source_id(self) -> None:
        assert AlegoPipeline.source_id == "alego"


class TestTransform:
    def test_legislator_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.legislators) == 2

    def test_expense_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.expenses) == 2

    def test_proposition_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.propositions) == 1

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for leg in pipeline.legislators:
            # CPF must never be stored in cleartext.
            assert "111" not in leg["cpf"]
            assert "***" in leg["cpf"]

    def test_expense_cnpj_formatted(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {e["cnpj_supplier"] for e in pipeline.expenses}
        assert "44.455.566/0001-88" in cnpjs

    def test_source_tagged(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in (
            pipeline.legislators + pipeline.expenses + pipeline.propositions
        ):
            assert r["source"] == "alego"

    def test_provenance_stamped_on_legislators(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.legislators
        for r in pipeline.legislators:
            assert r["source_id"] == "alego"
            # record_id is name|party|legislature.
            assert r["source_record_id"].count("|") == 2
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("alego_")

    def test_provenance_stamped_on_expenses_and_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.expenses
        for r in pipeline.expenses:
            assert r["source_id"] == "alego"
            # legislator_name|date|supplier|amount composite.
            assert r["source_record_id"].count("|") == 3
            assert r["source_url"].startswith("http")
        for rel in pipeline.expense_rels:
            assert rel["source_id"] == "alego"
            assert rel["source_record_id"]
            assert rel["run_id"].startswith("alego_")

    def test_provenance_stamped_on_propositions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.propositions:
            assert r["source_id"] == "alego"
            assert r["source_record_id"]
            assert r["source_url"].startswith("http")


class TestLoad:
    def test_load_calls_session(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0
