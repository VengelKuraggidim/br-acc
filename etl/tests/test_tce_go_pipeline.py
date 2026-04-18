"""Tests for the TCE Goias scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.tce_go import TceGoPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TceGoPipeline:
    return TceGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert TceGoPipeline.name == "tce_go"

    def test_source_id(self) -> None:
        assert TceGoPipeline.source_id == "tce_go"


class TestExtract:
    def test_extract_all_three_domains(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert len(pipeline._raw_decisions) == 2
        assert len(pipeline._raw_irregular) == 1
        assert len(pipeline._raw_audits) == 2


class TestTransform:
    def test_decisions_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.decisions) == 2

    def test_irregular_accounts_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.irregular_accounts) == 1

    def test_irregular_cnpj_formatted(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {r["cnpj"] for r in pipeline.irregular_accounts}
        assert "55.667.788/0001-99" in cnpjs

    def test_audits_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.audits) == 2

    def test_uf_always_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.decisions + pipeline.irregular_accounts + pipeline.audits:
            assert r["uf"] == "GO"

    def test_source_tagged(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.decisions + pipeline.irregular_accounts + pipeline.audits:
            assert r["source"] == "tce_go"

    def test_provenance_stamped_on_decisions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.decisions
        for r in pipeline.decisions:
            assert r["source_id"] == "tce_go"
            assert r["source_record_id"]  # numero|published_at composite
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("tce_go_")

    def test_provenance_stamped_on_irregular_and_audits(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.irregular_accounts:
            assert r["source_id"] == "tce_go"
            assert r["source_record_id"]
            assert r["source_url"].startswith("http")
        for rel in pipeline.impedido_rels:
            assert rel["source_id"] == "tce_go"
            assert rel["source_record_id"]
        for a in pipeline.audits:
            assert a["source_id"] == "tce_go"
            assert a["source_record_id"]
            assert a["source_url"].startswith("http")

    def test_provenance_stamped_unit(self) -> None:
        """Scaffold coverage without relying on fixture presence."""
        import pandas as pd

        pipeline = _make_pipeline()
        pipeline._raw_decisions = pd.DataFrame([
            {
                "numero": "2024/1234",
                "tipo": "acordao",
                "data": "2024-05-01",
                "orgao": "Secretaria X",
                "ementa": "ementa teste",
                "relator": "Conselheiro A",
            },
        ])
        pipeline._raw_irregular = pd.DataFrame()
        pipeline._raw_audits = pd.DataFrame()
        pipeline.transform()
        d = pipeline.decisions[0]
        assert d["source_id"] == "tce_go"
        assert d["source_record_id"] == "2024/1234|2024-05-01"
        assert d["source_url"].startswith("http")


class TestLoad:
    def test_load_creates_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0
