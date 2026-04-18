"""Tests for the TCM-GO sanctions scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.tcmgo_sancoes import TcmgoSancoesPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TcmgoSancoesPipeline:
    return TcmgoSancoesPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert TcmgoSancoesPipeline.name == "tcmgo_sancoes"

    def test_source_id(self) -> None:
        assert TcmgoSancoesPipeline.source_id == "tcmgo_sancoes"


class TestTransform:
    def test_impedidos_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.impedidos) == 2

    def test_rejected_accounts_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.rejected_accounts) == 1

    def test_cnpj_and_cpf_distinguished(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        kinds = {r["document_kind"] for r in pipeline.impedidos}
        assert kinds == {"CNPJ", "CPF"}

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cpfs = [
            r["document"] for r in pipeline.impedidos
            if r["document_kind"] == "CPF"
        ]
        assert all("***" in c for c in cpfs)

    def test_impedido_rels_only_for_cnpj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # Only the CNPJ row should produce a relationship.
        assert len(pipeline.impedido_rels) == 1

    def test_uf_and_source(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.impedidos + pipeline.rejected_accounts:
            assert r["uf"] == "GO"
            assert r["source"] == "tcmgo_sancoes"

    def test_provenance_stamped_on_impedidos(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.impedidos
        for r in pipeline.impedidos:
            assert r["source_id"] == "tcmgo_sancoes"
            # document|processo composite.
            assert "|" in r["source_record_id"]
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("tcmgo_sancoes_")
        for rel in pipeline.impedido_rels:
            assert rel["source_id"] == "tcmgo_sancoes"
            assert "|" in rel["source_record_id"]

    def test_provenance_stamped_on_rejected_accounts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.rejected_accounts:
            assert r["source_id"] == "tcmgo_sancoes"
            # cod_ibge|exercicio|processo composite.
            assert r["source_record_id"].count("|") == 2
            assert r["source_url"].startswith("http")


class TestLoad:
    def test_load_runs(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0
