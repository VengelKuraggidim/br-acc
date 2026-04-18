"""Tests for the TCE Goias scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.tce_go import TceGoPipeline
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TceGoPipeline:
    # Default ``archive_local=False`` mantém o pipeline opt-out pros testes
    # legados — o fluxo é operator-fed (sem HTTP), então não há snapshot
    # automático a menos que o caller ative explicitamente. Cobertura do
    # caminho online (retrofit archival) fica em ``TestArchivalRetrofit``.
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


# ---------------------------------------------------------------------------
# Archival — snapshot dos CSVs operator-fed (retrofit #5 do plano em
# todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Caso especial: TCE-GO não tem endpoint público, então toda ingestão é
# operator-fed (CSVs colocados em ``data/tce_go/`` por quem exportou os
# dashboards). Archival roda opt-in via ``archive_local=True``. Os bytes
# preservados vêm dos próprios fixtures de disco — se o operador deletar
# os arquivos, a cópia content-addressed sob ``BRACC_ARCHIVAL_ROOT/tce_go/``
# sobrevive e satisfaz o requisito de proveniência rastreável.
#
# Este módulo cobre:
#  * três snapshots distintos (decisoes, irregulares, fiscalizacoes);
#  * rows de cada domínio carregam a URI do seu CSV de origem;
#  * impedido_rels replicam a URI do irregular de origem;
#  * o path default (``archive_local=False``) deixa o campo fora,
#    preservando o contrato opt-in e os testes legados.
# ---------------------------------------------------------------------------


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


@pytest.fixture()
def online_pipeline(
    archival_root: Path,  # noqa: ARG001 — just activates the env var
) -> TceGoPipeline:
    """Pipeline com ``archive_local=True`` rodando sobre os fixtures de disco."""
    pipeline = TceGoPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
        archive_local=True,
    )
    # run_id canônico (``{source}_YYYYMMDDHHMMSS``) pra bucket 2025-01 no
    # asserting — facilita conferência visual do layout sob o root.
    pipeline.run_id = "tce_go_20250115000000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: tce_go agora grava snapshots dos CSVs operator-fed."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: TceGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Cada domínio tem seu próprio snapshot (três CSVs distintos =
        # três bytes distintos = três hashes distintos).
        decisions_uri = online_pipeline._decisions_snapshot_uri
        irregular_uri = online_pipeline._irregular_snapshot_uri
        audits_uri = online_pipeline._audits_snapshot_uri
        assert isinstance(decisions_uri, str) and decisions_uri
        assert isinstance(irregular_uri, str) and irregular_uri
        assert isinstance(audits_uri, str) and audits_uri
        assert decisions_uri != irregular_uri != audits_uri

        # Shape da URI: ``tce_go/YYYY-MM/hash12.csv``.
        for uri in (decisions_uri, irregular_uri, audits_uri):
            parts = uri.split("/")
            assert parts[0] == "tce_go"
            assert parts[1] == "2025-01"
            assert parts[2].endswith(".csv")

        # Rows de cada domínio carregam a URI do CSV de origem.
        assert online_pipeline.decisions
        for d in online_pipeline.decisions:
            assert d.get("source_snapshot_uri") == decisions_uri
        assert online_pipeline.irregular_accounts
        for ir in online_pipeline.irregular_accounts:
            assert ir.get("source_snapshot_uri") == irregular_uri
        assert online_pipeline.audits
        for a in online_pipeline.audits:
            assert a.get("source_snapshot_uri") == audits_uri

        # impedido_rels (CNPJ-only) replicam a URI do irregular de origem.
        assert online_pipeline.impedido_rels
        for rel in online_pipeline.impedido_rels:
            assert rel.get("source_snapshot_uri") == irregular_uri

        # Storage: arquivos fisicamente presentes sob o root configurado.
        for uri in (decisions_uri, irregular_uri, audits_uri):
            absolute = archival_root / uri
            assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do CSV.
        restored_irregular = restore_snapshot(irregular_uri)
        source_bytes = (
            FIXTURES / "tce_go" / "irregulares.csv"
        ).read_bytes()
        assert restored_irregular == source_bytes

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Pipeline com ``archive_local=False`` (default) deixa o campo fora."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.decisions
        for d in pipeline.decisions:
            # Ausência do campo == opt-in não ativado (contrato do
            # attach_provenance: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in d
        for ir in pipeline.irregular_accounts:
            assert "source_snapshot_uri" not in ir
        for a in pipeline.audits:
            assert "source_snapshot_uri" not in a
        for rel in pipeline.impedido_rels:
            assert "source_snapshot_uri" not in rel
