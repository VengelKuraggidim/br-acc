"""Tests for the SSP-GO scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.ssp_go import SspGoPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SspGoPipeline:
    return SspGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert SspGoPipeline.name == "ssp_go"

    def test_source_id(self) -> None:
        assert SspGoPipeline.source_id == "ssp_go"


class TestTransform:
    def test_stats_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.stats) == 3

    def test_counts_parsed_as_int(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        counts = {s["count"] for s in pipeline.stats}
        assert 42 in counts
        assert 128 in counts
        assert 5 in counts

    def test_uf_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert all(s["uf"] == "GO" for s in pipeline.stats)

    def test_provenance_stamped_on_stats(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.stats
        for s in pipeline.stats:
            assert s["source_id"] == "ssp_go"
            # cod_ibge|crime_type|periodo composite.
            assert s["source_record_id"].count("|") == 2
            assert s["source_url"].startswith("http")
            assert s["ingested_at"].startswith("20")
            assert s["run_id"].startswith("ssp_go_")

    def test_provenance_stamped_unit(self) -> None:
        """Unit-level test so the scaffold is covered even without fixture."""
        pipeline = _make_pipeline()
        # Directly build a single raw row matching the shape ``transform``
        # expects. This exercises attach_provenance on the scaffold path
        # regardless of whether operator-provided CSVs exist.
        import pandas as pd

        pipeline._raw_stats = pd.DataFrame([
            {
                "cod_ibge": "5208707",
                "municipio": "Goiania",
                "natureza": "Roubo",
                "periodo": "2024-01",
                "quantidade": "128",
            },
        ])
        pipeline.transform()
        assert len(pipeline.stats) == 1
        stat = pipeline.stats[0]
        assert stat["source_id"] == "ssp_go"
        assert stat["source_record_id"] == "5208707|ROUBO|2024-01"
        assert stat["source_url"].startswith("http")


class TestLoad:
    def test_load_creates_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0
