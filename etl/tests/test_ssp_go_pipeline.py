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


class TestLoad:
    def test_load_creates_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0
