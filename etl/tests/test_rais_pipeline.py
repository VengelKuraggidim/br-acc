from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.rais import RaisPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> RaisPipeline:
    driver = MagicMock()
    return RaisPipeline(driver, data_dir=str(FIXTURES))


def _extract(pipeline: RaisPipeline) -> None:
    """Run extract against pre-aggregated CSV in fixtures/rais/."""
    pipeline.extract()


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "rais"
    assert pipeline.source_id == "rais_mte"


def test_transform_aggregates_by_cnae_uf() -> None:
    """Extract loads from pre-aggregated CSV; each valid row becomes a LaborStats entry."""
    pipeline = _make_pipeline()
    _extract(pipeline)

    # Fixture has 5 rows: 4 valid (non-empty cnae+uf), 1 with empty cnae (skipped)
    assert len(pipeline.labor_stats) == 4

    stats_ids = {s["stats_id"] for s in pipeline.labor_stats}
    assert "rais_2022_4711302_SP" in stats_ids
    assert "rais_2022_4711302_RJ" in stats_ids
    assert "rais_2022_8411600_DF" in stats_ids
    assert "rais_2022_8512100_MG" in stats_ids


def test_transform_produces_labor_stats() -> None:
    """Verify the structure and values of extracted labor stats."""
    pipeline = _make_pipeline()
    _extract(pipeline)

    sp_stat = next(s for s in pipeline.labor_stats if s["stats_id"] == "rais_2022_4711302_SP")
    assert sp_stat["cnae_subclass"] == "4711302"
    assert sp_stat["uf"] == "SP"
    assert sp_stat["year"] == 2022
    assert sp_stat["establishment_count"] == 1500
    assert sp_stat["total_employees"] == 45000
    assert sp_stat["total_clt"] == 42000
    assert sp_stat["total_statutory"] == 0
    assert sp_stat["avg_employees"] == 30.0
    assert sp_stat["source"] == "rais_mte"

    df_stat = next(s for s in pipeline.labor_stats if s["stats_id"] == "rais_2022_8411600_DF")
    assert df_stat["total_statutory"] == 14500
    assert df_stat["total_clt"] == 0


def test_transform_skips_empty_cnae() -> None:
    """Rows with empty cnae_subclass should be skipped."""
    pipeline = _make_pipeline()
    _extract(pipeline)

    cnae_values = [s["cnae_subclass"] for s in pipeline.labor_stats]
    assert "" not in cnae_values


def test_transform_is_noop() -> None:
    """RAIS transform() is a no-op since aggregation happens in extract."""
    pipeline = _make_pipeline()
    _extract(pipeline)

    count_before = len(pipeline.labor_stats)
    pipeline.transform()
    assert len(pipeline.labor_stats) == count_before


def test_load_calls_session() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should call session.run for: LaborStats nodes + 2 index creations = at least 3
    assert session.run.call_count >= 3
