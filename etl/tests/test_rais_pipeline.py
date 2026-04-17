from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.rais import UF_CODE_MAP, RaisPipeline
from tests._mock_helpers import mock_driver, mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None, **kwargs: object) -> RaisPipeline:
    driver = MagicMock()
    return RaisPipeline(
        driver,
        data_dir=data_dir or str(FIXTURES),
        **kwargs,  # type: ignore[arg-type]
    )


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


def test_from_aggregated_skips_empty_uf(tmp_path: Path) -> None:
    """Rows with blank UF must be dropped just like rows with blank CNAE."""
    rais_dir = tmp_path / "rais"
    rais_dir.mkdir()
    (rais_dir / "rais_2022_aggregated.csv").write_text(
        "cnae_subclass,uf,establishment_count,total_employees,total_clt,"
        "total_statutory,avg_employees\n"
        "4711302,,10,100,95,5,10.0\n"  # blank UF → skipped
        "4711302,SP,20,200,180,20,10.0\n",
        encoding="utf-8",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    ufs = {s["uf"] for s in pipeline.labor_stats}
    assert ufs == {"SP"}


def test_extract_with_no_data_files_leaves_stats_empty(tmp_path: Path) -> None:
    """Extract should log a warning and return quietly when no data is present."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline.labor_stats == []


def test_load_short_circuits_when_no_stats(tmp_path: Path) -> None:
    """When labor_stats is empty, load() must warn and not touch the driver."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    # No extract performed, so labor_stats stays empty.
    pipeline.load()
    driver = mock_driver(pipeline)
    # No session context was opened → no `.session()` call made.
    assert not driver.session.called


def test_aggregate_raw_aggregates_by_cnae_uf(tmp_path: Path) -> None:
    """Raw-file path: aggregates multiple rows per (cnae, uf) with UF code lookup."""
    rais_dir = tmp_path / "rais"
    rais_dir.mkdir()
    raw = (
        "CNAE 2.0 Subclasse;Qtd Vínculos Ativos;Qtd Vínculos CLT;"
        "Qtd Vínculos Estatutários;UF\n"
        "4711302;10;8;2;35\n"  # SP
        "4711302;20;15;5;35\n"  # SP, same CNAE → aggregated
        "4711302;30;25;5;33\n"  # RJ
        "0;5;4;1;35\n"  # cnae "0" → skipped
        ";7;6;1;35\n"  # blank cnae → skipped
    )
    (rais_dir / "RAIS_ESTAB_PUB_test.txt").write_text(raw, encoding="latin-1")
    pipeline = _make_pipeline(data_dir=str(tmp_path), chunk_size=10)
    pipeline.extract()

    by_id = {s["stats_id"]: s for s in pipeline.labor_stats}
    assert set(by_id) == {"rais_2022_4711302_SP", "rais_2022_4711302_RJ"}

    sp = by_id["rais_2022_4711302_SP"]
    assert sp["establishment_count"] == 2
    assert sp["total_employees"] == 30  # 10 + 20
    assert sp["total_clt"] == 23  # 8 + 15
    assert sp["total_statutory"] == 7  # 2 + 5
    assert sp["avg_employees"] == 15.0  # 30 / 2

    rj = by_id["rais_2022_4711302_RJ"]
    assert rj["uf"] == "RJ"
    assert rj["avg_employees"] == 30.0


def test_aggregate_raw_preserves_unknown_uf_code(tmp_path: Path) -> None:
    """If the raw file has a UF code not in UF_CODE_MAP, the code is kept verbatim."""
    rais_dir = tmp_path / "rais"
    rais_dir.mkdir()
    raw = (
        "CNAE 2.0 Subclasse;Qtd Vínculos Ativos;Qtd Vínculos CLT;"
        "Qtd Vínculos Estatutários;UF\n"
        "4711302;10;8;2;99\n"  # code 99 not in map
    )
    (rais_dir / "RAIS_ESTAB_PUB_test.txt").write_text(raw, encoding="latin-1")
    pipeline = _make_pipeline(data_dir=str(tmp_path), chunk_size=10)
    pipeline.extract()

    assert pipeline.labor_stats[0]["uf"] == "99"


def test_uf_code_map_covers_all_brazilian_states() -> None:
    """Guard against silent drift in the IBGE → abbr lookup used by _aggregate_raw."""
    # 26 states + DF = 27 entries.
    assert len(UF_CODE_MAP) == 27
    # Spot-check a representative of each region.
    assert UF_CODE_MAP["35"] == "SP"  # Southeast
    assert UF_CODE_MAP["53"] == "DF"  # Center-West
    assert UF_CODE_MAP["23"] == "CE"  # Northeast
    assert UF_CODE_MAP["43"] == "RS"  # South
    assert UF_CODE_MAP["13"] == "AM"  # North
