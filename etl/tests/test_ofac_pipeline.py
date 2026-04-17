from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.ofac import (
    VALID_SDN_TYPES,
    OfacPipeline,
    _clean_sdn_type,
)
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> OfacPipeline:
    driver = MagicMock()
    return OfacPipeline(driver, data_dir=str(FIXTURES))


def _extract_and_transform(pipeline: OfacPipeline) -> None:
    pipeline.extract()
    pipeline.transform()


# --- Pipeline metadata ---


def test_pipeline_name() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "ofac"


def test_pipeline_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.source_id == "ofac_sdn"


# --- Extract ---


def test_extract_reads_headerless_csv() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    assert len(pipeline._raw) == 8


def test_extract_assigns_column_names() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    assert "ent_num" in pipeline._raw.columns
    assert "sdn_name" in pipeline._raw.columns
    assert "sdn_type" in pipeline._raw.columns
    assert "remarks" in pipeline._raw.columns


def test_extract_missing_file_no_error() -> None:
    driver = MagicMock()
    pipeline = OfacPipeline(driver, data_dir="/nonexistent")
    pipeline.extract()
    assert len(pipeline._raw) == 0


# --- Clean SDN type ---


def test_clean_sdn_type_individual() -> None:
    assert _clean_sdn_type(" individual") == "individual"


def test_clean_sdn_type_entity() -> None:
    assert _clean_sdn_type(" entity") == "entity"


def test_clean_sdn_type_dash_zero() -> None:
    # "-0- " is sometimes used for individuals in older data
    assert _clean_sdn_type(" -0- ") == "0"


def test_clean_sdn_type_vessel() -> None:
    assert _clean_sdn_type(" vessel") == "vessel"


# --- Transform ---


def test_transform_filters_valid_types() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # 12345: individual, 12346: entity, 12347: individual, 12348: individual
    # 12349: individual but empty name (skipped)
    # 12350: vessel (excluded), 12351: aircraft (excluded)
    # 12352: -0- cleaned to "0" (not in VALID_SDN_TYPES, excluded)
    assert len(pipeline.sanctions) == 4


def test_transform_sanction_ids() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {s["sanction_id"] for s in pipeline.sanctions}
    assert "ofac_12345" in ids
    assert "ofac_12346" in ids
    assert "ofac_12347" in ids
    assert "ofac_12348" in ids


def test_transform_skips_empty_name() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {s["sanction_id"] for s in pipeline.sanctions}
    assert "ofac_12349" not in ids


def test_transform_skips_vessel_and_aircraft() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {s["sanction_id"] for s in pipeline.sanctions}
    assert "ofac_12350" not in ids
    assert "ofac_12351" not in ids


def test_transform_normalizes_name() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    sanction = next(s for s in pipeline.sanctions if s["sanction_id"] == "ofac_12345")
    assert sanction["name"] == "SILVA, JOAO"
    assert sanction["original_name"] == 'SILVA, Joao'


def test_transform_preserves_fields() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    sanction = next(s for s in pipeline.sanctions if s["sanction_id"] == "ofac_12345")
    assert sanction["sdn_type"] == "individual"
    assert sanction["program"] == "SDGT"
    assert "Brazil" in sanction["remarks"]
    assert sanction["source"] == "ofac_sdn"


def test_transform_entity_type() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    sanction = next(s for s in pipeline.sanctions if s["sanction_id"] == "ofac_12346")
    assert sanction["sdn_type"] == "entity"
    assert sanction["name"] == "EMPRESA FANTASMA LTDA"


def test_transform_preserves_title() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    sanction = next(s for s in pipeline.sanctions if s["sanction_id"] == "ofac_12346")
    assert sanction["title"] == "Director"


# --- Deduplication ---


def test_transform_deduplicates() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    # Inject duplicate row
    dup = pipeline._raw.iloc[0:1].copy()
    pipeline._raw = pd.concat([pipeline._raw, dup], ignore_index=True)
    pipeline.transform()
    ids = [s["sanction_id"] for s in pipeline.sanctions]
    assert ids.count("ofac_12345") == 1


# --- Load ---


def test_load_calls_batch_loader() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    assert session.run.call_count >= 1


def test_load_empty_pipeline_no_calls() -> None:
    pipeline = _make_pipeline()
    pipeline.sanctions = []
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    assert session.run.call_count == 0


# --- Constants ---


def test_valid_sdn_types() -> None:
    assert "individual" in VALID_SDN_TYPES
    assert "entity" in VALID_SDN_TYPES
    assert "vessel" not in VALID_SDN_TYPES
