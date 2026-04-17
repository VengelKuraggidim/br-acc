from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.icij import (
    EXACT_MATCH,
    HIGH_CONFIDENCE,
    MIN_CONFIDENCE,
    ICIJPipeline,
    name_similarity,
)
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> ICIJPipeline:
    driver = MagicMock()
    return ICIJPipeline(driver, data_dir=str(FIXTURES))


def _extract_and_transform(pipeline: ICIJPipeline) -> None:
    pipeline.extract()
    pipeline.transform()


# --- Pipeline metadata ---


def test_pipeline_name() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "icij"


def test_pipeline_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.source_id == "icij_offshore_leaks"


# --- Extract ---


def test_extract_reads_all_csvs() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    assert len(pipeline._entities_raw) == 7
    assert len(pipeline._officers_raw) == 5
    assert len(pipeline._intermediaries_raw) == 2
    assert len(pipeline._relationships_raw) == 6


# --- Transform entities ---


def test_transform_filters_brazilian_entities() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # Entities with BRA connection: 10001 (jurisdiction=BRA), 10003 (country_codes has BRA),
    # 10005 (address has Brasil), 10006 (empty name, skipped)
    # 10002 (PAN only), 10004 (BMU only), 10007 (empty) = excluded
    assert len(pipeline.offshore_entities) == 3


def test_transform_entity_ids() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {e["offshore_id"] for e in pipeline.offshore_entities}
    assert "icij_10001" in ids
    assert "icij_10003" in ids
    assert "icij_10005" in ids


def test_transform_skips_non_brazilian_entities() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {e["offshore_id"] for e in pipeline.offshore_entities}
    assert "icij_10002" not in ids  # Panama only
    assert "icij_10004" not in ids  # Bermuda only


def test_transform_skips_empty_name_entity() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {e["offshore_id"] for e in pipeline.offshore_entities}
    assert "icij_10006" not in ids  # Brazilian but empty name


def test_transform_skips_empty_node_id() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {e["offshore_id"] for e in pipeline.offshore_entities}
    assert "icij_" not in ids


def test_transform_entity_normalizes_name() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    entity = next(e for e in pipeline.offshore_entities if e["offshore_id"] == "icij_10001")
    # normalize_name uppercases and strips
    assert entity["name"] == "OFFSHORE HOLDING BRASIL LTDA"
    assert entity["original_name"] == "OFFSHORE HOLDING BRASIL LTDA"


def test_transform_entity_preserves_metadata() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    entity = next(e for e in pipeline.offshore_entities if e["offshore_id"] == "icij_10001")
    assert entity["jurisdiction"] == "BRA"
    assert entity["source_investigation"] == "Panama Papers"
    assert entity["status"] == "Active"
    assert entity["incorporation_date"] == "2005-03-15"
    assert entity["source"] == "icij_offshore_leaks"


# --- Transform officers ---


def test_transform_filters_brazilian_officers() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # 20001 (BRA), 20002 (BRA), 20004 (countries has Brasil), 20005 (empty name, skipped)
    # 20003 (USA only) = excluded
    assert len(pipeline.offshore_officers) == 3


def test_transform_officer_ids() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {o["offshore_officer_id"] for o in pipeline.offshore_officers}
    assert "icij_20001" in ids
    assert "icij_20002" in ids
    assert "icij_20004" in ids


def test_transform_skips_non_brazilian_officers() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {o["offshore_officer_id"] for o in pipeline.offshore_officers}
    assert "icij_20003" not in ids  # USA only


def test_transform_skips_empty_name_officer() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {o["offshore_officer_id"] for o in pipeline.offshore_officers}
    assert "icij_20005" not in ids  # Brazilian but empty name


def test_transform_officer_normalizes_name() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    officer = next(
        o for o in pipeline.offshore_officers
        if o["offshore_officer_id"] == "icij_20001"
    )
    assert officer["name"] == "JOAO DA SILVA"


# --- Transform relationships ---


def test_transform_officer_of_relationships() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # 20001->10001 (both Brazilian), 20002->10003 (both Brazilian), 20004->10005 (both Brazilian)
    # 20003->10004 excluded (20003 not in officers, 10004 not in entities)
    assert len(pipeline.officer_of_rels) == 3


def test_transform_intermediary_of_relationships() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # 30001->10001 (10001 is Brazilian entity, 30001 is Brazilian intermediary)
    # 30002->10002 (10002 not in filtered entities) = excluded
    assert len(pipeline.intermediary_of_rels) == 1


def test_transform_officer_rel_has_link() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    rel = next(r for r in pipeline.officer_of_rels if r["source_key"] == "icij_20001")
    assert rel["target_key"] == "icij_10001"
    assert rel["link"] == "officer of"
    assert rel["source_investigation"] == "Panama Papers"


def test_transform_intermediary_rel_has_link() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    rel = pipeline.intermediary_of_rels[0]
    assert rel["source_key"] == "icij_30001"
    assert rel["target_key"] == "icij_10001"
    assert rel["link"] == "intermediary of"


# --- Deduplication ---


def test_transform_deduplicates_entities() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    # Inject duplicate entity row
    dup = pipeline._entities_raw.iloc[0:1].copy()
    pipeline._entities_raw = pd.concat([pipeline._entities_raw, dup], ignore_index=True)
    pipeline.transform()
    ids = [e["offshore_id"] for e in pipeline.offshore_entities]
    assert ids.count("icij_10001") == 1


def test_transform_deduplicates_officers() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    dup = pipeline._officers_raw.iloc[0:1].copy()
    pipeline._officers_raw = pd.concat([pipeline._officers_raw, dup], ignore_index=True)
    pipeline.transform()
    ids = [o["offshore_officer_id"] for o in pipeline.offshore_officers]
    assert ids.count("icij_20001") == 1


# --- Name similarity ---


def test_name_similarity_exact() -> None:
    assert name_similarity("JOAO DA SILVA", "JOAO DA SILVA") == EXACT_MATCH


def test_name_similarity_case_insensitive() -> None:
    assert name_similarity("Joao da Silva", "JOAO DA SILVA") == EXACT_MATCH


def test_name_similarity_high_confidence() -> None:
    score = name_similarity("JOAO DA SILVA", "JOAO D SILVA")
    assert score >= HIGH_CONFIDENCE


def test_name_similarity_low_confidence() -> None:
    score = name_similarity("JOAO DA SILVA", "COMPLETELY DIFFERENT")
    assert score < MIN_CONFIDENCE


def test_name_similarity_partial() -> None:
    score = name_similarity("MARIA OLIVEIRA SANTOS", "MARIA O SANTOS")
    assert score >= MIN_CONFIDENCE


# --- Brazilian detection ---


def test_is_brazilian_jurisdiction() -> None:
    row = pd.Series({"jurisdiction": "BRA", "country_codes": "", "countries": "", "address": ""})
    assert ICIJPipeline._is_brazilian(row)


def test_is_brazilian_country_codes() -> None:
    row = pd.Series({
        "jurisdiction": "", "country_codes": "BRA;VGB",
        "countries": "", "address": "",
    })
    assert ICIJPipeline._is_brazilian(row)


def test_is_brazilian_address() -> None:
    row = pd.Series({
        "jurisdiction": "", "country_codes": "",
        "countries": "", "address": "Sao Paulo Brasil",
    })
    assert ICIJPipeline._is_brazilian(row)


def test_is_not_brazilian() -> None:
    row = pd.Series({
        "jurisdiction": "PAN", "country_codes": "PAN",
        "countries": "Panama", "address": "Panama City",
    })
    assert not ICIJPipeline._is_brazilian(row)


def test_is_brazilian_countries_field() -> None:
    row = pd.Series({"jurisdiction": "", "country_codes": "", "countries": "Brazil", "address": ""})
    assert ICIJPipeline._is_brazilian(row)


# --- Load ---


def test_load_calls_batch_loader() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    assert session.run.call_count >= 2


def test_load_empty_pipeline_no_calls() -> None:
    pipeline = _make_pipeline()
    # Don't extract — everything is empty
    pipeline.offshore_entities = []
    pipeline.offshore_officers = []
    pipeline.officer_of_rels = []
    pipeline.intermediary_of_rels = []
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    assert session.run.call_count == 0
