from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from bracc_etl.pipelines.opensanctions import (
    BRAZIL_COUNTRY_CODES,
    BRAZIL_POSITION_TERMS,
    EXACT_CPF_MATCH,
    OpenSanctionsPipeline,
    _extract_cpf,
    _is_brazilian_entity,
)
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> OpenSanctionsPipeline:
    driver = MagicMock()
    return OpenSanctionsPipeline(driver, data_dir=str(FIXTURES))


def _extract_and_transform(pipeline: OpenSanctionsPipeline) -> None:
    pipeline.extract()
    pipeline.transform()


# --- Pipeline metadata ---


def test_pipeline_name() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "opensanctions"


def test_pipeline_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.source_id == "opensanctions"


# --- Extract ---


def test_extract_reads_jsonl() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    assert len(pipeline._raw_entities) == 10


def test_extract_missing_file_no_error() -> None:
    driver = MagicMock()
    pipeline = OpenSanctionsPipeline(driver, data_dir="/nonexistent")
    pipeline.extract()
    assert pipeline._raw_entities == []


# --- Brazilian entity detection ---


def test_is_brazilian_by_country() -> None:
    entity = {"properties": {"country": ["br"]}}
    assert _is_brazilian_entity(entity)


def test_is_brazilian_by_nationality() -> None:
    entity = {"properties": {"nationality": ["bra"]}}
    assert _is_brazilian_entity(entity)


def test_is_brazilian_by_position_term() -> None:
    entity = {"properties": {"country": ["de"], "position": ["Member of Brazilian Congress"]}}
    assert _is_brazilian_entity(entity)


def test_is_brazilian_by_deputado() -> None:
    entity = {"properties": {"position": ["Deputado Federal"]}}
    assert _is_brazilian_entity(entity)


def test_is_not_brazilian() -> None:
    entity = {"properties": {"country": ["us"], "position": ["Senator"]}}
    assert not _is_brazilian_entity(entity)


def test_is_not_brazilian_empty() -> None:
    entity: dict[str, Any] = {"properties": {}}
    assert not _is_brazilian_entity(entity)


# --- CPF extraction ---


def test_extract_cpf_from_tax_number() -> None:
    entity = {"properties": {"taxNumber": ["12345678901"]}}
    assert _extract_cpf(entity) == "123.456.789-01"


def test_extract_cpf_ignores_non_cpf() -> None:
    entity = {"properties": {"taxNumber": ["ABC123"]}}
    assert _extract_cpf(entity) is None


def test_extract_cpf_picks_first_valid() -> None:
    entity = {"properties": {"taxNumber": ["ABC", "98765432100"]}}
    assert _extract_cpf(entity) == "987.654.321-00"


def test_extract_cpf_empty_props() -> None:
    entity: dict[str, Any] = {"properties": {}}
    assert _extract_cpf(entity) is None


# --- Transform ---


def test_transform_filters_brazilian_persons() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # Q10001 (country=br), Q10002 (country=br), Q10005 (nationality=bra),
    # Q10006 (position has "Brazilian"), Q10008 (country=br), Q10009 (country=br, position=Vereador)
    # Excluded: Q10003 (us), Q10004 (Company schema), Q10007 (empty name), Q10008's empty id
    assert len(pipeline.global_peps) == 6


def test_transform_skips_company_schema() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {p["pep_id"] for p in pipeline.global_peps}
    assert "os_Q10004" not in ids


def test_transform_skips_non_brazilian() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {p["pep_id"] for p in pipeline.global_peps}
    assert "os_Q10003" not in ids


def test_transform_skips_empty_name() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {p["pep_id"] for p in pipeline.global_peps}
    assert "os_Q10007" not in ids


def test_transform_skips_empty_id() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    ids = {p["pep_id"] for p in pipeline.global_peps}
    assert "os_" not in ids


def test_transform_pep_node_fields() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pep = next(p for p in pipeline.global_peps if p["pep_id"] == "os_Q10001")
    assert pep["name"] == "JOAO DA SILVA"
    assert pep["original_name"] == "JOAO DA SILVA"
    assert pep["country"] == "br"
    assert pep["position"] == "Deputado Federal"
    assert pep["start_date"] == "2019-01-01"
    assert pep["end_date"] == "2023-01-01"
    assert "br_transparency" in pep["datasets"]
    assert pep["cpf"] == "123.456.789-01"
    assert pep["source"] == "opensanctions"


def test_transform_pep_without_dates() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pep = next(p for p in pipeline.global_peps if p["pep_id"] == "os_Q10005")
    assert pep["start_date"] == ""
    assert pep["end_date"] == ""


def test_transform_pep_without_cpf() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pep = next(p for p in pipeline.global_peps if p["pep_id"] == "os_Q10002")
    assert pep["cpf"] == ""


def test_transform_normalizes_name() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pep = next(p for p in pipeline.global_peps if p["pep_id"] == "os_Q10002")
    assert pep["name"] == "MARIA OLIVEIRA SANTOS"


def test_transform_pep_multiple_tax_numbers() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pep = next(p for p in pipeline.global_peps if p["pep_id"] == "os_Q10008")
    assert pep["cpf"] == "987.654.321-00"


# --- CPF match relationships ---


def test_cpf_match_rels_created() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # Q10001 has CPF, Q10008 has CPF = 2 CPF match rels
    assert len(pipeline.pep_match_rels) == 2


def test_cpf_match_rel_fields() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    rel = next(r for r in pipeline.pep_match_rels if r["target_key"] == "os_Q10001")
    assert rel["source_key"] == "123.456.789-01"
    assert rel["match_type"] == "cpf_exact"
    assert rel["confidence"] == EXACT_CPF_MATCH


def test_no_cpf_match_without_cpf() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    targets = {r["target_key"] for r in pipeline.pep_match_rels}
    assert "os_Q10002" not in targets  # No CPF


# --- Deduplication ---


def test_transform_deduplicates() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    # Inject duplicate
    pipeline._raw_entities.append(pipeline._raw_entities[0].copy())
    pipeline.transform()
    ids = [p["pep_id"] for p in pipeline.global_peps]
    assert ids.count("os_Q10001") == 1


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
    pipeline.global_peps = []
    pipeline.pep_match_rels = []
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    assert session.run.call_count == 0


# --- Constants ---


def test_brazil_country_codes_lowercase() -> None:
    for code in BRAZIL_COUNTRY_CODES:
        assert code == code.lower()


def test_brazil_position_terms_lowercase() -> None:
    for term in BRAZIL_POSITION_TERMS:
        assert term == term.lower()
