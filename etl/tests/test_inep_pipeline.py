from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.inep import InepPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> InepPipeline:
    driver = MagicMock()
    return InepPipeline(driver, data_dir=str(FIXTURES))


def _extract(pipeline: InepPipeline) -> None:
    """Run extract against fixture CSV in fixtures/inep/."""
    pipeline.extract()


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "inep"
    assert pipeline.source_id == "inep_censo_escolar"


def test_transform_creates_education_nodes() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    # Fixture has 7 rows: 6 with valid CO_ENTIDADE, 1 with empty (skipped)
    assert len(pipeline.schools) == 6

    school_ids = {s["school_id"] for s in pipeline.schools}
    assert "11001001" in school_ids
    assert "22002002" in school_ids
    assert "33003003" in school_ids
    assert "44004004" in school_ids
    assert "55005005" in school_ids
    assert "66006006" in school_ids


def test_transform_links_private_schools_to_companies() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    # School 33003003 has NU_CNPJ_ESCOLA_PRIVADA=11222333000181 and
    # NU_CNPJ_MANTENEDORA=44555666000199 (different, so both create links)
    # School 55005005 has NU_CNPJ_ESCOLA_PRIVADA=77888999000100 (1 link)
    # School 66006006 has same CNPJ for both fields -> only 1 link (dedup via condition)
    assert len(pipeline.school_company_links) == 4

    source_keys = [lnk["source_key"] for lnk in pipeline.school_company_links]
    assert "11.222.333/0001-81" in source_keys
    assert "44.555.666/0001-99" in source_keys
    assert "77.888.999/0001-00" in source_keys
    assert "99.000.111/0001-88" in source_keys


def test_transform_skips_public_schools() -> None:
    """Public schools (federal, estadual, municipal) should have no company links."""
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    # Public school IDs: 11001001 (municipal), 22002002 (estadual), 44004004 (federal)
    public_ids = {"11001001", "22002002", "44004004"}
    linked_school_ids = {lnk["target_key"] for lnk in pipeline.school_company_links}
    assert linked_school_ids.isdisjoint(public_ids)


def test_transform_maps_admin_type() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    by_id = {s["school_id"]: s for s in pipeline.schools}
    assert by_id["11001001"]["admin_type"] == "municipal"
    assert by_id["22002002"]["admin_type"] == "estadual"
    assert by_id["33003003"]["admin_type"] == "privada"
    assert by_id["44004004"]["admin_type"] == "federal"


def test_transform_maps_status() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    by_id = {s["school_id"]: s for s in pipeline.schools}
    assert by_id["11001001"]["status"] == "em_atividade"
    assert by_id["55005005"]["status"] == "extinta"


def test_transform_parses_enrollment_and_staff() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    by_id = {s["school_id"]: s for s in pipeline.schools}
    assert by_id["22002002"]["enrollment"] == 1200
    assert by_id["22002002"]["staff"] == 95
    assert by_id["55005005"]["enrollment"] == 0
    assert by_id["55005005"]["staff"] == 0


def test_transform_normalizes_names() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    by_id = {s["school_id"]: s for s in pipeline.schools}
    # normalize_name uppercases and strips accents
    assert by_id["11001001"]["name"] == "ESCOLA MUNICIPAL SAO PAULO"


def test_transform_sets_source() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    for s in pipeline.schools:
        assert s["source"] == "inep_censo_escolar"
        assert s["year"] == 2022


def test_load_calls_session() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should call session.run for: Education nodes + MANTEDORA_DE relationships = at least 2
    assert session.run.call_count >= 2
