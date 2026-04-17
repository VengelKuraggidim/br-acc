from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.holdings import HoldingsPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> HoldingsPipeline:
    driver = MagicMock()
    return HoldingsPipeline(driver, data_dir=str(FIXTURES))


def _extract_and_transform(pipeline: HoldingsPipeline) -> None:
    pipeline.extract()
    pipeline.transform()


# --- Pipeline metadata ---


def test_pipeline_name() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "holdings"


def test_pipeline_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.source_id == "brasil_io_holdings"


# --- Extract ---


def test_extract_reads_csv() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    assert len(pipeline._raw) == 6


def test_extract_missing_file_no_error() -> None:
    driver = MagicMock()
    pipeline = HoldingsPipeline(driver, data_dir="/nonexistent")
    pipeline.extract()
    assert len(pipeline._raw) == 0


# --- Transform ---


def test_transform_creates_valid_rels() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # Row 1: valid (11222333000155, 99888777000166)
    # Row 2: valid (44555666000177, 11222333000155)
    # Row 3: invalid CNPJ ("invalid") -> skipped
    # Row 4: empty cnpj_empresa -> skipped
    # Row 5: self-holding (same CNPJ both sides) -> skipped
    # Row 6: valid (77888999000100, 33444555000188)
    assert len(pipeline.holding_rels) == 3


def test_transform_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # First row: socia=99888777000166 holds empresa=11222333000155
    rel = pipeline.holding_rels[0]
    assert rel["source_key"] == "99.888.777/0001-66"
    assert rel["target_key"] == "11.222.333/0001-55"


def test_transform_skips_invalid_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    all_keys = set()
    for rel in pipeline.holding_rels:
        all_keys.add(rel["source_key"])
        all_keys.add(rel["target_key"])
    # "invalid" should not appear in any key
    assert all("." in k for k in all_keys)


def test_transform_skips_empty_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    assert len(pipeline.holding_rels) == 3


def test_transform_skips_self_holding() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    for rel in pipeline.holding_rels:
        assert rel["source_key"] != rel["target_key"]


def test_transform_direction_socia_holds_empresa() -> None:
    """HOLDING_DE direction: socia (source) -> empresa (target)."""
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    # Row 2: empresa=44555666000177, socia=11222333000155
    # socia holds empresa, so source=socia, target=empresa
    rel = pipeline.holding_rels[1]
    assert rel["source_key"] == "11.222.333/0001-55"
    assert rel["target_key"] == "44.555.666/0001-77"


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
    pipeline.holding_rels = []
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    assert session.run.call_count == 0


def test_load_cypher_uses_holding_de_rel() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Check that HOLDING_DE appears in the cypher query
    first_call = session.run.call_args_list[0]
    query = first_call[0][0]
    assert "HOLDING_DE" in query
    assert "Company" in query
