from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.bndes import BndesPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> BndesPipeline:
    driver = MagicMock()
    return BndesPipeline(driver, data_dir=str(FIXTURES))


def _extract_and_transform(pipeline: BndesPipeline) -> None:
    """Run extract + transform from fixture data."""
    pipeline.extract()
    pipeline.transform()


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "bndes"
    assert pipeline.source_id == "bndes"


def test_transform_produces_correct_finances() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # 4 rows: 2 valid, 1 invalid CNPJ (skipped), 1 empty contract (skipped) = 2
    assert len(pipeline.finances) == 2


def test_transform_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    cnpjs = [r["source_key"] for r in pipeline.relationships]
    assert "11.222.333/0001-81" in cnpjs
    assert "44.555.666/0001-99" in cnpjs


def test_transform_skips_invalid_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # The row with CNPJ "12345" (contract GHI-003) must not appear
    contract_nums = [f["contract_number"] for f in pipeline.finances]
    assert "GHI-003" not in contract_nums


def test_transform_skips_empty_contract() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # The row with empty contract number must not appear
    # It has CNPJ 11222333000181 and description "Sem contrato"
    descriptions = [f["description"] for f in pipeline.finances]
    assert "Sem contrato" not in descriptions


def test_transform_parses_brazilian_values() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    values = {f["contract_number"]: f["value_contracted"] for f in pipeline.finances}
    assert values["ABC-001"] == 1234567.89
    assert values["DEF-002"] == 500000.00


def test_transform_deduplicates() -> None:
    """Duplicate finance_id entries should be deduplicated."""
    pipeline = _make_pipeline()
    pipeline.extract()

    # Inject a duplicate row with same contract number as first row
    import pandas as pd

    dup_row = pipeline._raw.iloc[0:1].copy()
    pipeline._raw = pd.concat([pipeline._raw, dup_row], ignore_index=True)

    pipeline.transform()

    # deduplicate_rows on finance_id means only one ABC-001
    ids = [f["finance_id"] for f in pipeline.finances]
    assert ids.count("bndes_ABC-001") == 1


def test_load_calls_batch_loader() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # load_nodes for Finance + _run_with_retry for relationships
    assert session.run.call_count >= 2
