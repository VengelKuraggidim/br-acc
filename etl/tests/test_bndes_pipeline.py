from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.bndes import BndesPipeline
from tests._mock_helpers import mock_driver, mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> BndesPipeline:
    driver = MagicMock()
    return BndesPipeline(driver, data_dir=data_dir or str(FIXTURES))


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


class TestParseValue:
    def test_blank_returns_zero(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._parse_value("") == 0.0
        assert pipeline._parse_value("   ") == 0.0

    def test_brazilian_thousands_and_decimal(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._parse_value("1.234.567,89") == 1234567.89

    def test_comma_only_decimal(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._parse_value("500,00") == 500.0

    def test_invalid_returns_zero(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._parse_value("abc") == 0.0


def test_extract_with_missing_data_dir(tmp_path: Path) -> None:
    """Missing `bndes/` data dir must warn and leave `_raw` empty."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline._raw.empty


def test_extract_with_missing_csv(tmp_path: Path) -> None:
    """Dir exists but CSV doesn't → `_raw` empty, no raise."""
    (tmp_path / "bndes").mkdir()
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline._raw.empty


def test_finance_value_falls_back_to_contracted_when_disbursed_zero() -> None:
    """Finance `value` should be disbursed OR contracted — prefers disbursed."""
    pipeline = _make_pipeline()
    pipeline._raw = pd.DataFrame(
        [
            {
                "cnpj": "11222333000181",
                "numero_do_contrato": "ABC-999",
                "valor_contratado_reais": "1.000,00",
                "valor_desembolsado_reais": "",  # zero disbursed
                "data_da_contratacao": "",
                "descricao_do_projeto": "",
                "cliente": "",
                "produto": "",
                "juros": "",
                "uf": "",
                "municipio": "",
                "setor_bndes": "",
                "porte_do_cliente": "",
                "situacao_do_contrato": "",
            },
            {
                "cnpj": "11222333000181",
                "numero_do_contrato": "DEF-999",
                "valor_contratado_reais": "1.000,00",
                "valor_desembolsado_reais": "500,00",
                "data_da_contratacao": "",
                "descricao_do_projeto": "",
                "cliente": "",
                "produto": "",
                "juros": "",
                "uf": "",
                "municipio": "",
                "setor_bndes": "",
                "porte_do_cliente": "",
                "situacao_do_contrato": "",
            },
        ]
    )
    pipeline.transform()
    by_id = {f["finance_id"]: f for f in pipeline.finances}
    # Disbursed == 0 → falls back to contracted (1000)
    assert by_id["bndes_ABC-999"]["value"] == 1000.0
    # Disbursed > 0 → wins over contracted
    assert by_id["bndes_DEF-999"]["value"] == 500.0


def test_load_short_circuits_when_empty(tmp_path: Path) -> None:
    """No finances/relationships → loader should not open a session."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()  # empty
    pipeline.transform()
    pipeline.load()
    assert not mock_driver(pipeline).session.called
