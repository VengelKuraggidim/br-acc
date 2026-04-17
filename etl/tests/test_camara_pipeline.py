from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.camara import CamaraPipeline, _make_expense_id, _parse_brl_value
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CamaraPipeline:
    driver = MagicMock()
    return CamaraPipeline(driver=driver, data_dir=str(FIXTURES.parent))


def _load_fixture_data(pipeline: CamaraPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "camara_ceap.csv",
        sep=";",
        dtype=str,
        encoding="latin-1",
        keep_default_na=False,
    )


class TestCamaraPipelineMetadata:
    def test_name(self) -> None:
        assert CamaraPipeline.name == "camara"

    def test_source_id(self) -> None:
        assert CamaraPipeline.source_id == "camara"


class TestParseBrlValue:
    def test_standard_format(self) -> None:
        assert _parse_brl_value("1.234,56") == 1234.56

    def test_simple_value(self) -> None:
        assert _parse_brl_value("567,89") == 567.89

    def test_empty_string(self) -> None:
        assert _parse_brl_value("") == 0.0

    def test_invalid_value(self) -> None:
        assert _parse_brl_value("abc") == 0.0


class TestMakeExpenseId:
    def test_deterministic(self) -> None:
        id1 = _make_expense_id("1001", "2024-01-15", "12.345.678/0001-99", "1234.56")
        id2 = _make_expense_id("1001", "2024-01-15", "12.345.678/0001-99", "1234.56")
        assert id1 == id2

    def test_different_inputs_different_ids(self) -> None:
        id1 = _make_expense_id("1001", "2024-01-15", "12.345.678/0001-99", "1234.56")
        id2 = _make_expense_id("1002", "2024-01-15", "12.345.678/0001-99", "1234.56")
        assert id1 != id2

    def test_returns_16_char_hex(self) -> None:
        result = _make_expense_id("1001", "2024-01-15", "12.345.678/0001-99", "1234.56")
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)


class TestCamaraTransform:
    def test_produces_expenses(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.expenses) == 8

    def test_produces_deputies(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 3 unique deputies by CPF
        assert len(pipeline.deputies) == 3

    def test_produces_suppliers(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 7 unique CNPJ suppliers (one shared: FORNECEDOR LTDA)
        assert len(pipeline.suppliers) >= 7

    def test_normalizes_deputy_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {d["name"] for d in pipeline.deputies}
        assert "DEPUTADO EXEMPLO" in names
        assert "DEPUTADA TESTE" in names
        assert "DEPUTADO SILVA" in names

    def test_formats_supplier_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cnpjs = {s.get("cnpj", "") for s in pipeline.suppliers}
        assert "12.345.678/0001-99" in cnpjs

    def test_parses_values(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        values = {e["value"] for e in pipeline.expenses}
        assert 1234.56 in values
        assert 567.89 in values

    def test_parses_dates(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        dates = {e["date"] for e in pipeline.expenses}
        assert "2024-01-15" in dates

    def test_expense_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        e = pipeline.expenses[0]
        assert "expense_id" in e
        assert "deputy_id" in e
        assert "type" in e
        assert "value" in e
        assert "date" in e
        assert "source" in e
        assert e["source"] == "camara"

    def test_gastou_rels_created(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 7 CPF-based gastou (deputy without CPF uses deputy_id path)
        assert len(pipeline.gastou_rels) == 7

    def test_gastou_by_deputy_id_created(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 1 deputy without CPF falls back to deputy_id
        assert len(pipeline.gastou_by_deputy_id_rels) == 1
        rel = pipeline.gastou_by_deputy_id_rels[0]
        assert rel["deputy_id"] == "1004"
        assert "target_key" in rel

    def test_deputies_by_id_created(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.deputies_by_id) == 1
        deputy = pipeline.deputies_by_id[0]
        assert deputy["deputy_id"] == "1004"
        assert deputy["name"] == "DEPUTADO SEM CPF"

    def test_forneceu_rels_created(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.forneceu_rels) == 8

    def test_limit_truncates(self) -> None:
        pipeline = _make_pipeline()
        pipeline.limit = 3
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.expenses) <= 3

    def test_empty_dataframe_skips(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw = pd.DataFrame()
        pipeline.transform()

        assert len(pipeline.expenses) == 0


class TestCamaraLoad:
    def test_loads_expense_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        expense_calls = [
            c for c in run_calls if "MERGE (n:Expense" in str(c)
        ]
        assert len(expense_calls) >= 1

    def test_loads_person_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        person_calls = [
            c for c in run_calls if "MERGE (n:Person" in str(c)
        ]
        assert len(person_calls) >= 1

    def test_loads_company_supplier_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        company_calls = [
            c for c in run_calls if "MERGE (n:Company" in str(c)
        ]
        assert len(company_calls) >= 1

    def test_creates_gastou_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        gastou_calls = [
            c for c in run_calls if "GASTOU" in str(c)
        ]
        assert len(gastou_calls) >= 1

    def test_creates_forneceu_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        forneceu_calls = [
            c for c in run_calls if "FORNECEU" in str(c)
        ]
        assert len(forneceu_calls) >= 1

    def test_empty_expenses_skips_load(self) -> None:
        pipeline = _make_pipeline()
        pipeline.expenses = []
        pipeline.load()

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0
