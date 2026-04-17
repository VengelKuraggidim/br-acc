from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.senado import SenadoPipeline, _make_expense_id, _parse_brl_value
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SenadoPipeline:
    driver = MagicMock()
    return SenadoPipeline(driver=driver, data_dir=str(FIXTURES.parent))


def _load_fixture_data(pipeline: SenadoPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "senado_ceaps.csv",
        sep=";",
        dtype=str,
        encoding="latin-1",
        keep_default_na=False,
        skiprows=1,
    )


class TestSenadoPipelineMetadata:
    def test_name(self) -> None:
        assert SenadoPipeline.name == "senado"

    def test_source_id(self) -> None:
        assert SenadoPipeline.source_id == "senado"


class TestParseBrlValue:
    def test_standard_format(self) -> None:
        assert _parse_brl_value("5.000,00") == 5000.0

    def test_simple_value(self) -> None:
        assert _parse_brl_value("678,90") == 678.9

    def test_empty_string(self) -> None:
        assert _parse_brl_value("") == 0.0

    def test_invalid_value(self) -> None:
        assert _parse_brl_value("abc") == 0.0


class TestMakeExpenseId:
    def test_deterministic(self) -> None:
        id1 = _make_expense_id("SENADOR EXEMPLO", "2024-01-15", "12.345.678/0001-99", "5000.0")
        id2 = _make_expense_id("SENADOR EXEMPLO", "2024-01-15", "12.345.678/0001-99", "5000.0")
        assert id1 == id2

    def test_different_inputs_different_ids(self) -> None:
        id1 = _make_expense_id("SENADOR EXEMPLO", "2024-01-15", "12.345.678/0001-99", "5000.0")
        id2 = _make_expense_id("SENADORA TESTE", "2024-01-15", "12.345.678/0001-99", "5000.0")
        assert id1 != id2

    def test_returns_16_char_hex(self) -> None:
        result = _make_expense_id("SENADOR", "2024-01-15", "12.345.678/0001-99", "5000.0")
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)


class TestSenadoTransform:
    def test_produces_expenses(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.expenses) == 6

    def test_produces_suppliers(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 6 unique CNPJ suppliers
        assert len(pipeline.suppliers) == 6

    def test_normalizes_senator_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {e["senator_name"] for e in pipeline.expenses}
        assert "SENADOR EXEMPLO" in names
        assert "SENADORA TESTE" in names
        assert "SENADOR SILVA" in names

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
        assert 5000.0 in values
        assert 1234.56 in values

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
        assert "senator_name" in e
        assert "type" in e
        assert "value" in e
        assert "date" in e
        assert "source" in e
        assert e["source"] == "senado"

    def test_expense_has_description(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # First expense has DETALHAMENTO
        e = pipeline.expenses[0]
        assert e["description"]

    def test_forneceu_rels_created(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.forneceu_rels) == 6

    def test_limit_truncates(self) -> None:
        pipeline = _make_pipeline()
        pipeline.limit = 2
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.expenses) <= 2

    def test_empty_dataframe_skips(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw = pd.DataFrame()
        pipeline.transform()

        assert len(pipeline.expenses) == 0

    def test_skips_rows_without_supplier_doc(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw = pd.DataFrame([{
            "SENADOR": "TESTE",
            "TIPO_DESPESA": "Aluguel",
            "CNPJ_CPF": "",
            "FORNECEDOR": "FORNECEDOR",
            "DOCUMENTO": "DOC",
            "DATA": "15/01/2024",
            "DETALHAMENTO": "Teste",
            "VALOR_REEMBOLSADO": "1.000,00",
        }])
        pipeline.transform()

        assert len(pipeline.expenses) == 0


class TestSenadoLoad:
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
        pipeline.suppliers = []
        pipeline.forneceu_rels = []
        pipeline.load()

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0


class TestSenatorLookupEnrichment:
    def test_loads_senator_lookup(self) -> None:
        """Senator lookup loads correctly from parlamentares.json."""
        with tempfile.TemporaryDirectory() as tmpdir:
            senado_dir = Path(tmpdir) / "senado"
            senado_dir.mkdir()
            parlamentares = [
                {
                    "codigo": "5012",
                    "nome_parlamentar": "FLAVIO BOLSONARO",
                    "nome_completo": "FLAVIO NANTES BOLSONARO",
                    "cpf": "12345678901",
                },
                {
                    "codigo": "5555",
                    "nome_parlamentar": "SENADOR SEM CPF",
                    "nome_completo": "SENADOR SEM CPF COMPLETO",
                    "cpf": "",
                },
            ]
            (senado_dir / "parlamentares.json").write_text(
                json.dumps(parlamentares), encoding="utf-8"
            )

            driver = MagicMock()
            pipeline = SenadoPipeline(driver=driver, data_dir=tmpdir)
            lookup = pipeline._load_senator_lookup()

            assert "FLAVIO BOLSONARO" in lookup
            assert lookup["FLAVIO BOLSONARO"]["cpf"] == "12345678901"
            # Also indexed by nome_completo
            assert "FLAVIO NANTES BOLSONARO" in lookup
            # Senator without CPF still in lookup
            assert "SENADOR SEM CPF" in lookup
            assert lookup["SENADOR SEM CPF"]["cpf"] == ""

    def test_missing_parlamentares_returns_empty(self) -> None:
        """Missing parlamentares.json returns empty lookup (graceful degradation)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            senado_dir = Path(tmpdir) / "senado"
            senado_dir.mkdir()

            driver = MagicMock()
            pipeline = SenadoPipeline(driver=driver, data_dir=tmpdir)
            lookup = pipeline._load_senator_lookup()

            assert lookup == {}

    def test_cpf_enriched_gastou(self) -> None:
        """Senators with CPF in lookup get CPF-based GASTOU rels."""
        with tempfile.TemporaryDirectory() as tmpdir:
            senado_dir = Path(tmpdir) / "senado"
            senado_dir.mkdir()
            parlamentares = [{
                "codigo": "1001",
                "nome_parlamentar": "SENADOR EXEMPLO",
                "nome_completo": "SENADOR EXEMPLO COMPLETO",
                "cpf": "12345678901",
            }]
            (senado_dir / "parlamentares.json").write_text(
                json.dumps(parlamentares), encoding="utf-8"
            )

            driver = MagicMock()
            pipeline = SenadoPipeline(driver=driver, data_dir=tmpdir)
            pipeline._senator_lookup = pipeline._load_senator_lookup()
            # Manually set raw data with one row
            pipeline._raw = pd.DataFrame([{
                "SENADOR": "SENADOR EXEMPLO",
                "TIPO_DESPESA": "Aluguel",
                "CNPJ_CPF": "12345678000199",
                "FORNECEDOR": "FORNECEDOR LTDA",
                "DOCUMENTO": "DOC1",
                "DATA": "15/01/2024",
                "DETALHAMENTO": "Teste",
                "VALOR_REEMBOLSADO": "1.000,00",
            }])
            pipeline.transform()

            # Should have CPF-based gastou
            assert len(pipeline.gastou_rels) == 1
            assert pipeline.gastou_rels[0]["source_key"] == "123.456.789-01"
            # No name-based fallback needed
            assert len(pipeline.gastou_by_name_rels) == 0

    def test_name_fallback_when_no_cpf(self) -> None:
        """Senators without CPF in lookup use name-based GASTOU."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        # No senator lookup → all go to name-based path
        pipeline._senator_lookup = {}
        pipeline.transform()

        assert len(pipeline.gastou_rels) == 0
        assert len(pipeline.gastou_by_name_rels) == 6
