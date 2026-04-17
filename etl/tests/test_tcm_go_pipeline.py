from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bracc_etl.pipelines.tcm_go import TcmGoPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def pipeline() -> TcmGoPipeline:
    driver = MagicMock()
    return TcmGoPipeline(driver=driver, data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self, pipeline: TcmGoPipeline) -> None:
        assert pipeline.name == "tcm_go"

    def test_source_id(self, pipeline: TcmGoPipeline) -> None:
        assert pipeline.source_id == "tcm_go"


class TestExtract:
    def test_extract_reads_csv(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        assert len(pipeline._municipalities) == 3
        assert len(pipeline._raw_fiscal) == 4

    def test_extract_with_limit(self) -> None:
        driver = MagicMock()
        p = TcmGoPipeline(driver=driver, data_dir=str(FIXTURES), limit=2)
        p.extract()
        assert len(p._raw_fiscal) == 2

    @patch("bracc_etl.pipelines.tcm_go.httpx.Client")
    def test_extract_empty_dir(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        (tmp_path / "tcm_go").mkdir()
        # Mock API to return empty results
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"items": []}
        mock_client.get.return_value = mock_resp
        driver = MagicMock()
        p = TcmGoPipeline(driver=driver, data_dir=str(tmp_path))
        p.extract()
        assert len(p._municipalities) == 0
        assert len(p._raw_fiscal) == 0

    def test_extract_filters_goias_only(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        for m in pipeline._municipalities:
            assert str(m["cod_ibge"]).startswith("52")


class TestTransform:
    def test_transform_produces_municipalities(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.municipalities) == 3

    def test_transform_municipality_fields(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for m in pipeline.municipalities:
            assert m["uf"] == "GO"
            assert m["source"] == "tcm_go"
            assert m["municipality_id"].startswith("52")
            assert m["name"]  # not empty

    def test_transform_separates_revenues_and_expenditures(
        self, pipeline: TcmGoPipeline
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.revenues) == 2
        assert len(pipeline.expenditures) == 2

    def test_transform_revenue_fields(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.revenues:
            assert "revenue_id" in r
            assert isinstance(r["amount"], float)
            assert r["amount"] > 0
            assert r["source"] == "tcm_go"
            assert r["municipality_id"].startswith("52")

    def test_transform_expenditure_fields(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for e in pipeline.expenditures:
            assert "expenditure_id" in e
            assert isinstance(e["amount"], float)
            assert e["amount"] > 0
            assert e["source"] == "tcm_go"

    def test_transform_generates_unique_ids(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        rev_ids = {r["revenue_id"] for r in pipeline.revenues}
        exp_ids = {e["expenditure_id"] for e in pipeline.expenditures}
        assert len(rev_ids) == len(pipeline.revenues)
        assert len(exp_ids) == len(pipeline.expenditures)

    def test_transform_creates_rels(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.revenue_rels) == 2
        assert len(pipeline.expenditure_rels) == 2
        for rel in pipeline.revenue_rels:
            assert "source_key" in rel
            assert "target_key" in rel

    def test_transform_empty_input(self, pipeline: TcmGoPipeline) -> None:
        pipeline._municipalities = []
        pipeline._raw_fiscal = []
        pipeline.transform()
        assert len(pipeline.municipalities) == 0
        assert len(pipeline.revenues) == 0
        assert len(pipeline.expenditures) == 0

    def test_transform_skips_null_valor(self, pipeline: TcmGoPipeline) -> None:
        pipeline._municipalities = [
            {"cod_ibge": "5208707", "ente": "Goiania", "populacao": "1555626"}
        ]
        pipeline._raw_fiscal = [
            {
                "cod_ibge": "5208707",
                "exercicio": "2023",
                "conta": "Receita Corrente",
                "coluna": "Valor",
                "valor": None,
            }
        ]
        pipeline.transform()
        assert len(pipeline.revenues) == 0
        assert len(pipeline.expenditures) == 0

    def test_transform_skips_non_goias(self, pipeline: TcmGoPipeline) -> None:
        pipeline._municipalities = [
            {"cod_ibge": "3550308", "ente": "Sao Paulo", "populacao": "12345678"}
        ]
        pipeline._raw_fiscal = [
            {
                "cod_ibge": "3550308",
                "exercicio": "2023",
                "conta": "Receita Corrente",
                "coluna": "Valor",
                "valor": "1000000",
            }
        ]
        pipeline.transform()
        assert len(pipeline.municipalities) == 0
        assert len(pipeline.revenues) == 0

    def test_is_revenue_classification(self) -> None:
        assert TcmGoPipeline._is_revenue("Receita Corrente Liquida") is True
        assert TcmGoPipeline._is_revenue("Receita Tributaria") is True
        assert TcmGoPipeline._is_revenue("Despesa Total com Pessoal") is False
        assert TcmGoPipeline._is_revenue("Despesa de Capital") is False


class TestLoad:
    def test_load_creates_nodes_and_rels(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called

    def test_load_empty_data(self, pipeline: TcmGoPipeline) -> None:
        pipeline.municipalities = []
        pipeline.revenues = []
        pipeline.expenditures = []
        pipeline.revenue_rels = []
        pipeline.expenditure_rels = []
        pipeline.load()
        # No errors on empty data

    def test_load_calls_loader(self, pipeline: TcmGoPipeline) -> None:
        pipeline.municipalities = [
            {
                "municipality_id": "5208707",
                "name": "GOIANIA",
                "uf": "GO",
                "population": "1555626",
                "source": "tcm_go",
            }
        ]
        pipeline.revenues = [
            {
                "revenue_id": "abc123",
                "municipality_id": "5208707",
                "year": "2023",
                "account": "Receita Corrente Liquida",
                "description": "Valor",
                "amount": 8923456000.50,
                "source": "tcm_go",
            }
        ]
        pipeline.expenditures = []
        pipeline.revenue_rels = [
            {"source_key": "5208707", "target_key": "abc123"},
        ]
        pipeline.expenditure_rels = []
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called

    def test_load_sets_rows_loaded(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        assert pipeline.rows_loaded == len(pipeline.revenues) + len(pipeline.expenditures)
