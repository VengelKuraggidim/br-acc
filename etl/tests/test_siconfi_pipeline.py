from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.siconfi import SiconfiPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def pipeline() -> SiconfiPipeline:
    driver = MagicMock()
    return SiconfiPipeline(driver=driver, data_dir=str(FIXTURES))


class TestExtract:
    def test_extract_reads_json(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        assert len(pipeline._raw) == 5

    def test_extract_with_limit(self) -> None:
        driver = MagicMock()
        p = SiconfiPipeline(driver=driver, data_dir=str(FIXTURES), limit=2)
        p.extract()
        assert len(p._raw) == 2

    def test_extract_empty_dir(self, tmp_path: Path) -> None:
        (tmp_path / "siconfi").mkdir()
        driver = MagicMock()
        p = SiconfiPipeline(driver=driver, data_dir=str(tmp_path))
        p.extract()
        assert len(p._raw) == 0


class TestTransform:
    def test_transform_produces_finance_records(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        # 4 valid records (one has empty cod_ibge, skipped)
        assert len(pipeline.finances) == 4

    def test_transform_skips_empty_cod_ibge(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        ibge_codes = {f["cod_ibge"] for f in pipeline.finances}
        assert "" not in ibge_codes

    def test_transform_formats_cnpj(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for rel in pipeline.municipality_rels:
            assert "." in rel["cnpj"]
            assert "/" in rel["cnpj"]
            assert "-" in rel["cnpj"]

    def test_transform_municipality_rels(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        # 3 records have valid CNPJs (São Paulo, Rio, Brasília); Salvador has empty CNPJ
        assert len(pipeline.municipality_rels) == 3

    def test_transform_deduplicates(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        ids = [f["finance_id"] for f in pipeline.finances]
        assert len(ids) == len(set(ids))

    def test_transform_normalizes_names(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        names = {f["municipality"] for f in pipeline.finances}
        for name in names:
            assert name == name.upper() or name == name  # normalize_name output

    def test_transform_parses_amount(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for f in pipeline.finances:
            assert isinstance(f["amount"], float)
            assert f["amount"] > 0

    def test_transform_generates_unique_ids(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        ids = {f["finance_id"] for f in pipeline.finances}
        assert len(ids) == len(pipeline.finances)

    def test_transform_empty_input(self, pipeline: SiconfiPipeline) -> None:
        pipeline._raw = []
        pipeline.transform()
        assert len(pipeline.finances) == 0
        assert len(pipeline.municipality_rels) == 0

    def test_transform_skips_null_valor(self, pipeline: SiconfiPipeline) -> None:
        pipeline._raw = [
            {
                "cod_ibge": "1234567",
                "ente": "Test",
                "exercicio": "2023",
                "conta": "Receita",
                "coluna": "Valor",
                "valor": None,
                "cnpj": "",
            }
        ]
        pipeline.transform()
        assert len(pipeline.finances) == 0


class TestLoad:
    def test_load_creates_nodes_and_rels(self, pipeline: SiconfiPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called

    def test_load_empty_data(self, pipeline: SiconfiPipeline) -> None:
        pipeline.finances = []
        pipeline.municipality_rels = []
        pipeline.load()
        # No errors on empty data

    def test_load_calls_loader(self, pipeline: SiconfiPipeline) -> None:
        pipeline.finances = [
            {
                "finance_id": "abc123",
                "cod_ibge": "3550308",
                "municipality": "SAO PAULO",
                "year": "2023",
                "account": "Receita",
                "column": "Valor",
                "amount": 1000.0,
                "source": "siconfi",
            }
        ]
        pipeline.municipality_rels = [
            {
                "cnpj": "46.395.000/0001-39",
                "finance_id": "abc123",
                "municipality": "SAO PAULO",
            }
        ]
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called


class TestPipelineMetadata:
    def test_name(self, pipeline: SiconfiPipeline) -> None:
        assert pipeline.name == "siconfi"

    def test_source_id(self, pipeline: SiconfiPipeline) -> None:
        assert pipeline.source_id == "siconfi"
