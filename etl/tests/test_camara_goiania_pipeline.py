from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.camara_goiania import CamaraGoianiaPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CamaraGoianiaPipeline:
    return CamaraGoianiaPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert CamaraGoianiaPipeline.name == "camara_goiania"

    def test_source_id(self) -> None:
        assert CamaraGoianiaPipeline.source_id == "camara_goiania"


class TestTransform:
    def test_extract_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()

        assert len(pipeline._raw_vereadores) == 2
        assert len(pipeline._raw_expenses) == 2
        assert len(pipeline._raw_proposicoes) == 2

    def test_transform_vereadores(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.vereadores) == 2
        names = {v["name"] for v in pipeline.vereadores}
        assert "JOAO DA SILVA" in names
        assert "MARIA OLIVEIRA" in names

        for v in pipeline.vereadores:
            assert v["uf"] == "GO"
            assert v["municipality"] == "Goiania"
            assert v["municipality_code"] == "5208707"
            assert v["source"] == "camara_goiania"
            assert v["vereador_id"]

    def test_transform_expenses(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.expenses) == 2
        amounts = {e["vereador_name"]: e["amount"] for e in pipeline.expenses}
        assert amounts["JOAO DA SILVA"] == 1250.0
        assert amounts["MARIA OLIVEIRA"] == 800.5

    def test_transform_proposals(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.proposals) == 2
        types = {p["number"]: p["type"] for p in pipeline.proposals}
        assert types["1234"] == "PL"
        assert types["1235"] == "Resolucao"

    def test_autor_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.autor_rels) == 2

    def test_despesa_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.despesa_rels) == 2

    def test_stable_ids_are_deterministic(self) -> None:
        p1 = _make_pipeline()
        p1.extract()
        p1.transform()

        p2 = _make_pipeline()
        p2.extract()
        p2.transform()

        ids1 = {v["vereador_id"] for v in p1.vereadores}
        ids2 = {v["vereador_id"] for v in p2.vereadores}
        assert ids1 == ids2


class TestLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_empty_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.load()
