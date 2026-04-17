from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.querido_diario_go import (
    QueridoDiarioGoPipeline,
    _classify_act,
    _extract_appointments,
    _extract_cnpjs,
    _stable_id,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> QueridoDiarioGoPipeline:
    return QueridoDiarioGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert QueridoDiarioGoPipeline.name == "querido_diario_go"

    def test_source_id(self) -> None:
        assert QueridoDiarioGoPipeline.source_id == "querido_diario_go"


class TestHelpers:
    def test_stable_id_deterministic(self) -> None:
        a = _stable_id("a", "b", "c")
        b = _stable_id("a", "b", "c")
        assert a == b
        assert len(a) == 24

    def test_stable_id_different_inputs(self) -> None:
        a = _stable_id("a", "b")
        b = _stable_id("x", "y")
        assert a != b

    def test_classify_act_nomeacao(self) -> None:
        assert _classify_act("resolve nomear FULANO") == "nomeacao"

    def test_classify_act_exoneracao(self) -> None:
        assert _classify_act("resolve exonerar FULANO") == "exoneracao"

    def test_classify_act_contrato(self) -> None:
        assert _classify_act("extrato de contrato celebrado") == "contrato"

    def test_classify_act_outro(self) -> None:
        assert _classify_act("publicação genérica sem palavras-chave") == "outro"

    def test_extract_cnpjs(self) -> None:
        text = "Empresa 12.345.678/0001-95 contratada."
        results = _extract_cnpjs(text)
        assert len(results) == 1
        assert results[0][0] == "12.345.678/0001-95"

    def test_extract_cnpjs_dedup(self) -> None:
        text = "CNPJ 12.345.678/0001-95 e novamente 12.345.678/0001-95."
        results = _extract_cnpjs(text)
        assert len(results) == 1

    def test_extract_appointments(self) -> None:
        text = "nomear MARIA DA SILVA SANTOS para o cargo de Diretora do Departamento."
        results = _extract_appointments(text)
        assert len(results) == 1
        assert "MARIA" in results[0]["person_name"].upper()
        assert "Diretora" in results[0]["role"]

class TestTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.acts) == 2

    def test_act_fields(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        act = pipeline.acts[0]
        assert act["uf"] == "GO"
        assert act["source"] == "querido_diario_go"
        assert "act_id" in act
        assert "territory_id" in act
        assert "act_type" in act
        assert "excerpt" in act

    def test_extracts_cnpj_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.company_mentions) >= 1
        cnpjs = [m["cnpj"] for m in pipeline.company_mentions]
        assert "12.345.678/0001-95" in cnpjs

    def test_extracts_appointments(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.appointments) >= 1
        appt = pipeline.appointments[0]
        assert appt["uf"] == "GO"
        assert appt["appointment_type"] in ("nomeacao", "exoneracao")
        assert "person_name" in appt
        assert "role" in appt
        assert "act_id" in appt

    def test_act_types_classified(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        types = {a["act_id"]: a["act_type"] for a in pipeline.acts}
        type_values = list(types.values())
        # First fixture has "nomear" -> nomeacao (or contrato since both match)
        assert any(t in ("nomeacao", "contrato") for t in type_values)

    def test_excerpt_max_length(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for act in pipeline.acts:
            assert len(act["excerpt"]) <= 500


class TestLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_calls_driver(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

        driver = pipeline.driver
        assert driver.session.called
