from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.folha_go import FolhaGoPipeline, _is_commissioned, mask_cpf

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> FolhaGoPipeline:
    return FolhaGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert FolhaGoPipeline.name == "folha_go"

    def test_source_id(self) -> None:
        assert FolhaGoPipeline.source_id == "folha_go"


class TestHelpers:
    def test_mask_cpf_valid(self) -> None:
        result = mask_cpf("12345678901")
        assert result == "***.***.*89-01"
        # Only last 4 digits visible
        assert "1234" not in result

    def test_mask_cpf_invalid(self) -> None:
        assert mask_cpf("123") == "***.***.***-**"
        assert mask_cpf("") == "***.***.***-**"

    def test_is_commissioned_das(self) -> None:
        assert _is_commissioned("ASSESSOR DAS-3 COMISSIONADO") is True

    def test_is_commissioned_regular(self) -> None:
        assert _is_commissioned("ANALISTA DE SISTEMAS") is False

    def test_is_commissioned_cc(self) -> None:
        assert _is_commissioned("CC-2 COORDENADOR") is True

    def test_is_commissioned_cds(self) -> None:
        assert _is_commissioned("DIRETOR CDS-4") is True

    def test_is_commissioned_dai(self) -> None:
        assert _is_commissioned("CHEFE DAI-1") is True

    def test_is_commissioned_fcpe(self) -> None:
        assert _is_commissioned("ASSESSOR FCPE 101.4") is True


class TestTransform:
    def test_transform_employee_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.employees) == 3

    def test_transform_agency_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 3 employees across 3 distinct agencies
        assert len(pipeline.agencies) == 3

    def test_commissioned_flag(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        commissioned = [e for e in pipeline.employees if e["is_commissioned"]]
        regular = [e for e in pipeline.employees if not e["is_commissioned"]]
        assert len(commissioned) == 1
        assert len(regular) == 2

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for emp in pipeline.employees:
            # CPF must not contain full digits
            assert "12345678901" not in emp["cpf"]
            assert "98765432100" not in emp["cpf"]
            assert "***" in emp["cpf"]

    def test_uf_always_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for emp in pipeline.employees:
            assert emp["uf"] == "GO"

    def test_employee_agency_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.employee_agency_rels) == 3

    def test_salary_values(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        gross_values = sorted([e["salary_gross"] for e in pipeline.employees])
        assert 4500.0 in gross_values
        assert 8500.0 in gross_values
        assert 12000.0 in gross_values

    def test_stable_ids_are_deterministic(self) -> None:
        p1 = _make_pipeline()
        p1.extract()
        p1.transform()

        p2 = _make_pipeline()
        p2.extract()
        p2.transform()

        ids1 = sorted([e["employee_id"] for e in p1.employees])
        ids2 = sorted([e["employee_id"] for e in p2.employees])
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
