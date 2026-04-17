from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.renuncias import RenunciasPipeline, _parse_brl
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def pipeline() -> RenunciasPipeline:
    driver = MagicMock()
    return RenunciasPipeline(driver=driver, data_dir=str(FIXTURES))


class TestParseBrl:
    def test_simple(self) -> None:
        assert _parse_brl("1.234,56") == 1234.56

    def test_large(self) -> None:
        assert _parse_brl("1.234.567,89") == 1234567.89

    def test_no_thousands(self) -> None:
        assert _parse_brl("567,89") == 567.89

    def test_zero(self) -> None:
        assert _parse_brl("0,00") == 0.0

    def test_invalid(self) -> None:
        assert _parse_brl("abc") is None

    def test_empty(self) -> None:
        assert _parse_brl("") is None


class TestExtract:
    def test_extract_reads_csv(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        assert len(pipeline._raw) == 5

    def test_extract_with_limit(self) -> None:
        driver = MagicMock()
        p = RenunciasPipeline(driver=driver, data_dir=str(FIXTURES), limit=2)
        p.extract()
        assert len(p._raw) == 2

    def test_extract_empty_dir(self, tmp_path: Path) -> None:
        (tmp_path / "renuncias").mkdir()
        driver = MagicMock()
        p = RenunciasPipeline(driver=driver, data_dir=str(tmp_path))
        p.extract()
        assert len(p._raw) == 0


class TestTransform:
    def test_transform_produces_waivers(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        # 3 valid: Bradesco IRPJ, Petrobras CSLL, BB PIS. Empty CNPJ skipped, zero amount skipped.
        assert len(pipeline.waivers) == 3

    def test_transform_skips_empty_cnpj(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        cnpjs = {w["cnpj"] for w in pipeline.waivers}
        for cnpj in cnpjs:
            assert len(cnpj) > 0

    def test_transform_skips_zero_amount(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        amounts = [w["amount"] for w in pipeline.waivers]
        assert all(a > 0 for a in amounts)

    def test_transform_formats_cnpj(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for w in pipeline.waivers:
            assert "." in w["cnpj"]
            assert "/" in w["cnpj"]
            assert "-" in w["cnpj"]

    def test_transform_company_rels(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.company_rels) == 3

    def test_transform_deduplicates(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        ids = [w["waiver_id"] for w in pipeline.waivers]
        assert len(ids) == len(set(ids))

    def test_transform_parses_amount(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for w in pipeline.waivers:
            assert isinstance(w["amount"], float)

    def test_transform_empty_input(self, pipeline: RenunciasPipeline) -> None:
        pipeline._raw = pipeline._raw.head(0)
        pipeline.extract()  # re-extract to get DataFrame
        pipeline._raw = pipeline._raw.head(0)
        pipeline.transform()
        assert len(pipeline.waivers) == 0

    def test_transform_preserves_fields(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for w in pipeline.waivers:
            assert "waiver_id" in w
            assert "cnpj" in w
            assert "beneficiary_name" in w
            assert "tax_type" in w
            assert "waiver_type" in w
            assert "year" in w
            assert "amount" in w
            assert w["source"] == "renuncias_fiscais"


class TestLoad:
    def test_load_calls_session(self, pipeline: RenunciasPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called

    def test_load_empty_data(self, pipeline: RenunciasPipeline) -> None:
        pipeline.waivers = []
        pipeline.company_rels = []
        pipeline.load()


class TestMetadata:
    def test_name(self, pipeline: RenunciasPipeline) -> None:
        assert pipeline.name == "renuncias"

    def test_source_id(self, pipeline: RenunciasPipeline) -> None:
        assert pipeline.source_id == "renuncias"
