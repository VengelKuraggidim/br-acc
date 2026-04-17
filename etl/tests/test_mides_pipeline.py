from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.mides import (
    MidesPipeline,
    _stable_id,
    _valid_cnpj,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> MidesPipeline:
    return MidesPipeline(driver=MagicMock(), data_dir=data_dir or str(FIXTURES))


def _write(dir_: Path, name: str, csv: str) -> None:
    (dir_ / name).write_text(csv, encoding="utf-8")


class TestMidesMetadata:
    def test_name(self) -> None:
        assert MidesPipeline.name == "mides"

    def test_source_id(self) -> None:
        assert MidesPipeline.source_id == "mides"


class TestValidCnpj:
    def test_14_digit_formatted_passes_through(self) -> None:
        assert _valid_cnpj("11.222.333/0001-81") == "11.222.333/0001-81"

    def test_14_digit_raw_becomes_formatted(self) -> None:
        assert _valid_cnpj("11222333000181") == "11.222.333/0001-81"

    def test_short_returns_empty(self) -> None:
        assert _valid_cnpj("12345") == ""

    def test_blank_returns_empty(self) -> None:
        assert _valid_cnpj("") == ""


class TestStableId:
    def test_deterministic(self) -> None:
        assert _stable_id("a", "b") == _stable_id("a", "b")

    def test_default_length(self) -> None:
        assert len(_stable_id("x")) == 24


class TestMidesTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.bids) == 2
        assert len(pipeline.contracts) == 2
        assert len(pipeline.items) == 2

    def test_links_companies(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        bid_cnpjs = {row["cnpj"] for row in pipeline.bid_company_rels}
        contract_cnpjs = {row["cnpj"] for row in pipeline.contract_company_rels}

        assert "11.222.333/0001-81" in bid_cnpjs
        assert "11.222.333/0001-81" in contract_cnpjs
        assert "22.333.444/0001-90" in contract_cnpjs

    def test_contract_item_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.contract_item_rels) == 2

    def test_bid_stable_id_fallback(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "mides"
        data_dir.mkdir()
        _write(
            data_dir,
            "licitacao.csv",
            "municipal_bid_id,process_number,cod_ibge,objeto,data_publicacao\n"
            ",P-001,1234567,Aquisicao de medicamentos,2026-02-01\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.bids) == 1
        assert pipeline.bids[0]["municipal_bid_id"] != ""
        assert len(pipeline.bids[0]["municipal_bid_id"]) == 24

    def test_contract_stable_id_fallback_and_bid_link(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "mides"
        data_dir.mkdir()
        _write(
            data_dir,
            "contrato.csv",
            "municipal_contract_id,contract_number,municipal_bid_id,cod_ibge,"
            "objeto,data_assinatura,supplier_cnpj\n"
            ",C-001,bid-xyz,1234567,Servicos,2026-02-10,11222333000181\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.contracts) == 1
        assert pipeline.contracts[0]["municipal_contract_id"] != ""
        # bid_ref present → contract_bid_rels populated
        assert len(pipeline.contract_bid_rels) == 1
        assert pipeline.contract_bid_rels[0]["target_key"] == "bid-xyz"

    def test_contract_skips_invalid_supplier_cnpj(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "mides"
        data_dir.mkdir()
        _write(
            data_dir,
            "contrato.csv",
            "municipal_contract_id,cod_ibge,objeto,data_assinatura,supplier_cnpj\n"
            "c-1,1234567,X,2026-02-10,123\n",  # too short
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert pipeline.contract_company_rels == []

    def test_item_stable_id_fallback(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "mides"
        data_dir.mkdir()
        _write(
            data_dir,
            "item.csv",
            "municipal_item_id,municipal_contract_id,item_number,descricao,"
            "quantidade,valor_total\n"
            ",c-1,1,Medicamento X,100,5000.00\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.items) == 1
        assert pipeline.items[0]["municipal_item_id"] != ""
        assert len(pipeline.items[0]["municipal_item_id"]) == 24

    def test_item_without_contract_id_has_no_rel(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "mides"
        data_dir.mkdir()
        _write(
            data_dir,
            "item.csv",
            "municipal_item_id,municipal_contract_id,descricao\n"
            "i-1,,Orphan item\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.items) == 1
        assert pipeline.contract_item_rels == []

    def test_transform_with_empty_inputs(self, tmp_path: Path) -> None:
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()  # no files → all _raw_ frames stay empty
        pipeline.transform()  # must not raise
        assert pipeline.bids == []
        assert pipeline.contracts == []
        assert pipeline.items == []


class TestMidesLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_short_circuits_when_empty(self, tmp_path: Path) -> None:
        """All collections empty → loader is instantiated but never called."""
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        # Driver session should not have been opened.
        assert not pipeline.driver.session.called  # type: ignore[attr-defined]
