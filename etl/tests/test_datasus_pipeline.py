from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.datasus import DatasusPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> DatasusPipeline:
    driver = MagicMock()
    return DatasusPipeline(driver, data_dir=str(FIXTURES))


def _load_fixture(pipeline: DatasusPipeline) -> None:
    """Read the fixture CSV into the pipeline's internal DataFrame."""
    csv_path = FIXTURES / "datasus" / "cnes_all.csv"
    pipeline._raw = pd.read_csv(csv_path, dtype=str, keep_default_na=False)


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "datasus"
    assert pipeline.source_id == "cnes"


def test_extract_raises_when_csv_missing() -> None:
    driver = MagicMock()
    pipeline = DatasusPipeline(driver, data_dir="/nonexistent/path")
    try:
        pipeline.extract()
        raise AssertionError("Expected FileNotFoundError")
    except FileNotFoundError:
        pass


def test_extract_reads_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    assert len(pipeline._raw) == 4


def test_transform_creates_facilities() -> None:
    """4 rows: 2 valid with CNPJ, 1 empty cnes_code (skip), 1 valid no CNPJ = 3 facilities."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    assert len(pipeline.facilities) == 3


def test_transform_facility_fields() -> None:
    """Verify correct fields on first facility (Hospital Municipal de Altamira)."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    hospital = pipeline.facilities[0]
    assert hospital["cnes_code"] == "0012345"
    assert hospital["tipo_unidade"] == "05"
    assert hospital["uf"] == "15"
    assert hospital["source"] == "cnes"
    # normalize_name uppercases
    assert hospital["name"] == "HMA"
    assert hospital["razao_social"] == "HOSPITAL MUNICIPAL DE ALTAMIRA"


def test_transform_normalizes_atende_sus() -> None:
    """SIM -> '1', NAO -> '0', raw '1' -> '1'."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    sus_values = {f["cnes_code"]: f["atende_sus"] for f in pipeline.facilities}
    assert sus_values["0012345"] == "1"   # SIM -> 1
    assert sus_values["0067890"] == "0"   # NAO -> 0
    assert sus_values["0099999"] == "1"   # raw "1" -> 1


def test_transform_creates_company_links_for_valid_cnpj() -> None:
    """Only rows with 14-digit CNPJ get company links."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    assert len(pipeline.company_links) == 2
    cnpjs = {link["source_key"] for link in pipeline.company_links}
    assert "11.222.333/0001-81" in cnpjs
    assert "44.555.666/0001-99" in cnpjs


def test_transform_skips_empty_cnes_code() -> None:
    """Row 3 has empty codigo_cnes and should be skipped."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    cnes_codes = {f["cnes_code"] for f in pipeline.facilities}
    assert "" not in cnes_codes
    # "Estabelecimento Sem Codigo" should not appear
    names = {f["razao_social"] for f in pipeline.facilities}
    assert "ESTABELECIMENTO SEM CODIGO" not in names


def test_transform_formats_cnpj_correctly() -> None:
    """CNPJs in company_links must be formatted XX.XXX.XXX/XXXX-XX."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    for link in pipeline.company_links:
        cnpj = link["source_key"]
        # Verify the XX.XXX.XXX/XXXX-XX format
        assert cnpj[2] == "."
        assert cnpj[6] == "."
        assert cnpj[10] == "/"
        assert cnpj[15] == "-"
        assert len(cnpj) == 18


def test_transform_no_company_link_without_cnpj() -> None:
    """Row with empty CNPJ (UBS Sem CNPJ) should produce facility but no company link."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    link_targets = {link["target_key"] for link in pipeline.company_links}
    assert "0099999" not in link_targets


def test_load_calls_batch_loader() -> None:
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should have called session.run for:
    # Health nodes, Company nodes, OPERA_UNIDADE rels = 3 calls minimum
    assert session.run.call_count >= 3
