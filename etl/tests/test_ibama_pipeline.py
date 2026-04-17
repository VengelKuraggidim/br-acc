from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.ibama import IbamaPipeline
from tests._mock_helpers import mock_driver, mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None, **kwargs: object) -> IbamaPipeline:
    driver = MagicMock()
    return IbamaPipeline(
        driver,
        data_dir=data_dir or str(FIXTURES),
        **kwargs,  # type: ignore[arg-type]
    )


def _load_fixture(pipeline: IbamaPipeline) -> None:
    """Read the fixture CSV into the pipeline's internal DataFrame."""
    csv_path = FIXTURES / "ibama" / "areas_embargadas.csv"
    pipeline._raw = pd.read_csv(
        csv_path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
    )


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "ibama"
    assert pipeline.source_id == "ibama"


def test_transform_creates_embargoes() -> None:
    """5 rows: 2 valid companies, 1 valid person, 1 invalid doc (skip), 1 empty SEQ (skip) = 3."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    assert len(pipeline.embargoes) == 3


def test_transform_links_companies_and_persons() -> None:
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.persons) == 1

    company_cnpjs = {c["cnpj"] for c in pipeline.companies}
    assert "11.222.333/0001-81" in company_cnpjs
    assert "44.555.666/0001-99" in company_cnpjs

    person_cpfs = {p["cpf"] for p in pipeline.persons}
    assert "123.456.789-01" in person_cpfs


def test_transform_skips_invalid_document() -> None:
    """Row with 5-digit document should be skipped entirely."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    all_ids = {e["embargo_id"] for e in pipeline.embargoes}
    assert "ibama_embargo_1003" not in all_ids


def test_transform_skips_empty_seq() -> None:
    """Row with empty SEQ_TAD should be skipped."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    all_names = set()
    for c in pipeline.companies:
        all_names.add(c["razao_social"])
    for p in pipeline.persons:
        all_names.add(p["name"])
    assert "SEM SEQ EMPRESA" not in all_names


def test_transform_parses_dates() -> None:
    """Both dd/mm/yyyy HH:MM:SS and dd/mm/yyyy formats should parse to ISO."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    dates = {e["embargo_id"]: e["date"] for e in pipeline.embargoes}
    # datetime format: 15/03/2023 10:30:00
    assert dates["ibama_embargo_1001"] == "2023-03-15"
    # date-only format: 20/06/2023
    assert dates["ibama_embargo_1002"] == "2023-06-20"


def test_transform_parses_area() -> None:
    """Brazilian comma-decimal format should be converted to float."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    areas = {e["embargo_id"]: e["area_ha"] for e in pipeline.embargoes}
    assert areas["ibama_embargo_1001"] == 150.5
    assert areas["ibama_embargo_1002"] == 30.0


def test_transform_extracts_primary_biome() -> None:
    """Comma-separated biome list should return only the first entry."""
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()

    biomes = {e["embargo_id"]: e["biome"] for e in pipeline.embargoes}
    # "Amazonia, Cerrado" -> "Amazonia"
    assert biomes["ibama_embargo_1001"] == "Amazonia"
    # single biome
    assert biomes["ibama_embargo_1002"] == "Cerrado"
    # empty biome
    assert biomes["ibama_embargo_1004"] == ""


def test_load_calls_batch_loader() -> None:
    pipeline = _make_pipeline()
    _load_fixture(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should have called session.run for:
    # Embargo nodes, Company nodes, Person nodes, EMBARGADA rels = 4 calls minimum
    assert session.run.call_count >= 4


class TestPrimaryBiome:
    def test_single_biome(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._primary_biome("Cerrado") == "Cerrado"

    def test_comma_separated_returns_first(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._primary_biome("Amazonia, Cerrado, Caatinga") == "Amazonia"

    def test_strips_whitespace_from_first(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._primary_biome("  Mata Atlantica , Pampa") == "Mata Atlantica"

    def test_empty_returns_empty(self) -> None:
        pipeline = _make_pipeline()
        assert pipeline._primary_biome("") == ""
        assert pipeline._primary_biome("   ") == ""


def test_extract_missing_data_dir(tmp_path: Path) -> None:
    """Missing `ibama/` dir leaves `_raw` empty without raising."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline._raw.empty


def test_extract_dir_without_csv(tmp_path: Path) -> None:
    """Dir exists but CSV doesn't → `_raw` empty, no raise."""
    (tmp_path / "ibama").mkdir()
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline._raw.empty


def test_extract_respects_limit(tmp_path: Path) -> None:
    """`limit=1` keeps only the first row."""
    ibama_dir = tmp_path / "ibama"
    ibama_dir.mkdir()
    (ibama_dir / "areas_embargadas.csv").write_text(
        "SEQ_TAD;CPF_CNPJ_EMBARGADO;NOME_PESSOA_EMBARGADA;DAT_EMBARGO;"
        "QTD_AREA_EMBARGADA;DES_TIPO_BIOMA;SIG_UF_TAD;NOM_MUNICIPIO_TAD;"
        "DES_INFRACAO;NUM_AUTO_INFRACAO;NUM_PROCESSO\n"
        "1;11222333000181;EMPRESA A;15/03/2023;10,5;Cerrado;DF;BRASILIA;INFR A;AUTO-1;PROC-1\n"
        "2;11222333000181;EMPRESA A;16/03/2023;11,5;Cerrado;DF;BRASILIA;INFR B;AUTO-2;PROC-2\n",
        encoding="utf-8",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path), limit=1)
    pipeline.extract()
    assert len(pipeline._raw) == 1


def test_load_short_circuits_when_empty(tmp_path: Path) -> None:
    """No embargoes/companies/persons/rels → loader should not open a session."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    pipeline.load()
    assert not mock_driver(pipeline).session.called
