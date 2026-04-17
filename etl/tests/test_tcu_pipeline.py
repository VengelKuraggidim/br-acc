from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.tcu import TcuPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TcuPipeline:
    driver = MagicMock()
    return TcuPipeline(driver, data_dir=str(FIXTURES))


def _load_fixtures(pipeline: TcuPipeline) -> None:
    """Read all 4 fixture CSVs into the pipeline's internal DataFrames."""
    tcu_dir = FIXTURES / "tcu"
    pipeline._raw_inabilitados = pipeline._read_csv(
        tcu_dir / "inabilitados-funcao-publica.csv"
    )
    pipeline._raw_inidoneos = pipeline._read_csv(
        tcu_dir / "licitantes-inidoneos.csv"
    )
    pipeline._raw_irregulares = pipeline._read_csv(
        tcu_dir / "resp-contas-julgadas-irregulares.csv"
    )
    pipeline._raw_irregulares_eleitorais = pipeline._read_csv(
        tcu_dir / "resp-contas-julgadas-irreg-implicacao-eleitoral.csv"
    )


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "tcu"
    assert pipeline.source_id == "tcu"


def test_transform_creates_sanctions() -> None:
    """Total sanctions across all 4 files.

    inabilitados: 2 valid CPF, 1 invalid skipped = 2
    inidoneos: all 3 rows create sanctions (entity linking is separate) = 3
    irregulares: all 3 rows create sanctions (entity linking is separate) = 3
    irregulares_eleitorais: 1 valid CPF, 1 invalid skipped = 1
    Total: 9 sanctions
    """
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.sanctions) == 9


def test_transform_links_entities() -> None:
    """Check person and company link counts.

    Persons: 2 inabilitados + 1 inidoneo(CPF) + 1 irregular(CPF) + 1 eleitoral = 5
    Companies: 1 inidoneo(CNPJ) + 1 irregular(CNPJ) = 2
    """
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.sanctioned_persons) == 5
    assert len(pipeline.sanctioned_companies) == 2

    person_cpfs = {p["cpf"] for p in pipeline.sanctioned_persons}
    assert "111.222.333-44" in person_cpfs
    assert "555.666.777-88" in person_cpfs
    assert "888.777.666-55" in person_cpfs

    company_cnpjs = {c["cnpj"] for c in pipeline.sanctioned_companies}
    assert "11.222.333/0001-81" in company_cnpjs
    assert "44.555.666/0001-99" in company_cnpjs


def test_transform_skips_invalid_docs() -> None:
    """Rows with documents that are neither 11 nor 14 digits should be skipped."""
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    all_names = set()
    for p in pipeline.sanctioned_persons:
        all_names.add(p["name"])
    for c in pipeline.sanctioned_companies:
        all_names.add(c["name"])

    assert "DOCUMENTO INVALIDO" not in all_names
    assert "DOC CURTO" not in all_names
    assert "INVALIDO ELEITORAL" not in all_names


def test_transform_sanction_types() -> None:
    """Each file produces a distinct sanction type."""
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    types = {s["type"] for s in pipeline.sanctions}
    assert "tcu_inabilitado" in types
    assert "tcu_inidoneo" in types
    assert "tcu_conta_irregular" in types
    assert "tcu_conta_irregular_eleitoral" in types


def test_transform_parses_dates() -> None:
    """Dates in dd/mm/yyyy format should parse to ISO yyyy-mm-dd."""
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    # Find the first inabilitado sanction by type
    inab = [s for s in pipeline.sanctions if s["type"] == "tcu_inabilitado"]
    assert len(inab) == 2
    dates_start = {s["date_start"] for s in inab}
    assert "2022-03-15" in dates_start
    assert "2023-06-20" in dates_start


def test_transform_sets_court_and_source() -> None:
    """All sanctions should have court=TCU and source=tcu."""
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    for s in pipeline.sanctions:
        assert s["court"] == "TCU"
        assert s["source"] == "tcu"


def test_transform_eleitoral_captures_cargo() -> None:
    """Electoral irregularity sanctions should capture the CARGO/FUNCAO field."""
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()

    eleitoral = [s for s in pipeline.sanctions if s["type"] == "tcu_conta_irregular_eleitoral"]
    assert len(eleitoral) == 1
    assert eleitoral[0]["cargo"] == "PREFEITO"


def test_load_calls_session() -> None:
    pipeline = _make_pipeline()
    _load_fixtures(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should have called session.run for:
    # Sanction nodes, Person nodes, Person rels, Company nodes, Company rels = 5 calls minimum
    assert session.run.call_count >= 5
