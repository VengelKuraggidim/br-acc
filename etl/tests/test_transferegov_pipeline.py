from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.transferegov import TransferegovPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TransferegovPipeline:
    driver = MagicMock()
    return TransferegovPipeline(driver, data_dir=str(FIXTURES))


def _extract(pipeline: TransferegovPipeline) -> None:
    """Run extract against fixture CSVs in fixtures/transferegov/."""
    pipeline.extract()


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "transferegov"
    assert pipeline.source_id == "transferegov"


def test_transform_creates_amendments_and_convenios() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    # Fixture has 3 valid amendment codes: EMD001, EMD002, EMD003
    # "Sem informação" is skipped
    assert len(pipeline.amendments) == 3

    amendment_ids = {a["amendment_id"] for a in pipeline.amendments}
    assert amendment_ids == {"EMD001", "EMD002", "EMD003"}

    # Convenios: CONV001 and CONV002 are valid (CONV linked to "Sem informação" skipped,
    # EMD003 row has empty Número Convênio)
    assert len(pipeline.convenios) == 2
    convenio_ids = {c["convenio_id"] for c in pipeline.convenios}
    assert convenio_ids == {"CONV001", "CONV002"}


def test_transform_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    cnpjs = [c["cnpj"] for c in pipeline.favorecido_companies]
    assert "11.222.333/0001-81" in cnpjs
    assert "44.555.666/0001-99" in cnpjs


def test_transform_skips_invalid() -> None:
    """Rows with 'Sem informação' emenda code or invalid entity types are skipped."""
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    # "Sem informação" amendment should not appear
    amendment_ids = {a["amendment_id"] for a in pipeline.amendments}
    assert "Sem informação" not in amendment_ids

    # "Unidade Gestora" favorecido should not create a company or person
    all_names = [c["razao_social"] for c in pipeline.favorecido_companies] + [
        p["name"] for p in pipeline.favorecido_persons
    ]
    assert "ORGAO PUBLICO" not in all_names

    # Favorecido linked to "Sem informação" emenda should not appear
    assert "EMPRESA FANTASMA" not in [c["razao_social"] for c in pipeline.favorecido_companies]


def test_transform_sums_values() -> None:
    """EMD001 has two rows with Valor Empenhado 1.500.000 + 500.000 = 2.000.000."""
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    emd001 = next(a for a in pipeline.amendments if a["amendment_id"] == "EMD001")
    assert emd001["value_committed"] == 2_000_000.0
    assert emd001["value_paid"] == 1_000_000.0


def test_transform_creates_authors() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    author_keys = {a["author_key"] for a in pipeline.authors}
    # A001 and A002 valid; S/I skipped; A999 linked to "Sem informação" emenda (skipped)
    assert "A001" in author_keys
    assert "A002" in author_keys


def test_transform_creates_persons() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()

    # CPF favorecido should produce a Person node
    cpfs = [p["cpf"] for p in pipeline.favorecido_persons]
    assert "123.456.789-01" in cpfs


def test_load_calls_session() -> None:
    pipeline = _make_pipeline()
    _extract(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should have called session.run for nodes + relationships
    assert session.run.call_count >= 3
