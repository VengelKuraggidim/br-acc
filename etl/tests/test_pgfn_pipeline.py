from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.pgfn import PgfnPipeline
from tests._mock_helpers import mock_driver, mock_session

FIXTURES = Path(__file__).parent / "fixtures"

_PGFN_HEADER = (
    "CPF_CNPJ;TIPO_PESSOA;TIPO_DEVEDOR;NUMERO_INSCRICAO;VALOR_CONSOLIDADO;"
    "DATA_INSCRICAO;NOME_DEVEDOR;SITUACAO_INSCRICAO;RECEITA_PRINCIPAL;"
    "INDICADOR_AJUIZADO\n"
)


def _make_pipeline(data_dir: str | None = None, **kwargs: object) -> PgfnPipeline:
    driver = MagicMock()
    return PgfnPipeline(
        driver,
        data_dir=data_dir or str(FIXTURES),
        **kwargs,  # type: ignore[arg-type]
    )


def _write_pgfn_csv(dir_: Path, name: str, rows: str) -> None:
    (dir_ / name).write_text(_PGFN_HEADER + rows, encoding="latin-1")


def _extract_and_transform(pipeline: PgfnPipeline) -> None:
    """Run extract + transform from fixture data."""
    pipeline.extract()
    pipeline.transform()


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "pgfn"
    assert pipeline.source_id == "pgfn"


def test_transform_filters_pj_principal_only() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # 5 rows: 2 valid PJ PRINCIPAL, 1 PF (skip), 1 CORRESPONSAVEL (skip), 1 bad CNPJ (skip) = 2
    assert len(pipeline.finances) == 2


def test_transform_skips_pessoa_fisica() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # PF row (JOAO DA SILVA) must not appear
    names = [r["company_name"] for r in pipeline.relationships]
    assert "JOAO DA SILVA" not in names


def test_transform_skips_corresponsavel() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # CORRESPONSAVEL inscription 100004 must not appear
    inscricoes = [f["inscription_number"] for f in pipeline.finances]
    assert "100004" not in inscricoes


def test_transform_skips_bad_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    # Row with CNPJ "12345" (inscription 100005) must not appear
    inscricoes = [f["inscription_number"] for f in pipeline.finances]
    assert "100005" not in inscricoes


def test_transform_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    cnpjs = [r["source_key"] for r in pipeline.relationships]
    assert "11.222.333/0001-81" in cnpjs
    assert "44.555.666/0001-99" in cnpjs


def test_transform_parses_values() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)

    values = {f["inscription_number"]: f["value"] for f in pipeline.finances}
    assert values["100001"] == 50000.00
    assert values["100002"] == 75000.50


def test_transform_deduplicates_inscricao() -> None:
    """Duplicate inscription numbers should be deduplicated via seen_inscricoes."""
    pipeline = _make_pipeline()
    pipeline.extract()

    # Duplicate the CSV file list so transform reads the same file twice
    pipeline._csv_files = pipeline._csv_files * 2

    pipeline.transform()

    # Inscriptions should still be unique despite reading the file twice
    inscricoes = [f["inscription_number"] for f in pipeline.finances]
    assert len(inscricoes) == len(set(inscricoes))
    assert len(inscricoes) == 2


def test_load_calls_session_run() -> None:
    pipeline = _make_pipeline()
    _extract_and_transform(pipeline)
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # load_nodes for Finance + _run_with_retry batches for relationships
    assert session.run.call_count >= 2


def test_extract_missing_data_dir(tmp_path: Path) -> None:
    """Missing `pgfn/` dir leaves _csv_files empty without raising."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline._csv_files == []


def test_extract_dir_without_matching_files(tmp_path: Path) -> None:
    """Dir exists but has no `arquivo_lai_SIDA_*` files → _csv_files empty."""
    pgfn_dir = tmp_path / "pgfn"
    pgfn_dir.mkdir()
    (pgfn_dir / "irrelevant.csv").write_text("a,b\n1,2", encoding="latin-1")
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    assert pipeline._csv_files == []


def test_transform_empty_csv_list_yields_empty_collections(tmp_path: Path) -> None:
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()  # nothing found
    pipeline.transform()
    assert pipeline.finances == []
    assert pipeline.relationships == []


def test_transform_skips_row_with_empty_inscricao(tmp_path: Path) -> None:
    """Row passes PJ/PRINCIPAL/CNPJ checks but has blank NUMERO_INSCRICAO."""
    pgfn_dir = tmp_path / "pgfn"
    pgfn_dir.mkdir()
    _write_pgfn_csv(
        pgfn_dir,
        "arquivo_lai_SIDA_2024_01.csv",
        "11222333000181;juridica;PRINCIPAL;;1000,00;"
        "2024-01-01;EMPRESA X;ATIVA;IRPJ;NAO_AJUIZADO\n",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    assert pipeline.finances == []


def test_transform_limit_short_circuits(tmp_path: Path) -> None:
    """`limit` stops iteration after the Nth finance record."""
    pgfn_dir = tmp_path / "pgfn"
    pgfn_dir.mkdir()
    rows = "".join(
        f"11222333000181;juridica;PRINCIPAL;INSC{i};1000,00;"
        f"2024-01-01;EMPRESA X;ATIVA;IRPJ;NAO_AJUIZADO\n"
        for i in range(5)
    )
    _write_pgfn_csv(pgfn_dir, "arquivo_lai_SIDA_2024_02.csv", rows)
    pipeline = _make_pipeline(data_dir=str(tmp_path), limit=3)
    pipeline.extract()
    pipeline.transform()
    assert len(pipeline.finances) == 3


def test_load_short_circuits_when_empty(tmp_path: Path) -> None:
    """No finances → loader must not open a session."""
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    pipeline.load()
    assert not mock_driver(pipeline).session.called
