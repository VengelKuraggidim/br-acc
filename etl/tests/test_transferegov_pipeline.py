from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.transferegov import TransferegovPipeline, _parse_brl
from tests._mock_helpers import mock_driver, mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> TransferegovPipeline:
    driver = MagicMock()
    return TransferegovPipeline(driver, data_dir=data_dir or str(FIXTURES))


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


# --- _parse_brl helper ---


class TestParseBrl:
    def test_none_returns_zero(self) -> None:
        assert _parse_brl(None) == 0.0

    def test_empty_string_returns_zero(self) -> None:
        assert _parse_brl("") == 0.0
        assert _parse_brl("   ") == 0.0

    def test_plain_brazilian_format(self) -> None:
        # Thousands-dot + decimal-comma.
        assert _parse_brl("1.234.567,89") == 1234567.89

    def test_with_currency_symbol(self) -> None:
        assert _parse_brl("R$ 1.234,56") == 1234.56
        assert _parse_brl("R$500,00") == 500.00

    def test_integer_without_comma(self) -> None:
        # No comma → treated as-is (period not stripped since no comma present)
        assert _parse_brl("500") == 500.0

    def test_invalid_returns_zero(self) -> None:
        assert _parse_brl("nao numerico") == 0.0

    def test_only_currency_symbol_returns_zero(self) -> None:
        assert _parse_brl("R$") == 0.0


# --- Value-summing across duplicate amendment rows ---


def test_amendment_sums_only_from_its_own_rows(tmp_path: Path) -> None:
    """Values from a different amendment must not leak into another's total."""
    data_dir = tmp_path / "transferegov"
    data_dir.mkdir()
    header = (
        "Código da Emenda;Código do Autor da Emenda;Nome do Autor da Emenda;"
        "Tipo de Emenda;Nome Função;Município;UF;Valor Empenhado;Valor Pago\n"
    )
    rows = (
        "E1;A1;Autor 1;I;Saude;Rio;RJ;1.000,00;500,00\n"
        "E1;A1;Autor 1;I;Saude;Rio;RJ;2.000,00;1.000,00\n"
        "E2;A2;Autor 2;I;Educacao;SP;SP;9.999,00;0,00\n"
    )
    (data_dir / "EmendasParlamentares.csv").write_text(
        header + rows, encoding="latin-1"
    )
    (data_dir / "EmendasParlamentares_PorFavorecido.csv").write_text(
        "Código da Emenda;Código do Favorecido;Tipo Favorecido;Favorecido;"
        "Valor Recebido;Município Favorecido;UF Favorecido\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_Convenios.csv").write_text(
        "Código da Emenda;Número Convênio;Convenente;Objeto Convênio;"
        "Valor Convênio;Data Publicação Convênio;Nome Função\n",
        encoding="latin-1",
    )

    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()

    by_id = {a["amendment_id"]: a for a in pipeline.amendments}
    assert by_id["E1"]["value_committed"] == 3000.0  # 1k + 2k
    assert by_id["E1"]["value_paid"] == 1500.0  # 0.5k + 1k
    assert by_id["E2"]["value_committed"] == 9999.0


# --- Favorecido branches ---


def test_favorecido_with_short_cnpj_is_dropped(tmp_path: Path) -> None:
    data_dir = tmp_path / "transferegov"
    data_dir.mkdir()
    (data_dir / "EmendasParlamentares.csv").write_text(
        "Código da Emenda;Código do Autor da Emenda;Nome do Autor da Emenda;"
        "Tipo de Emenda;Nome Função;Município;UF;Valor Empenhado;Valor Pago\n"
        "E1;A1;Autor 1;I;Saude;Rio;RJ;100,00;0,00\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_PorFavorecido.csv").write_text(
        "Código da Emenda;Código do Favorecido;Tipo Favorecido;Favorecido;"
        "Valor Recebido;Município Favorecido;UF Favorecido\n"
        "E1;12345;Pessoa Jurídica;Empresa Fake;100,00;Rio;RJ\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_Convenios.csv").write_text(
        "Código da Emenda;Número Convênio;Convenente;Objeto Convênio;"
        "Valor Convênio;Data Publicação Convênio;Nome Função\n",
        encoding="latin-1",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    assert pipeline.favorecido_companies == []


def test_favorecido_unknown_tipo_is_dropped(tmp_path: Path) -> None:
    """Tipo that isn't Pessoa Jurídica or Pessoa Fisica creates no entity."""
    data_dir = tmp_path / "transferegov"
    data_dir.mkdir()
    (data_dir / "EmendasParlamentares.csv").write_text(
        "Código da Emenda;Código do Autor da Emenda;Nome do Autor da Emenda;"
        "Tipo de Emenda;Nome Função;Município;UF;Valor Empenhado;Valor Pago\n"
        "E1;A1;Autor;I;Saude;Rio;RJ;100,00;0,00\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_PorFavorecido.csv").write_text(
        "Código da Emenda;Código do Favorecido;Tipo Favorecido;Favorecido;"
        "Valor Recebido;Município Favorecido;UF Favorecido\n"
        "E1;11222333000181;Unidade Gestora;ORGAO PUBLICO;100,00;Rio;RJ\n"
        "E1;11111111111;Inscrição Genérica;IG;50,00;Rio;RJ\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_Convenios.csv").write_text(
        "Código da Emenda;Número Convênio;Convenente;Objeto Convênio;"
        "Valor Convênio;Data Publicação Convênio;Nome Função\n",
        encoding="latin-1",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    assert pipeline.favorecido_companies == []
    assert pipeline.favorecido_persons == []


def test_convenio_without_numero_is_skipped(tmp_path: Path) -> None:
    data_dir = tmp_path / "transferegov"
    data_dir.mkdir()
    (data_dir / "EmendasParlamentares.csv").write_text(
        "Código da Emenda;Código do Autor da Emenda;Nome do Autor da Emenda;"
        "Tipo de Emenda;Nome Função;Município;UF;Valor Empenhado;Valor Pago\n"
        "E1;A1;Autor;I;Saude;Rio;RJ;100,00;0,00\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_PorFavorecido.csv").write_text(
        "Código da Emenda;Código do Favorecido;Tipo Favorecido;Favorecido;"
        "Valor Recebido;Município Favorecido;UF Favorecido\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_Convenios.csv").write_text(
        "Código da Emenda;Número Convênio;Convenente;Objeto Convênio;"
        "Valor Convênio;Data Publicação Convênio;Nome Função\n"
        "E1;;Convenente X;Objeto Y;100,00;2026-02-01;Saude\n"
        "E1;CONV-1;Convenente X;Objeto Y;100,00;2026-02-01;Saude\n",
        encoding="latin-1",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    ids = {c["convenio_id"] for c in pipeline.convenios}
    assert ids == {"CONV-1"}


def test_author_with_si_code_is_dropped(tmp_path: Path) -> None:
    """Author code 'S/I' (sem informação) must not create an author node/rel."""
    data_dir = tmp_path / "transferegov"
    data_dir.mkdir()
    (data_dir / "EmendasParlamentares.csv").write_text(
        "Código da Emenda;Código do Autor da Emenda;Nome do Autor da Emenda;"
        "Tipo de Emenda;Nome Função;Município;UF;Valor Empenhado;Valor Pago\n"
        "E1;S/I;Sem Info;I;Saude;Rio;RJ;100,00;0,00\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_PorFavorecido.csv").write_text(
        "Código da Emenda;Código do Favorecido;Tipo Favorecido;Favorecido;"
        "Valor Recebido;Município Favorecido;UF Favorecido\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_Convenios.csv").write_text(
        "Código da Emenda;Número Convênio;Convenente;Objeto Convênio;"
        "Valor Convênio;Data Publicação Convênio;Nome Função\n",
        encoding="latin-1",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    # Amendment still created — only the author side is filtered.
    assert len(pipeline.amendments) == 1
    assert pipeline.authors == []
    assert pipeline.author_rels == []


def test_load_short_circuits_for_empty_collections(tmp_path: Path) -> None:
    """When nothing transformed, load should not open a session."""
    data_dir = tmp_path / "transferegov"
    data_dir.mkdir()
    header_emendas = (
        "Código da Emenda;Código do Autor da Emenda;Nome do Autor da Emenda;"
        "Tipo de Emenda;Nome Função;Município;UF;Valor Empenhado;Valor Pago\n"
    )
    (data_dir / "EmendasParlamentares.csv").write_text(
        header_emendas, encoding="latin-1"
    )
    (data_dir / "EmendasParlamentares_PorFavorecido.csv").write_text(
        "Código da Emenda;Código do Favorecido;Tipo Favorecido;Favorecido;"
        "Valor Recebido;Município Favorecido;UF Favorecido\n",
        encoding="latin-1",
    )
    (data_dir / "EmendasParlamentares_Convenios.csv").write_text(
        "Código da Emenda;Número Convênio;Convenente;Objeto Convênio;"
        "Valor Convênio;Data Publicação Convênio;Nome Função\n",
        encoding="latin-1",
    )
    pipeline = _make_pipeline(data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    pipeline.load()
    assert not mock_driver(pipeline).session.called
