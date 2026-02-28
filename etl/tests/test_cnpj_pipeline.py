from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from icarus_etl.pipelines.cnpj import CNPJPipeline, parse_capital_social

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None, **kwargs: object) -> CNPJPipeline:
    driver = MagicMock()
    if data_dir is None:
        data_dir = str(FIXTURES)
    return CNPJPipeline(driver=driver, data_dir=data_dir, **kwargs)  # type: ignore[arg-type]


def _write_bq_fixtures(tmp_path: Path) -> Path:
    """Create BQ-format CSV fixtures in a temp directory.

    Returns the data_dir path (parent of cnpj/).
    """
    extracted = tmp_path / "cnpj" / "extracted"
    extracted.mkdir(parents=True)

    empresas_csv = (
        "ano,mes,data,cnpj_basico,razao_social,natureza_juridica,"
        "qualificacao_responsavel,capital_social,porte,ente_federativo\n"
        "2024,01,2024-01-01,00000000,BANCO DO BRASIL S.A.,2046,10,7500000000,05,\n"
        "2024,01,2024-01-01,33000167,PETROLEO BRASILEIRO S.A.,2046,10,205432000000,05,\n"
        "2024,01,2024-01-01,60746948,COMPANHIA BRASILEIRA DE DISTRIBUICAO,2046,10,7016038000,05,\n"
    )
    (extracted / "empresas_DF.csv").write_text(empresas_csv)

    socios_csv = (
        "ano,mes,data,cnpj_basico,tipo,nome,documento,qualificacao,"
        "data_entrada_sociedade,id_pais,cpf_representante_legal,"
        "nome_representante_legal,qualificacao_representante_legal,faixa_etaria\n"
        "2024,01,2024-01-01,00000000,2,JOAO DA SILVA,***123456**,22,20200115,0,,0,0,6\n"
        "2024,01,2024-01-01,00000000,2,MARIA SANTOS,***987654**,22,20190308,0,,0,0,5\n"
        "2024,01,2024-01-01,33000167,2,CARLOS OLIVEIRA,***111222**,16,20210520,0,,0,0,4\n"
    )
    (extracted / "socios_DF.csv").write_text(socios_csv)

    estabelecimentos_csv = (
        "ano,mes,data,cnpj,cnpj_basico,cnpj_ordem,cnpj_dv,"
        "identificador_matriz_filial,nome_fantasia,situacao_cadastral,"
        "data_situacao_cadastral,motivo_situacao_cadastral,nome_cidade_exterior,"
        "id_pais,data_inicio_atividade,cnae_fiscal_principal,cnae_fiscal_secundaria,"
        "sigla_uf,id_municipio,id_municipio_rf,tipo_logradouro,logradouro,numero,"
        "complemento,bairro,cep,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,fax,"
        "email,situacao_especial,data_situacao_especial\n"
        "2024,01,2024-01-01,00000000000100,00000000,0001,00,1,BB AGENCIA CENTRAL,"
        "02,20050101,0,,0,19660101,6421200,,DF,9701,,SCS,QUADRA 01,SN,BLOCO A,"
        "ASA SUL,70073900,61,30000000,,,,,,,\n"
        "2024,01,2024-01-01,33000167000101,33000167,0001,01,1,PETROBRAS,"
        "02,20050101,0,,0,19531003,0600001,,RJ,6001,,AV,REPUBLICA DO CHILE,65,,"
        "CENTRO,20031170,21,30000000,,,,,,,\n"
        "2024,01,2024-01-01,60746948000112,60746948,0001,12,1,PAO DE ACUCAR,"
        "02,20050101,0,,0,19480907,4711302,,SP,7107,,AV,BRIGADEIRO LUIS ANTONIO,3172,,"
        "JARDIM PAULISTA,01402901,11,30000000,,,,,,,\n"
    )
    (extracted / "estabelecimentos_DF.csv").write_text(estabelecimentos_csv)

    return tmp_path


def _write_bq_history_fixtures(tmp_path: Path) -> Path:
    extracted = tmp_path / "cnpj" / "extracted"
    extracted.mkdir(parents=True)

    (extracted / "empresas_history.csv").write_text(
        (
            "ano,mes,data,cnpj_basico,razao_social,natureza_juridica,"
            "qualificacao_responsavel,capital_social,porte,ente_federativo\n"
            "2024,01,2024-01-01,00000000,BANCO TESTE S.A.,2046,10,1000,05,\n"
            "2024,02,2024-02-01,00000000,BANCO TESTE S.A.,2046,10,1000,05,\n"
        ),
    )
    (extracted / "socios_history.csv").write_text(
        (
            "ano,mes,data,cnpj_basico,tipo,nome,documento,qualificacao,"
            "data_entrada_sociedade,id_pais,cpf_representante_legal,"
            "nome_representante_legal,qualificacao_representante_legal,faixa_etaria\n"
            "2024,01,2024-01-01,00000000,2,JOAO DA SILVA,***123456**,22,20200115,0,,0,0,6\n"
            "2024,02,2024-02-01,00000000,2,JOAO DA SILVA,***123456**,22,20200115,0,,0,0,6\n"
        ),
    )
    (extracted / "estabelecimentos_history.csv").write_text(
        (
            "ano,mes,data,cnpj_basico,cnpj_ordem,cnpj_dv,identificador_matriz_filial,"
            "nome_fantasia,situacao_cadastral,data_situacao_cadastral,motivo_situacao_cadastral,"
            "nome_cidade_exterior,id_pais,data_inicio_atividade,cnae_fiscal_principal,"
            "cnae_fiscal_secundaria,sigla_uf,id_municipio,id_municipio_rf,tipo_logradouro,logradouro,"
            "numero,complemento,bairro,cep,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,fax,email,"
            "situacao_especial,data_situacao_especial\n"
            "2024,01,2024-01-01,00000000,0001,00,1,TESTE,02,20050101,0,,0,19660101,6421200,,DF,9701,,SCS,"
            "QUADRA 01,SN,BLOCO A,ASA SUL,70073900,61,30000000,,,,,,,\n"
            "2024,02,2024-02-01,00000000,0001,00,1,TESTE,02,20050101,0,,0,19660101,6421200,,DF,9701,,SCS,"
            "QUADRA 01,SN,BLOCO A,ASA SUL,70073900,61,30000000,,,,,,,\n"
        ),
    )
    return tmp_path


# --- parse_capital_social ---


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("7500000000,00", 7500000000.0),
        ("750000000,00", 750000000.0),
        ("0,00", 0.0),
        ("1234,56", 1234.56),
        ("7500000000", 7500000000.0),
        ("", 0.0),
        ("  ", 0.0),
    ],
)
def test_parse_capital_social(raw: str, expected: float) -> None:
    assert parse_capital_social(raw) == expected


# --- Pipeline metadata ---


def test_pipeline_metadata() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "cnpj"
    assert pipeline.source_id == "receita_federal"


# --- RF format (extract + transform) ---


def test_extract_rf_format() -> None:
    """Extract reads RF-format files from fixtures/cnpj/."""
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw_empresas) == 3
    assert len(pipeline._raw_socios) == 3
    assert len(pipeline._raw_estabelecimentos) == 3
    assert "cnpj_basico" in pipeline._raw_empresas.columns


def test_transform_rf_format_companies() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    first = pipeline.companies[0]
    assert "cnpj" in first
    assert "razao_social" in first
    assert "capital_social" in first
    assert "uf" in first
    assert "municipio" in first
    assert "natureza_juridica" in first
    assert "porte_empresa" in first


def test_transform_rf_format_parses_capital_social() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    bb = next(c for c in pipeline.companies if "BANCO DO BRASIL" in c["razao_social"])
    assert bb["capital_social"] == 7500000000.0


def test_transform_rf_format_builds_full_cnpj() -> None:
    """Full CNPJ constructed from cnpj_basico + cnpj_ordem + cnpj_dv via Estabelecimentos."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    cnpjs = [c["cnpj"] for c in pipeline.companies]
    for cnpj in cnpjs:
        assert "/" in cnpj
        assert "-" in cnpj


def test_transform_rf_format_normalizes_names() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    names = [c["razao_social"] for c in pipeline.companies]
    for name in names:
        assert name == name.upper()
        assert "  " not in name


def test_transform_rf_format_extracts_partners() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    # RF fixture socios use masked CPFs -> Partner nodes, not Person
    assert len(pipeline.partners) == 0
    assert len(pipeline.partial_partners) == 3
    partner_names = [p["name"] for p in pipeline.partial_partners]
    assert "JOAO DA SILVA" in partner_names
    assert "MARIA SANTOS" in partner_names


def test_transform_rf_format_extracts_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.relationships) == 0
    assert len(pipeline.partner_relationships) == 3
    rel = pipeline.partner_relationships[0]
    assert "source_key" in rel
    assert "target_key" in rel
    assert "tipo_socio" in rel
    assert "qualificacao" in rel
    assert "data_entrada" in rel


def test_transform_rf_format_preserves_partial_cpfs() -> None:
    """Partial CPFs from Receita Federal are kept as Partner partial docs."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    docs = [p["doc_partial"] for p in pipeline.partial_partners]
    assert all(len(doc) == 6 for doc in docs)
    assert "123456" in docs
    assert "987654" in docs


def test_transform_socios_rf_formats_cpf() -> None:
    """Full 11-digit CPFs in RF socios are formatted as XXX.XXX.XXX-XX."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)

    # Inject a full 11-digit CPF into the socios dataframe
    pipeline._raw_socios = pd.DataFrame([
        {
            "cnpj_basico": "00000000",
            "identificador_socio": "2",
            "nome_socio": "TEST PERSON",
            "cpf_cnpj_socio": "12345678901",
            "qualificacao_socio": "22",
            "data_entrada": "20200101",
            "pais": "0",
            "representante_legal": "",
            "nome_representante": "0",
            "qualificacao_representante": "0",
            "faixa_etaria": "6",
        },
    ])
    partners, partial_partners, pf_rels, partial_rels, pj_rels = pipeline._transform_socios_rf(
        pipeline._raw_socios,
    )

    assert len(partners) == 1
    assert len(partial_partners) == 0
    assert partners[0]["cpf"] == "123.456.789-01"
    assert pf_rels[0]["source_key"] == "123.456.789-01"
    assert len(partial_rels) == 0
    assert len(pj_rels) == 0


# --- BQ format (extract + transform) ---


def test_extract_bq_format(tmp_path: Path) -> None:
    """Extract reads BQ-format CSV files and populates raw dataframes."""
    data_dir = _write_bq_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir))
    pipeline.extract()

    assert len(pipeline._raw_empresas) == 3
    assert len(pipeline._raw_socios) == 3
    assert len(pipeline._raw_estabelecimentos) == 3


def test_extract_bq_format_renames_columns(tmp_path: Path) -> None:
    """BQ column names are renamed to RF equivalents after extract."""
    data_dir = _write_bq_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir))
    pipeline.extract()

    # Empresas: BQ "porte" -> RF "porte_empresa"
    assert "porte_empresa" in pipeline._raw_empresas.columns
    assert "porte" not in pipeline._raw_empresas.columns

    # Socios: BQ "documento" -> RF "cpf_cnpj_socio", BQ "nome" -> RF "nome_socio"
    assert "cpf_cnpj_socio" in pipeline._raw_socios.columns
    assert "documento" not in pipeline._raw_socios.columns
    assert "nome_socio" in pipeline._raw_socios.columns
    assert "nome" not in pipeline._raw_socios.columns
    assert "identificador_socio" in pipeline._raw_socios.columns
    assert "tipo" not in pipeline._raw_socios.columns

    # Estabelecimentos: BQ "cnae_fiscal_principal" -> RF "cnae_principal"
    assert "cnae_principal" in pipeline._raw_estabelecimentos.columns
    assert "cnae_fiscal_principal" not in pipeline._raw_estabelecimentos.columns
    assert "uf" in pipeline._raw_estabelecimentos.columns
    assert "sigla_uf" not in pipeline._raw_estabelecimentos.columns


def test_extract_bq_format_drops_metadata(tmp_path: Path) -> None:
    """BQ metadata columns (ano, mes, data) are dropped during extract."""
    data_dir = _write_bq_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir))
    pipeline.extract()

    for df in [pipeline._raw_empresas, pipeline._raw_socios, pipeline._raw_estabelecimentos]:
        assert "ano" not in df.columns
        assert "mes" not in df.columns
        assert "data" not in df.columns

    # Estabelecimentos also drops "cnpj" (full) and "id_municipio_rf"
    assert "id_municipio_rf" not in pipeline._raw_estabelecimentos.columns


def test_extract_bq_history_preserves_snapshot_columns(tmp_path: Path) -> None:
    data_dir = _write_bq_history_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir), history=True)
    pipeline.extract()

    assert len(pipeline._raw_socios) == 2
    assert "data" in pipeline._raw_socios.columns
    assert "ano" in pipeline._raw_socios.columns
    assert "mes" in pipeline._raw_socios.columns


def test_transform_bq_history_builds_snapshot_and_latest_projection(tmp_path: Path) -> None:
    data_dir = _write_bq_history_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir), history=True)
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.snapshot_relationships) == 2
    assert len(pipeline.partner_relationships) == 1
    latest = pipeline.partner_relationships[0]
    assert latest["target_key"] == "00.000.000/0001-00"
    # Latest projection must come from the latest snapshot.
    snapshots = sorted(r["snapshot_date"] for r in pipeline.snapshot_relationships)
    assert snapshots[-1] == "2024-02-01"


def test_transform_bq_format_companies(tmp_path: Path) -> None:
    """Full BQ pipeline: extract + transform produces correct company records."""
    data_dir = _write_bq_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir))
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    first = pipeline.companies[0]
    assert "cnpj" in first
    assert "razao_social" in first
    assert "capital_social" in first
    assert "uf" in first

    # Verify specific data made it through
    bb = next(c for c in pipeline.companies if "BANCO DO BRASIL" in c["razao_social"])
    assert bb["capital_social"] == 7500000000.0
    assert bb["uf"] == "DF"

    # CNPJ should be formatted (from estabelecimentos merge)
    for company in pipeline.companies:
        assert "/" in company["cnpj"]
        assert "-" in company["cnpj"]


# --- Simple CSV format (transform only, bypass extract) ---


def test_transform_simple_format() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False,
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False,
    )
    pipeline.transform()

    assert len(pipeline.companies) == 3
    assert len(pipeline.partners) == 3
    assert len(pipeline.partial_partners) == 0
    assert len(pipeline.relationships) == 3
    assert len(pipeline.partner_relationships) == 0


def test_transform_simple_format_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False,
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False,
    )
    pipeline.transform()

    cnpjs = [c["cnpj"] for c in pipeline.companies]
    for cnpj in cnpjs:
        assert "/" in cnpj
        assert "-" in cnpj


# --- Deduplication ---


def test_transform_deduplicates_by_cnpj() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.DataFrame([
        {
            "cnpj": "00000000000191",
            "razao_social": "Banco do Brasil",
            "cnae_principal": "6421200",
            "capital_social": "7500000000",
            "uf": "DF",
            "municipio": "Brasilia",
        },
        {
            "cnpj": "00000000000191",
            "razao_social": "Banco do Brasil (duplicate)",
            "cnae_principal": "6421200",
            "capital_social": "7500000000",
            "uf": "DF",
            "municipio": "Brasilia",
        },
    ])
    pipeline._raw_socios = pd.DataFrame(columns=["cnpj", "nome_socio", "cpf_socio", "tipo_socio"])
    pipeline.transform()

    assert len(pipeline.companies) == 1


# --- Limit / chunk_size ---


def test_limit_caps_rows() -> None:
    pipeline = _make_pipeline(limit=2)
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) <= 2


def test_chunk_size_parameter() -> None:
    pipeline = _make_pipeline(chunk_size=1)
    pipeline.extract()
    # Should still read all rows, just in smaller chunks
    assert len(pipeline._raw_empresas) == 3


# --- Vectorized transform helpers ---


def test_build_estab_lookup() -> None:
    """_build_estab_lookup populates basico -> (cnpj, cnae, uf, municipio) dict."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)

    assert len(pipeline._estab_lookup) == 3
    # BB: basico=00000000
    bb = pipeline._estab_lookup["00000000"]
    assert "/" in bb[0]  # formatted CNPJ
    assert "-" in bb[0]
    assert bb[1] == "6421200"  # cnae
    assert bb[2] == "DF"  # uf


def test_build_estab_lookup_deduplicates() -> None:
    """First occurrence per basico wins — subsequent rows for same basico are ignored."""
    pipeline = _make_pipeline()
    pipeline.extract()
    # Build twice — second call should not overwrite
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)
    first_cnpj = pipeline._estab_lookup["00000000"][0]
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)
    assert pipeline._estab_lookup["00000000"][0] == first_cnpj
    assert len(pipeline._estab_lookup) == 3


def test_transform_empresas_rf_vectorized() -> None:
    """_transform_empresas_rf returns correct records without iterrows."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)
    companies = pipeline._transform_empresas_rf(pipeline._raw_empresas)

    assert len(companies) == 3
    bb = next(c for c in companies if "BANCO DO BRASIL" in c["razao_social"])
    assert bb["capital_social"] == 7500000000.0
    assert "/" in bb["cnpj"]
    assert bb["uf"] == "DF"


def test_transform_socios_rf_vectorized() -> None:
    """_transform_socios_rf returns partners and relationships without iterrows."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)
    partners, partial_partners, pf_rels, partial_rels, pj_rels = pipeline._transform_socios_rf(
        pipeline._raw_socios,
    )

    # All 3 fixture rows are PF but with masked docs -> Partner path
    assert len(partners) == 0
    assert len(partial_partners) == 3
    assert len(pf_rels) == 0
    assert len(partial_rels) == 3
    assert len(pj_rels) == 0
    assert partial_partners[0]["name"] == "JOAO DA SILVA"
    assert "source_key" in partial_rels[0]
    assert "target_key" in partial_rels[0]


# --- Streaming pipeline ---


def test_run_streaming_rf_format() -> None:
    """run_streaming processes RF files chunk-by-chunk and calls loader."""
    pipeline = _make_pipeline(chunk_size=2)
    mock_session = MagicMock()
    pipeline.driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
    pipeline.driver.session.return_value.__exit__ = MagicMock(return_value=False)

    pipeline.run_streaming()

    # Verify loader was called (driver.session() was used)
    assert pipeline.driver.session.called


def test_run_streaming_bq_format(tmp_path: Path) -> None:
    """run_streaming detects BQ-format files and processes them correctly."""
    data_dir = _write_bq_fixtures(tmp_path)
    pipeline = _make_pipeline(data_dir=str(data_dir), chunk_size=2)
    mock_session = MagicMock()
    pipeline.driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
    pipeline.driver.session.return_value.__exit__ = MagicMock(return_value=False)

    pipeline.run_streaming()

    # Verify loader was called with data
    assert pipeline.driver.session.called
    # estab_lookup should be populated from BQ estabelecimentos
    assert len(pipeline._estab_lookup) == 3
    bb = pipeline._estab_lookup["00000000"]
    assert "/" in bb[0]  # formatted CNPJ
    assert bb[2] == "DF"  # UF


def test_run_streaming_bq_format_no_data(tmp_path: Path) -> None:
    """run_streaming with empty directory logs warning and returns."""
    empty_dir = tmp_path / "cnpj" / "extracted"
    empty_dir.mkdir(parents=True)
    pipeline = _make_pipeline(data_dir=str(tmp_path))

    pipeline.run_streaming()

    # No crash, no loader calls
    assert not pipeline.driver.session.called


# --- DM-002: PJ partner classification ---


def test_transform_socios_rf_pj_partner_uses_format_cnpj() -> None:
    """PJ partners (identificador_socio=1) get format_cnpj, not format_cpf."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)

    pipeline._raw_socios = pd.DataFrame([
        {
            "cnpj_basico": "00000000",
            "identificador_socio": "1",
            "nome_socio": "HOLDING ABC LTDA",
            "cpf_cnpj_socio": "12345678000199",
            "qualificacao_socio": "22",
            "data_entrada": "20200115",
            "pais": "0",
            "representante_legal": "",
            "nome_representante": "0",
            "qualificacao_representante": "0",
            "faixa_etaria": "0",
        },
        {
            "cnpj_basico": "00000000",
            "identificador_socio": "2",
            "nome_socio": "JOAO DA SILVA",
            "cpf_cnpj_socio": "12345678901",
            "qualificacao_socio": "22",
            "data_entrada": "20200115",
            "pais": "0",
            "representante_legal": "",
            "nome_representante": "0",
            "qualificacao_representante": "0",
            "faixa_etaria": "6",
        },
    ])

    partners, partial_partners, pf_rels, partial_rels, pj_rels = pipeline._transform_socios_rf(
        pipeline._raw_socios,
    )

    # Only PF partner returned as Person
    assert len(partners) == 1
    assert len(partial_partners) == 0
    assert partners[0]["cpf"] == "123.456.789-01"

    # PF relationship: Person→Company
    assert len(pf_rels) == 1
    assert pf_rels[0]["source_key"] == "123.456.789-01"
    assert len(partial_rels) == 0

    # PJ relationship: Company→Company with formatted CNPJ
    assert len(pj_rels) == 1
    assert pj_rels[0]["source_key"] == "12.345.678/0001-99"


def test_transform_stores_pj_relationships() -> None:
    """Full transform populates pj_relationships list."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)

    pipeline._raw_socios = pd.DataFrame([
        {
            "cnpj_basico": "00000000",
            "identificador_socio": "1",
            "nome_socio": "HOLDING XYZ",
            "cpf_cnpj_socio": "98765432000188",
            "qualificacao_socio": "22",
            "data_entrada": "20210101",
            "pais": "0",
            "representante_legal": "",
            "nome_representante": "0",
            "qualificacao_representante": "0",
            "faixa_etaria": "0",
        },
    ])

    pipeline.transform()

    assert len(pipeline.partners) == 0  # no PF partners
    assert len(pipeline.relationships) == 0  # no PF relationships
    assert len(pipeline.pj_relationships) == 1
    assert pipeline.pj_relationships[0]["source_key"] == "98.765.432/0001-88"


# --- DM-003: Date normalization ---


def test_transform_socios_rf_normalizes_data_entrada() -> None:
    """data_entrada in YYYYMMDD format is normalized to YYYY-MM-DD."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline._build_estab_lookup(pipeline._raw_estabelecimentos)

    pipeline._raw_socios = pd.DataFrame([
        {
            "cnpj_basico": "00000000",
            "identificador_socio": "2",
            "nome_socio": "TEST",
            "cpf_cnpj_socio": "12345678901",
            "qualificacao_socio": "22",
            "data_entrada": "20200115",
            "pais": "0",
            "representante_legal": "",
            "nome_representante": "0",
            "qualificacao_representante": "0",
            "faixa_etaria": "6",
        },
    ])

    _, _, pf_rels, _, _ = pipeline._transform_socios_rf(pipeline._raw_socios)
    assert pf_rels[0]["data_entrada"] == "2020-01-15"
