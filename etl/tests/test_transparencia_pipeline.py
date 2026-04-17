from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.transparencia import (
    TransparenciaPipeline,
    _extract_cpf_middle6,
    _make_office_id,
    _make_servidor_id,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TransparenciaPipeline:
    driver = MagicMock()
    pipeline = TransparenciaPipeline(driver, data_dir=str(FIXTURES))
    # Fixtures are at fixtures/transparencia_*.csv but pipeline expects
    # {data_dir}/transparencia/*.csv — symlink by overriding extraction
    return pipeline


def _extract_from_fixtures(pipeline: TransparenciaPipeline) -> None:
    """Extract directly from fixture files instead of subdirectory."""
    import pandas as pd

    pipeline._raw_contratos = pd.read_csv(
        FIXTURES / "transparencia_contratos.csv",
        dtype=str,
        keep_default_na=False,
    )
    pipeline._raw_servidores = pd.read_csv(
        FIXTURES / "transparencia_servidores.csv",
        dtype=str,
        keep_default_na=False,
    )
    pipeline._raw_emendas = pd.read_csv(
        FIXTURES / "transparencia_emendas.csv",
        dtype=str,
        keep_default_na=False,
    )


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "transparencia"
    assert pipeline.source_id == "portal_transparencia"


def test_transform_produces_correct_contracts() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.contracts) == 3
    contract = pipeline.contracts[0]
    assert contract["contracting_org"] == "MINISTERIO DA SAUDE"
    assert contract["object"] == "SERVICO DE LIMPEZA"
    assert contract["cnpj"] == "11.222.333/0001-81"
    assert contract["date"] == "2024-01-15"


def test_transform_filters_sigiloso_contracts() -> None:
    """Contracts with CNPJ=-11 (classified) should be filtered out."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Add a sigiloso row to the raw data
    sigiloso = pd.DataFrame([{
        "cnpj_contratada": "-11",
        "razao_social": "Sigiloso",
        "objeto": "Classificado",
        "valor": "100.000,00",
        "orgao_contratante": "Policia Federal",
        "data_inicio": "2024-01-01",
    }])
    pipeline._raw_contratos = pd.concat(
        [pipeline._raw_contratos, sigiloso], ignore_index=True,
    )

    pipeline.transform()
    cnpjs = [c["cnpj"] for c in pipeline.contracts]
    assert all(c != "-11" for c in cnpjs)
    # Original 3 contracts still present
    assert len(pipeline.contracts) == 3


def test_transform_parses_monetary_values() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert pipeline.contracts[0]["value"] == 1_500_000.00
    assert pipeline.contracts[1]["value"] == 3_200_000.50
    assert pipeline.offices[0]["salary"] == 15_500.00
    assert pipeline.offices[1]["salary"] == 22_300.50


def test_transform_deduplicates_contracts() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    # 3 rows, all unique contract_ids
    assert len(pipeline.contracts) == 3
    ids = [c["contract_id"] for c in pipeline.contracts]
    assert len(set(ids)) == 3


def test_transform_normalizes_server_names() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.offices) == 2
    assert pipeline.offices[0]["name"] == "MARIA DA SILVA SANTOS"
    assert "servidor_id" in pipeline.offices[0]
    assert "office_id" in pipeline.offices[0]


def test_transform_creates_amendment_nodes() -> None:
    """Emendas should produce Amendment nodes, not link to Contract."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.amendments) == 2
    amendment = pipeline.amendments[0]
    assert "amendment_id" in amendment
    assert "author_key" in amendment
    assert "object" in amendment
    assert "value" in amendment


def test_transform_amendment_ids_are_unique() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    ids = [a["amendment_id"] for a in pipeline.amendments]
    assert len(set(ids)) == len(ids)


def test_transform_skips_empty_cnpj_contracts() -> None:
    """Contracts with empty or non-digit CNPJ should be filtered out."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Add a row with empty CNPJ (only non-digit chars)
    empty_cnpj = pd.DataFrame([{
        "cnpj_contratada": "",
        "razao_social": "Fantasma Ltda",
        "objeto": "Servico Fantasma",
        "valor": "50.000,00",
        "orgao_contratante": "Orgao Inexistente",
        "data_inicio": "2024-01-15",
    }])
    pipeline._raw_contratos = pd.concat(
        [pipeline._raw_contratos, empty_cnpj], ignore_index=True,
    )

    pipeline.transform()
    # No malformed contract_ids (starting with underscore)
    assert all(not c["contract_id"].startswith("_") for c in pipeline.contracts)
    # Original 3 contracts still present
    assert len(pipeline.contracts) == 3


def test_transform_skips_short_cnpj_contracts() -> None:
    """Contracts with CNPJ shorter than 14 digits should be filtered out."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    short_cnpj = pd.DataFrame([{
        "cnpj_contratada": "11",
        "razao_social": "Fantasma Curta Ltda",
        "objeto": "Servico Invalido",
        "valor": "10.000,00",
        "orgao_contratante": "Orgao Teste",
        "data_inicio": "2024-02-01",
    }])
    pipeline._raw_contratos = pd.concat(
        [pipeline._raw_contratos, short_cnpj], ignore_index=True,
    )

    pipeline.transform()
    # Short CNPJ row should be rejected — only original 3 remain
    assert len(pipeline.contracts) == 3
    # No CNPJ with fewer than 14 formatted chars
    for c in pipeline.contracts:
        assert len(c["cnpj"]) == 18  # XX.XXX.XXX/XXXX-XX


def test_transform_caps_absurd_contract_value() -> None:
    """Contracts with values above R$ 10B (data entry errors) get value=None."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Add a row with a garbage R$ 50B value
    absurd = pd.DataFrame([{
        "cnpj_contratada": "88777666000100",
        "razao_social": "Empresa Absurda Ltda",
        "objeto": "Mudanca de 2 pessoas",
        "valor": "50.000.000.000,00",
        "orgao_contratante": "Prefeitura Municipal",
        "data_inicio": "2024-07-01",
    }])
    pipeline._raw_contratos = pd.concat(
        [pipeline._raw_contratos, absurd], ignore_index=True,
    )

    pipeline.transform()

    absurd_contract = next(
        c for c in pipeline.contracts if c["razao_social"] == "EMPRESA ABSURDA LTDA"
    )
    assert absurd_contract["value"] is None


# ── cpf_partial extraction tests ───────────────────────────────────


def test_extract_cpf_middle6_masked_cpf() -> None:
    """LGPD-masked CPF (***.ABC.DEF-**) should return 6 middle digits."""
    assert _extract_cpf_middle6("***.017.623-**") == "017623"
    assert _extract_cpf_middle6("***.123.456-**") == "123456"


def test_extract_cpf_middle6_full_cpf_returns_none() -> None:
    """Full 11-digit CPFs should return None (not partial)."""
    assert _extract_cpf_middle6("12345678901") is None
    assert _extract_cpf_middle6("123.456.789-01") is None


def test_extract_cpf_middle6_blank_returns_none() -> None:
    """Blank or empty CPFs should return None."""
    assert _extract_cpf_middle6("") is None
    assert _extract_cpf_middle6("***.***.***-**") is None


def test_extract_cpf_middle6_malformed_returns_none() -> None:
    """Malformed CPFs with wrong digit count should return None."""
    assert _extract_cpf_middle6("***.01-**") is None
    assert _extract_cpf_middle6("12345") is None
    assert _extract_cpf_middle6("1234567") is None


def test_transform_hash_ids_avoid_cpf_collisions() -> None:
    """Different people sharing same 6-digit partial CPF get unique servidor_ids."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Two different people with same partial CPF
    pipeline._raw_servidores = pd.DataFrame([
        {
            "cpf": "***.017.623-**",
            "nome": "Ana Souza",
            "orgao": "STF",
            "remuneracao": "30.000,00",
        },
        {
            "cpf": "***.017.623-**",
            "nome": "Carlos Lima",
            "orgao": "STF",
            "remuneracao": "25.000,00",
        },
    ])

    pipeline.transform()

    assert len(pipeline.offices) == 2
    ids = {o["servidor_id"] for o in pipeline.offices}
    assert len(ids) == 2  # Different people → different servidor_ids

    office_ids = {o["office_id"] for o in pipeline.offices}
    assert len(office_ids) == 2  # Different people → different office_ids


def test_transform_adds_cpf_partial_for_masked_cpf() -> None:
    """Masked CPFs in servidores should produce cpf_partial in transform output."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Replace servidores with masked CPF rows
    pipeline._raw_servidores = pd.DataFrame([
        {
            "cpf": "***.017.623-**",
            "nome": "Jose Dias Toffoli",
            "orgao": "STF",
            "remuneracao": "39.293,32",
        },
        {
            "cpf": "12345678901",
            "nome": "Maria da Silva Santos",
            "orgao": "Ministerio da Saude",
            "remuneracao": "15.500,00",
        },
        {
            "cpf": "",
            "nome": "Agente Sigiloso",
            "orgao": "Policia Federal",
            "remuneracao": "12.000,00",
        },
    ])

    pipeline.transform()

    # Masked CPF → cpf_partial extracted
    toffoli = next(o for o in pipeline.offices if o["name"] == "JOSE DIAS TOFFOLI")
    assert toffoli["cpf_partial"] == "017623"
    assert toffoli["servidor_id"] == _make_servidor_id("017623", "JOSE DIAS TOFFOLI")
    assert toffoli["office_id"] == _make_office_id("017623", "JOSE DIAS TOFFOLI", "STF")

    # Full CPF → cpf_partial is None
    maria = next(o for o in pipeline.offices if o["name"] == "MARIA DA SILVA SANTOS")
    assert maria["cpf_partial"] is None
    assert maria["servidor_id"] == _make_servidor_id(None, "MARIA DA SILVA SANTOS")

    # Blank CPF → cpf_partial is None
    agente = next(o for o in pipeline.offices if o["name"] == "AGENTE SIGILOSO")
    assert agente["cpf_partial"] is None
    assert agente["servidor_id"] == _make_servidor_id(None, "AGENTE SIGILOSO")
