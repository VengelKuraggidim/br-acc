from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.tse import TSEPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TSEPipeline:
    driver = MagicMock()
    pipeline = TSEPipeline(driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _extract_from_fixtures(pipeline: TSEPipeline) -> None:
    """Extract from test fixtures instead of data_dir/tse/."""
    import pandas as pd

    pipeline._raw_candidatos = pd.read_csv(
        FIXTURES / "tse_candidatos.csv", encoding="latin-1", dtype=str
    )
    pipeline._raw_doacoes = pd.read_csv(
        FIXTURES / "tse_doacoes.csv", encoding="latin-1", dtype=str
    )


def test_pipeline_metadata() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "tse"
    assert pipeline.source_id == "tribunal_superior_eleitoral"


def test_transform_produces_candidates_keyed_by_sq() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    # 3 rows but 2 unique sq_candidato values (100001 appears twice)
    assert len(pipeline.candidates) == 2
    sqs = {c["sq_candidato"] for c in pipeline.candidates}
    assert "100001" in sqs
    assert "100002" in sqs


def test_transform_keeps_real_cpf_drops_masked() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    by_sq = {c["sq_candidato"]: c for c in pipeline.candidates}
    # 100001 has real CPF in first row (12345678901) — should keep it
    assert by_sq["100001"].get("cpf") == "123.456.789-01"
    # 100002 has real CPF
    assert by_sq["100002"].get("cpf") == "987.654.321-00"


def test_transform_normalizes_names() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    names = {c["name"] for c in pipeline.candidates}
    assert "JOAO DA SILVA" in names
    assert "MARIA JOSE SANTOS" in names


def test_transform_stores_partido() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    partidos = {c["partido"] for c in pipeline.candidates}
    assert "PL" in partidos
    assert "PT" in partidos


def test_transform_creates_elections() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.elections) == 3
    years = {e["year"] for e in pipeline.elections}
    assert years == {2022, 2024}


def test_transform_elections_use_sq_candidato() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    for e in pipeline.elections:
        assert "candidate_sq" in e
        assert e["candidate_sq"] in {"100001", "100002"}


def test_transform_parses_donation_values() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.donations) == 3
    valores = sorted(d["valor"] for d in pipeline.donations)
    assert valores[0] == 200.00
    assert valores[1] == 1500.50
    assert valores[2] == 50000.00


def test_transform_donations_link_via_sq() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    for d in pipeline.donations:
        assert "candidate_sq" in d
        assert d["candidate_sq"] in {"100001", "100002"}


def test_transform_identifies_company_donors() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    company_donations = [d for d in pipeline.donations if d["donor_is_company"]]
    person_donations = [d for d in pipeline.donations if not d["donor_is_company"]]

    assert len(company_donations) == 1
    assert company_donations[0]["donor_name"] == "EMPRESA ABC LTDA"
    # Company donor CNPJ must be formatted (not raw digits) for cross-source MERGE
    assert company_donations[0]["donor_doc"] == "12.345.678/0001-99"
    assert len(person_donations) == 2


def test_load_doou_merge_preserves_multi_year_donations() -> None:
    """DOOU MERGE key includes year — multiple donations to same candidate across years survive."""
    import pandas as pd

    pipeline = _make_pipeline()
    pipeline._raw_candidatos = pd.read_csv(
        FIXTURES / "tse_candidatos.csv", encoding="latin-1", dtype=str,
    )
    # Create two donations from same person to same candidate in different years
    pipeline._raw_doacoes = pd.DataFrame([
        {
            "sq_candidato": "100001",
            "cpf_cnpj_doador": "111.222.333-44",
            "nome_doador": "Pedro Oliveira",
            "valor": "1500.50",
            "ano": "2022",
        },
        {
            "sq_candidato": "100001",
            "cpf_cnpj_doador": "111.222.333-44",
            "nome_doador": "Pedro Oliveira",
            "valor": "3000.00",
            "ano": "2024",
        },
    ])
    pipeline.transform()
    pipeline.load()

    # Find the person DOOU query
    session_mock = mock_session(pipeline)
    run_calls = session_mock.run.call_args_list

    doou_calls = [
        call for call in run_calls
        if "DOOU" in str(call) and "Person {cpf:" in str(call)
    ]
    assert len(doou_calls) >= 1

    # The query should include {year: row.year} in the MERGE key
    query_str = str(doou_calls[0][0][0])
    assert "{year: row.year}" in query_str, (
        f"DOOU MERGE should include year in key to avoid collapsing. Got: {query_str}"
    )

    # Both donations should be in the data rows
    call = doou_calls[0]
    call_kwargs = call[1] if call[1] else {}
    rows = call_kwargs.get("rows") or call[0][1]["rows"]
    assert len(rows) == 2, f"Expected 2 donation rows, got {len(rows)}"
    years = {r["year"] for r in rows}
    assert years == {2022, 2024}


def test_load_doou_person_rel_carries_provenance() -> None:
    """DOOU rel rows (pessoa→candidato) carregam os 5 campos de proveniência.

    Regressão: sem isso, a API não achava chip de fonte nos doadores e só
    rendezia provenance via fallback no nó target (cobertura parcial).
    """
    import pandas as pd

    pipeline = _make_pipeline()
    pipeline._raw_candidatos = pd.read_csv(
        FIXTURES / "tse_candidatos.csv", encoding="latin-1", dtype=str,
    )
    pipeline._raw_doacoes = pd.DataFrame([
        {
            "sq_candidato": "100001",
            "cpf_cnpj_doador": "111.222.333-44",
            "nome_doador": "Pedro Oliveira",
            "valor": "1500.50",
            "ano": "2022",
        },
    ])
    pipeline.transform()
    pipeline.load()

    session_mock = mock_session(pipeline)
    run_calls = session_mock.run.call_args_list
    doou_person_calls = [
        call for call in run_calls
        if "DOOU" in str(call) and "Person {cpf:" in str(call)
    ]
    assert len(doou_person_calls) >= 1

    call = doou_person_calls[0]
    query_str = str(call[0][0])
    for field in ("source_id", "source_record_id", "source_url", "ingested_at", "run_id"):
        assert f"r.{field} = row.{field}" in query_str, (
            f"Query Cypher não seta r.{field}: {query_str}"
        )

    call_kwargs = call[1] if call[1] else {}
    rows = call_kwargs.get("rows") or call[0][1]["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["source_id"] == "tribunal_superior_eleitoral"
    assert row["source_url"] == "https://dadosabertos.tse.jus.br/"
    assert row["source_record_id"].startswith("2022:")
    assert row["ingested_at"]
    assert row["run_id"].startswith("tribunal_superior_eleitoral_")


def test_load_doou_company_rel_carries_provenance() -> None:
    """Análogo ao teste de pessoa, mas para doador empresa."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()
    pipeline.load()

    session_mock = mock_session(pipeline)
    run_calls = session_mock.run.call_args_list
    doou_company_calls = [
        call for call in run_calls
        if "DOOU" in str(call) and "Company {cnpj:" in str(call)
    ]
    assert len(doou_company_calls) >= 1

    call = doou_company_calls[0]
    call_kwargs = call[1] if call[1] else {}
    rows = call_kwargs.get("rows") or call[0][1]["rows"]
    for row in rows:
        for field in ("source_id", "source_record_id", "source_url", "ingested_at", "run_id"):
            assert field in row, f"Row sem {field}: {row}"


def test_load_company_donors_include_razao_social() -> None:
    """Company donor nodes must include razao_social for API compatibility."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()
    pipeline.load()

    # Collect all load_nodes calls from the mock driver
    session_mock = mock_session(pipeline)
    run_calls = session_mock.run.call_args_list

    # Find the MERGE (n:Company ...) call — its rows should include razao_social
    company_merge_calls = [
        call for call in run_calls
        if "MERGE (n:Company" in str(call)
    ]
    assert len(company_merge_calls) >= 1, "Expected at least one Company MERGE call"

    # Extract the rows from the first Company MERGE call
    call = company_merge_calls[0]
    call_kwargs = call[1] if call[1] else {}
    company_rows = (
        call_kwargs["rows"] if "rows" in call_kwargs else call[0][1]["rows"]
    )
    for row in company_rows:
        assert "razao_social" in row, f"Company donor row missing razao_social: {row}"
        assert row["razao_social"] == row["name"], (
            f"razao_social should match name for TSE donors: {row}"
        )
