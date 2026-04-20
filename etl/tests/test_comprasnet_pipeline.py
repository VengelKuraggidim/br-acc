from __future__ import annotations

import io
import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.comprasnet import (
    ComprasnetPipeline,
    _stream_json_array,
)
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> ComprasnetPipeline:
    driver = MagicMock()
    return ComprasnetPipeline(driver, data_dir=str(FIXTURES))


def _extract_from_fixtures(pipeline: ComprasnetPipeline) -> None:
    """Load raw records from fixture JSON."""
    fixture_file = FIXTURES / "comprasnet_contratos.json"
    pipeline._raw_records = json.loads(fixture_file.read_text(encoding="utf-8"))


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "comprasnet"
    assert pipeline.source_id == "comprasnet"


def test_transform_produces_correct_contracts() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    # 5 records: 3 valid PJ, 1 PF (skipped), 1 zero-value (skipped) = 3
    assert len(pipeline.contracts) == 3


def test_transform_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    cnpjs = [c["cnpj"] for c in pipeline.contracts]
    assert "11.222.333/0001-81" in cnpjs
    assert "44.555.666/0001-99" in cnpjs
    assert "77.888.999/0001-00" in cnpjs


def test_transform_skips_pessoa_fisica() -> None:
    """Contracts with tipoPessoa=PF should be skipped."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    names = [c["razao_social"] for c in pipeline.contracts]
    assert "JOAO DA SILVA" not in names


def test_transform_skips_zero_value() -> None:
    """Contracts with zero value should be skipped."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    names = [c["razao_social"] for c in pipeline.contracts]
    assert "FORNECEDOR ZERADO LTDA" not in names


def test_transform_normalizes_names() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    # "Serviços Gerais ME" -> "SERVICOS GERAIS ME" (normalized)
    names = [c["razao_social"] for c in pipeline.contracts]
    assert any("SERVICOS GERAIS" in n for n in names)


def test_transform_extracts_contracting_org() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    orgs = {c["contracting_org"] for c in pipeline.contracts}
    assert "MINISTERIO DA SAUDE" in orgs
    assert "MINISTERIO DA EDUCACAO" in orgs


def test_transform_sets_source() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    for c in pipeline.contracts:
        assert c["source"] == "comprasnet"


def test_transform_contract_ids_are_unique() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    ids = [c["contract_id"] for c in pipeline.contracts]
    assert len(set(ids)) == len(ids)


def test_transform_extracts_values() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    values = sorted(c["value"] for c in pipeline.contracts)
    assert 150000.00 in values
    assert 480000.00 in values
    assert 3200000.50 in values


def test_transform_extracts_bid_reference() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    bid_refs = {c["bid_id"] for c in pipeline.contracts}
    assert "11222333000181-1-000050/2023" in bid_refs
    assert "44555666000199-1-000010/2024" in bid_refs
    assert "77888999000100-1-000020/2024" in bid_refs


def test_transform_extracts_dates() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    dates = {c["date"] for c in pipeline.contracts}
    assert "2024-01-15" in dates
    assert "2024-03-01" in dates
    assert "2024-02-15" in dates


def test_transform_sanitizes_absurd_future_date() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    pipeline._raw_records.append({
        "numeroControlePNCP": "00600371000104-2-000035/2024",
        "niFornecedor": "00600371000104",
        "tipoPessoa": "PJ",
        "nomeRazaoSocialFornecedor": "FORNECEDOR FUTURO LTDA",
        "objetoContrato": "OBJETO TESTE",
        "valorGlobal": 1000.0,
        "dataAssinatura": "2102-09-24",
        "dataVigenciaFim": "2103-01-01",
        "orgaoEntidade": {
            "cnpj": "00394445000166",
            "razaoSocial": "CAMARA MUNICIPAL DE CORDEIROPOLIS",
        },
        "tipoContrato": {"id": 1, "nome": "Empenho"},
        "anoContrato": 2024,
        "sequencialContrato": 35,
    })

    pipeline.transform()
    target = next(
        c for c in pipeline.contracts if c["contract_id"] == "00600371000104-2-000035/2024"
    )
    assert target["date"] == ""
    assert target["date_end"] == ""


def test_transform_limit() -> None:
    pipeline = _make_pipeline()
    pipeline.limit = 2
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.contracts) == 2


def test_transform_caps_absurd_value() -> None:
    """Contracts with values above R$ 10B (data entry errors) get value=None."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Inject a record with a garbage R$ 50B value
    pipeline._raw_records.append({
        "numeroControlePNCP": "88777666000100-2-000099/2024",
        "niFornecedor": "88777666000100",
        "tipoPessoa": "PJ",
        "nomeRazaoSocialFornecedor": "EMPRESA ABSURDA LTDA",
        "objetoContrato": "MUDANCA DE 2 PESSOAS",
        "valorGlobal": 50_000_000_000.0,
        "dataAssinatura": "2024-06-01",
        "dataVigenciaFim": "2024-12-31",
        "orgaoEntidade": {
            "cnpj": "00394445000166",
            "razaoSocial": "MINISTERIO DA SAUDE",
        },
        "tipoContrato": {"id": 1, "nome": "Contrato"},
        "anoContrato": 2024,
        "sequencialContrato": 99,
    })

    pipeline.transform()

    absurd = next(
        c for c in pipeline.contracts if c["razao_social"] == "EMPRESA ABSURDA LTDA"
    )
    assert absurd["value"] is None


def test_load_calls_batch_loader() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()
    pipeline.load()

    driver = pipeline.driver
    session = mock_session(driver)
    # Should have called session.run for Contract nodes, Company nodes, VENCEU and REFERENTE_A rels
    assert session.run.call_count >= 4


def test_provenance_stamped_on_contract_nodes() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert pipeline.contract_nodes
    for node in pipeline.contract_nodes:
        assert node["source_id"] == "comprasnet"
        assert node["source_record_id"] == node["contract_id"]
        assert node["source_url"].startswith("http")
        assert node["ingested_at"].startswith("20")
        assert node["run_id"].startswith("comprasnet_")


def test_provenance_stamped_on_company_nodes() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert pipeline.company_nodes
    for node in pipeline.company_nodes:
        assert node["source_id"] == "comprasnet"
        assert node["source_record_id"] == node["cnpj"]
        assert node["source_url"].startswith("http")
        assert node["run_id"].startswith("comprasnet_")


def test_provenance_stamped_on_venceu_rels() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert pipeline.venceu_rels
    for rel in pipeline.venceu_rels:
        assert rel["source_id"] == "comprasnet"
        assert rel["source_record_id"] == rel["target_key"]  # contract_id
        assert rel["source_url"].startswith("http")
        assert rel["run_id"].startswith("comprasnet_")


def test_provenance_stamped_on_referente_a_rels() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert pipeline.referente_a_rels
    for rel in pipeline.referente_a_rels:
        assert rel["source_id"] == "comprasnet"
        assert rel["source_record_id"] == rel["source_key"]  # contract_id
        assert rel["source_url"].startswith("http")


def test_contract_nodes_count_matches_contracts() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.contract_nodes) == len(pipeline.contracts)
    assert len(pipeline.venceu_rels) == len(pipeline.contracts)


def test_stream_json_array_empty() -> None:
    assert list(_stream_json_array(io.StringIO("[]"))) == []


def test_stream_json_array_pretty_printed() -> None:
    src = '[\n  {"id": "a", "n": {"k": [1, 2]}},\n  {"id": "b"}\n]\n'
    assert list(_stream_json_array(io.StringIO(src))) == [
        {"id": "a", "n": {"k": [1, 2]}},
        {"id": "b"},
    ]


def test_stream_json_array_consolidated_format() -> None:
    """Matches the shape written by scripts/download_comprasnet.py."""
    src = '[{"id": 1},\n{"id": 2},\n{"id": 3}]\n'
    assert list(_stream_json_array(io.StringIO(src))) == [
        {"id": 1}, {"id": 2}, {"id": 3},
    ]


def test_stream_json_array_small_chunk_size() -> None:
    """Decoder must reassemble objects split across chunk boundaries."""
    src = '[{"aaaaaaaaaa": 1, "bbbbbbbbbb": 2}, {"cccccccccc": 3}]'
    assert list(_stream_json_array(io.StringIO(src), chunk_size=4)) == [
        {"aaaaaaaaaa": 1, "bbbbbbbbbb": 2},
        {"cccccccccc": 3},
    ]


def test_stream_json_array_truncated_raises() -> None:
    with pytest.raises(json.JSONDecodeError):
        list(_stream_json_array(io.StringIO('[{"id": 1')))


def test_run_loops_per_year_file(tmp_path: Path) -> None:
    """run() must process each *_contratos.json independently."""
    comprasnet_dir = tmp_path / "comprasnet"
    comprasnet_dir.mkdir()
    for year in (2023, 2024, 2025):
        shutil.copyfile(
            FIXTURES / "comprasnet_contratos.json",
            comprasnet_dir / f"{year}_contratos.json",
        )

    driver = MagicMock()
    pipeline = ComprasnetPipeline(driver, data_dir=str(tmp_path))
    pipeline.run()

    # Each fixture has 5 raw records; three years -> 15 records ingested.
    assert pipeline.rows_in == 15
    # After run completes state is cleared; rows_loaded aggregates per-year
    # Contract-node counts (3 per year * 3 years = 9).
    assert pipeline.rows_loaded == 9
    # State reset: no leftover working sets.
    assert pipeline.contracts == []
    assert pipeline.contract_nodes == []

    session = mock_session(driver)
    # 4 load_* calls per year × 3 years = 12 session.run calls minimum
    # (plus 2 _upsert_ingestion_run calls at start/end).
    assert session.run.call_count >= 12


def test_run_respects_global_limit(tmp_path: Path) -> None:
    """limit caps total contracts across years, not per year."""
    comprasnet_dir = tmp_path / "comprasnet"
    comprasnet_dir.mkdir()
    for year in (2023, 2024, 2025):
        shutil.copyfile(
            FIXTURES / "comprasnet_contratos.json",
            comprasnet_dir / f"{year}_contratos.json",
        )

    driver = MagicMock()
    pipeline = ComprasnetPipeline(driver, data_dir=str(tmp_path), limit=4)
    pipeline.run()

    # limit=4, fixture yields 3 valid contracts per year:
    # year 1 loads 3, year 2 loads 1, year 3 short-circuits.
    assert pipeline.rows_loaded == 4
    # original limit restored after run.
    assert pipeline.limit == 4


def test_run_no_input_files_is_noop(tmp_path: Path) -> None:
    (tmp_path / "comprasnet").mkdir()
    driver = MagicMock()
    pipeline = ComprasnetPipeline(driver, data_dir=str(tmp_path))
    pipeline.run()

    assert pipeline.rows_in == 0
    assert pipeline.rows_loaded == 0
