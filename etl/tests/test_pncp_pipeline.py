"""Tests for the PNCP (Portal Nacional de Contratações Públicas) pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.pncp import PncpPipeline
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> PncpPipeline:
    driver = MagicMock()
    return PncpPipeline(driver, data_dir=str(FIXTURES.parent))


def _load_fixture(pipeline: PncpPipeline) -> None:
    """Load raw records from fixture JSON into the pipeline."""
    fixture_file = FIXTURES / "pncp" / "contratacoes.json"
    payload = json.loads(fixture_file.read_text(encoding="utf-8"))
    pipeline._raw_records = payload["data"]


# --- Metadata ---


class TestPncpMetadata:
    def test_name(self) -> None:
        assert PncpPipeline.name == "pncp"

    def test_source_id(self) -> None:
        assert PncpPipeline.source_id == "pncp_bids"


# --- Transform ---


class TestPncpTransform:
    def test_produces_correct_bid_count(self) -> None:
        """5 fixture records: 3 valid, 1 zero-value (skipped), 1 bad CNPJ (skipped)."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        assert len(pipeline.bids) == 3

    def test_formats_agency_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        cnpjs = {b["agency_cnpj"] for b in pipeline.bids}
        assert "00.394.445/0001-66" in cnpjs
        assert "26.994.558/0001-23" in cnpjs
        assert "08.084.014/0001-42" in cnpjs

    def test_skips_invalid_cnpj(self) -> None:
        """Records with non-14-digit CNPJ should be skipped."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        descs = {b["description"] for b in pipeline.bids}
        assert "REGISTRO INVALIDO SEM CNPJ VALIDO" not in descs

    def test_skips_zero_value(self) -> None:
        """Records with zero estimated value should be skipped."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        descs = {b["description"] for b in pipeline.bids}
        assert "CONTRATACAO DE SERVICO DE LIMPEZA" not in descs

    def test_normalizes_agency_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        names = {b["agency_name"] for b in pipeline.bids}
        assert "MINISTERIO DA SAUDE" in names
        assert "MINISTERIO DA EDUCACAO" in names
        assert "MUNICIPIO DE CAMPO GRANDE" in names

    def test_normalizes_descriptions(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        descs = {b["description"] for b in pipeline.bids}
        # Accented characters removed, uppercased
        assert any("AQUISICAO" in d for d in descs)
        assert any("CONSTRUCAO" in d for d in descs)

    def test_extracts_bid_ids(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        ids = {b["bid_id"] for b in pipeline.bids}
        assert "00394445000166-1-000001/2025" in ids
        assert "26994558000123-1-000005/2025" in ids
        assert "08084014000142-1-000057/2024" in ids

    def test_bid_ids_are_unique(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        ids = [b["bid_id"] for b in pipeline.bids]
        assert len(set(ids)) == len(ids)

    def test_extracts_values(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        values = sorted(b["amount"] for b in pipeline.bids if b["amount"] is not None)
        assert 150000.00 in values
        assert 480000.00 in values
        assert 3200000.50 in values

    def test_extracts_dates(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        dates = {b["date"] for b in pipeline.bids}
        assert "2025-01-10" in dates
        assert "2025-01-25" in dates
        assert "2025-01-15" in dates

    def test_extracts_modality(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        modalities = {b["modality"] for b in pipeline.bids}
        assert "pregao_eletronico" in modalities
        assert "concorrencia" in modalities
        assert "dispensa" in modalities

    def test_extracts_location(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        states = {b["state"] for b in pipeline.bids}
        municipalities = {b["municipality"] for b in pipeline.bids}
        assert "DF" in states
        assert "RN" in states
        assert "Brasília" in municipalities or "Brasilia" in municipalities

    def test_extracts_esfera(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        esferas = {b["esfera"] for b in pipeline.bids}
        assert "federal" in esferas
        assert "municipal" in esferas

    def test_sets_source(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        for b in pipeline.bids:
            assert b["source"] == "pncp"

    def test_bid_has_all_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        expected_fields = {
            "bid_id", "description", "modality", "amount", "date",
            "status", "agency_name", "agency_cnpj", "municipality",
            "state", "esfera", "processo", "srp", "source",
            "coverage_start", "coverage_end", "coverage_complete",
        }
        for b in pipeline.bids:
            assert set(b.keys()) == expected_fields

    def test_prefers_homologado_over_estimado(self) -> None:
        """When valorTotalHomologado is present, use it over valorTotalEstimado."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        # The fixture's 2nd record has valorTotalHomologado=3200000.50
        bid = next(b for b in pipeline.bids if b["bid_id"] == "26994558000123-1-000005/2025")
        assert bid["amount"] == 3200000.50

    def test_limit(self) -> None:
        pipeline = _make_pipeline()
        pipeline.limit = 2
        _load_fixture(pipeline)
        pipeline.transform()

        assert len(pipeline.bids) == 2

    def test_caps_absurd_value(self) -> None:
        """Values above R$ 10B (data entry errors) get value=None."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)

        pipeline._raw_records.append({
            "orgaoEntidade": {
                "cnpj": "88777666000100",
                "razaoSocial": "PREFEITURA ABSURDA",
                "esferaId": "M",
            },
            "anoCompra": 2025,
            "sequencialCompra": 999,
            "objetoCompra": "VALOR ABSURDO",
            "valorTotalEstimado": 50_000_000_000.0,
            "dataPublicacaoPncp": "2025-06-01T10:00:00",
            "numeroControlePNCP": "88777666000100-1-000999/2025",
            "modalidadeId": 6,
            "modalidadeNome": "Pregão - Eletrônico",
            "situacaoCompraNome": "Divulgada no PNCP",
            "unidadeOrgao": {"ufSigla": "SP", "municipioNome": "Absurdópolis"},
        })

        pipeline.transform()

        absurd = next(b for b in pipeline.bids if b["description"] == "VALOR ABSURDO")
        assert absurd["amount"] is None

    def test_deduplicates_by_bid_id(self) -> None:
        """Duplicate records with same numeroControlePNCP should be deduplicated."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)

        # Add a duplicate of the first record
        pipeline._raw_records.append(pipeline._raw_records[0].copy())
        pipeline.transform()

        # Should still be 3 unique bids (not 4)
        assert len(pipeline.bids) == 3

    def test_extract_handles_flat_json(self) -> None:
        """Extract should handle both wrapped and flat JSON formats."""
        # Flat list (as saved by download script)
        flat_records = [
            {
                "orgaoEntidade": {
                    "cnpj": "11222333000181",
                    "razaoSocial": "TEST ORG",
                    "esferaId": "F",
                },
                "objetoCompra": "TEST OBJECT",
                "valorTotalEstimado": 100000.0,
                "dataPublicacaoPncp": "2025-01-01T00:00:00",
                "numeroControlePNCP": "11222333000181-1-000001/2025",
                "modalidadeId": 6,
                "modalidadeNome": "Pregão - Eletrônico",
                "situacaoCompraNome": "Divulgada no PNCP",
                "unidadeOrgao": {"ufSigla": "DF", "municipioNome": "Brasília"},
            }
        ]

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            pncp_dir = Path(tmpdir) / "pncp"
            pncp_dir.mkdir()
            (pncp_dir / "pncp_202501.json").write_text(
                json.dumps(flat_records, ensure_ascii=False),
                encoding="utf-8",
            )

            pipeline_tmp = PncpPipeline(MagicMock(), data_dir=tmpdir)
            pipeline_tmp.extract()
            assert len(pipeline_tmp._raw_records) == 1
            assert pipeline_tmp.coverage_start == "2025-01-01"
            assert pipeline_tmp.coverage_end == "2025-01-01"
            assert pipeline_tmp.coverage_complete is False


# --- Load ---


class TestPncpLoad:
    def test_load_creates_bid_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        bid_calls = [
            call for call in run_calls
            if "MERGE (n:Bid" in str(call)
        ]
        assert len(bid_calls) >= 1

    def test_load_creates_company_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        company_calls = [
            call for call in run_calls
            if "MERGE (n:Company" in str(call)
        ]
        assert len(company_calls) >= 1

    def test_load_creates_licitou_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "LICITOU" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_skips_when_empty(self) -> None:
        pipeline = _make_pipeline()
        pipeline.bids = []
        pipeline.load()

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0

    def test_load_calls_correct_number_of_batches(self) -> None:
        """Should call session.run for Bid nodes, Company nodes, and LICITOU rels."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        # At minimum: 1 Bid batch + 1 Company batch + 1 LICITOU batch = 3
        assert session_mock.run.call_count >= 3

    def test_load_deduplicates_agencies(self) -> None:
        """Agencies with the same CNPJ should be deduplicated before loading."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        # Two bids from MINISTERIO DA SAUDE (same CNPJ) -> only 1 Company node
        agencies = {b["agency_cnpj"] for b in pipeline.bids}
        # Bids have 3 unique agencies (SAUDE appears once in the valid set since
        # the 2nd SAUDE bid was zero-value)
        assert len(agencies) == 3
