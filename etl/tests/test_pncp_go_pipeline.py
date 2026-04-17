"""Tests for the PNCP GO (Goias) procurement pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.pncp_go import PncpGoPipeline, _make_procurement_id

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> PncpGoPipeline:
    driver = MagicMock()
    return PncpGoPipeline(driver, data_dir=str(FIXTURES.parent))


def _load_fixture(pipeline: PncpGoPipeline) -> None:
    """Load raw records from fixture JSON into the pipeline."""
    fixture_file = FIXTURES / "pncp_go" / "contratacoes.json"
    payload = json.loads(fixture_file.read_text(encoding="utf-8"))
    pipeline._raw_records = payload["data"]


# --- Metadata ---


class TestMetadata:
    def test_name(self) -> None:
        assert PncpGoPipeline.name == "pncp_go"

    def test_source_id(self) -> None:
        assert PncpGoPipeline.source_id == "pncp_go"


# --- Transform ---


class TestTransform:
    def test_produces_correct_procurement_count(self) -> None:
        """2 fixture records, both valid."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        assert len(pipeline.procurements) == 2

    def test_formats_agency_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        cnpjs = {p["cnpj_agency"] for p in pipeline.procurements}
        assert "01.409.580/0001-38" in cnpjs
        assert "01.005.580/0001-70" in cnpjs

    def test_normalizes_agency_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        names = {p["agency_name"] for p in pipeline.procurements}
        assert "GOVERNO DO ESTADO DE GOIAS" in names
        assert "PREFEITURA MUNICIPAL DE ANAPOLIS" in names

    def test_normalizes_descriptions(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        descs = {p["object"] for p in pipeline.procurements}
        assert any("AQUISICAO" in d for d in descs)
        assert any("PAVIMENTACAO" in d for d in descs)

    def test_creates_stable_procurement_ids(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        ids = [p["procurement_id"] for p in pipeline.procurements]
        assert len(ids) == 2
        # IDs are deterministic hashes
        expected_id_1 = _make_procurement_id("01409580000138", 2025, 12)
        expected_id_2 = _make_procurement_id("01005580000170", 2025, 3)
        id_set = set(ids)
        assert expected_id_1 in id_set
        assert expected_id_2 in id_set

    def test_procurement_ids_are_unique(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        ids = [p["procurement_id"] for p in pipeline.procurements]
        assert len(set(ids)) == len(ids)

    def test_extracts_values(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        values = sorted(
            p["amount_estimated"]
            for p in pipeline.procurements
            if p["amount_estimated"] is not None
        )
        assert 750000.00 in values
        assert 2500000.00 in values

    def test_prefers_homologado_over_estimado(self) -> None:
        """When valorTotalHomologado is present, use it over valorTotalEstimado."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        anapolis_id = _make_procurement_id("01005580000170", 2025, 3)
        proc = next(p for p in pipeline.procurements if p["procurement_id"] == anapolis_id)
        assert proc["amount_estimated"] == 2500000.00

    def test_extracts_dates(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        dates = {p["published_at"] for p in pipeline.procurements}
        assert "2025-03-01" in dates
        assert "2025-02-15" in dates

    def test_extracts_modality(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        modalities = {p["modality"] for p in pipeline.procurements}
        assert "pregao_eletronico" in modalities
        assert "concorrencia" in modalities

    def test_all_records_have_uf_go(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        for p in pipeline.procurements:
            assert p["uf"] == "GO"

    def test_sets_source(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        for p in pipeline.procurements:
            assert p["source"] == "pncp_go"

    def test_extracts_municipality(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        municipalities = {p["municipality"] for p in pipeline.procurements}
        assert "Goiania" in municipalities
        assert "Anapolis" in municipalities

    def test_extracts_supplier_info(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        goias_id = _make_procurement_id("01409580000138", 2025, 12)
        proc = next(p for p in pipeline.procurements if p["procurement_id"] == goias_id)
        assert len(proc["fornecedores"]) == 1
        assert proc["fornecedores"][0]["cnpj"] == "12.345.678/0001-95"

    def test_procurement_has_all_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()

        expected_fields = {
            "procurement_id", "cnpj_agency", "agency_name", "year",
            "sequential", "object", "modality", "amount_estimated",
            "published_at", "uf", "municipality", "source", "fornecedores",
        }
        for p in pipeline.procurements:
            assert set(p.keys()) == expected_fields

    def test_skips_invalid_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)

        pipeline._raw_records.append({
            "orgaoEntidade": {
                "cnpj": "INVALID",
                "razaoSocial": "ORGAO INVALIDO",
                "esferaId": "E",
            },
            "anoCompra": 2025,
            "sequencialCompra": 99,
            "objetoCompra": "ITEM INVALIDO",
            "valorTotalEstimado": 100000.0,
            "dataPublicacaoPncp": "2025-01-01T00:00:00",
            "modalidadeId": 6,
            "modalidadeNome": "Pregao - Eletronico",
            "unidadeOrgao": {"ufSigla": "GO", "municipioNome": "Goiania"},
        })
        pipeline.transform()

        descs = {p["object"] for p in pipeline.procurements}
        assert "ITEM INVALIDO" not in descs

    def test_skips_zero_value(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)

        pipeline._raw_records.append({
            "orgaoEntidade": {
                "cnpj": "01409580000138",
                "razaoSocial": "GOVERNO DO ESTADO DE GOIAS",
                "esferaId": "E",
            },
            "anoCompra": 2025,
            "sequencialCompra": 999,
            "objetoCompra": "ITEM ZERO",
            "valorTotalEstimado": 0.0,
            "dataPublicacaoPncp": "2025-01-01T00:00:00",
            "modalidadeId": 6,
            "modalidadeNome": "Pregao - Eletronico",
            "unidadeOrgao": {"ufSigla": "GO", "municipioNome": "Goiania"},
        })
        pipeline.transform()

        descs = {p["object"] for p in pipeline.procurements}
        assert "ITEM ZERO" not in descs

    def test_caps_absurd_value(self) -> None:
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
            "modalidadeId": 6,
            "modalidadeNome": "Pregao - Eletronico",
            "unidadeOrgao": {"ufSigla": "GO", "municipioNome": "Absurdopolis"},
        })
        pipeline.transform()

        absurd = next(p for p in pipeline.procurements if p["object"] == "VALOR ABSURDO")
        assert absurd["amount_estimated"] is None

    def test_deduplicates_by_procurement_id(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)

        pipeline._raw_records.append(pipeline._raw_records[0].copy())
        pipeline.transform()

        assert len(pipeline.procurements) == 2

    def test_limit(self) -> None:
        pipeline = _make_pipeline()
        pipeline.limit = 1
        _load_fixture(pipeline)
        pipeline.transform()

        assert len(pipeline.procurements) == 1


# --- Load ---


class TestLoad:
    def test_load_creates_go_procurement_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        procurement_calls = [
            call for call in run_calls
            if "MERGE (n:GoProcurement" in str(call)
        ]
        assert len(procurement_calls) >= 1

    def test_load_creates_company_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        company_calls = [
            call for call in run_calls
            if "MERGE (n:Company" in str(call)
        ]
        assert len(company_calls) >= 1

    def test_load_creates_contratou_go_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "CONTRATOU_GO" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_creates_forneceu_go_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "FORNECEU_GO" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_skips_when_empty(self) -> None:
        pipeline = _make_pipeline()
        pipeline.procurements = []
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        assert session_mock.run.call_count == 0

    def test_load_calls_correct_number_of_batches(self) -> None:
        """Should call session.run for GoProcurement, Company (agency), CONTRATOU_GO,
        Company (supplier), and FORNECEU_GO."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        # At minimum: 1 GoProcurement + 1 Company(agency) + 1 CONTRATOU_GO
        # + 1 Company(supplier) + 1 FORNECEU_GO = 5
        assert session_mock.run.call_count >= 5
