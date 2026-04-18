"""Tests for the PNCP GO (Goias) procurement pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.pncp_go import PncpGoPipeline, _make_procurement_id
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

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

        # ``__snapshot_uri`` é a chave privada de propagação do archival
        # (retrofit #2 do plano em high_priority/11-archival-retrofit-go.md).
        # Fica ``None`` no offline-path (fixture) e é filtrada em ``load``
        # antes de chegar ao Neo4jBatchLoader — opt-in preservado.
        expected_fields = {
            "procurement_id", "cnpj_agency", "agency_name", "year",
            "sequential", "object", "modality", "amount_estimated",
            "published_at", "uf", "municipality", "source", "fornecedores",
            "__snapshot_uri",
        }
        for p in pipeline.procurements:
            assert set(p.keys()) == expected_fields
            # Offline-path não popula URI.
            assert p["__snapshot_uri"] is None

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

        session_mock = mock_session(pipeline)
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

        session_mock = mock_session(pipeline)
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

        session_mock = mock_session(pipeline)
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

        session_mock = mock_session(pipeline)
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

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0

    def test_provenance_stamped_on_procurement_nodes(self) -> None:
        """load() stamps the 5 provenance fields on each GoProcurement dict."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        # Find the UNWIND/MERGE call for GoProcurement and inspect the rows.
        procurement_calls = [
            call for call in session_mock.run.call_args_list
            if "MERGE (n:GoProcurement" in str(call)
        ]
        assert procurement_calls
        _, kwargs = procurement_calls[0][0], procurement_calls[0][1]
        # session.run(query, {"rows": batch}) -> batch is the 2nd positional.
        params = procurement_calls[0][0][1] if len(procurement_calls[0][0]) > 1 else kwargs
        rows = params["rows"]
        assert rows
        for r in rows:
            assert r["source_id"] == "pncp_go"
            # record_id is cnpj_digits|year|sequential (raw composite).
            assert "|" in r["source_record_id"]
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("pncp_go_")

    def test_provenance_stamped_on_forneceu_rels(self) -> None:
        """load() stamps provenance on FORNECEU_GO relationship rows."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        rel_calls = [
            call for call in session_mock.run.call_args_list
            if "FORNECEU_GO" in str(call)
        ]
        assert rel_calls
        params = rel_calls[0][0][1]
        rows = params["rows"]
        assert rows
        for r in rows:
            assert r["source_id"] == "pncp_go"
            assert "|" in r["source_record_id"]
            assert r["source_url"].startswith("http")

    def test_load_calls_correct_number_of_batches(self) -> None:
        """Should call session.run for GoProcurement, Company (agency), CONTRATOU_GO,
        Company (supplier), and FORNECEU_GO."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        # At minimum: 1 GoProcurement + 1 Company(agency) + 1 CONTRATOU_GO
        # + 1 Company(supplier) + 1 FORNECEU_GO = 5
        assert session_mock.run.call_count >= 5


# ---------------------------------------------------------------------------
# Archival — snapshot do payload PNCP no momento do fetch (retrofit #2 do
# plano em todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: ``data_dir`` vazio -> extract cai no fallback HTTP; mockamos
# ``httpx.Client`` no módulo ``pncp_go`` com um ``MockTransport`` que devolve
# uma única página por (modalidade, window) e zero nas demais, daí conferimos:
#   * snapshot file gravado em ``BRACC_ARCHIVAL_ROOT/pncp_go/YYYY-MM/*.json``;
#   * todas as rows transformadas carregam ``__snapshot_uri`` internamente e
#     ``load()`` repassa ``source_snapshot_uri`` pra cada row do batch;
#   * ``restore_snapshot`` devolve os bytes originais (round-trip).
# O path offline (fixture JSON local) NÃO deve popular o campo — rodado em
# paralelo pra garantir que o retrofit continua opt-in.
# ---------------------------------------------------------------------------


# Uma modalidade só (pregao eletronico, código 6) + uma window — o mock
# devolve ``paginasRestantes=0`` na primeira página pra fechar o loop
# imediatamente. Demais modalidades retornam ``data: []`` → break.
_PNCP_PAGE_RECORDS: list[dict[str, Any]] = [
    {
        "orgaoEntidade": {
            "cnpj": "01409580000138",
            "razaoSocial": "GOVERNO DO ESTADO DE GOIAS",
            "esferaId": "E",
        },
        "anoCompra": 2024,
        "sequencialCompra": 42,
        "objetoCompra": "AQUISICAO DE EQUIPAMENTOS",
        "valorTotalEstimado": 850000.00,
        "dataPublicacaoPncp": "2024-03-10T00:00:00",
        "modalidadeId": 6,
        "modalidadeNome": "Pregao - Eletronico",
        "unidadeOrgao": {"ufSigla": "GO", "municipioNome": "Goiania"},
        "fornecedores": [
            {"cnpj": "12345678000195", "razaoSocial": "FORNECEDORA GO LTDA"},
        ],
    },
]


def _pncp_handler() -> httpx.MockTransport:
    """MockTransport que emula o PNCP (1 registro para modalidade=6)."""

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"content-type": "application/json; charset=utf-8"}
        mod = request.url.params.get("codigoModalidadeContratacao")
        # Só modalidade 6 devolve registros — demais iteram e saem vazias.
        if str(mod) == "6" and request.url.params.get("pagina") == "1":
            body = {"data": _PNCP_PAGE_RECORDS, "paginasRestantes": 0}
        else:
            body = {"data": [], "paginasRestantes": 0}
        return httpx.Response(
            200,
            content=json.dumps(body).encode("utf-8"),
            headers=headers,
        )

    return httpx.MockTransport(handler)


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


@pytest.fixture()
def online_pipeline(
    tmp_path: Path,
    archival_root: Path,  # noqa: ARG001 — apenas ativa o env var
    monkeypatch: pytest.MonkeyPatch,
) -> PncpGoPipeline:
    """Pipeline com data_dir vazio (força fallback HTTP) + transport mockado."""
    empty_data = tmp_path / "data_empty"
    (empty_data / "pncp_go").mkdir(parents=True)

    transport = _pncp_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.pncp_go.httpx.Client",
        _client_factory,
    )
    pipeline = PncpGoPipeline(driver=MagicMock(), data_dir=str(empty_data))
    # run_id canônico → bucket 2024-03 no archival (alinhado com data do mock).
    pipeline.run_id = "pncp_go_20240315120000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: pncp_go agora grava snapshots dos payloads JSON do PNCP."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: PncpGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Proveniência interna: cada procurement carrega o URI na chave
        # privada ``__snapshot_uri``.
        assert online_pipeline.procurements
        sample_uri: str | None = None
        for p in online_pipeline.procurements:
            uri = p.get("__snapshot_uri")
            assert isinstance(uri, str) and uri
            parts = uri.split("/")
            assert parts[0] == "pncp_go"
            assert parts[1] == "2024-03"
            assert parts[2].endswith(".json")
            sample_uri = uri

        # load() propaga a URI via ``source_snapshot_uri`` nas rows que
        # chegam ao Neo4jBatchLoader (tanto no nó GoProcurement quanto
        # nos relacionamentos CONTRATOU_GO/FORNECEU_GO).
        online_pipeline.load()
        session_mock = mock_session(online_pipeline)

        procurement_calls = [
            call for call in session_mock.run.call_args_list
            if "MERGE (n:GoProcurement" in str(call)
        ]
        assert procurement_calls
        rows = procurement_calls[0][0][1]["rows"]
        assert rows
        for r in rows:
            assert isinstance(r.get("source_snapshot_uri"), str)
            assert r["source_snapshot_uri"].startswith("pncp_go/2024-03/")
            # ``__snapshot_uri`` é chave interna — não deve vazar pro loader.
            assert "__snapshot_uri" not in r

        forneceu_calls = [
            call for call in session_mock.run.call_args_list
            if "FORNECEU_GO" in str(call)
        ]
        assert forneceu_calls
        forn_rows = forneceu_calls[0][0][1]["rows"]
        for r in forn_rows:
            assert r.get("source_snapshot_uri", "").startswith("pncp_go/2024-03/")

        # Storage: arquivo fisicamente presente sob o root configurado.
        assert sample_uri is not None
        absolute = archival_root / sample_uri
        assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored = restore_snapshot(sample_uri)
        assert b'"data"' in restored
        assert b"GOVERNO DO ESTADO DE GOIAS" in restored

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Fixture local (sem HTTP) mantém o campo ausente — opt-in preservado."""
        pipeline = _make_pipeline()
        _load_fixture(pipeline)
        pipeline.transform()
        pipeline.load()

        assert pipeline.procurements
        # ``__snapshot_uri`` existe internamente mas fica ``None`` no
        # offline-path (raw records vêm do fixture, sem passagem por
        # ``archive_fetch``).
        for p in pipeline.procurements:
            assert p.get("__snapshot_uri") is None

        # E as rows que chegam ao Neo4jBatchLoader nunca ganham o campo
        # público ``source_snapshot_uri`` — ``attach_provenance`` omite
        # a chave quando ``snapshot_uri`` é ``None``.
        session_mock = mock_session(pipeline)
        procurement_calls = [
            call for call in session_mock.run.call_args_list
            if "MERGE (n:GoProcurement" in str(call)
        ]
        assert procurement_calls
        rows = procurement_calls[0][0][1]["rows"]
        for r in rows:
            assert "source_snapshot_uri" not in r
            assert "__snapshot_uri" not in r
