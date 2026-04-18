"""Tests for the Goias state transparency portal pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.state_portal_go import StatePortalGoPipeline, _hash_id
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> StatePortalGoPipeline:
    return StatePortalGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert StatePortalGoPipeline.name == "state_portal_go"

    def test_source_id(self) -> None:
        assert StatePortalGoPipeline.source_id == "state_portal_go"


class TestExtract:
    def test_extract_loads_all_three_domains(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert len(pipeline._raw_contracts) == 3
        assert len(pipeline._raw_suppliers) == 4
        assert len(pipeline._raw_sanctions) == 2


class TestTransform:
    def test_transform_contract_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.contracts) == 3

    def test_transform_supplier_count_skips_non_cnpj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # Fixture has 4 rows including one invalid CNPJ that should be dropped.
        assert len(pipeline.suppliers) == 3

    def test_transform_sanction_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.sanctions) == 2

    def test_contract_cnpj_formatted(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {c["cnpj_supplier"] for c in pipeline.contracts}
        assert "12.345.678/0001-95" in cnpjs

    def test_contract_amount_parsed(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        amounts = sorted(
            c["amount"] for c in pipeline.contracts if c["amount"] is not None
        )
        assert 850750.50 in amounts
        assert 1500000.00 in amounts
        assert 3200000.00 in amounts

    def test_contract_rels_created_for_suppliers(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.contract_rels) == 3

    def test_sanction_rels_created_for_cnpj_targets(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.sanction_rels) == 2

    def test_all_uf_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for record in pipeline.contracts + pipeline.suppliers + pipeline.sanctions:
            assert record["uf"] == "GO"

    def test_source_tagged(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for record in pipeline.contracts + pipeline.suppliers + pipeline.sanctions:
            assert record["source"] == "state_portal_go"

    def test_provenance_stamped_on_contracts_and_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.contracts
        for c in pipeline.contracts:
            assert c["source_id"] == "state_portal_go"
            # numero|cnpj_fmt|published composite.
            assert c["source_record_id"].count("|") == 2
            assert c["source_url"].startswith("http")
            assert c["ingested_at"].startswith("20")
            assert c["run_id"].startswith("state_portal_go_")
        for rel in pipeline.contract_rels:
            assert rel["source_id"] == "state_portal_go"
            assert rel["source_record_id"].count("|") == 2

    def test_provenance_stamped_on_suppliers(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.suppliers
        for s in pipeline.suppliers:
            assert s["source_id"] == "state_portal_go"
            # Natural record_id is cnpj_fmt.
            assert s["source_record_id"] == s["cnpj"]
            assert s["source_url"].startswith("http")

    def test_provenance_stamped_on_sanctions_and_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.sanctions
        for s in pipeline.sanctions:
            assert s["source_id"] == "state_portal_go"
            # cnpj|tipo|processo composite.
            assert s["source_record_id"].count("|") == 2
            assert s["source_url"].startswith("http")
        for rel in pipeline.sanction_rels:
            assert rel["source_id"] == "state_portal_go"
            assert "|" in rel["source_record_id"]

    def test_hash_id_is_stable(self) -> None:
        assert _hash_id("a", "b") == _hash_id("a", "b")
        assert _hash_id("a", "b") != _hash_id("b", "a")


class TestLoad:
    def test_load_calls_session_run(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0

    def test_load_creates_contract_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        contract_calls = [
            call for call in session.run.call_args_list
            if "GoStateContract" in str(call)
        ]
        assert len(contract_calls) >= 1

    def test_load_creates_supplier_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        supplier_calls = [
            call for call in session.run.call_args_list
            if "GoStateSupplier" in str(call)
        ]
        assert len(supplier_calls) >= 1

    def test_load_creates_sanction_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        sanction_calls = [
            call for call in session.run.call_args_list
            if "GoStateSanction" in str(call)
        ]
        assert len(sanction_calls) >= 1

    def test_load_creates_contratou_estado_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        rel_calls = [
            call for call in session.run.call_args_list
            if "CONTRATOU_ESTADO_GO" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_skips_when_empty(self) -> None:
        pipeline = _make_pipeline()
        pipeline.contracts = []
        pipeline.suppliers = []
        pipeline.sanctions = []
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count == 0


# ---------------------------------------------------------------------------
# Archival — snapshot do CSV/JSON bruto do CKAN no momento do fetch
# (retrofit #6 do plano em high_priority/11-archival-retrofit-go.md).
#
# Estratégia: ``data_dir`` vazio -> ``extract`` cai no fallback HTTP pro
# CKAN. Mockamos ``httpx.Client`` no módulo ``state_portal_go`` com um
# ``MockTransport`` que emula dois endpoints: ``package_show`` (JSON com
# URL do CSV mais recente) e o download do CSV em si. Conferimos:
#   * snapshot do CSV gravado em ``BRACC_ARCHIVAL_ROOT/state_portal_go/
#     YYYY-MM/*.csv``;
#   * todas as rows transformadas carregam ``source_snapshot_uri`` via
#     ``attach_provenance`` apontando pro URI do CSV do dataset;
#   * ``restore_snapshot`` devolve os bytes originais do CSV (round-trip).
# O path offline (fixture CSV local) NÃO deve popular o campo — rodado
# em paralelo pra garantir que o retrofit continua opt-in.
# ---------------------------------------------------------------------------


_MOCK_CSV_CONTRATOS = (
    "numero_contrato;cnpj_fornecedor;razao_social;objeto;valor;data_publicacao\n"
    "CT-ONLINE-1;12.345.678/0001-95;FORNECEDORA ONLINE LTDA;Aquisicao;1000,00;2024-03-10\n"
)
_MOCK_CSV_FORNECEDORES = (
    "cnpj;razao_social;situacao;data_cadastro\n"
    "98765432000110;EMPRESA ONLINE LTDA;ATIVA;2023-01-15\n"
)
_MOCK_CSV_SANCOES = (
    "cnpj;razao_social;tipo_sancao;orgao_sancionador;data_inicio;processo\n"
    "11222333000144;SANCIONADA ONLINE LTDA;IMPEDIMENTO;SEAD;2024-02-01;PROC-001\n"
)


def _ckan_handler() -> httpx.MockTransport:
    """MockTransport que emula o CKAN (package_show + download CSV)."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "package_show" in url:
            package_id = request.url.params.get("id", "")
            csv_url = f"https://dadosabertos.go.gov.br/dataset/{package_id}/latest.csv"
            body = {
                "result": {
                    "resources": [
                        {
                            "format": "CSV",
                            "url": csv_url,
                            "created": "2024-03-01T00:00:00",
                        },
                    ],
                },
            }
            return httpx.Response(
                200,
                content=json.dumps(body).encode("utf-8"),
                headers={"content-type": "application/json; charset=utf-8"},
            )

        # Download do CSV — uma fixture distinta por dataset.
        if "contratos" in url:
            payload = _MOCK_CSV_CONTRATOS
        elif "fornecedores" in url:
            payload = _MOCK_CSV_FORNECEDORES
        else:
            payload = _MOCK_CSV_SANCOES
        return httpx.Response(
            200,
            content=payload.encode("utf-8"),
            headers={"content-type": "text/csv; charset=utf-8"},
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
) -> StatePortalGoPipeline:
    """Pipeline com data_dir vazio (força fallback CKAN) + transport mockado."""
    empty_data = tmp_path / "data_empty"
    (empty_data / "state_portal_go").mkdir(parents=True)

    transport = _ckan_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.state_portal_go.httpx.Client",
        _client_factory,
    )
    pipeline = StatePortalGoPipeline(driver=MagicMock(), data_dir=str(empty_data))
    # run_id canônico → bucket 2024-03 no archival.
    pipeline.run_id = "state_portal_go_20240315120000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: state_portal_go agora grava snapshots do CSV/JSON do CKAN."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: StatePortalGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Cada dataset gera uma URI distinta (content-addressed por payload).
        assert set(online_pipeline._snapshot_uris) == {
            "contratos", "fornecedores", "sancoes",
        }
        for key, uri in online_pipeline._snapshot_uris.items():
            parts = uri.split("/")
            assert parts[0] == "state_portal_go", key
            assert parts[1] == "2024-03", key
            assert parts[2].endswith(".csv"), key

        # attach_provenance propaga a URI pra cada row (nós + rels).
        assert online_pipeline.contracts
        for c in online_pipeline.contracts:
            assert c["source_snapshot_uri"].startswith("state_portal_go/2024-03/")
        assert online_pipeline.suppliers
        for s in online_pipeline.suppliers:
            assert s["source_snapshot_uri"].startswith("state_portal_go/2024-03/")
        assert online_pipeline.sanctions
        for s in online_pipeline.sanctions:
            assert s["source_snapshot_uri"].startswith("state_portal_go/2024-03/")
        for rel in online_pipeline.contract_rels:
            assert rel["source_snapshot_uri"].startswith("state_portal_go/2024-03/")
        for rel in online_pipeline.sanction_rels:
            assert rel["source_snapshot_uri"].startswith("state_portal_go/2024-03/")

        # load() mantém a URI nas rows entregues ao loader (incluindo
        # Company derivadas de contracts/suppliers/sanctions).
        online_pipeline.load()
        session_mock = mock_session(online_pipeline)
        contract_calls = [
            call for call in session_mock.run.call_args_list
            if "MERGE (n:GoStateContract" in str(call)
        ]
        assert contract_calls
        rows = contract_calls[0][0][1]["rows"]
        assert rows
        for r in rows:
            assert r.get("source_snapshot_uri", "").startswith(
                "state_portal_go/2024-03/",
            )

        company_calls = [
            call for call in session_mock.run.call_args_list
            if "MERGE (n:Company" in str(call)
        ]
        assert company_calls
        for call in company_calls:
            for r in call[0][1]["rows"]:
                assert r.get("source_snapshot_uri", "").startswith(
                    "state_portal_go/2024-03/",
                )

        # Storage: arquivos fisicamente presentes sob o root configurado.
        for uri in online_pipeline._snapshot_uris.values():
            assert (archival_root / uri).exists(), f"snapshot ausente: {uri}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        contratos_uri = online_pipeline._snapshot_uris["contratos"]
        restored = restore_snapshot(contratos_uri)
        assert b"FORNECEDORA ONLINE LTDA" in restored

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Fixture local (sem HTTP) mantém o campo ausente — opt-in preservado."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

        # Mapa de URIs fica vazio no offline-path (sem HTTP).
        assert pipeline._snapshot_uris == {}

        # E nenhuma row ganha ``source_snapshot_uri``.
        assert pipeline.contracts
        for c in pipeline.contracts:
            assert "source_snapshot_uri" not in c
        for s in pipeline.suppliers:
            assert "source_snapshot_uri" not in s
        for s in pipeline.sanctions:
            assert "source_snapshot_uri" not in s

        # E as rows que chegam ao Neo4jBatchLoader também não ganham o
        # campo — ``attach_provenance`` omite a chave quando ``snapshot_uri``
        # é ``None``.
        session_mock = mock_session(pipeline)
        contract_calls = [
            call for call in session_mock.run.call_args_list
            if "MERGE (n:GoStateContract" in str(call)
        ]
        assert contract_calls
        for r in contract_calls[0][0][1]["rows"]:
            assert "source_snapshot_uri" not in r
