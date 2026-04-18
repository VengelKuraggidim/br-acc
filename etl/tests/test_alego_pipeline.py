"""Tests for the ALEGO scaffold pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.alego import AlegoPipeline
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> AlegoPipeline:
    return AlegoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert AlegoPipeline.name == "alego"

    def test_source_id(self) -> None:
        assert AlegoPipeline.source_id == "alego"


class TestTransform:
    def test_legislator_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.legislators) == 2

    def test_expense_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.expenses) == 2

    def test_proposition_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.propositions) == 1

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for leg in pipeline.legislators:
            # CPF must never be stored in cleartext.
            assert "111" not in leg["cpf"]
            assert "***" in leg["cpf"]

    def test_expense_cnpj_formatted(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cnpjs = {e["cnpj_supplier"] for e in pipeline.expenses}
        assert "44.455.566/0001-88" in cnpjs

    def test_source_tagged(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in (
            pipeline.legislators + pipeline.expenses + pipeline.propositions
        ):
            assert r["source"] == "alego"

    def test_provenance_stamped_on_legislators(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.legislators
        for r in pipeline.legislators:
            assert r["source_id"] == "alego"
            # record_id is name|party|legislature.
            assert r["source_record_id"].count("|") == 2
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("alego_")

    def test_provenance_stamped_on_expenses_and_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.expenses
        for r in pipeline.expenses:
            assert r["source_id"] == "alego"
            # legislator_name|date|supplier|amount composite.
            assert r["source_record_id"].count("|") == 3
            assert r["source_url"].startswith("http")
        for rel in pipeline.expense_rels:
            assert rel["source_id"] == "alego"
            assert rel["source_record_id"]
            assert rel["run_id"].startswith("alego_")

    def test_provenance_stamped_on_propositions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.propositions:
            assert r["source_id"] == "alego"
            assert r["source_record_id"]
            assert r["source_url"].startswith("http")


class TestLoad:
    def test_load_calls_session(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0


# ---------------------------------------------------------------------------
# Archival — snapshot do payload ALEGO no momento do fetch (retrofit #3 do
# plano em todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: ``data_dir`` vazio força o fallback online em ``extract`` →
# cai em ``_fetch_from_api`` → dispara ``archive_fetch`` pra cada endpoint
# da API transparência (periodos, listing, exibir, processos/recentes,
# proposicoes-mais-votadas). Mockamos ``httpx.Client`` no módulo alego
# via ``MockTransport`` pra servir bytes determinísticos e verificamos
# que:
#   * arquivos de snapshot existem sob ``BRACC_ARCHIVAL_ROOT``;
#   * todas as rows transformadas ganharam ``source_snapshot_uri``;
#   * ``restore_snapshot`` devolve os bytes originais (round-trip);
#   * rows diferentes (listing vs exibir vs proposicoes) carimbam URIs
#     *distintas* porque vêm de endpoints com content-hash diferente.
# O caminho offline (fixtures CSV) NÃO popula o campo — garante opt-in.
# ---------------------------------------------------------------------------


_PERIODOS_PAYLOAD: list[dict[str, Any]] = [
    {"ano": 2024, "meses": [3]},
]
_LISTING_PAYLOAD: list[dict[str, Any]] = [
    {"id": 42, "nome": "DEPUTADA MOCKADA"},
]
_EXIBIR_PAYLOAD: dict[str, Any] = {
    "deputado": {"partido": "PARTIDO X"},
    "grupos": [
        {
            "descricao": "COMBUSTIVEL",
            "subgrupos": [
                {
                    "descricao": "GASOLINA",
                    "lancamentos": [
                        {
                            "fornecedor": {
                                "nome": "POSTO TESTE LTDA",
                                "cnpj_cpf": "12345678000199",
                                "valor_indenizado": "500,00",
                                "data": "2024-03-10",
                                "numero": "NF-001",
                            },
                        },
                    ],
                },
            ],
        },
    ],
}
_PROCESSOS_RECENTES_PAYLOAD: list[list[dict[str, Any]]] = [
    [
        {
            "numero": "PL 999/2024",
            "assunto": "PROJETO DE LEI MOCK",
            "ementa": "EMENTA MOCK",
            "autores": ["DEPUTADA MOCKADA"],
            "data_autuacao": "2024-03-15",
            "situacao": "EM TRAMITACAO",
        },
    ],
]
_MAIS_VOTADAS_PAYLOAD: dict[str, Any] = {"processos": []}


def _alego_handler() -> httpx.MockTransport:
    """MockTransport que emula transparencia.al.go.leg.br."""

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"content-type": "application/json; charset=utf-8"}
        path = request.url.path
        body: Any
        if path.endswith("/verbas_indenizatorias/periodos"):
            body = _PERIODOS_PAYLOAD
        elif path.endswith("/verbas_indenizatorias/deputados"):
            body = _LISTING_PAYLOAD
        elif path.endswith("/verbas_indenizatorias/exibir"):
            body = _EXIBIR_PAYLOAD
        elif path.endswith("/processos/recentes"):
            body = _PROCESSOS_RECENTES_PAYLOAD
        elif path.endswith("/processos/proposicoes-mais-votadas"):
            body = _MAIS_VOTADAS_PAYLOAD
        else:
            return httpx.Response(
                404,
                content=b'{"error": "unhandled"}',
                headers=headers,
            )
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
    archival_root: Path,  # noqa: ARG001 — just activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> AlegoPipeline:
    """Pipeline com data_dir vazio (força API fallback) + HTTP mockado."""
    empty_data = tmp_path / "data_empty"
    (empty_data / "alego").mkdir(parents=True)

    transport = _alego_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.alego.httpx.Client",
        _client_factory,
    )
    # Neutraliza o sleep de rate-limit pra não derrubar a suíte em 1s/req.
    monkeypatch.setattr(
        "bracc_etl.pipelines.alego.time.sleep",
        lambda _s: None,
    )
    # run_id canônico cai no bucket 2024-03 pra conferir o layout.
    pipeline = AlegoPipeline(driver=MagicMock(), data_dir=str(empty_data))
    pipeline.run_id = "alego_20240315120000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: alego agora grava snapshots dos payloads da API."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: AlegoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Proveniência: rows derivadas dos endpoints archivados carregam URI.
        assert online_pipeline.legislators
        for leg in online_pipeline.legislators:
            uri = leg.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            parts = uri.split("/")
            assert parts[0] == "alego"
            assert parts[1] == "2024-03"
            assert parts[2].endswith(".json")

        assert online_pipeline.expenses
        for exp in online_pipeline.expenses:
            assert exp.get("source_snapshot_uri")
        for rel in online_pipeline.expense_rels:
            assert rel.get("source_snapshot_uri")

        assert online_pipeline.propositions
        for prop in online_pipeline.propositions:
            assert prop.get("source_snapshot_uri")

        # Distinção de snapshots: endpoint /exibir (expenses) tem payload
        # diferente do /deputados (legislators), então URIs diferem —
        # archival é content-addressed, mesmo conteúdo viraria mesma URI.
        leg_uri = online_pipeline.legislators[0]["source_snapshot_uri"]
        exp_uri = online_pipeline.expenses[0]["source_snapshot_uri"]
        prop_uri = online_pipeline.propositions[0]["source_snapshot_uri"]
        assert leg_uri != exp_uri
        assert leg_uri != prop_uri
        assert exp_uri != prop_uri

        # Storage: arquivos fisicamente presentes sob o root configurado.
        for uri in (leg_uri, exp_uri, prop_uri):
            absolute = archival_root / uri
            assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored_exp = restore_snapshot(exp_uri)
        assert b"POSTO TESTE LTDA" in restored_exp
        restored_prop = restore_snapshot(prop_uri)
        assert b"PL 999/2024" in restored_prop

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Fixture local (sem HTTP) mantém o campo ``None`` — opt-in preservado."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.legislators
        for leg in pipeline.legislators:
            # Ausência do campo == opt-in não ativado (contrato do
            # attach_provenance: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in leg
        for exp in pipeline.expenses:
            assert "source_snapshot_uri" not in exp
        for prop in pipeline.propositions:
            assert "source_snapshot_uri" not in prop
