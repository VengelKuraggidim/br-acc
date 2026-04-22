"""Tests for the TCM-GO sanctions scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.tcmgo_sancoes import TcmgoSancoesPipeline
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TcmgoSancoesPipeline:
    # ``archive_online=False`` pra fixture offline: evita hit network no
    # endpoint público do TCM-GO durante teste unitário rodado local.
    # O caminho com archival online é coberto em ``TestArchivalRetrofit``
    # abaixo, onde ``httpx.Client`` é monkeypatched com ``MockTransport``.
    return TcmgoSancoesPipeline(
        driver=MagicMock(), data_dir=str(FIXTURES), archive_online=False,
    )


class TestMetadata:
    def test_name(self) -> None:
        assert TcmgoSancoesPipeline.name == "tcmgo_sancoes"

    def test_source_id(self) -> None:
        assert TcmgoSancoesPipeline.source_id == "tcmgo_sancoes"


class TestTransform:
    def test_impedidos_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 1 CNPJ + 1 CPF cru + 1 CPF pre-mascarado (upstream TCM-GO).
        assert len(pipeline.impedidos) == 3

    def test_rejected_accounts_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.rejected_accounts) == 1

    def test_cnpj_and_cpf_distinguished(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        kinds = {r["document_kind"] for r in pipeline.impedidos}
        assert kinds == {"CNPJ", "CPF"}

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cpfs = [
            r["document"] for r in pipeline.impedidos
            if r["document_kind"] == "CPF"
        ]
        assert all("***" in c for c in cpfs)

    def test_premasked_cpf_classified_as_cpf(self) -> None:
        """Upstream TCM-GO entrega CPFs ja mascarados (``NN***.***-***``) —
        pipeline precisa reconhecer esse shape e carimbar ``kind=CPF`` +
        preservar a mascara. Sem isso, as 1422 rows de producao caem em
        ``kind=""`` e quebram a validation query documentada no TODO 03.
        """
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        premasked = [
            r for r in pipeline.impedidos
            if r["name"] == "RESPONSAVEL PRE MASCARADO"
        ]
        assert len(premasked) == 1
        assert premasked[0]["document_kind"] == "CPF"
        assert premasked[0]["document"] == "76***.***-***"

    def test_impedido_rels_only_for_cnpj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # Only the CNPJ row should produce a relationship.
        assert len(pipeline.impedido_rels) == 1

    def test_uf_and_source(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.impedidos + pipeline.rejected_accounts:
            assert r["uf"] == "GO"
            assert r["source"] == "tcmgo_sancoes"

    def test_provenance_stamped_on_impedidos(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.impedidos
        for r in pipeline.impedidos:
            assert r["source_id"] == "tcmgo_sancoes"
            # document|processo composite.
            assert "|" in r["source_record_id"]
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("tcmgo_sancoes_")
        for rel in pipeline.impedido_rels:
            assert rel["source_id"] == "tcmgo_sancoes"
            assert "|" in rel["source_record_id"]

    def test_provenance_stamped_on_rejected_accounts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.rejected_accounts:
            assert r["source_id"] == "tcmgo_sancoes"
            # cod_ibge|exercicio|processo composite.
            assert r["source_record_id"].count("|") == 2
            assert r["source_url"].startswith("http")


class TestLoad:
    def test_load_runs(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0


# ---------------------------------------------------------------------------
# Archival — snapshot do CSV de contas-irregulares no momento do fetch
# (retrofit #5 do plano em
# todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: fixture local (``impedidos.csv`` + ``rejeitados.csv``) fornece
# as rows; mockamos ``httpx.Client`` no módulo ``tcmgo_sancoes`` com um
# ``MockTransport`` que devolve bytes determinísticos pro endpoint público
# (``ws.tcm.go.gov.br/api/rest/dados/contas-irregulares``). Daí:
#  * snapshot gravado em ``BRACC_ARCHIVAL_ROOT/tcmgo_sancoes/YYYY-MM/*.csv``;
#  * todas as rows de impedidos ganham ``source_snapshot_uri``;
#  * impedido_rels (CNPJ) também recebem URI;
#  * rows de ``rejeitados`` continuam sem URI — não há fonte pública
#    correspondente, então o contrato opt-in vale;
#  * ``restore_snapshot`` devolve os bytes originais do CSV mockado.
# O path offline (``archive_online=False``) NÃO deve popular o campo —
# rodado em ``TestTransform`` acima pra garantir que o retrofit continua
# opt-in.
# ---------------------------------------------------------------------------


_FAKE_CONTAS_CSV = (
    b"CPF;Nome;Assunto;Processo/Fase\n"
    b"12345678000199;EMPRESA MOCK TCMGO LTDA;Irregularidade fake;"
    b"2024.MOCK.001\n"
)


def _tcmgo_handler() -> httpx.MockTransport:
    """MockTransport que emula ws.tcm.go.gov.br (contas-irregulares CSV)."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/contas-irregulares"):
            return httpx.Response(
                200,
                content=_FAKE_CONTAS_CSV,
                headers={"content-type": "text/csv; charset=utf-8"},
            )
        return httpx.Response(
            404,
            content=b"not found",
            headers={"content-type": "text/plain"},
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
    archival_root: Path,  # noqa: ARG001 — just activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> TcmgoSancoesPipeline:
    """Pipeline com HTTP mockado, ``archive_online=True`` e fixtures locais.

    ``data_dir`` reusa a fixture ``impedidos.csv`` pra transform produzir
    rows determinísticas (o parsing continua a partir do disk), enquanto o
    mock devolve um CSV fake pro endpoint público — o snapshot gravado vem
    desses bytes mockados, não do fixture de disco.
    """
    transport = _tcmgo_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.tcmgo_sancoes.httpx.Client",
        _client_factory,
    )
    pipeline = TcmgoSancoesPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
        archive_online=True,
    )
    # run_id canônico (``{source}_YYYYMMDDHHMMSS``) cai no bucket 2024-09,
    # só pra facilitar conferência visual do path no assert.
    pipeline.run_id = "tcmgo_sancoes_20240915000000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: tcmgo_sancoes agora grava snapshots do CSV público."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: TcmgoSancoesPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Todas as rows de impedidos (fixture: 1 CNPJ + 1 CPF) saem do
        # mesmo CSV de contas-irregulares archivado, então compartilham
        # a mesma URI.
        assert online_pipeline.impedidos
        expected_uri: str | None = None
        for imp in online_pipeline.impedidos:
            uri = imp.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            # Shape: ``tcmgo_sancoes/YYYY-MM/hash12.csv``.
            parts = uri.split("/")
            assert parts[0] == "tcmgo_sancoes"
            assert parts[1] == "2024-09"
            assert parts[2].endswith(".csv")
            if expected_uri is None:
                expected_uri = uri
            else:
                assert uri == expected_uri

        # impedido_rels (CNPJ-only) replicam a URI do impedido-pai.
        assert online_pipeline.impedido_rels
        for rel in online_pipeline.impedido_rels:
            assert rel.get("source_snapshot_uri") == expected_uri

        # rejeitados.csv não tem fonte pública — row continua sem URI.
        assert online_pipeline.rejected_accounts
        for rej in online_pipeline.rejected_accounts:
            assert "source_snapshot_uri" not in rej

        # Storage: arquivo fisicamente presente sob o root configurado.
        assert expected_uri is not None
        absolute = archival_root / expected_uri
        assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored = restore_snapshot(expected_uri)
        assert restored == _FAKE_CONTAS_CSV

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Pipeline com ``archive_online=False`` deixa o campo fora (opt-in)."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.impedidos
        for imp in pipeline.impedidos:
            # Ausência do campo == opt-in não ativado (contrato do
            # attach_provenance: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in imp
        for rel in pipeline.impedido_rels:
            assert "source_snapshot_uri" not in rel
        for rej in pipeline.rejected_accounts:
            assert "source_snapshot_uri" not in rej
