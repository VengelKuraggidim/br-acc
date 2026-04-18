"""Tests for the SSP-GO scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.ssp_go import SspGoPipeline
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator  # noqa: F401

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SspGoPipeline:
    # ``archive_pdfs=False`` pra fixture offline: evita hit network no
    # portal da SSP-GO quando o teste só exercita o path ``ocorrencias.csv``
    # local. O caminho com archival online é coberto em ``TestArchivalRetrofit``
    # abaixo, onde ``httpx.Client`` é monkeypatched com ``MockTransport``.
    return SspGoPipeline(
        driver=MagicMock(), data_dir=str(FIXTURES), archive_pdfs=False,
    )


class TestMetadata:
    def test_name(self) -> None:
        assert SspGoPipeline.name == "ssp_go"

    def test_source_id(self) -> None:
        assert SspGoPipeline.source_id == "ssp_go"


class TestTransform:
    def test_stats_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.stats) == 3

    def test_counts_parsed_as_int(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        counts = {s["count"] for s in pipeline.stats}
        assert 42 in counts
        assert 128 in counts
        assert 5 in counts

    def test_uf_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert all(s["uf"] == "GO" for s in pipeline.stats)

    def test_provenance_stamped_on_stats(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.stats
        for s in pipeline.stats:
            assert s["source_id"] == "ssp_go"
            # cod_ibge|crime_type|periodo composite.
            assert s["source_record_id"].count("|") == 2
            assert s["source_url"].startswith("http")
            assert s["ingested_at"].startswith("20")
            assert s["run_id"].startswith("ssp_go_")

    def test_provenance_stamped_unit(self) -> None:
        """Unit-level test so the scaffold is covered even without fixture."""
        pipeline = _make_pipeline()
        # Directly build a single raw row matching the shape ``transform``
        # expects. This exercises attach_provenance on the scaffold path
        # regardless of whether operator-provided CSVs exist.
        import pandas as pd

        pipeline._raw_stats = pd.DataFrame([
            {
                "cod_ibge": "5208707",
                "municipio": "Goiania",
                "natureza": "Roubo",
                "periodo": "2024-01",
                "quantidade": "128",
            },
        ])
        pipeline.transform()
        assert len(pipeline.stats) == 1
        stat = pipeline.stats[0]
        assert stat["source_id"] == "ssp_go"
        assert stat["source_record_id"] == "5208707|ROUBO|2024-01"
        assert stat["source_url"].startswith("http")


class TestLoad:
    def test_load_creates_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0


# ---------------------------------------------------------------------------
# Archival — snapshot dos PDFs anuais no momento do fetch (retrofit #4 do
# plano em todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: fixture (``ocorrencias.csv`` do ano 2025) fornece as rows;
# mockamos ``httpx.Client`` no módulo ``ssp_go`` com um ``MockTransport``
# que devolve um HTML de índice apontando pra um PDF fake de 2025. Daí:
#  * snapshot gravado em ``BRACC_ARCHIVAL_ROOT/ssp_go/YYYY-MM/*.pdf``;
#  * todas as rows com ``periodo=2025-*`` ganham ``source_snapshot_uri``;
#  * ``restore_snapshot`` devolve os bytes originais do PDF (round-trip).
# O path offline (``archive_pdfs=False``) NÃO deve popular o campo —
# rodado em paralelo pra garantir que o retrofit continua opt-in.
# ---------------------------------------------------------------------------


_FAKE_PDF_URL = (
    "https://goias.gov.br/seguranca/wp-content/uploads/"
    "sites/56/2025/01/estatisticas_2025.pdf"
)
_FAKE_PDF_BYTES = b"%PDF-1.4\n%ssp_go fake bulletin payload\n%%EOF"
_FAKE_INDEX_HTML = (
    "<html><body>"
    f'<a href="{_FAKE_PDF_URL}">Estatisticas 2025</a>'
    "</body></html>"
).encode()


def _ssp_handler() -> httpx.MockTransport:
    """MockTransport que emula o portal SSP-GO (index HTML + PDF binário)."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/estatisticas/") or url.endswith("/estatisticas"):
            return httpx.Response(
                200,
                content=_FAKE_INDEX_HTML,
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if url == _FAKE_PDF_URL:
            return httpx.Response(
                200,
                content=_FAKE_PDF_BYTES,
                headers={"content-type": "application/pdf"},
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
) -> SspGoPipeline:
    """Pipeline com HTTP mockado, ``archive_pdfs=True`` e fixtures locais.

    ``data_dir`` reusa a fixture ``ocorrencias.csv`` (rows com
    ``periodo=2025-01``) pra transform produzir rows, enquanto o mock
    devolve um PDF fake de 2025 pra popular o mapa de snapshot URIs.
    """
    transport = _ssp_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.ssp_go.httpx.Client",
        _client_factory,
    )
    pipeline = SspGoPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
        archive_pdfs=True,
    )
    # run_id canônico (``{source}_YYYYMMDDHHMMSS``) cai no bucket 2025-01,
    # só pra facilitar conferência visual do path no assert.
    pipeline.run_id = "ssp_go_20250115000000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: ssp_go agora grava snapshots dos PDFs anuais."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: SspGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Todas as rows do fixture têm ``periodo=2025-*`` e casam com o
        # PDF fake de 2025 — logo todas ganham ``source_snapshot_uri``.
        assert online_pipeline.stats
        for stat in online_pipeline.stats:
            uri = stat.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            # Shape: ``ssp_go/YYYY-MM/hash12.pdf``
            parts = uri.split("/")
            assert parts[0] == "ssp_go"
            assert parts[1] == "2025-01"
            assert parts[2].endswith(".pdf")

        # Storage: arquivo fisicamente presente sob o root configurado.
        sample_uri = online_pipeline.stats[0]["source_snapshot_uri"]
        absolute = archival_root / sample_uri
        assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored = restore_snapshot(sample_uri)
        assert restored == _FAKE_PDF_BYTES

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Pipeline com ``archive_pdfs=False`` deixa o campo fora (opt-in)."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.stats
        for stat in pipeline.stats:
            # Ausência do campo == opt-in não ativado (contrato do
            # attach_provenance: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in stat
