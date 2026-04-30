"""Tests for the SSP-GO scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.ssp_go import SspGoPipeline, _parse_bulletin_pdf
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


# ---------------------------------------------------------------------------
# PDF parser — os boletins anuais da SSP-GO são 1 página com tabela
# ``NATUREZAS × JAN..DEZ + TOTAL``. ``_parse_bulletin_pdf`` é o único
# ponto de entrada; os testes abaixo usam o PDF real de 2025 como fixture
# (22 KB, pequeno o bastante pra ficar versionado) e validam shape,
# valores amostrais e comportamento em PDF malformado.
# ---------------------------------------------------------------------------


_FIXTURE_2025_PDF = FIXTURES / "ssp_go" / "estatisticas_2025.pdf"


class TestParseBulletinPdf:
    """Cobertura do parser isolado."""

    def test_year_extracted_from_header(self) -> None:
        year, _rows = _parse_bulletin_pdf(_FIXTURE_2025_PDF.read_bytes())
        assert year == 2025

    def test_row_count_matches_15x12(self) -> None:
        """15 naturezas conhecidas × 12 meses = 180 rows, sem TOTAL."""
        _year, rows = _parse_bulletin_pdf(_FIXTURE_2025_PDF.read_bytes())
        assert len(rows) == 180

    def test_row_shape_state_level(self) -> None:
        """Todas as rows carregam o sentinela estadual (sem granularidade municipal)."""
        _year, rows = _parse_bulletin_pdf(_FIXTURE_2025_PDF.read_bytes())
        assert {r["municipio"] for r in rows} == {"ESTADO DE GOIAS"}
        assert {r["cod_ibge"] for r in rows} == {"5200000"}

    def test_values_match_pdf_sample(self) -> None:
        """Sanity: HOMICIDIO DOLOSO 2025-01 = 70 e FEMINICIDIO 2025-12 = 9."""
        # Valores conferidos manualmente contra o PDF original. Se o
        # parser regredir (shift de coluna, perda de linha), esses
        # casos quebram primeiro.
        _year, rows = _parse_bulletin_pdf(_FIXTURE_2025_PDF.read_bytes())
        by_key = {(r["natureza"], r["periodo"]): r["quantidade"] for r in rows}
        assert by_key[("HOMICIDIO DOLOSO", "2025-01")] == "70"
        assert by_key[("FEMINICIDIO", "2025-12")] == "9"
        assert by_key[("ROUBO A INSTITUICAO FINANCEIRA", "2025-06")] == "0"

    def test_thousand_separator_stripped(self) -> None:
        """Números com separador de milhar (ex.: ``1.097``) parseiam como int."""
        _year, rows = _parse_bulletin_pdf(_FIXTURE_2025_PDF.read_bytes())
        # FURTO EM RESIDENCIA tem valores >1000 em todos os meses de 2025.
        by_key = {(r["natureza"], r["periodo"]): r["quantidade"] for r in rows}
        assert int(by_key[("FURTO EM RESIDENCIA", "2025-01")]) == 1097

    def test_corrupt_pdf_returns_empty(self) -> None:
        """Bytes inválidos não explodem o pipeline — retornam (None, [])."""
        year, rows = _parse_bulletin_pdf(b"not a pdf at all")
        assert year is None
        assert rows == []


class TestMunicipalCsvOverride:
    """LAI/operator-supplied CSV com granularidade município × naturaza × mês.

    Quando a SSP-GO devolver o CSV via LAI (ver
    ``todo-list-prompts/high_priority/debitos/ssp-go-lai-pedido.md``),
    a usuária dropa em ``data/ssp_go/ocorrencias.csv`` e o pipeline
    deve ingerir sem regredir as rows estaduais já carregadas. O
    contrato é: ``stat_id`` inclui ``cod_ibge`` no hash, então as
    granularidades coexistem.
    """

    def test_csv_municipal_codigos_chegam_no_stat(
        self, tmp_path: Path,
    ) -> None:
        ssp_dir = tmp_path / "ssp_go"
        ssp_dir.mkdir()
        # CSV sintético no shape esperado do retorno LAI: município ×
        # naturaza × mês, com cod_ibge real (Goiânia, Anápolis,
        # Aparecida de Goiânia) — os 3 municípios de teste do critério
        # de aceite no débito de granularidade.
        (ssp_dir / "ocorrencias.csv").write_text(
            "municipio;cod_ibge;natureza;periodo;quantidade\n"
            "GOIANIA;5208707;FEMINICIDIO;2025-01;3\n"
            "GOIANIA;5208707;ESTUPRO;2025-01;47\n"
            "ANAPOLIS;5201108;FEMINICIDIO;2025-01;1\n"
            "APARECIDA DE GOIANIA;5201405;FEMINICIDIO;2025-01;2\n",
            encoding="utf-8",
        )
        pipeline = SspGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            archive_pdfs=False,
        )
        pipeline.extract()
        pipeline.transform()

        cod_ibges = {s["cod_ibge"] for s in pipeline.stats}
        municipios = {s["municipality"] for s in pipeline.stats}
        assert cod_ibges == {"5208707", "5201108", "5201405"}
        assert "5200000" not in cod_ibges  # sentinela estadual ausente
        assert municipios == {"GOIANIA", "ANAPOLIS", "APARECIDA DE GOIANIA"}

    def test_stat_id_distingue_estadual_de_municipal(
        self, tmp_path: Path,
    ) -> None:
        """stat_id hash inclui cod_ibge — estado e município coexistem."""
        ssp_dir = tmp_path / "ssp_go"
        ssp_dir.mkdir()
        # Mesma naturaza × mês em 2 granularidades: estadual e municipal.
        (ssp_dir / "ocorrencias.csv").write_text(
            "municipio;cod_ibge;natureza;periodo;quantidade\n"
            "ESTADO DE GOIAS;5200000;FEMINICIDIO;2025-01;9\n"
            "GOIANIA;5208707;FEMINICIDIO;2025-01;3\n",
            encoding="utf-8",
        )
        pipeline = SspGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            archive_pdfs=False,
        )
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.stats) == 2
        ids = {s["stat_id"] for s in pipeline.stats}
        # Hashes distintos -> nós Neo4j não colidem e a deduplicação
        # por stat_id no transform preserva ambos.
        assert len(ids) == 2


class TestOfflinePdfFallback:
    """Extract cai nos PDFs locais quando não há CSV e archive_pdfs=False."""

    def test_parses_local_pdf_when_csv_absent(
        self, tmp_path: Path,
    ) -> None:
        ssp_dir = tmp_path / "ssp_go"
        ssp_dir.mkdir()
        # Só copia o PDF (sem ocorrencias.csv) pra forçar o fallback.
        (ssp_dir / "estatisticas_2025.pdf").write_bytes(
            _FIXTURE_2025_PDF.read_bytes(),
        )
        pipeline = SspGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            archive_pdfs=False,
        )
        pipeline.extract()
        pipeline.transform()

        # 180 rows do PDF, todas com provenance estampada.
        assert len(pipeline.stats) == 180
        assert all(s["uf"] == "GO" for s in pipeline.stats)
        # Sem archive_pdfs, o campo snapshot_uri permanece fora (opt-in).
        for stat in pipeline.stats:
            assert "source_snapshot_uri" not in stat
