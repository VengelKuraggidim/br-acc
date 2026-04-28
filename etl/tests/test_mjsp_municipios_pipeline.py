"""Tests for the MJSP/SINESP municipal homicide pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.mjsp_municipios import (
    MjspMunicipiosPipeline,
    _format_period,
)
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"
_FIXTURE_XLSX = FIXTURES / "mjsp_municipios" / "indicadoressegurancapublicamunic.xlsx"


def _make_pipeline() -> MjspMunicipiosPipeline:
    # ``archive_xlsx=False`` evita hit network. ``data_dir`` aponta pra
    # FIXTURES → ``data_dir/mjsp_municipios/indicadoressegurancapublicamunic.xlsx``
    # já existe como fixture, então o fallback offline lê dela.
    return MjspMunicipiosPipeline(
        driver=MagicMock(), data_dir=str(FIXTURES), archive_xlsx=False,
    )


class TestMetadata:
    def test_name(self) -> None:
        assert MjspMunicipiosPipeline.name == "mjsp_municipios"

    def test_source_id(self) -> None:
        assert MjspMunicipiosPipeline.source_id == "mjsp_municipios"


class TestExtract:
    def test_reads_only_go_sheet(self) -> None:
        """Fixture tem GO + SP; só GO entra em _raw_stats."""
        pipeline = _make_pipeline()
        pipeline.extract()
        assert not pipeline._raw_stats.empty
        ufs = set(pipeline._raw_stats["Sigla UF"].unique())
        assert ufs == {"GO"}

    def test_rows_in_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        # 3 municípios × 2 meses = 6 rows GO na fixture.
        assert pipeline.rows_in == 6


class TestTransform:
    def test_stats_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.stats) == 6

    def test_three_test_municipalities_present(self) -> None:
        """Critério de aceite do débito: Goiânia + Anápolis + Aparecida."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        municipios = {s["municipality"] for s in pipeline.stats}
        assert "GOIANIA" in municipios
        assert "ANAPOLIS" in municipios
        assert "APARECIDA DE GOIANIA" in municipios

    def test_uf_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert all(s["uf"] == "GO" for s in pipeline.stats)

    def test_crime_type_homicidio_doloso(self) -> None:
        """O XLSX MJSP só publica 1 indicador (Portaria 229/2018)."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert {s["crime_type"] for s in pipeline.stats} == {"HOMICIDIO DOLOSO"}

    def test_period_iso_yyyy_mm(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        periodos = {s["period"] for s in pipeline.stats}
        assert periodos == {"2024-01", "2024-02"}

    def test_count_parsed_as_int(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # Goiânia 2024-01 = 27 vítimas (fixture).
        match = next(
            s for s in pipeline.stats
            if s["municipality"] == "GOIANIA" and s["period"] == "2024-01"
        )
        assert match["count"] == 27
        assert isinstance(match["count"], int)

    def test_cod_ibge_distinguishes_from_state_level(self) -> None:
        """Garantia de coexistência com ssp_go (state sentinel = 5200000)."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cods = {s["cod_ibge"] for s in pipeline.stats}
        # Nenhum row carrega o sentinela estadual; todos têm o IBGE
        # municipal de 7 dígitos.
        assert "5200000" not in cods
        assert all(len(c) == 7 for c in cods)

    def test_provenance_stamped(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for s in pipeline.stats:
            assert s["source_id"] == "mjsp_municipios"
            # cod_ibge|crime_type|periodo composite.
            assert s["source_record_id"].count("|") == 2
            assert s["source_url"].startswith("http")
            assert s["ingested_at"].startswith("20")
            assert s["run_id"].startswith("mjsp_municipios_")

    def test_offline_path_no_snapshot_uri(self) -> None:
        """Sem ``archive_xlsx=True``, opt-in não dispara — campo ausente."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.stats
        for s in pipeline.stats:
            assert "source_snapshot_uri" not in s


class TestLoad:
    def test_load_creates_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0


class TestFormatPeriod:
    """``_format_period`` aceita Timestamp, string ISO e MM/YYYY."""

    def test_timestamp(self) -> None:
        assert _format_period(pd.Timestamp("2024-03-15")) == "2024-03"

    def test_iso_datetime_string(self) -> None:
        assert _format_period("2024-03-01 00:00:00") == "2024-03"

    def test_iso_date_string(self) -> None:
        assert _format_period("2024-03-15") == "2024-03"

    def test_mm_slash_yyyy(self) -> None:
        assert _format_period("03/2024") == "2024-03"

    def test_empty_returns_empty(self) -> None:
        assert _format_period("") == ""
        assert _format_period(None) == ""

    def test_garbage_returns_empty(self) -> None:
        assert _format_period("xyz") == ""


# ---------------------------------------------------------------------------
# Archival — opt-in path, mocks o CKAN package_show + download do XLSX e
# valida que o snapshot é content-addressed e round-trippable.
# ---------------------------------------------------------------------------


_FAKE_XLSX_URL = (
    "http://dados.mj.gov.br/dataset/abc/resource/def/download/"
    "indicadoressegurancapublicamunic.xlsx"
)
_FAKE_CKAN_RESPONSE = {
    "result": {
        "resources": [
            {
                "name": "Dados Nacionais de Segurança Pública - Municípios",
                "url": _FAKE_XLSX_URL,
                "format": "XLSX",
            },
        ],
    },
}


def _mjsp_handler(xlsx_bytes: bytes) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "package_show" in url:
            import json
            return httpx.Response(
                200,
                content=json.dumps(_FAKE_CKAN_RESPONSE).encode(),
                headers={"content-type": "application/json"},
            )
        if url == _FAKE_XLSX_URL:
            return httpx.Response(
                200,
                content=xlsx_bytes,
                headers={
                    "content-type": (
                        "application/vnd.openxmlformats-officedocument."
                        "spreadsheetml.sheet"
                    ),
                },
            )
        return httpx.Response(404, content=b"not found")

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
    archival_root: Path,  # noqa: ARG001 — activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> MjspMunicipiosPipeline:
    xlsx_bytes = _FIXTURE_XLSX.read_bytes()
    transport = _mjsp_handler(xlsx_bytes)
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.mjsp_municipios.httpx.Client",
        _client_factory,
    )
    pipeline = MjspMunicipiosPipeline(
        driver=MagicMock(), data_dir=str(FIXTURES), archive_xlsx=True,
    )
    # run_id canônico → bucket 2025-01 só pra estabilidade do path do snapshot.
    pipeline.run_id = "mjsp_municipios_20250115000000"
    return pipeline


class TestArchival:
    def test_snapshot_uri_stamped_on_rows(
        self,
        online_pipeline: MjspMunicipiosPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        assert online_pipeline.stats
        for s in online_pipeline.stats:
            uri = s.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            parts = uri.split("/")
            assert parts[0] == "mjsp_municipios"
            assert parts[1] == "2025-01"
            assert parts[2].endswith(".xlsx")

        sample_uri = online_pipeline.stats[0]["source_snapshot_uri"]
        absolute = archival_root / sample_uri
        assert absolute.exists()

        restored = restore_snapshot(sample_uri)
        assert restored == _FIXTURE_XLSX.read_bytes()
