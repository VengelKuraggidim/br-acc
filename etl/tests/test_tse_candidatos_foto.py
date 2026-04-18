"""Tests pro pipeline ``tse_candidatos_foto``.

Cobre:

* metadata + registry wiring;
* discovery — query Cypher mockada lista candidatos GO sem foto;
* extract — happy path (fetch + archival), placeholder TSE (skip),
  ano sem mapping, HTTP error, content-type não-imagem;
* load — Cypher SET com sq_candidato + props + proveniência prefixada
  ``foto_*``.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.tse_candidatos_foto import (
    _ANO_TO_CD_ELEICAO,
    _SOURCE_ID,
    _TSE_PLACEHOLDER_SHA256,
    _TSE_PLACEHOLDER_SIZE,
    TseCandidatosFotoPipeline,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures: payloads sintéticos (binários determinísticos)
# ---------------------------------------------------------------------------

# JPG real (não-placeholder) — bytes determinísticos pra hash content-address.
_FAKE_JPG_BYTES = b"\xff\xd8\xff\xe0" + b"fake-tse-photo-payload-A" * 8
_FAKE_JPG_BYTES_2 = b"\xff\xd8\xff\xe0" + b"fake-tse-photo-payload-B" * 8

# Placeholder TSE: 4704 bytes idênticos ao SHA hardcoded no pipeline.
# Geramos um payload com size correto e hash que o pipeline reconhece.
# Em vez de baixar o real, criamos um stub com o SHA esperado pelo
# código — o teste valida a *lógica* de detecção, não a aparência do
# binário. Pra isso fingimos: o teste mocka o byte string `b"X"*4704`
# e patcheia o constante de SHA via monkeypatch.
_PLACEHOLDER_FAKE_BYTES = b"X" * _TSE_PLACEHOLDER_SIZE
_PLACEHOLDER_FAKE_SHA = hashlib.sha256(_PLACEHOLDER_FAKE_BYTES).hexdigest()


def _photo_url_2022(sq: str) -> str:
    return (
        f"https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/img/"
        f"{_ANO_TO_CD_ELEICAO[2022]}/{sq}/GO"
    )


def _photo_url_2024(sq: str) -> str:
    return (
        f"https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/img/"
        f"{_ANO_TO_CD_ELEICAO[2024]}/{sq}/GO"
    )


def _build_transport(
    *,
    placeholder_for_sq: set[str] | None = None,
    http_500_for_sq: set[str] | None = None,
    html_for_sq: set[str] | None = None,
) -> httpx.MockTransport:
    """Roteador HTTP que devolve foto / placeholder / erro conforme sq.

    ``placeholder_for_sq``: devolve o stub de placeholder (4704 bytes).
    ``http_500_for_sq``: devolve HTTP 500.
    ``html_for_sq``: devolve text/html (simula erro de CDN servindo
    página de erro com HTTP 200).
    Default: devolve um JPG válido distinto por sq (varia o byte
    extra pra evitar colisão de SHA no archival).
    """
    placeholder_for_sq = placeholder_for_sq or set()
    http_500_for_sq = http_500_for_sq or set()
    html_for_sq = html_for_sq or set()

    def handler(request: httpx.Request) -> httpx.Response:
        # URL formato: .../img/{cd_eleicao}/{sq}/GO
        path = request.url.path
        parts = path.rstrip("/").split("/")
        sq = parts[-2] if len(parts) >= 3 else ""

        if sq in http_500_for_sq:
            return httpx.Response(500, content=b"server error")
        if sq in html_for_sq:
            return httpx.Response(
                200,
                content=b"<html>error</html>",
                headers={"content-type": "text/html"},
            )
        if sq in placeholder_for_sq:
            return httpx.Response(
                200,
                content=_PLACEHOLDER_FAKE_BYTES,
                headers={"content-type": "image/jpeg"},
            )
        # Default: JPG real, byte-distinct por sq (suffix do sq diferencia).
        body = _FAKE_JPG_BYTES + sq.encode("utf-8")
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "image/jpeg"},
        )

    return httpx.MockTransport(handler)


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


def _mock_driver_with_targets(targets: list[dict[str, Any]]) -> MagicMock:
    """Driver Neo4j mockado com `session.run` devolvendo ``targets``."""
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value

    # Cada record é um objeto que responde a ``record.get(key)``. Usamos
    # MagicMock parametrizado com side_effect.
    def make_record(d: dict[str, Any]) -> MagicMock:
        rec = MagicMock()
        rec.get.side_effect = lambda k, _default=None, _d=d: _d.get(k, _default)
        return rec

    session.run.return_value = iter([make_record(t) for t in targets])
    return driver


def _make_pipeline(
    *,
    driver: MagicMock,
    transport: httpx.MockTransport,
    batch_size: int = 50,
) -> TseCandidatosFotoPipeline:
    return TseCandidatosFotoPipeline(
        driver=driver,
        data_dir="./data",
        batch_size=batch_size,
        throttle_seconds=0.0,
        sleep_fn=lambda _s: None,
        http_client_factory=lambda: httpx.Client(
            transport=transport,
            follow_redirects=True,
        ),
    )


# ---------------------------------------------------------------------------
# Metadata / registry wiring
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert TseCandidatosFotoPipeline.name == "tse_candidatos_foto"

    def test_source_id(self) -> None:
        assert TseCandidatosFotoPipeline.source_id == _SOURCE_ID

    def test_supported_years_cover_recent_cycles(self) -> None:
        # Garante que ciclos eleitorais recentes estão mapeados.
        assert 2018 in _ANO_TO_CD_ELEICAO
        assert 2020 in _ANO_TO_CD_ELEICAO
        assert 2022 in _ANO_TO_CD_ELEICAO
        assert 2024 in _ANO_TO_CD_ELEICAO

    def test_placeholder_constants_consistent(self) -> None:
        # Sanity: o SHA hardcoded deve ter formato hex 64 chars.
        assert len(_TSE_PLACEHOLDER_SHA256) == 64
        assert all(c in "0123456789abcdef" for c in _TSE_PLACEHOLDER_SHA256)
        assert _TSE_PLACEHOLDER_SIZE == 4704


# ---------------------------------------------------------------------------
# Discovery — Cypher mockado retorna candidatos GO sem foto
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_discover_filters_to_target_shape(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "RONALDO CAIADO", "year": 2022},
            {"sq_candidato": "90001615815", "name": "GUSTAVO MENDANHA", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(
            driver=driver, transport=_build_transport(),
        )
        discovered = p._discover_targets()
        assert len(discovered) == 2
        sqs = {t["sq_candidato"] for t in discovered}
        assert sqs == {"90001646326", "90001615815"}
        assert all(t["year"] == 2022 for t in discovered)

    def test_discover_handles_query_failure(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        driver = MagicMock()
        driver.session.return_value.__enter__.side_effect = RuntimeError(
            "neo4j down",
        )
        p = _make_pipeline(driver=driver, transport=_build_transport())
        # Não levanta — apenas retorna lista vazia + log warning.
        assert p._discover_targets() == []

    def test_discover_skips_empty_sq_candidato(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets: list[dict[str, Any]] = [
            {"sq_candidato": "90001646326", "name": "VALIDO", "year": 2022},
            {"sq_candidato": "", "name": "VAZIO", "year": 2022},
            {"sq_candidato": "90001615815", "name": "OUTRO", "year": None},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(driver=driver, transport=_build_transport())
        discovered = p._discover_targets()
        assert len(discovered) == 1
        assert discovered[0]["sq_candidato"] == "90001646326"

    def test_discover_respects_limit(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": f"9000{i:07d}", "name": f"NAME {i}", "year": 2022}
            for i in range(10)
        ]
        driver = _mock_driver_with_targets(targets)
        p = TseCandidatosFotoPipeline(
            driver=driver,
            data_dir="./data",
            limit=3,
            throttle_seconds=0.0,
            sleep_fn=lambda _s: None,
            http_client_factory=lambda: httpx.Client(
                transport=_build_transport(),
            ),
        )
        assert len(p._discover_targets()) == 3


# ---------------------------------------------------------------------------
# Extract — happy path + edge cases
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_archives_real_photos(
        self, archival_root: Path,
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "CAIADO", "year": 2022},
            {"sq_candidato": "90001615815", "name": "MENDANHA", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()

        # 2 atualizações, 2 binários no archival.
        assert p._stats["matched"] == 2
        assert len(p._updates) == 2
        foto_dir = archival_root / _SOURCE_ID
        jpg_files = list(foto_dir.rglob("*.jpg"))
        assert len(jpg_files) == 2

    def test_extract_skips_placeholder(
        self, archival_root: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Patcheia o SHA hardcoded pro stub determinístico do teste — o
        # pipeline real reconhece o SHA do TSE; aqui validamos a lógica
        # com um SHA de teste sem precisar baixar o binário real do TSE.
        monkeypatch.setattr(
            "bracc_etl.pipelines.tse_candidatos_foto._TSE_PLACEHOLDER_SHA256",
            _PLACEHOLDER_FAKE_SHA,
        )
        targets = [
            {"sq_candidato": "90001646326", "name": "REAL", "year": 2022},
            {"sq_candidato": "99999999999", "name": "PLACEHOLDER", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        transport = _build_transport(placeholder_for_sq={"99999999999"})
        p = _make_pipeline(driver=driver, transport=transport)
        p.extract()

        assert p._stats["matched"] == 1
        assert p._stats["skipped_placeholder"] == 1
        # Apenas o sq real entra nos updates.
        assert {u["sq_candidato"] for u in p._updates} == {"90001646326"}

    def test_extract_skips_unsupported_year(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "10002", "name": "ANTIGO", "year": 2002},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()
        assert p._stats["skipped_unsupported_year"] == 1
        assert p._stats["matched"] == 0
        assert p._updates == []

    def test_extract_skips_http_error(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "OK", "year": 2022},
            {"sq_candidato": "90001615815", "name": "FAIL", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        transport = _build_transport(http_500_for_sq={"90001615815"})
        p = _make_pipeline(driver=driver, transport=transport)
        p.extract()
        assert p._stats["skipped_http_error"] == 1
        assert p._stats["matched"] == 1

    def test_extract_skips_non_image_response(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "HTML", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        transport = _build_transport(html_for_sq={"90001646326"})
        p = _make_pipeline(driver=driver, transport=transport)
        p.extract()
        assert p._stats["skipped_non_image"] == 1
        assert p._stats["matched"] == 0

    def test_extract_uses_correct_cd_eleicao_per_year(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        # Captura a URL chamada — deve carregar o cd_eleicao certo por ano.
        seen_urls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_urls.append(str(request.url))
            return httpx.Response(
                200,
                content=_FAKE_JPG_BYTES_2 + str(request.url).encode("utf-8"),
                headers={"content-type": "image/jpeg"},
            )

        targets = [
            {"sq_candidato": "AAA", "name": "C2022", "year": 2022},
            {"sq_candidato": "BBB", "name": "C2024", "year": 2024},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(
            driver=driver,
            transport=httpx.MockTransport(handler),
        )
        p.extract()
        assert any(_ANO_TO_CD_ELEICAO[2022] in u for u in seen_urls)
        assert any(_ANO_TO_CD_ELEICAO[2024] in u for u in seen_urls)
        # URL termina em /GO
        for url in seen_urls:
            assert url.endswith("/GO")

    def test_extract_empty_targets_short_circuits(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        driver = _mock_driver_with_targets([])
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()
        assert p.rows_in == 0
        assert p._updates == []


# ---------------------------------------------------------------------------
# Transform + Load — snapshot URI propagada, Cypher SET com prov prefix
# ---------------------------------------------------------------------------


class TestTransform:
    def test_transform_counts_rows_loaded(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "C1", "year": 2022},
            {"sq_candidato": "90001615815", "name": "C2", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()
        p.transform()
        assert p.rows_loaded == 2

    def test_update_carries_snapshot_uri_with_source_prefix(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "C1", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()
        upd = p._updates[0]
        assert upd["foto_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
        assert upd["foto_snapshot_uri"].endswith(".jpg")
        assert upd["foto_content_type"] == "image/jpeg"
        assert upd["foto_url"].startswith(
            "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/img/",
        )


class TestLoad:
    def test_load_calls_session_run_with_prov_fields(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"sq_candidato": "90001646326", "name": "C1", "year": 2022},
        ]
        driver = _mock_driver_with_targets(targets)
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()
        p.transform()

        # Reseta o mock pra observar SÓ o call do load (discovery já
        # consumiu o iter de targets).
        driver_mock = cast("MagicMock", p.driver)
        session_cm = driver_mock.session.return_value
        session = session_cm.__enter__.return_value
        session.run.reset_mock()

        p.load()
        # session.run chamado uma vez no load (UNWIND batch).
        assert session.run.call_count == 1
        call = session.run.call_args
        cypher = call.args[0]
        params = call.args[1]
        assert "MATCH (p:Person {sq_candidato: row.sq_candidato})" in cypher
        assert "SET p.foto_url" in cypher
        assert "p.foto_source_id = $source_id" in cypher
        assert params["source_id"] == _SOURCE_ID
        assert params["run_id"].startswith(f"{_SOURCE_ID}_")
        assert params["ingested_at"]
        assert len(params["rows"]) == 1
        row = params["rows"][0]
        assert row["sq_candidato"] == "90001646326"
        assert row["foto_url"]
        assert row["foto_snapshot_uri"]
        assert row["foto_content_type"] == "image/jpeg"

    def test_load_noop_when_no_updates(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        driver = _mock_driver_with_targets([])
        p = _make_pipeline(driver=driver, transport=_build_transport())
        p.extract()
        p.transform()

        driver_mock = cast("MagicMock", p.driver)
        session_cm = driver_mock.session.return_value
        session = session_cm.__enter__.return_value
        session.run.reset_mock()

        p.load()
        # Nenhum update -> nenhum run extra.
        assert session.run.call_count == 0
