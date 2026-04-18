"""Tests for the ``alego_deputados_foto`` pipeline.

Cobre:

* parsing dos selectors HTML (listagem + perfil) com fixtures reduzidas;
* archival — cada fetch (listing + perfil + foto binária) gera snapshot;
* matching com ``StateLegislator`` do ``alego.py`` via ``_hash_id``;
* foto ausente / fetch falha → skip gracioso, não aborta;
* listagem vazia → ``RuntimeError`` explícito;
* propagação de ``foto_url`` / ``foto_snapshot_uri`` / ``foto_content_type``.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.alego import _hash_id
from bracc_etl.pipelines.alego_deputados_foto import (
    _SOURCE_ID,
    AlegoDeputadosFotoPipeline,
    _parse_listing,
    _parse_photo_url,
)
from bracc_etl.transforms import normalize_name

if TYPE_CHECKING:
    from collections.abc import Iterator


_FIXTURES = Path(__file__).parent / "fixtures" / "alego_deputados_foto"

# Fake binários determinísticos (content-addressed → hash estável → idempotente).
_FAKE_JPG = b"\xff\xd8\xff\xe0fake-jpeg-808-payload"
_FAKE_PNG = b"\x89PNG\r\n\x1a\nfake-png-137-payload"


def _load_fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def _build_transport() -> httpx.MockTransport:
    """Roteia listagem, perfis e fotos do portal/saba ALEGO via fixtures locais."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        # Fotos binárias do saba (URLs com hash opaco). Mapeamos por substring.
        if "saba.al.go.leg.br" in url and "HASH808" in url:
            return httpx.Response(
                200, content=_FAKE_JPG, headers={"content-type": "image/jpeg"},
            )
        if "saba.al.go.leg.br" in url and "HASH137" in url:
            return httpx.Response(
                200, content=_FAKE_PNG, headers={"content-type": "image/png"},
            )
        # Listagem (com redirect 302 → /em-exercicio).
        if url.endswith("/deputados") and "perfil" not in url:
            return httpx.Response(
                302,
                headers={"location": "https://portal.al.go.leg.br/deputados/em-exercicio"},
            )
        if url.endswith("/deputados/em-exercicio"):
            return httpx.Response(
                200,
                content=_load_fixture("listing.html"),
                headers={"content-type": "text/html; charset=utf-8"},
            )
        # Perfis individuais.
        if url.endswith("/deputados/perfil/808"):
            return httpx.Response(
                200,
                content=_load_fixture("perfil_808.html"),
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if url.endswith("/deputados/perfil/137"):
            return httpx.Response(
                200,
                content=_load_fixture("perfil_137.html"),
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if url.endswith("/deputados/perfil/51"):
            return httpx.Response(
                200,
                content=_load_fixture("perfil_51_no_photo.html"),
                headers={"content-type": "text/html; charset=utf-8"},
            )
        return httpx.Response(404, content=b"unhandled " + url.encode("utf-8"))

    return httpx.MockTransport(handler)


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


@pytest.fixture()
def pipeline(
    archival_root: Path,  # noqa: ARG001 — só ativa o env var
) -> AlegoDeputadosFotoPipeline:
    transport = _build_transport()
    return AlegoDeputadosFotoPipeline(
        driver=MagicMock(),
        data_dir="./data",
        http_client_factory=lambda: httpx.Client(
            transport=transport, follow_redirects=True,
        ),
        rate_limit_seconds=0.0,  # tests não querem dormir.
    )


# ---------------------------------------------------------------------------
# Selector unit tests — confirmam que o parser pega os campos certos.
# ---------------------------------------------------------------------------


class TestParseListing:
    def test_extracts_three_unique_deputies(self) -> None:
        html = _load_fixture("listing.html").decode("utf-8")
        entries = _parse_listing(html)
        assert len(entries) == 3
        ids = {dep_id for dep_id, _ in entries}
        assert ids == {"808", "137", "51"}

    def test_first_occurrence_wins_for_duplicates(self) -> None:
        # A listagem real expõe cada deputado 2x (tabela + sidebar).
        # ``_parse_listing`` deve dedupar mas preservar o nome.
        html = _load_fixture("listing.html").decode("utf-8")
        entries = dict(_parse_listing(html))
        assert entries["808"] == "Alessandro Moreira"
        assert entries["137"] == "Amauri Ribeiro"

    def test_returns_empty_on_unrelated_html(self) -> None:
        assert _parse_listing("<html><body>nada aqui</body></html>") == []


class TestParsePhotoUrl:
    def test_extracts_saba_url(self) -> None:
        html = _load_fixture("perfil_808.html").decode("utf-8")
        url = _parse_photo_url(html)
        assert url == (
            "https://saba.al.go.leg.br/v1/view/portal/public/HASH808==?t=1776546035"
        )

    def test_handles_extra_classes(self) -> None:
        # ``class="foto destaque"`` — composição de classes não pode quebrar.
        html = _load_fixture("perfil_137.html").decode("utf-8")
        url = _parse_photo_url(html)
        assert url is not None
        assert "HASH137" in url

    def test_returns_none_when_missing(self) -> None:
        html = _load_fixture("perfil_51_no_photo.html").decode("utf-8")
        assert _parse_photo_url(html) is None

    def test_handles_attribute_order_swap(self) -> None:
        # Fallback regex pra ``<img src="..." class="foto">``.
        html = (
            '<img src="https://example.com/x.jpg" class="foto" alt="foto">'
        )
        assert _parse_photo_url(html) == "https://example.com/x.jpg"


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert AlegoDeputadosFotoPipeline.name == "alego_deputados_foto"

    def test_source_id(self) -> None:
        assert AlegoDeputadosFotoPipeline.source_id == _SOURCE_ID

    def test_source_id_constant(self) -> None:
        assert _SOURCE_ID == "alego_deputados_foto"


# ---------------------------------------------------------------------------
# extract — HTTP + archival
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_collects_three_deputies(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        # 3 deputados na fixture (incluindo um sem foto).
        assert len(pipeline._raw_deputies) == 3
        ids = {dep["deputy_id"] for dep in pipeline._raw_deputies}
        assert ids == {"808", "137", "51"}

    def test_extract_archives_listing_html(
        self,
        pipeline: AlegoDeputadosFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # 1 listing + 3 perfis = 4 HTMLs distintos.
        html_files = list((archival_root / _SOURCE_ID).rglob("*.html"))
        assert len(html_files) >= 4

    def test_extract_archives_photo_binaries(
        self,
        pipeline: AlegoDeputadosFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # 1 JPEG (808) + 1 PNG (137) — perfil 51 não tem foto.
        jpgs = list((archival_root / _SOURCE_ID).rglob("*.jpg"))
        pngs = list((archival_root / _SOURCE_ID).rglob("*.png"))
        assert len(jpgs) == 1
        assert len(pngs) == 1

    def test_extract_captures_photo_metadata(
        self,
        pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        by_id = {dep["deputy_id"]: dep for dep in pipeline._raw_deputies}
        assert by_id["808"]["photo_content_type"] == "image/jpeg"
        assert by_id["808"]["photo_snapshot_uri"]
        assert by_id["808"]["photo_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
        assert by_id["137"]["photo_content_type"] == "image/png"
        # Perfil sem foto: tudo None mas o registro ainda existe (graceful).
        assert by_id["51"]["photo_url"] is None
        assert by_id["51"]["photo_snapshot_uri"] is None
        assert by_id["51"]["photo_content_type"] is None

    def test_extract_respects_limit(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        transport = _build_transport()
        p = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            rate_limit_seconds=0.0,
            limit=1,
        )
        p.extract()
        assert len(p._raw_deputies) == 1


class TestExtractFailureModes:
    def test_listing_fetch_failure_raises(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(503, content=b"down")

        p = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(handler),
            ),
            rate_limit_seconds=0.0,
        )
        with pytest.raises(RuntimeError, match="failed to fetch listing"):
            p.extract()

    def test_listing_with_zero_matches_raises(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            # Listagem volta 200 mas com HTML que NÃO bate o selector
            # (simula portal mudando estrutura).
            if url.endswith("/deputados") or url.endswith("/em-exercicio"):
                return httpx.Response(
                    200,
                    content=b"<html><body>portal mudou</body></html>",
                    headers={"content-type": "text/html"},
                )
            return httpx.Response(404)

        p = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(handler), follow_redirects=True,
            ),
            rate_limit_seconds=0.0,
        )
        with pytest.raises(RuntimeError, match="parsed 0 deputies"):
            p.extract()

    def test_profile_fetch_failure_skips_deputy(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url.endswith("/deputados") or url.endswith("/em-exercicio"):
                return httpx.Response(
                    200 if url.endswith("/em-exercicio") else 302,
                    content=_load_fixture("listing.html") if url.endswith("/em-exercicio") else b"",
                    headers=(
                        {"content-type": "text/html"}
                        if url.endswith("/em-exercicio")
                        else {"location": "https://portal.al.go.leg.br/deputados/em-exercicio"}
                    ),
                )
            # Todos os perfis devolvem 500 — todos os deputados pulados.
            if "/perfil/" in url:
                return httpx.Response(500, content=b"oops")
            return httpx.Response(404)

        p = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(handler), follow_redirects=True,
            ),
            rate_limit_seconds=0.0,
        )
        p.extract()
        # Listagem teve 3, mas todos perfis falharam → 0 raw_deputies.
        assert len(p._raw_deputies) == 0

    def test_photo_fetch_failure_keeps_deputy_without_snapshot(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url.endswith("/deputados"):
                return httpx.Response(
                    302,
                    headers={"location": "https://portal.al.go.leg.br/deputados/em-exercicio"},
                )
            if url.endswith("/em-exercicio"):
                return httpx.Response(
                    200,
                    content=_load_fixture("listing.html"),
                    headers={"content-type": "text/html"},
                )
            if "/perfil/" in url:
                return httpx.Response(
                    200,
                    content=_load_fixture("perfil_808.html"),
                    headers={"content-type": "text/html"},
                )
            if "saba.al.go.leg.br" in url:
                # CDN devolve HTML de erro com 200 — deve ser rejeitado.
                return httpx.Response(
                    200, content=b"<html>erro</html>",
                    headers={"content-type": "text/html"},
                )
            return httpx.Response(404)

        p = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(handler), follow_redirects=True,
            ),
            rate_limit_seconds=0.0,
        )
        p.extract()
        # Cada deputy tem entry, mas photo_snapshot_uri é None.
        for dep in p._raw_deputies:
            assert dep["photo_snapshot_uri"] is None
            assert dep["photo_content_type"] is None


# ---------------------------------------------------------------------------
# transform — matching key + provenance
# ---------------------------------------------------------------------------


class TestTransform:
    def test_transform_produces_legislator_per_deputy(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.legislators) == 3

    def test_legislator_id_matches_alego_pipeline_formula(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        """``legislator_id`` deve bater com o do ``alego.py`` pra MERGE."""
        pipeline.extract()
        pipeline.transform()
        by_alego_id = {leg["alego_deputy_id"]: leg for leg in pipeline.legislators}
        # Alego.py calcula: _hash_id(normalize_name(nome), cpf_digits[-4:] or "", legislature)
        # Como upstream não tem CPF/legislatura: _hash_id("ALESSANDRO MOREIRA", "", "")
        expected_808 = _hash_id(normalize_name("Alessandro Moreira"), "", "")
        expected_137 = _hash_id(normalize_name("Amauri Ribeiro"), "", "")
        assert by_alego_id["808"]["legislator_id"] == expected_808
        assert by_alego_id["137"]["legislator_id"] == expected_137

    def test_propagates_photo_props(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        by_alego_id = {leg["alego_deputy_id"]: leg for leg in pipeline.legislators}
        leg_808 = by_alego_id["808"]
        assert leg_808["foto_url"].startswith("https://saba.al.go.leg.br/")
        assert leg_808["foto_content_type"] == "image/jpeg"
        assert leg_808["foto_snapshot_uri"]
        assert leg_808["foto_snapshot_uri"].endswith(".jpg")
        # url_foto (alias) carrega a mesma URL pra parity com camara_politicos_go.
        assert leg_808["url_foto"] == leg_808["foto_url"]

    def test_deputy_without_photo_still_carries_legislator_id(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        by_alego_id = {leg["alego_deputy_id"]: leg for leg in pipeline.legislators}
        leg_51 = by_alego_id["51"]
        assert leg_51["foto_url"] is None
        assert leg_51["foto_snapshot_uri"] is None
        assert leg_51["foto_content_type"] is None
        # Mas o legislator_id e o stub estão lá pra MERGE no node existente.
        assert leg_51["legislator_id"]
        assert leg_51["uf"] == "GO"
        assert leg_51["scope"] == "estadual"

    def test_provenance_uses_profile_url_and_snapshot(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for leg in pipeline.legislators:
            assert leg["source_id"] == _SOURCE_ID
            assert leg["source_url"].startswith(
                "https://portal.al.go.leg.br/deputados/perfil/",
            )
            assert leg["run_id"].startswith(f"{_SOURCE_ID}_")
            # Profile snapshot URI deve estar carimbado.
            assert leg["source_snapshot_uri"]
            assert leg["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
            assert leg["source_record_id"] in {"808", "137", "51"}


# ---------------------------------------------------------------------------
# load — atualiza :StateLegislator (mock driver)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_calls_session_for_state_legislator(
        self, pipeline: AlegoDeputadosFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        driver_mock = cast("MagicMock", pipeline.driver)
        session = driver_mock.session.return_value.__enter__.return_value
        # Pelo menos 1 batch de StateLegislator + ingestion run upserts.
        assert session.run.call_count >= 1
        # Confere que o MERGE alvo é :StateLegislator (snippet do query).
        all_queries = [call.args[0] for call in session.run.call_args_list]
        assert any("MERGE (n:StateLegislator" in q for q in all_queries)

    def test_load_noop_when_listing_empty(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        # Listing vazia → RuntimeError no extract; nada chega no load.
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url.endswith("/deputados"):
                return httpx.Response(
                    302,
                    headers={"location": "https://portal.al.go.leg.br/deputados/em-exercicio"},
                )
            return httpx.Response(
                200,
                content=b"<html><body>vazio</body></html>",
                headers={"content-type": "text/html"},
            )

        p = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(handler), follow_redirects=True,
            ),
            rate_limit_seconds=0.0,
        )
        with pytest.raises(RuntimeError):
            p.extract()
        assert p.legislators == []


# ---------------------------------------------------------------------------
# Idempotência archival (content-addressed)
# ---------------------------------------------------------------------------


class TestArchivalIdempotency:
    def test_two_runs_share_same_snapshot_uri(
        self,
        pipeline: AlegoDeputadosFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        first_uris: set[str] = set()
        for dep in pipeline._raw_deputies:
            if dep["photo_snapshot_uri"]:
                first_uris.add(dep["photo_snapshot_uri"])
        # Re-roda outra extract com pipeline novo — bytes idênticos →
        # mesmo hash → mesma URI → archive_fetch é idempotente.
        transport = _build_transport()
        p2 = AlegoDeputadosFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            rate_limit_seconds=0.0,
        )
        p2.extract()
        second_uris: set[str] = set()
        for dep in p2._raw_deputies:
            if dep["photo_snapshot_uri"]:
                second_uris.add(dep["photo_snapshot_uri"])
        assert first_uris == second_uris
        # Sanity: archival root só tem 2 fotos (uma por deputy com foto)
        # mesmo após 2 runs.
        all_photos = list((archival_root / _SOURCE_ID).rglob("*.jpg")) + list(
            (archival_root / _SOURCE_ID).rglob("*.png"),
        )
        assert len(all_photos) == 2


def test_unused_typing_import_is_referenced() -> None:
    """Sanity: garante que ``Any`` é alcançável em runtime (silence linter)."""
    x: Any = 1
    assert x == 1
