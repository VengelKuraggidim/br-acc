from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.camara_goiania import (
    CamaraGoianiaPipeline,
    _extract_bio_summary,
    _extract_field,
    _extract_profile_slugs,
    _parse_profile_html,
    _strip_html,
    fetch_to_disk,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CamaraGoianiaPipeline:
    return CamaraGoianiaPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


# ---------------------------------------------------------------------------
# Listing parser — grabs the 28 active slugs and filters legislaturas-anteriores
# ---------------------------------------------------------------------------


_LISTAGEM_HTML_SAMPLE = """
<html><head>
<base href="https://www.goiania.go.leg.br/institucional/parlamentares/Parlamentares_20-Legislatura">
</head><body>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/Bessa">Bessa</a>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/Maria-Oliveira">Maria Oliveira</a>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/legislaturas-anteriores">Legislaturas Anteriores</a>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/legislaturas-anteriores/parlamentares-1">Anteriores 1</a>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/Parlamentares_20-Legislatura">Self ref</a>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/Bessa">Bessa duplicate</a>
</body></html>
"""


class TestExtractProfileSlugs:
    def test_returns_only_active_slugs(self) -> None:
        slugs = _extract_profile_slugs(_LISTAGEM_HTML_SAMPLE)
        assert slugs == ["Bessa", "Maria-Oliveira"]

    def test_dedups_repeated_slugs(self) -> None:
        # Slug repetido na listagem aparece 1 vez só (ordem de primeira ocorrência).
        slugs = _extract_profile_slugs(_LISTAGEM_HTML_SAMPLE)
        assert slugs.count("Bessa") == 1

    def test_filters_self_referential_legislatura_slug(self) -> None:
        # ``<base href>`` da listagem aponta pra própria página
        # ``Parlamentares_20-Legislatura`` — não é vereador, deve sumir.
        slugs = _extract_profile_slugs(_LISTAGEM_HTML_SAMPLE)
        assert "Parlamentares_20-Legislatura" not in slugs


# ---------------------------------------------------------------------------
# Profile parser — replicates the Plone field block observed in /Bessa
# ---------------------------------------------------------------------------


_PROFILE_HTML_SAMPLE = """
<html>
<head><title>Bessa — Câmara Municipal de Goiânia</title></head>
<body>
<div id="content">
  <img src="https://www.goiania.go.leg.br/institucional/parlamentares/Fotos-de-parlamentares/antigas/19_bessa.jpeg/@@images/abc.jpeg" alt="Bessa" />
  <p>
  Partido: Mobiliza
  Nascimento: 17/06/1984
  Telefones: 62 3524-4327 , 3524-4326 e 3524-4325
  E-mail: bessagoiania@gmail.com
  Gabinete: 11
  Natural de São Francisco de Goiás, Wellington de Bessa Oliveira, advogado especializado em Direito do Trabalho, professor de Direito na PUC Goiás. Reeleito vereador para o mandato de 2025-2028 com 6.123 votos.
  Facebook: facebook.com/bessa
  </p>
</div>
</body>
</html>
"""


class TestParseProfileHtml:
    def test_extracts_all_labeled_fields(self) -> None:
        record = _parse_profile_html(
            _PROFILE_HTML_SAMPLE, "Bessa",
            "https://www.goiania.go.leg.br/institucional/parlamentares/Bessa",
        )
        assert record["slug"] == "Bessa"
        assert record["name"] == "Bessa"
        assert record["party"] == "Mobiliza"
        assert record["birth_date"] == "1984-06-17"
        assert "3524-4327" in record["phones"]
        assert record["email"] == "bessagoiania@gmail.com"
        assert record["gabinete"] == "11"
        assert record["photo_url"].startswith(
            "https://www.goiania.go.leg.br/institucional/parlamentares/"
            "Fotos-de-parlamentares/",
        )
        assert record["legislature"] == "20"
        assert record["profile_url"].endswith("/Bessa")

    def test_bio_summary_starts_at_natural_and_excludes_socials(self) -> None:
        record = _parse_profile_html(
            _PROFILE_HTML_SAMPLE, "Bessa",
            "https://www.goiania.go.leg.br/institucional/parlamentares/Bessa",
        )
        assert record["bio_summary"].startswith("de São Francisco")
        assert "Facebook" not in record["bio_summary"]

    def test_missing_fields_default_to_empty(self) -> None:
        minimal_html = (
            "<html><head><title>Foo — Câmara Municipal de Goiânia</title>"
            "</head><body><p>Sem campos.</p></body></html>"
        )
        record = _parse_profile_html(
            minimal_html, "Foo",
            "https://www.goiania.go.leg.br/institucional/parlamentares/Foo",
        )
        assert record["name"] == "Foo"
        assert record["party"] == ""
        assert record["birth_date"] == ""
        assert record["photo_url"] == ""
        assert record["gabinete"] == ""

    def test_strip_html_removes_scripts_and_collapses_spaces(self) -> None:
        text = _strip_html(
            "<p>Hello <script>alert(1)</script> <b>world</b></p>",
        )
        assert text == "Hello world"

    def test_extract_field_stops_at_next_label(self) -> None:
        text = "Partido: PT Nascimento: 01/01/1980"
        assert _extract_field("Partido:", text) == "PT"

    def test_extract_bio_truncates_at_max_chars(self) -> None:
        text = "Natural " + ("a " * 500)
        bio = _extract_bio_summary(text, max_chars=80)
        assert len(bio) <= 81  # +1 pra ellipsis
        assert bio.endswith("…")

    def test_gabinete_only_captures_digits_when_no_natural_prefix(self) -> None:
        # Aava: bio começa direto com o nome (sem "Natural"); gabinete
        # tem que ficar só com o número, sem invadir a biografia.
        html = (
            "<html><head><title>Aava — Câmara Municipal de Goiânia</title>"
            "</head><body>Partido: Mock Nascimento: 20/10/1989 "
            "Telefones: 62 9 9669-2507 E-mail: gabinete@x.com Gabinete: 19 "
            "Aava Santiago é socióloga e ativista, mãe de um garoto.</body>"
            "</html>"
        )
        record = _parse_profile_html(
            html, "Aava-Santiago",
            "https://www.goiania.go.leg.br/institucional/parlamentares/Aava-Santiago",
        )
        assert record["gabinete"] == "19"
        assert "socióloga" in record["bio_summary"]
        assert "Aava" in record["bio_summary"] or "ativista" in record["bio_summary"]


# ---------------------------------------------------------------------------
# Pipeline metadata + offline transform
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert CamaraGoianiaPipeline.name == "camara_goiania"

    def test_source_id(self) -> None:
        assert CamaraGoianiaPipeline.source_id == "camara_goiania"


class TestTransform:
    def test_extract_loads_fixture(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert len(pipeline._raw_vereadores) == 2

    def test_transform_creates_govereador_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.vereadores) == 2
        names = {v["name"] for v in pipeline.vereadores}
        assert "JOAO DA SILVA" in names
        assert "MARIA OLIVEIRA" in names

    def test_transform_keeps_rich_fields(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        joao = next(
            v for v in pipeline.vereadores if v["name"] == "JOAO DA SILVA"
        )
        assert joao["party"] == "PSD"
        assert joao["gabinete"] == "1"
        assert joao["birth_date"] == "1980-05-15"
        assert joao["photo_url"].endswith("joao.jpeg")
        assert joao["uf"] == "GO"
        assert joao["municipality"] == "Goiania"
        assert joao["municipality_code"] == "5208707"
        assert joao["source"] == "camara_goiania"
        assert joao["legislature"] == "20"

    def test_transform_skips_rows_without_name(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        # Inject a malformed row alongside the fixture rows.
        pipeline._raw_vereadores.append({"party": "??", "slug": "no-name"})
        pipeline.transform()
        assert len(pipeline.vereadores) == 2

    def test_provenance_stamped_on_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for item in pipeline.vereadores:
            assert item["source_id"] == "camara_goiania"
            assert item["source_record_id"]
            assert item["source_url"].startswith("http")
            assert item["ingested_at"].startswith("20")
            assert item["run_id"].startswith("camara_goiania_")

    def test_offline_path_does_not_stamp_snapshot_uri(self) -> None:
        # Fixture local não passou por archive_fetch — opt-in deve permanecer
        # ausente (contrato preservado de antes).
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for item in pipeline.vereadores:
            assert "source_snapshot_uri" not in item

    def test_stable_ids_are_deterministic(self) -> None:
        p1 = _make_pipeline()
        p1.extract()
        p1.transform()

        p2 = _make_pipeline()
        p2.extract()
        p2.transform()

        ids1 = {v["vereador_id"] for v in p1.vereadores}
        ids2 = {v["vereador_id"] for v in p2.vereadores}
        assert ids1 == ids2


class TestLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_empty_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.load()


# ---------------------------------------------------------------------------
# fetch_to_disk + archival — listagem + 1 perfil mockados via MockTransport
# ---------------------------------------------------------------------------


_MOCK_LISTAGEM = """
<html><body>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/Mock-Vereador">Mock</a>
<a href="https://www.goiania.go.leg.br/institucional/parlamentares/legislaturas-anteriores">Antigas</a>
</body></html>
"""

_MOCK_PROFILE = """
<html><head><title>Mock Vereador — Câmara Municipal de Goiânia</title></head>
<body><div id="content">
<img src="https://www.goiania.go.leg.br/institucional/parlamentares/Fotos-de-parlamentares/mock.jpeg" alt="Mock" />
Partido: TESTE
Nascimento: 01/01/1990
Telefones: 62 0000-0000
E-mail: mock@cmg.go.leg.br
Gabinete: 99
Natural de Cidade Mock, biografia curta de teste.
</div></body></html>
"""


def _camara_handler() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/institucional/parlamentares/":
            body = _MOCK_LISTAGEM
        elif path.endswith("/Mock-Vereador"):
            body = _MOCK_PROFILE
        else:
            return httpx.Response(404, content=b"not found")
        return httpx.Response(
            200,
            content=body.encode("utf-8"),
            headers={"content-type": "text/html; charset=utf-8"},
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
def patched_httpx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _camara_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.camara_goiania.httpx.Client", _client_factory,
    )
    # Sleep entre requests não agrega aqui — patcheia pra zero.
    monkeypatch.setattr(
        "bracc_etl.pipelines.camara_goiania.time.sleep", lambda _s: None,
    )


class TestFetchToDisk:
    def test_writes_vereadores_json_and_raw_html(
        self,
        tmp_path: Path,
        patched_httpx: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        out_dir = tmp_path / "out"
        written = fetch_to_disk(out_dir)

        assert len(written) == 1
        assert written[0].name == "vereadores.json"
        # Raw HTML preservado em out/raw/.
        assert (out_dir / "raw" / "parlamentares.html").exists()
        assert (out_dir / "raw" / "perfil_Mock-Vereador.html").exists()

    def test_archival_stamps_snapshot_uri_and_roundtrips(
        self,
        tmp_path: Path,
        patched_httpx: None,  # noqa: ARG002
        archival_root: Path,
    ) -> None:
        out_dir = tmp_path / "out"
        fetch_to_disk(out_dir)

        # vereadores.json carrega __snapshot_uri por vereador.
        import json
        rows = json.loads(
            (out_dir / "vereadores.json").read_text(encoding="utf-8"),
        )
        assert len(rows) == 1
        uri = rows[0]["__snapshot_uri"]
        assert uri.startswith("camara_goiania/")
        assert uri.endswith(".html")
        # Round-trip: bytes preservados.
        restored = restore_snapshot(uri)
        assert b"Mock Vereador" in restored
        # E o arquivo físico existe sob o root configurado.
        assert (archival_root / uri).exists()

    def test_pipeline_consumes_fetch_output_and_propagates_uri(
        self,
        tmp_path: Path,
        patched_httpx: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        # fetch_to_disk grava em data_dir/camara_goiania/, que é o que o
        # pipeline lê. Isso fecha o loop online → on-disk → ingest.
        data_dir = tmp_path / "data"
        out_dir = data_dir / "camara_goiania"
        fetch_to_disk(out_dir)

        pipeline = CamaraGoianiaPipeline(
            driver=MagicMock(), data_dir=str(data_dir),
        )
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.vereadores) == 1
        v = pipeline.vereadores[0]
        assert v["party"] == "TESTE"
        assert v["gabinete"] == "99"
        # URI carimbada em :GoVereador via attach_provenance.
        assert v["source_snapshot_uri"].startswith("camara_goiania/")

    def test_archival_disabled_skips_uri(
        self,
        tmp_path: Path,
        patched_httpx: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        out_dir = tmp_path / "out"
        fetch_to_disk(out_dir, archival=False)

        import json
        rows = json.loads(
            (out_dir / "vereadores.json").read_text(encoding="utf-8"),
        )
        assert "__snapshot_uri" not in rows[0]

    def test_limit_caps_profiles_fetched(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        # Listagem com 3 slugs ativos; limit=2 deve baixar só 2 perfis.
        listagem = """
        <a href="https://www.goiania.go.leg.br/institucional/parlamentares/A">A</a>
        <a href="https://www.goiania.go.leg.br/institucional/parlamentares/B">B</a>
        <a href="https://www.goiania.go.leg.br/institucional/parlamentares/C">C</a>
        """

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/institucional/parlamentares/":
                body = listagem
            elif path.endswith(("/A", "/B", "/C")):
                slug = path.rsplit("/", 1)[1]
                body = (
                    f"<html><head><title>{slug} — Câmara Municipal de Goiânia"
                    f"</title></head><body>Partido: P{slug}</body></html>"
                )
            else:
                return httpx.Response(404, content=b"")
            return httpx.Response(
                200,
                content=body.encode("utf-8"),
                headers={"content-type": "text/html; charset=utf-8"},
            )

        transport = httpx.MockTransport(handler)
        original_client = httpx.Client

        def _factory(*args: Any, **kwargs: Any) -> httpx.Client:
            kwargs["transport"] = transport
            return original_client(*args, **kwargs)

        monkeypatch.setattr(
            "bracc_etl.pipelines.camara_goiania.httpx.Client", _factory,
        )
        monkeypatch.setattr(
            "bracc_etl.pipelines.camara_goiania.time.sleep", lambda _s: None,
        )

        out_dir = tmp_path / "out"
        fetch_to_disk(out_dir, limit=2, archival=False)

        import json
        rows = json.loads(
            (out_dir / "vereadores.json").read_text(encoding="utf-8"),
        )
        slugs = {r["slug"] for r in rows}
        assert slugs == {"A", "B"}
