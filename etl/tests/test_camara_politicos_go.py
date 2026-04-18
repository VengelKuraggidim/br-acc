"""Tests for the ``camara_politicos_go`` pipeline.

Covers:

* happy path — ingests GO federal deputies + CEAP expenses from
  mocked Câmara API JSON payloads;
* archival — cada fetch produz um snapshot e carimba ``source_snapshot_uri``;
* provenance — ``attach_provenance`` é chamado em todo nó/relação;
* scope — deputados fora de GO são descartados defensivamente;
* LGPD — CPF é mascarado (``mask_cpf`` pattern).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.camara_politicos_go import (
    _SOURCE_ID_CADASTRO,
    _SOURCE_ID_CEAP,
    _SOURCE_ID_FOTO,
    CamaraPoliticosGoPipeline,
)

# Fake 1x1 PNG/JPG bytes — só precisa ser determinístico pra hash content-address.
_FAKE_PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"fake-png-payload-1001"
_FAKE_JPG_BYTES = b"\xff\xd8\xff\xe0" + b"fake-jpg-payload-1002"

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# HTTP fixtures — MockTransport simulando a API da Câmara.
# ---------------------------------------------------------------------------


def _deputy_listing_payload() -> dict[str, Any]:
    """Listagem de /deputados?siglaUf=GO com 2 deputados, sem paginação."""
    return {
        "dados": [
            {
                "id": 1001,
                "nome": "DEPUTADO UM",
                "siglaPartido": "XYZ",
                "siglaUf": "GO",
                "email": "d1@camara.leg.br",
                "urlFoto": "https://example.gov.br/foto1.jpg",
            },
            {
                "id": 1002,
                "nome": "DEPUTADA DOIS",
                "siglaPartido": "ABC",
                "siglaUf": "GO",
                "email": "d2@camara.leg.br",
                "urlFoto": "https://example.gov.br/foto2.jpg",
            },
        ],
        "links": [],
    }


def _deputy_detail_payload(deputy_id: int, cpf: str, nome: str) -> dict[str, Any]:
    return {
        "dados": {
            "id": deputy_id,
            "cpf": cpf,
            "nomeCivil": nome,
            "ultimoStatus": {
                "nomeEleitoral": nome,
                "siglaPartido": "XYZ",
                "siglaUf": "GO",
                "situacao": "Exercicio",
                "urlFoto": f"https://example.gov.br/foto{deputy_id}.jpg",
                "idLegislatura": 57,
                "gabinete": {"email": f"d{deputy_id}@camara.leg.br"},
            },
        },
    }


def _ceap_payload(deputy_id: int, ano: int) -> dict[str, Any]:
    """Página única de despesas CEAP pro deputado x ano."""
    return {
        "dados": [
            {
                "ano": ano,
                "mes": 3,
                "tipoDespesa": "COMBUSTIVEIS E LUBRIFICANTES",
                "cnpjCpfFornecedor": "12345678000199",
                "nomeFornecedor": "POSTO EXEMPLO LTDA",
                "numDocumento": f"NF-{deputy_id}-{ano}-0001",
                "valorLiquido": 500.00,
            },
            {
                "ano": ano,
                "mes": 4,
                "tipoDespesa": "PASSAGENS AEREAS",
                "cnpjCpfFornecedor": "98765432000155",
                "nomeFornecedor": "CIA AEREA EXEMPLO",
                "numDocumento": f"NF-{deputy_id}-{ano}-0002",
                "valorLiquido": 1500.00,
            },
        ],
        "links": [],
    }


def _build_transport() -> httpx.MockTransport:
    """Roteador HTTP que responde os endpoints da Câmara usados pelo pipeline."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        headers = {"content-type": "application/json; charset=utf-8"}
        # Fotos binárias da Câmara (mockadas em example.gov.br nas fixtures).
        # Deputy 1001 responde PNG; 1002 responde JPEG — garante cobertura
        # das duas extensões no archival.
        if url == "https://example.gov.br/foto1001.jpg":
            return httpx.Response(
                200,
                content=_FAKE_PNG_BYTES,
                headers={"content-type": "image/png"},
            )
        if url == "https://example.gov.br/foto1002.jpg":
            return httpx.Response(
                200,
                content=_FAKE_JPG_BYTES,
                headers={"content-type": "image/jpeg"},
            )
        # /deputados?siglaUf=GO (listagem)
        if url.startswith(
            "https://dadosabertos.camara.leg.br/api/v2/deputados?",
        ) or url.endswith(
            "/api/v2/deputados",
        ):
            return httpx.Response(
                200,
                content=json.dumps(_deputy_listing_payload()).encode("utf-8"),
                headers=headers,
            )
        # /deputados/{id}/despesas?ano=YYYY
        if "/despesas" in url:
            # formato: .../deputados/{id}/despesas?ano=YYYY...
            # Extrai deputy_id e ano sem regex: ambos vêm no path/query.
            path = request.url.path
            parts = path.split("/")
            deputy_id = int(parts[-2])
            ano = int(request.url.params.get("ano", "2024"))
            return httpx.Response(
                200,
                content=json.dumps(_ceap_payload(deputy_id, ano)).encode("utf-8"),
                headers=headers,
            )
        # /deputados/{id}
        if "/deputados/" in url:
            path = request.url.path
            deputy_id = int(path.rsplit("/", 1)[-1])
            cpf_by_id = {1001: "11122233344", 1002: "55566677788"}
            nome_by_id = {1001: "DEPUTADO UM", 1002: "DEPUTADA DOIS"}
            return httpx.Response(
                200,
                content=json.dumps(
                    _deputy_detail_payload(
                        deputy_id,
                        cpf_by_id.get(deputy_id, "00000000000"),
                        nome_by_id.get(deputy_id, "DESCONHECIDO"),
                    )
                ).encode("utf-8"),
                headers=headers,
            )
        return httpx.Response(
            404,
            content=json.dumps({"error": f"unhandled {url}"}).encode("utf-8"),
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
def pipeline(
    archival_root: Path,  # noqa: ARG001 — just activates the env var
) -> CamaraPoliticosGoPipeline:
    driver = MagicMock()
    transport = _build_transport()

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    return CamaraPoliticosGoPipeline(
        driver=driver,
        data_dir="./data",
        http_client_factory=factory,
        start_year=2024,
        end_year=2024,
    )


# ---------------------------------------------------------------------------
# Metadata / registry wiring
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert CamaraPoliticosGoPipeline.name == "camara_politicos_go"

    def test_source_id(self) -> None:
        assert CamaraPoliticosGoPipeline.source_id == _SOURCE_ID_CADASTRO

    def test_ceap_source_id_constant(self) -> None:
        assert _SOURCE_ID_CEAP == "camara_deputados_ceap"


# ---------------------------------------------------------------------------
# extract — HTTP + archival
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_fetches_both_deputies(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        assert set(pipeline._deputies_by_id.keys()) == {"1001", "1002"}

    def test_extract_fetches_ceap_pages(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        # 2 deputies x 1 year x 1 page each = 2 pages.
        assert len(pipeline._expense_pages) == 2

    def test_archival_writes_snapshots(
        self,
        pipeline: CamaraPoliticosGoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # Listagem + 2 detalhes + 2 páginas CEAP = 5 payloads distintos.
        # Archival root deve ter pelo menos um arquivo por fonte (cadastro +
        # ceap).
        cadastro_dir = archival_root / _SOURCE_ID_CADASTRO
        ceap_dir = archival_root / _SOURCE_ID_CEAP
        assert cadastro_dir.exists()
        assert ceap_dir.exists()
        cadastro_files = list(cadastro_dir.rglob("*.json"))
        ceap_files = list(ceap_dir.rglob("*.json"))
        assert len(cadastro_files) >= 3  # listagem + 2 detalhes
        assert len(ceap_files) >= 2      # 1 pagina CEAP por deputado

    def test_extract_captures_detail_snapshot_uris(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        assert "1001" in pipeline._detail_snapshot_by_id
        assert "1002" in pipeline._detail_snapshot_by_id
        # URI deve ser relativa ao root com o source_id no primeiro segmento.
        for uri in pipeline._detail_snapshot_by_id.values():
            assert uri.startswith(f"{_SOURCE_ID_CADASTRO}/")


# ---------------------------------------------------------------------------
# transform — dicts finais + provenance + CPF mascarado
# ---------------------------------------------------------------------------


class TestTransform:
    def test_transform_produces_legislators(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.legislators) == 2
        ids = {leg["id_camara"] for leg in pipeline.legislators}
        assert ids == {"1001", "1002"}

    def test_legislator_fields_shape(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        leg = pipeline.legislators[0]
        for field in (
            "id_camara", "legislator_id", "name", "cpf", "partido", "uf",
            "email", "url_foto", "situacao", "legislatura_atual", "scope",
        ):
            assert field in leg
        assert leg["uf"] == "GO"
        assert leg["scope"] == "federal"

    def test_cpf_is_masked(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Raw CPFs from the mocked detail: 11122233344 / 55566677788.
        # mask_cpf returns "***.***.*dd-dd" pattern (only last 4 visible).
        for leg in pipeline.legislators:
            assert "111" not in leg["cpf"]
            assert "555" not in leg["cpf"]
            assert "***" in leg["cpf"]

    def test_legislator_has_provenance_with_snapshot(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for leg in pipeline.legislators:
            assert leg["source_id"] == _SOURCE_ID_CADASTRO
            assert leg["source_record_id"] == leg["id_camara"]
            assert leg["source_url"].startswith("https://dadosabertos.camara.leg.br")
            assert leg["run_id"].startswith(f"{_SOURCE_ID_CADASTRO}_")
            assert leg["source_snapshot_uri"]
            assert leg["source_snapshot_uri"].startswith(
                f"{_SOURCE_ID_CADASTRO}/",
            )

    def test_transform_produces_expenses(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # 2 deputies x 2 items = 4 expenses (ids are distinct — different CNPJs).
        assert len(pipeline.expenses) == 4

    def test_expense_provenance_and_snapshot_from_ceap_source(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for exp in pipeline.expenses:
            assert exp["source_id"] == _SOURCE_ID_CEAP
            assert exp["source_url"].startswith(
                "https://dadosabertos.camara.leg.br",
            )
            assert exp["source_snapshot_uri"].startswith(
                f"{_SOURCE_ID_CEAP}/",
            )

    def test_expense_rels_have_provenance_and_snapshot(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.expense_rels) == 4
        for rel in pipeline.expense_rels:
            assert rel["source_key"].startswith("camara_")
            assert rel["source_id"] == _SOURCE_ID_CEAP
            assert rel["source_snapshot_uri"]
            assert rel["tipo"] == "CEAP"
            assert rel["ano"] == 2024

    def test_expense_skips_non_positive_values(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        # Inject a zero-valued expense page to confirm filtering.
        pipeline._expense_pages.append(
            (
                "1001",
                2024,
                [
                    {
                        "ano": 2024,
                        "mes": 1,
                        "tipoDespesa": "INVALIDO",
                        "cnpjCpfFornecedor": "00000000000000",
                        "nomeFornecedor": "ZERO",
                        "numDocumento": "NF-ZERO",
                        "valorLiquido": 0.0,
                    },
                ],
                "https://dadosabertos.camara.leg.br/api/v2/deputados/1001/despesas?ano=2024&pagina=2",
                f"{_SOURCE_ID_CEAP}/2024-01/deadbeefcafe.json",
            ),
        )
        pipeline.transform()
        # Ainda 4 (o zero foi descartado).
        assert all(e["valor_liquido"] > 0 for e in pipeline.expenses)


# ---------------------------------------------------------------------------
# load — gravação no grafo (mock driver)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_calls_session(
        self, pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        # pipeline.driver is a MagicMock in tests; cast to satisfy mypy.
        driver_mock = cast("MagicMock", pipeline.driver)
        session_cm = driver_mock.session.return_value
        session = session_cm.__enter__.return_value
        # Must hit neo4j multiple times: at least nodes legislators +
        # nodes expenses + rels.
        assert session.run.call_count >= 3

    def test_load_noop_when_empty(self, archival_root: Path) -> None:  # noqa: ARG002
        driver = MagicMock()
        p = CamaraPoliticosGoPipeline(
            driver=driver, data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(  # noqa: ARG005
                        200,
                        content=b'{"dados": [], "links": []}',
                        headers={"content-type": "application/json"},
                    ),
                ),
            ),
            start_year=2024, end_year=2024,
        )
        p.extract()
        p.transform()
        p.load()
        # No legislators fetched -> nothing loaded (but the call to
        # driver.session just to check is acceptable; we only assert
        # we didn't blow up).
        assert len(p.legislators) == 0
        assert len(p.expenses) == 0


# ---------------------------------------------------------------------------
# foto — binário arquivado, snapshot_uri + content_type propagados
# ---------------------------------------------------------------------------


class TestPhotoArchival:
    def test_extract_archives_png_and_jpeg(
        self,
        pipeline: CamaraPoliticosGoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        foto_dir = archival_root / _SOURCE_ID_FOTO
        assert foto_dir.exists()
        # 1 PNG (deputy 1001) + 1 JPG (deputy 1002).
        png_files = list(foto_dir.rglob("*.png"))
        jpg_files = list(foto_dir.rglob("*.jpg"))
        assert len(png_files) == 1
        assert len(jpg_files) == 1

    def test_extract_captures_photo_snapshot_uris(
        self,
        pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        assert set(pipeline._photo_snapshot_by_id.keys()) == {"1001", "1002"}
        for uri in pipeline._photo_snapshot_by_id.values():
            assert uri.startswith(f"{_SOURCE_ID_FOTO}/")

    def test_extract_captures_photo_content_types(
        self,
        pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        assert pipeline._photo_content_type_by_id["1001"] == "image/png"
        assert pipeline._photo_content_type_by_id["1002"] == "image/jpeg"

    def test_transform_propagates_photo_props_to_legislator(
        self,
        pipeline: CamaraPoliticosGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        by_id = {leg["id_camara"]: leg for leg in pipeline.legislators}
        # foto_url (novo alias público pra PWA) carrega a urlFoto original.
        assert by_id["1001"]["foto_url"].startswith("https://example.gov.br/")
        # snapshot_uri archival local + content-type normalizado.
        assert by_id["1001"]["foto_snapshot_uri"].startswith(
            f"{_SOURCE_ID_FOTO}/",
        )
        assert by_id["1001"]["foto_snapshot_uri"].endswith(".png")
        assert by_id["1001"]["foto_content_type"] == "image/png"
        assert by_id["1002"]["foto_snapshot_uri"].endswith(".jpg")
        assert by_id["1002"]["foto_content_type"] == "image/jpeg"

    def test_opt_out_skips_photo_fetch(
        self,
        archival_root: Path,
    ) -> None:
        """``archive_photos=False`` não baixa nem cria diretório de foto."""
        transport = _build_transport()
        p = CamaraPoliticosGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            start_year=2024,
            end_year=2024,
            archive_photos=False,
        )
        p.extract()
        p.transform()
        assert p._photo_snapshot_by_id == {}
        assert not (archival_root / _SOURCE_ID_FOTO).exists()
        # Nó ainda é ingerido — snapshot/content_type ficam None, foto_url
        # continua disponível da API pra fallback do frontend.
        for leg in p.legislators:
            assert leg["foto_snapshot_uri"] is None
            assert leg["foto_content_type"] is None
            assert leg["foto_url"]

    def test_fetch_error_does_not_abort_pipeline(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """CDN com 500/timeout: deputados continuam indo pro grafo sem snapshot."""
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            headers = {"content-type": "application/json; charset=utf-8"}
            if url.startswith("https://example.gov.br/"):
                # Falha completa da CDN pro binário da foto.
                return httpx.Response(503, content=b"service unavailable")
            if url.endswith("/api/v2/deputados") or "siglaUf=GO" in url:
                return httpx.Response(
                    200,
                    content=json.dumps(_deputy_listing_payload()).encode("utf-8"),
                    headers=headers,
                )
            if "/despesas" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({"dados": [], "links": []}).encode("utf-8"),
                    headers=headers,
                )
            if "/deputados/" in url:
                deputy_id = int(request.url.path.rsplit("/", 1)[-1])
                return httpx.Response(
                    200,
                    content=json.dumps(
                        _deputy_detail_payload(deputy_id, "11122233344", "X"),
                    ).encode("utf-8"),
                    headers=headers,
                )
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        p = CamaraPoliticosGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            start_year=2024,
            end_year=2024,
        )
        p.extract()
        p.transform()
        assert len(p.legislators) == 2
        for leg in p.legislators:
            assert leg["foto_snapshot_uri"] is None
            assert leg["foto_content_type"] is None
            assert leg["foto_url"]  # ainda exposto pra PWA

    def test_missing_url_foto_skips_archival(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Se o detail não traz urlFoto (campo opcional): skip gracioso."""
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            headers = {"content-type": "application/json; charset=utf-8"}
            if url.endswith("/api/v2/deputados") or "siglaUf=GO" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({
                        "dados": [{
                            "id": 3001,
                            "nome": "SEM FOTO",
                            "siglaPartido": "XYZ",
                            "siglaUf": "GO",
                            # urlFoto ausente na listagem.
                        }],
                        "links": [],
                    }).encode("utf-8"),
                    headers=headers,
                )
            if "/despesas" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({"dados": [], "links": []}).encode("utf-8"),
                    headers=headers,
                )
            if "/deputados/" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({
                        "dados": {
                            "id": 3001,
                            "cpf": "11122233344",
                            "ultimoStatus": {
                                "nomeEleitoral": "SEM FOTO",
                                "siglaPartido": "XYZ",
                                "siglaUf": "GO",
                                "situacao": "Exercicio",
                                # urlFoto ausente no detail.
                                "idLegislatura": 57,
                                "gabinete": {},
                            },
                        },
                    }).encode("utf-8"),
                    headers=headers,
                )
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        p = CamaraPoliticosGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            start_year=2024,
            end_year=2024,
        )
        p.extract()
        p.transform()
        assert len(p.legislators) == 1
        leg = p.legislators[0]
        assert leg["foto_snapshot_uri"] is None
        assert leg["foto_content_type"] is None
        # foto_url vira None (não "" vazio) quando a API não traz urlFoto.
        assert leg["foto_url"] is None

    def test_non_image_content_type_is_rejected(
        self,
        archival_root: Path,
    ) -> None:
        """Se a CDN devolve HTML de erro (200 + text/html): não arquiva."""
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            headers = {"content-type": "application/json; charset=utf-8"}
            if url.startswith("https://example.gov.br/"):
                return httpx.Response(
                    200,
                    content=b"<html>erro</html>",
                    headers={"content-type": "text/html"},
                )
            if url.endswith("/api/v2/deputados") or "siglaUf=GO" in url:
                return httpx.Response(
                    200,
                    content=json.dumps(_deputy_listing_payload()).encode("utf-8"),
                    headers=headers,
                )
            if "/despesas" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({"dados": [], "links": []}).encode("utf-8"),
                    headers=headers,
                )
            if "/deputados/" in url:
                deputy_id = int(request.url.path.rsplit("/", 1)[-1])
                return httpx.Response(
                    200,
                    content=json.dumps(
                        _deputy_detail_payload(deputy_id, "11122233344", "X"),
                    ).encode("utf-8"),
                    headers=headers,
                )
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        p = CamaraPoliticosGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            start_year=2024,
            end_year=2024,
        )
        p.extract()
        p.transform()
        # Nada arquivado no bucket de foto.
        assert not (archival_root / _SOURCE_ID_FOTO).exists()
        for leg in p.legislators:
            assert leg["foto_snapshot_uri"] is None


# ---------------------------------------------------------------------------
# scope — deputados fora de GO são descartados defensivamente
# ---------------------------------------------------------------------------


class TestScopeFilter:
    def test_rejects_non_go_detail(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Se o detail vem com uf != GO por algum motivo, o deputado some."""
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            headers = {"content-type": "application/json"}
            if url.endswith("/api/v2/deputados") or "siglaUf=GO" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({
                        "dados": [
                            {
                                "id": 2001,
                                "nome": "MIGRANTE",
                                "siglaPartido": "XYZ",
                                "siglaUf": "GO",
                                "email": "x@camara.leg.br",
                                "urlFoto": "https://example.gov.br/foto.jpg",
                            },
                        ],
                        "links": [],
                    }).encode("utf-8"),
                    headers=headers,
                )
            if "/despesas" in url:
                return httpx.Response(
                    200,
                    content=json.dumps({"dados": [], "links": []}).encode("utf-8"),
                    headers=headers,
                )
            if "/deputados/" in url:
                # detail diverges: uf=SP
                return httpx.Response(
                    200,
                    content=json.dumps({
                        "dados": {
                            "id": 2001,
                            "cpf": "11122233344",
                            "ultimoStatus": {
                                "nomeEleitoral": "MIGRANTE",
                                "siglaPartido": "XYZ",
                                "siglaUf": "SP",
                                "situacao": "Exercicio",
                                "urlFoto": "https://example.gov.br/foto.jpg",
                                "idLegislatura": 57,
                                "gabinete": {"email": "x@camara.leg.br"},
                            },
                        },
                    }).encode("utf-8"),
                    headers=headers,
                )
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        p = CamaraPoliticosGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            start_year=2024,
            end_year=2024,
        )
        p.extract()
        p.transform()
        assert len(p.legislators) == 0
