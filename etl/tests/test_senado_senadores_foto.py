"""Tests for the ``senado_senadores_foto`` pipeline.

Cobertura:

* happy path — ingere senadores GO em exercicio + foto arquivada;
* archival — cada fetch produz snapshot e carimba ``source_snapshot_uri``;
* provenance — ``attach_provenance`` e chamado em todo no;
* scope — senadores fora de GO sao descartados client-side;
* fallback — quando ``UrlFotoParlamentar`` esta vazio, monta URL do padrao
  estavel ``senador{codigo}.jpg``;
* opt-out + erro de CDN nao abortam o pipeline (no ainda vai pro grafo).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.senado_senadores_foto import (
    _SOURCE_ID_FOTO,
    _SOURCE_ID_LISTA,
    SenadoSenadoresFotoPipeline,
)

# Fake JPG bytes — so precisa ser deterministico pra hash content-address.
_FAKE_JPG_KAJURU = b"\xff\xd8\xff\xe0" + b"fake-jpg-payload-kajuru"
_FAKE_JPG_VANDERLAN = b"\xff\xd8\xff\xe0" + b"fake-jpg-payload-vanderlan"
_FAKE_JPG_WILDER = b"\xff\xd8\xff\xe0" + b"fake-jpg-payload-wilder"

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures — XML do Senado (3 senadores GO + 1 nao-GO pra validar filtro).
# ---------------------------------------------------------------------------


def _lista_atual_xml() -> bytes:
    """Aproximacao do ListaParlamentarEmExercicio com 4 parlamentares (3 GO)."""
    return b"""<?xml version='1.0' encoding='UTF-8'?>
<ListaParlamentarEmExercicio>
  <Metadados><Versao>2026-04-18</Versao></Metadados>
  <Parlamentares>
    <Parlamentar>
      <IdentificacaoParlamentar>
        <CodigoParlamentar>5895</CodigoParlamentar>
        <NomeParlamentar>Jorge Kajuru</NomeParlamentar>
        <NomeCompletoParlamentar>Jorge Kajuru Reis da Costa Nasser</NomeCompletoParlamentar>
        <SiglaPartidoParlamentar>PSB</SiglaPartidoParlamentar>
        <UfParlamentar>GO</UfParlamentar>
        <UrlFotoParlamentar>http://www.senado.leg.br/senadores/img/fotos-oficiais/senador5895.jpg</UrlFotoParlamentar>
      </IdentificacaoParlamentar>
    </Parlamentar>
    <Parlamentar>
      <IdentificacaoParlamentar>
        <CodigoParlamentar>5899</CodigoParlamentar>
        <NomeParlamentar>Vanderlan Cardoso</NomeParlamentar>
        <NomeCompletoParlamentar>Vanderlan Vieira Cardoso</NomeCompletoParlamentar>
        <SiglaPartidoParlamentar>PSD</SiglaPartidoParlamentar>
        <UfParlamentar>GO</UfParlamentar>
        <UrlFotoParlamentar>http://www.senado.leg.br/senadores/img/fotos-oficiais/senador5899.jpg</UrlFotoParlamentar>
      </IdentificacaoParlamentar>
    </Parlamentar>
    <Parlamentar>
      <IdentificacaoParlamentar>
        <CodigoParlamentar>5070</CodigoParlamentar>
        <NomeParlamentar>Wilder Morais</NomeParlamentar>
        <NomeCompletoParlamentar>Wilder Pedro de Morais</NomeCompletoParlamentar>
        <SiglaPartidoParlamentar>PL</SiglaPartidoParlamentar>
        <UfParlamentar>GO</UfParlamentar>
        <UrlFotoParlamentar></UrlFotoParlamentar>
      </IdentificacaoParlamentar>
    </Parlamentar>
    <Parlamentar>
      <IdentificacaoParlamentar>
        <CodigoParlamentar>9999</CodigoParlamentar>
        <NomeParlamentar>Senador Outro Estado</NomeParlamentar>
        <NomeCompletoParlamentar>Senador Outro Estado</NomeCompletoParlamentar>
        <SiglaPartidoParlamentar>XYZ</SiglaPartidoParlamentar>
        <UfParlamentar>SP</UfParlamentar>
        <UrlFotoParlamentar>http://www.senado.leg.br/senadores/img/fotos-oficiais/senador9999.jpg</UrlFotoParlamentar>
      </IdentificacaoParlamentar>
    </Parlamentar>
  </Parlamentares>
</ListaParlamentarEmExercicio>
"""


def _build_transport(
    *,
    foto_status: int = 200,
    foto_content_type: str = "image/jpeg",
    foto_body_override: bytes | None = None,
) -> httpx.MockTransport:
    """Mock transport servindo lista atual + 3 fotos GO."""
    foto_bodies = {
        "5895": _FAKE_JPG_KAJURU,
        "5899": _FAKE_JPG_VANDERLAN,
        "5070": _FAKE_JPG_WILDER,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.startswith(
            "https://legis.senado.leg.br/dadosabertos/senador/lista/atual",
        ):
            return httpx.Response(
                200,
                content=_lista_atual_xml(),
                headers={"content-type": "application/xml; charset=utf-8"},
            )
        # Foto: aceita HTTP OR HTTPS (o fallback monta sem https).
        if "/senadores/img/fotos-oficiais/senador" in url and url.endswith(
            ".jpg",
        ):
            # Extrai o codigo do path (.../senador5895.jpg).
            stem = url.rsplit("/", 1)[-1]
            codigo = stem.removeprefix("senador").removesuffix(".jpg")
            body = foto_body_override or foto_bodies.get(codigo, b"")
            return httpx.Response(
                foto_status,
                content=body,
                headers={"content-type": foto_content_type},
            )
        return httpx.Response(
            404,
            content=b"unhandled",
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
def pipeline(
    archival_root: Path,  # noqa: ARG001 — just activates the env var
) -> SenadoSenadoresFotoPipeline:
    driver = MagicMock()
    transport = _build_transport()

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    return SenadoSenadoresFotoPipeline(
        driver=driver,
        data_dir="./data",
        http_client_factory=factory,
    )


# ---------------------------------------------------------------------------
# Metadata / registry wiring
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert SenadoSenadoresFotoPipeline.name == "senado_senadores_foto"

    def test_source_id(self) -> None:
        assert SenadoSenadoresFotoPipeline.source_id == _SOURCE_ID_FOTO

    def test_source_ids_separados_pra_listagem_e_foto(self) -> None:
        # Bucket de archival da listagem XML e separado do bucket do binario
        # JPG — mesmo padrao do camara_politicos_go (cadastro vs foto).
        assert _SOURCE_ID_FOTO == "senado_senadores_foto"
        assert _SOURCE_ID_LISTA == "senado_senadores_lista_atual"
        assert _SOURCE_ID_FOTO != _SOURCE_ID_LISTA


# ---------------------------------------------------------------------------
# extract — XML + binarios + archival
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_filtra_so_go(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        codigos = {s["codigo"] for s in pipeline._raw_senators}
        # 3 GO esperados, senador SP descartado.
        assert codigos == {"5895", "5899", "5070"}
        assert "9999" not in codigos

    def test_archival_grava_xml_listagem(
        self,
        pipeline: SenadoSenadoresFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        lista_dir = archival_root / _SOURCE_ID_LISTA
        assert lista_dir.exists()
        # Bucket separado do bucket de foto.
        xml_files = list(lista_dir.rglob("*.xml"))
        assert len(xml_files) >= 1

    def test_archival_grava_3_fotos(
        self,
        pipeline: SenadoSenadoresFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        foto_dir = archival_root / _SOURCE_ID_FOTO
        assert foto_dir.exists()
        jpg_files = list(foto_dir.rglob("*.jpg"))
        assert len(jpg_files) == 3

    def test_extract_captura_snapshot_uri_listagem(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        assert pipeline._lista_snapshot_uri is not None
        assert pipeline._lista_snapshot_uri.startswith(f"{_SOURCE_ID_LISTA}/")

    def test_extract_captura_snapshot_uri_de_cada_foto(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        assert set(pipeline._photo_snapshot_by_codigo.keys()) == {
            "5895", "5899", "5070",
        }
        for uri in pipeline._photo_snapshot_by_codigo.values():
            assert uri.startswith(f"{_SOURCE_ID_FOTO}/")

    def test_extract_captura_content_type_normalizado(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        for ct in pipeline._photo_content_type_by_codigo.values():
            assert ct == "image/jpeg"


# ---------------------------------------------------------------------------
# fallback URL — UrlFotoParlamentar vazio (Wilder no fixture)
# ---------------------------------------------------------------------------


class TestFotoUrlFallback:
    def test_fallback_para_padrao_estavel_quando_xml_vazio(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        # Wilder (codigo 5070) tem UrlFotoParlamentar vazio no fixture; o
        # pipeline deve montar a URL do padrao estavel
        # ``senador{codigo}.jpg`` e arquivar a foto do mesmo jeito.
        assert "5070" in pipeline._photo_snapshot_by_codigo
        url_used = pipeline._photo_url_used_by_codigo["5070"]
        assert url_used.endswith("senador5070.jpg")


# ---------------------------------------------------------------------------
# transform — dicts finais + provenance + foto props
# ---------------------------------------------------------------------------


class TestTransform:
    def test_transform_produz_3_senadores(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.senators) == 3

    def test_senator_fields_shape(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for sen in pipeline.senators:
            for field in (
                "id_senado", "senator_id", "name", "partido", "uf",
                "url_foto", "foto_url", "foto_snapshot_uri",
                "foto_content_type", "scope",
            ):
                assert field in sen
            assert sen["uf"] == "GO"
            assert sen["scope"] == "senate"
            assert sen["senator_id"] == f"senado_{sen['id_senado']}"

    def test_senator_foto_props_propagadas(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        by_id = {s["id_senado"]: s for s in pipeline.senators}
        for codigo in ("5895", "5899", "5070"):
            sen = by_id[codigo]
            assert sen["foto_url"]
            assert sen["foto_snapshot_uri"].startswith(f"{_SOURCE_ID_FOTO}/")
            assert sen["foto_snapshot_uri"].endswith(".jpg")
            assert sen["foto_content_type"] == "image/jpeg"

    def test_senator_provenance_tem_snapshot_uri_da_listagem(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for sen in pipeline.senators:
            assert sen["source_id"] == _SOURCE_ID_FOTO
            assert sen["source_record_id"] == sen["id_senado"]
            assert sen["source_url"].startswith(
                "https://legis.senado.leg.br/dadosabertos",
            )
            assert sen["run_id"].startswith(f"{_SOURCE_ID_FOTO}_")
            assert sen["source_snapshot_uri"]
            # snapshot_uri carimbado no no e o da LISTAGEM (a foto fica
            # acessivel via foto_snapshot_uri separado).
            assert sen["source_snapshot_uri"].startswith(
                f"{_SOURCE_ID_LISTA}/",
            )


# ---------------------------------------------------------------------------
# load — gravacao no grafo (mock driver)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_chama_session(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        driver_mock = cast("MagicMock", pipeline.driver)
        session_cm = driver_mock.session.return_value
        session = session_cm.__enter__.return_value
        # Pelo menos 1 batch UNWIND pra :Senator.
        assert session.run.call_count >= 1

    def test_load_noop_quando_lista_vazia(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        # Listagem retorna 500 → pipeline encerra sem senadores.
        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(
                500, content=b"server error",
                headers={"content-type": "text/plain"},
            )
        transport = httpx.MockTransport(handler)
        p = SenadoSenadoresFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
        )
        p.extract()
        p.transform()
        p.load()
        assert p.senators == []


# ---------------------------------------------------------------------------
# opt-out + erros de fetch da foto
# ---------------------------------------------------------------------------


class TestPhotoArchivalEdgeCases:
    def test_opt_out_pula_fetch_da_foto(
        self,
        archival_root: Path,
    ) -> None:
        transport = _build_transport()
        p = SenadoSenadoresFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
            archive_photos=False,
        )
        p.extract()
        p.transform()
        assert p._photo_snapshot_by_codigo == {}
        assert not (archival_root / _SOURCE_ID_FOTO).exists()
        # No do senador continua sendo ingerido — snapshot/content_type
        # ficam None, foto_url continua (vinda do XML quando presente).
        for sen in p.senators:
            assert sen["foto_snapshot_uri"] is None
            assert sen["foto_content_type"] is None

    def test_erro_500_na_foto_nao_aborta_pipeline(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        transport = _build_transport(foto_status=503)
        p = SenadoSenadoresFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
        )
        p.extract()
        p.transform()
        # Senadores ainda chegam ao grafo.
        assert len(p.senators) == 3
        for sen in p.senators:
            assert sen["foto_snapshot_uri"] is None
            assert sen["foto_content_type"] is None

    def test_content_type_nao_imagem_e_rejeitado(
        self,
        archival_root: Path,
    ) -> None:
        transport = _build_transport(
            foto_content_type="text/html",
            foto_body_override=b"<html>erro</html>",
        )
        p = SenadoSenadoresFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
        )
        p.extract()
        p.transform()
        # Nada arquivado no bucket de foto.
        assert not (archival_root / _SOURCE_ID_FOTO).exists()
        for sen in p.senators:
            assert sen["foto_snapshot_uri"] is None


# ---------------------------------------------------------------------------
# Archival retrofit — camadas de archival + provenance integradas
# ---------------------------------------------------------------------------


class TestArchivalRetrofit:
    """Cobertura padrao de pipeline novo: archival + provenance + run_id."""

    def test_archival_root_separa_listagem_e_foto(
        self,
        pipeline: SenadoSenadoresFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # Os dois buckets devem existir, com conteudo distinto.
        assert (archival_root / _SOURCE_ID_LISTA).exists()
        assert (archival_root / _SOURCE_ID_FOTO).exists()

    def test_run_id_segue_padrao_canonico(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        # `{source_id}_YYYYMMDDHHMMSS` — mesma regra do
        # ``Pipeline.__init__``; archival usa pra derivar o bucket mensal.
        assert pipeline.run_id.startswith(f"{_SOURCE_ID_FOTO}_")
        timestamp = pipeline.run_id.removeprefix(f"{_SOURCE_ID_FOTO}_")
        assert len(timestamp) == 14
        assert timestamp.isdigit()

    def test_proveniencia_completa_em_todos_os_nos(
        self, pipeline: SenadoSenadoresFotoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        required_fields = (
            "source_id",
            "source_record_id",
            "source_url",
            "ingested_at",
            "run_id",
            "source_snapshot_uri",
        )
        for sen in pipeline.senators:
            for field in required_fields:
                assert sen.get(field), f"missing {field}: {sen}"

    def test_idempotencia_archival_quando_rerun_mesmo_payload(
        self,
        pipeline: SenadoSenadoresFotoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        snapshots_first = sorted(
            p.relative_to(archival_root)
            for p in archival_root.rglob("*")
            if p.is_file()
        )
        # Re-extract com o mesmo conteudo — archival e content-addressed,
        # entao o numero de arquivos no disco nao deve crescer.
        pipeline2 = SenadoSenadoresFotoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            http_client_factory=lambda: httpx.Client(
                transport=_build_transport(), follow_redirects=True,
            ),
        )
        pipeline2.extract()
        snapshots_second = sorted(
            p.relative_to(archival_root)
            for p in archival_root.rglob("*")
            if p.is_file()
        )
        assert snapshots_first == snapshots_second


# ---------------------------------------------------------------------------
# Sanity check: pipeline esta registrada no runner
# ---------------------------------------------------------------------------


class TestRunnerWiring:
    def test_pipeline_aparece_no_runner(self) -> None:
        from bracc_etl.runner import PIPELINES
        assert "senado_senadores_foto" in PIPELINES
        assert PIPELINES["senado_senadores_foto"] is SenadoSenadoresFotoPipeline


# ---------------------------------------------------------------------------
# Pipeline init aceita kwargs do runner sem TypeError.
# ---------------------------------------------------------------------------


class TestInitContract:
    def test_init_aceita_kwargs_padrao_do_runner(
        self, archival_root: Path,  # noqa: ARG002
    ) -> None:
        # O runner instancia com history=False alem dos kwargs basicos;
        # garante que nao quebramos quando chamado pelo CLI.
        kwargs: dict[str, Any] = {
            "driver": MagicMock(),
            "data_dir": "./data",
            "limit": None,
            "chunk_size": 50_000,
            "history": False,
        }
        p = SenadoSenadoresFotoPipeline(**kwargs)
        assert p.archive_photos is True
        assert p.limit is None
