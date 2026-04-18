"""Tests for ``wikidata_politicos_foto`` pipeline.

Cobre:

* discovery — Cypher devolve políticos GO sem foto;
* SPARQL match único — Q-id resolvido, P18 extraído, imagem arquivada;
* SPARQL ambíguo (>1 candidato) — pipeline pula com warning, sem update;
* SPARQL zero candidatos — pula silencioso;
* P18 ausente — Q-id matched mas sem foto, pula;
* CDN devolve content-type não-imagem — pula sem arquivar binário lixo;
* archival — cada um dos 3 fetches grava snapshot content-addressed;
* etiqueta — User-Agent identificavel + throttle entre requests.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.wikidata_politicos_foto import (
    _SOURCE_ID,
    WikidataPoliticosFotoPipeline,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Constantes / payloads
# ---------------------------------------------------------------------------


_QID_MARCONI = "Q6757791"
_QID_OUTRO = "Q9999999"
_FILENAME_MARCONI = "Marconi Perillo, June of 2024 (cropped).jpg"
_FAKE_JPG_BYTES = b"\xff\xd8\xff\xe0" + b"fake-jpeg-marconi"
_FAKE_PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"fake-png-payload"


def _sparql_response(qids: list[str]) -> dict[str, Any]:
    """Mocka resposta JSON do SPARQL com N candidatos."""
    bindings = [
        {
            "item": {
                "type": "uri",
                "value": f"http://www.wikidata.org/entity/{qid}",
            },
            "itemLabel": {"type": "literal", "value": qid},
        }
        for qid in qids
    ]
    return {"head": {"vars": ["item", "itemLabel"]}, "results": {"bindings": bindings}}


def _entity_response(qid: str, p18_filename: str | None) -> dict[str, Any]:
    """Mocka EntityData JSON com (ou sem) propriedade P18."""
    claims: dict[str, Any] = {}
    if p18_filename:
        claims["P18"] = [
            {
                "mainsnak": {
                    "snaktype": "value",
                    "property": "P18",
                    "datavalue": {"value": p18_filename, "type": "string"},
                },
                "type": "statement",
                "rank": "normal",
            },
        ]
    return {
        "entities": {
            qid: {
                "type": "item",
                "id": qid,
                "claims": claims,
            },
        },
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


def _build_driver(
    targets: list[dict[str, Any]],
) -> tuple[MagicMock, list[tuple[str, dict[str, Any]]]]:
    """Driver mock que serve `targets` na discovery e captura load query."""
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value
    calls: list[tuple[str, dict[str, Any]]] = []

    def run(query: str, params: dict[str, Any] | None = None) -> MagicMock:
        calls.append((query, params or {}))
        result = MagicMock()
        if "FederalLegislator" in query and "RETURN name" in query:
            result.__iter__ = lambda _self: iter(targets)
        else:
            # Load query: o pipeline não consome o resultado (write-only).
            result.__iter__ = lambda _self: iter([])
        return result

    session.run.side_effect = run
    return driver, calls


def _build_transport(
    sparql_qids_by_name: dict[str, list[str]],
    entity_p18_by_qid: dict[str, str | None],
    image_responses: dict[str, tuple[int, bytes, str]],
) -> httpx.MockTransport:
    """Roteia as 3 famílias de URL: SPARQL, EntityData, FilePath.

    - ``sparql_qids_by_name``: nome normalizado UPPER (sem acento) -> [Q-ids]
    - ``entity_p18_by_qid``: Q-id -> filename (ou None)
    - ``image_responses``: filename -> (status, bytes, content-type)
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        # SPARQL endpoint (POST). Body carrega ``query=...``.
        if url.startswith("https://query.wikidata.org/sparql"):
            from urllib.parse import unquote_plus
            body = request.content.decode("utf-8") if request.content else ""
            decoded = unquote_plus(body)
            # Encontra o nome dentro da query SPARQL (entre aspas).
            matched_qids: list[str] = []
            for name, qids in sparql_qids_by_name.items():
                if f'"{name}"' in decoded:
                    matched_qids = qids
                    break
            return httpx.Response(
                200,
                content=json.dumps(_sparql_response(matched_qids)).encode("utf-8"),
                headers={
                    "content-type": "application/sparql-results+json",
                },
            )
        # EntityData JSON (GET): ``/wiki/Special:EntityData/Q{id}.json``
        if "Special:EntityData" in url and url.endswith(".json"):
            qid = url.rsplit("/", 1)[-1].removesuffix(".json")
            filename = entity_p18_by_qid.get(qid)
            return httpx.Response(
                200,
                content=json.dumps(_entity_response(qid, filename)).encode("utf-8"),
                headers={"content-type": "application/json"},
            )
        # Special:FilePath (GET): redirect resolvido pelo httpx; mock entrega
        # bytes direto na URL canônica.
        if "Special:FilePath" in url:
            # filename é o último path component, URL-decodado pelo httpx.
            filename = request.url.path.rsplit("/", 1)[-1]
            # httpx URL-encoda no path; pra match com a key do dict, decodamos
            # caracteres ASCII percent-encoded (espaços, vírgulas, parênteses).
            from urllib.parse import unquote
            filename = unquote(filename)
            spec = image_responses.get(filename)
            if spec is None:
                return httpx.Response(404, content=b"not found")
            status, content, ct = spec
            return httpx.Response(
                status, content=content, headers={"content-type": ct},
            )
        return httpx.Response(404, content=b"unhandled")

    return httpx.MockTransport(handler)


def _make_pipeline(
    targets: list[dict[str, Any]],
    transport: httpx.MockTransport,
    *,
    batch_size: int = 10,
) -> tuple[
    WikidataPoliticosFotoPipeline,
    list[tuple[str, dict[str, Any]]],
]:
    driver, calls = _build_driver(targets)

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    pipeline = WikidataPoliticosFotoPipeline(
        driver=driver,
        data_dir="./data",
        batch_size=batch_size,
        http_client_factory=factory,
        sleep_fn=lambda _s: None,  # neutraliza throttle nos testes
    )
    pipeline.run_id = "wikidata_politicos_foto_20260418100000"
    return pipeline, calls


# ---------------------------------------------------------------------------
# Metadata / wiring
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert WikidataPoliticosFotoPipeline.name == "wikidata_politicos_foto"

    def test_source_id(self) -> None:
        assert WikidataPoliticosFotoPipeline.source_id == _SOURCE_ID
        assert _SOURCE_ID == "wikidata_politicos_foto"


# ---------------------------------------------------------------------------
# Discovery — Cypher pega só políticos GO sem foto, deduplicado
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_grafo_vazio_curto_circuita(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        transport = _build_transport({}, {}, {})
        pipeline, _ = _make_pipeline([], transport)
        pipeline.extract()
        assert pipeline.rows_in == 0
        assert pipeline._updates == []

    def test_dedup_por_nome_normalizado(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Mesmo nome em 2 labels (ex.: ex-deputado + Person) conta 1 vez."""
        targets = [
            {"name": "Marconi Perillo", "labels": ["FederalLegislator"], "key": "fl_1"},
            {"name": "MARCONI PERILLO", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            {"MARCONI PERILLO": []},  # zero matches: pipeline so confere dedup
            {},
            {},
        )
        pipeline, _ = _make_pipeline(targets, transport)
        pipeline.extract()
        # rows_in conta os targets pos-dedup (1).
        assert pipeline.rows_in == 1


# ---------------------------------------------------------------------------
# SPARQL match — happy path com 1 Q-id
# ---------------------------------------------------------------------------


class TestSparqlMatchUnico:
    def test_match_unico_happy_path(
        self,
        archival_root: Path,
    ) -> None:
        targets = [
            {"name": "Marconi Perillo", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            sparql_qids_by_name={"MARCONI PERILLO": [_QID_MARCONI]},
            entity_p18_by_qid={_QID_MARCONI: _FILENAME_MARCONI},
            image_responses={
                _FILENAME_MARCONI: (200, _FAKE_JPG_BYTES, "image/jpeg"),
            },
        )
        pipeline, _ = _make_pipeline(targets, transport)
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline._updates) == 1
        upd = pipeline._updates[0]
        assert upd["wikidata_qid"] == _QID_MARCONI
        assert upd["foto_content_type"] == "image/jpeg"
        assert upd["foto_url"].startswith(
            "https://commons.wikimedia.org/wiki/Special:FilePath/",
        )
        # Os 3 snapshots foram gravados (SPARQL JSON, entity JSON, binário).
        assert upd["sparql_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
        assert upd["entity_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
        assert upd["foto_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
        # E os arquivos de fato existem em disco.
        bin_files = list(archival_root.rglob("*.jpg"))
        json_files = list(archival_root.rglob("*.json"))
        assert len(bin_files) == 1
        # SPARQL retorna content-type application/sparql-results+json — vira
        # ``.bin`` por não bater nos content-types conhecidos do archival.
        # EntityData retorna application/json normal e bate em ``.json``.
        # Aceitamos qualquer combinação >= 1.
        assert len(json_files) >= 1

    def test_pipeline_persiste_load(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """``load()`` faz UNWIND com SET sem MERGE — apenas atualiza."""
        targets = [
            {"name": "Marconi Perillo", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            sparql_qids_by_name={"MARCONI PERILLO": [_QID_MARCONI]},
            entity_p18_by_qid={_QID_MARCONI: _FILENAME_MARCONI},
            image_responses={
                _FILENAME_MARCONI: (200, _FAKE_JPG_BYTES, "image/jpeg"),
            },
        )
        pipeline, calls = _make_pipeline(targets, transport)
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        # Discovery + load = 2 chamadas (no minimo).
        assert len(calls) >= 2
        load_call = calls[-1]
        load_query = load_call[0]
        # Garantir que NÃO usa MERGE (não cria nodes).
        assert "MERGE" not in load_query
        # Garantir que faz MATCH com filtro de label.
        assert "MATCH" in load_query
        assert "FederalLegislator" in load_query
        assert "StateLegislator" in load_query
        assert "Person" in load_query


# ---------------------------------------------------------------------------
# SPARQL ambíguo — pula sem update
# ---------------------------------------------------------------------------


class TestSparqlAmbiguity:
    def test_ambiguidade_pula_sem_update(
        self,
        archival_root: Path,  # noqa: ARG002
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """>1 Q-id retornado pelo SPARQL: pipeline pula com warning."""
        import logging
        targets = [
            {"name": "Joao Silva", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            sparql_qids_by_name={"JOAO SILVA": [_QID_MARCONI, _QID_OUTRO]},
            entity_p18_by_qid={},
            image_responses={},
        )
        pipeline, _ = _make_pipeline(targets, transport)
        with caplog.at_level(logging.WARNING):
            pipeline.extract()
        assert pipeline._updates == []
        assert pipeline._stats["skipped_ambiguous"] == 1
        assert any(
            "ambiguos" in record.message.lower()
            or "ambiguo" in record.message.lower()
            for record in caplog.records
        )


# ---------------------------------------------------------------------------
# SPARQL zero candidatos — pula silencioso
# ---------------------------------------------------------------------------


class TestSparqlZeroMatch:
    def test_zero_candidatos_pula_silencioso(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"name": "Pessoa Inexistente", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            sparql_qids_by_name={},  # nenhum match
            entity_p18_by_qid={},
            image_responses={},
        )
        pipeline, _ = _make_pipeline(targets, transport)
        pipeline.extract()
        assert pipeline._updates == []
        assert pipeline._stats["skipped_no_match"] == 1


# ---------------------------------------------------------------------------
# P18 ausente — Q-id matched mas sem foto
# ---------------------------------------------------------------------------


class TestP18Missing:
    def test_match_sem_p18_pula(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        targets = [
            {"name": "Marconi Perillo", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            sparql_qids_by_name={"MARCONI PERILLO": [_QID_MARCONI]},
            entity_p18_by_qid={_QID_MARCONI: None},  # sem P18
            image_responses={},
        )
        pipeline, _ = _make_pipeline(targets, transport)
        pipeline.extract()
        assert pipeline._updates == []
        assert pipeline._stats["skipped_no_p18"] == 1


# ---------------------------------------------------------------------------
# CDN devolve não-imagem — não arquiva binário lixo
# ---------------------------------------------------------------------------


class TestNonImageContentType:
    def test_html_error_nao_arquiva(
        self,
        archival_root: Path,
    ) -> None:
        targets = [
            {"name": "Marconi Perillo", "labels": ["Person"], "key": "p_1"},
        ]
        transport = _build_transport(
            sparql_qids_by_name={"MARCONI PERILLO": [_QID_MARCONI]},
            entity_p18_by_qid={_QID_MARCONI: _FILENAME_MARCONI},
            image_responses={
                # CDN devolve HTML de erro com 200 — pipeline rejeita.
                _FILENAME_MARCONI: (200, b"<html>err</html>", "text/html"),
            },
        )
        pipeline, _ = _make_pipeline(targets, transport)
        pipeline.extract()
        assert pipeline._updates == []
        assert pipeline._stats["skipped_image_fetch_failed"] == 1
        # Nenhum binário .jpg/.png no archival root.
        assert not list(archival_root.rglob("*.jpg"))
        assert not list(archival_root.rglob("*.png"))


# ---------------------------------------------------------------------------
# Etiqueta — User-Agent + throttle
# ---------------------------------------------------------------------------


class TestEtiquetaWikidata:
    def test_user_agent_identificavel(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Fetches devem carregar UA identificavel pra Wikimedia UA policy."""
        captured_uas: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_uas.append(request.headers.get("user-agent", ""))
            url = str(request.url)
            if url.startswith("https://query.wikidata.org/sparql"):
                return httpx.Response(
                    200,
                    content=json.dumps(_sparql_response([_QID_MARCONI])).encode(),
                    headers={"content-type": "application/sparql-results+json"},
                )
            if "Special:EntityData" in url:
                return httpx.Response(
                    200,
                    content=json.dumps(
                        _entity_response(_QID_MARCONI, _FILENAME_MARCONI),
                    ).encode(),
                    headers={"content-type": "application/json"},
                )
            if "Special:FilePath" in url:
                return httpx.Response(
                    200, content=_FAKE_JPG_BYTES,
                    headers={"content-type": "image/jpeg"},
                )
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        targets = [
            {"name": "Marconi Perillo", "labels": ["Person"], "key": "p_1"},
        ]
        pipeline, _ = _make_pipeline(targets, transport)
        pipeline.extract()
        # Cada UA capturado tem que conter "FiscalCidadao".
        assert captured_uas
        for ua in captured_uas:
            assert "FiscalCidadao" in ua

    def test_throttle_chamado_entre_requests(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Sleep deve ser chamado N vezes (>= 3 por politico encontrado)."""
        sleep_calls: list[float] = []
        transport = _build_transport(
            sparql_qids_by_name={"MARCONI PERILLO": [_QID_MARCONI]},
            entity_p18_by_qid={_QID_MARCONI: _FILENAME_MARCONI},
            image_responses={
                _FILENAME_MARCONI: (200, _FAKE_JPG_BYTES, "image/jpeg"),
            },
        )
        targets = [
            {"name": "Marconi Perillo", "labels": ["Person"], "key": "p_1"},
        ]
        driver, _ = _build_driver(targets)

        def factory() -> httpx.Client:
            return httpx.Client(transport=transport, follow_redirects=True)

        pipeline = WikidataPoliticosFotoPipeline(
            driver=driver,
            data_dir="./data",
            batch_size=10,
            http_client_factory=factory,
            sleep_fn=lambda s: sleep_calls.append(s),
        )
        pipeline.run_id = "wikidata_politicos_foto_20260418100000"
        pipeline.extract()
        # 3 fetches por politico bem-sucedido = 3 sleeps no minimo.
        assert len(sleep_calls) >= 3
        # Throttle default = 1.0s.
        assert all(s >= 1.0 for s in sleep_calls)
