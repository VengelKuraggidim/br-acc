"""Tests for the ``emendas_parlamentares_go`` pipeline.

Covers:

* happy path — fetches GO federal deputies from the graph, paginates the
  Portal da Transparência ``/emendas`` endpoint with ``chave-api-dados``
  header, produces Amendment nodes + PROPOS relationships;
* archival — cada página arquivada e ``source_snapshot_uri`` carimbado
  em cada row (TestArchivalRetrofit pattern);
* opt-in offline — ``archive_online=False`` não popula ``source_snapshot_uri``;
* provenance — ``attach_provenance`` aplicado em todo nó/relação;
* env obrigatória — ausência de ``TRANSPARENCIA_API_KEY`` levanta ValueError
  em ``extract()`` (Flask silenciava; o pipeline não deve).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.emendas_parlamentares_go import (
    _SOURCE_ID,
    EmendasParlamentaresGoPipeline,
)
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# HTTP / graph fixtures
# ---------------------------------------------------------------------------


def _emenda_item(
    codigo: str,
    autor: str,
    ano: int,
    tipo: str,
    funcao: str,
    municipio: str,
    valor_empenhado: float,
    valor_pago: float,
) -> dict[str, Any]:
    return {
        "codigoEmenda": codigo,
        "nomeAutor": autor,
        "ano": ano,
        "tipoEmenda": tipo,
        "funcao": funcao,
        "municipio": municipio,
        "uf": "GO",
        "valorEmpenhado": valor_empenhado,
        "valorPago": valor_pago,
    }


def _build_transport(
    items_by_author: dict[str, list[dict[str, Any]]],
    *,
    require_header: bool = True,
    force_status: int | None = None,
) -> httpx.MockTransport:
    """MockTransport para o endpoint /api-de-dados/emendas.

    Devolve todos os itens do autor numa única página (len < 100), o que
    encerra a paginação no primeiro loop — cobre o happy path.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        headers_out = {"content-type": "application/json; charset=utf-8"}
        if force_status is not None:
            return httpx.Response(
                force_status,
                content=b"{}",
                headers=headers_out,
            )
        if require_header and request.headers.get("chave-api-dados") != "test-key":
            return httpx.Response(401, content=b"{}", headers=headers_out)
        url = str(request.url)
        if "/api-de-dados/emendas" not in url:
            return httpx.Response(404, content=b"{}", headers=headers_out)
        autor = request.url.params.get("nomeAutor", "")
        ano_raw = request.url.params.get("ano", "0")
        try:
            ano = int(ano_raw)
        except (TypeError, ValueError):
            ano = 0
        pagina_raw = request.url.params.get("pagina", "1")
        try:
            pagina = int(pagina_raw)
        except (TypeError, ValueError):
            pagina = 1
        # Single page per (autor, ano): devolve tudo na pagina=1, depois vazio.
        if pagina == 1:
            items = [
                it for it in items_by_author.get(autor, [])
                if it.get("ano") == ano
            ]
        else:
            items = []
        return httpx.Response(
            200,
            content=json.dumps(items).encode("utf-8"),
            headers=headers_out,
        )

    return httpx.MockTransport(handler)


def _build_driver_with_targets(
    targets: list[dict[str, Any]],
) -> MagicMock:
    """Mock Neo4j driver: ``session.run(...)`` devolve os targets GO."""
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value

    def run(query: str, params: dict[str, Any] | None = None) -> MagicMock:
        _ = params
        result = MagicMock()
        if "FederalLegislator" in query:
            result.__iter__ = lambda _self: iter(
                [
                    {
                        "legislator_id": t["legislator_id"],
                        "id_camara": t["id_camara"],
                        "name": t["name"],
                    }
                    for t in targets
                ],
            )
        else:
            # _upsert_ingestion_run etc. — sessão só precisa aceitar a chamada.
            result.__iter__ = lambda _self: iter([])
        return result

    session.run.side_effect = run
    return driver


_DEPUTY_UM = {
    "legislator_id": "camara_1001",
    "id_camara": "1001",
    "name": "DEPUTADO UM",
}
_DEPUTY_DOIS = {
    "legislator_id": "camara_1002",
    "id_camara": "1002",
    "name": "DEPUTADA DOIS",
}


def _default_items() -> dict[str, list[dict[str, Any]]]:
    """2 deputados GO x 5 emendas cada (no ano 2024)."""
    items: dict[str, list[dict[str, Any]]] = {}
    for idx, autor in enumerate(("DEPUTADO UM", "DEPUTADA DOIS")):
        rows = []
        for i in range(5):
            rows.append(
                _emenda_item(
                    codigo=f"A{idx}-2024-{i:03d}",
                    autor=autor,
                    ano=2024,
                    tipo="Individual" if i % 2 == 0 else "Bancada",
                    funcao="Saude" if i % 2 == 0 else "Educacao",
                    municipio=f"Municipio{i}",
                    valor_empenhado=100_000.0 * (i + 1),
                    valor_pago=50_000.0 * (i + 1),
                ),
            )
        items[autor] = rows
    return items


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


@pytest.fixture()
def _api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRANSPARENCIA_API_KEY", "test-key")


def _make_pipeline(
    *,
    items: dict[str, list[dict[str, Any]]] | None = None,
    archive_online: bool = True,
    targets: list[dict[str, Any]] | None = None,
    require_header: bool = True,
    force_status: int | None = None,
) -> EmendasParlamentaresGoPipeline:
    items = items if items is not None else _default_items()
    targets = targets if targets is not None else [_DEPUTY_UM, _DEPUTY_DOIS]
    driver = _build_driver_with_targets(targets)
    transport = _build_transport(
        items,
        require_header=require_header,
        force_status=force_status,
    )

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    return EmendasParlamentaresGoPipeline(
        driver=driver,
        data_dir="./data",
        http_client_factory=factory,
        start_year=2024,
        end_year=2024,
        archive_online=archive_online,
    )


# ---------------------------------------------------------------------------
# Metadata / registry wiring
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert EmendasParlamentaresGoPipeline.name == "emendas_parlamentares_go"

    def test_source_id(self) -> None:
        assert EmendasParlamentaresGoPipeline.source_id == _SOURCE_ID
        assert _SOURCE_ID == "portal_transparencia_emendas"


# ---------------------------------------------------------------------------
# extract — env obrigatória + HTTP + graph discovery
# ---------------------------------------------------------------------------


class TestExtractRequiresApiKey:
    def test_missing_api_key_raises(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("TRANSPARENCIA_API_KEY", raising=False)
        pipeline = _make_pipeline()
        with pytest.raises(ValueError, match="TRANSPARENCIA_API_KEY"):
            pipeline.extract()

    def test_empty_api_key_raises(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("TRANSPARENCIA_API_KEY", "   ")
        pipeline = _make_pipeline()
        with pytest.raises(ValueError, match="TRANSPARENCIA_API_KEY"):
            pipeline.extract()


class TestExtractHappyPath:
    def test_fetches_all_targets(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        # 2 deputados x 1 ano x 1 pagina com items = 2 paginas com items.
        # (Páginas vazias após len < 100 não são contabilizadas pq o
        # loop já para nelas.)
        # As paginas acumuladas contêm items > 0 e aqui batem com 2.
        assert len(pipeline._pages) == 2

    def test_rows_in_counts_items(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        assert pipeline.rows_in == 10  # 2 deputados x 5 emendas

    def test_no_targets_in_graph_short_circuits(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline(targets=[])
        pipeline.extract()
        assert pipeline._pages == []
        assert pipeline.rows_in == 0

    def test_invalid_api_key_401_aborts(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        # MockTransport devolve 401 quando o header chave-api-dados não
        # bate com "test-key". Simula chave errada forçando a mismatch.
        pipeline = _make_pipeline(force_status=401)
        with pytest.raises(RuntimeError, match="401"):
            pipeline.extract()


# ---------------------------------------------------------------------------
# transform — Amendment nodes + PROPOS rels com proveniência
# ---------------------------------------------------------------------------


class TestTransform:
    def test_produces_amendments(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 2 deputados x 5 emendas, todos com codigoEmenda distinto.
        assert len(pipeline.amendments) == 10

    def test_produces_rels(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.proposed_rels) == 10
        for rel in pipeline.proposed_rels:
            assert rel["source_key"].startswith("camara_")
            assert rel["target_key"].startswith("pte_")

    def test_node_shape(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        node = pipeline.amendments[0]
        for field in (
            "amendment_id", "tipo", "funcao", "municipio", "uf",
            "valor_empenhado", "valor_pago", "ano", "autor_nome",
        ):
            assert field in node
        assert node["uf"] == "GO"
        assert isinstance(node["valor_empenhado"], float)
        assert isinstance(node["valor_pago"], float)


class TestProvenanceAndSnapshot:
    def test_every_node_has_full_provenance(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for node in pipeline.amendments:
            assert node["source_id"] == _SOURCE_ID
            assert node["source_record_id"] == node["amendment_id"]
            assert node["source_url"].startswith(
                "https://api.portaldatransparencia.gov.br",
            )
            assert node["ingested_at"]
            assert node["run_id"].startswith(f"{_SOURCE_ID}_")
            assert node["source_snapshot_uri"]
            assert node["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")

    def test_every_rel_has_full_provenance(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for rel in pipeline.proposed_rels:
            assert rel["source_id"] == _SOURCE_ID
            assert rel["source_url"].startswith(
                "https://api.portaldatransparencia.gov.br",
            )
            assert rel["source_snapshot_uri"]


# ---------------------------------------------------------------------------
# TestArchivalRetrofit — padrão dos 10 pipelines legados
# ---------------------------------------------------------------------------


class TestArchivalRetrofit:
    """Archival: emendas_parlamentares_go arquiva cada página do Portal."""

    def test_snapshot_files_on_disk(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        source_dir = archival_root / _SOURCE_ID
        assert source_dir.exists()
        files = list(source_dir.rglob("*.json"))
        # 2 autores x 1 ano x 1 pagina = 2 snapshots distintos.
        assert len(files) >= 2

    def test_snapshot_uri_shape(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for node in pipeline.amendments:
            uri = node["source_snapshot_uri"]
            parts = uri.split("/")
            # Formato: source_id/YYYY-MM/hash12.json
            assert parts[0] == _SOURCE_ID
            assert parts[2].endswith(".json")

    def test_snapshot_round_trip(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        sample_uri = pipeline.amendments[0]["source_snapshot_uri"]
        restored = restore_snapshot(sample_uri)
        # Deve ser um JSON válido (lista de items do fixture).
        payload = json.loads(restored.decode("utf-8"))
        assert isinstance(payload, list)

    def test_offline_path_nao_popula_snapshot_uri(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """archive_online=False deixa ``source_snapshot_uri`` fora (opt-in)."""
        pipeline = _make_pipeline(archive_online=False)
        pipeline.extract()
        pipeline.transform()
        assert pipeline.amendments
        for node in pipeline.amendments:
            # attach_provenance só injeta a chave quando snapshot_uri != None.
            assert "source_snapshot_uri" not in node
        for rel in pipeline.proposed_rels:
            assert "source_snapshot_uri" not in rel


# ---------------------------------------------------------------------------
# load — gravação no grafo (mock driver)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_calls_session(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        # Pelo menos nodes + rels + discovery query = 3 chamadas.
        assert session.run.call_count >= 3

    def test_load_noop_when_no_amendments(
        self,
        _api_key: None,  # noqa: ARG002
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        pipeline = _make_pipeline(targets=[])
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        assert pipeline.amendments == []
