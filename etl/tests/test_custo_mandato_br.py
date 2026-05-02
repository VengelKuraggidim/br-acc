"""Tests for the ``custo_mandato_br`` pipeline.

Cobre:

* metadata — name + source_id + cargos cobertos no MVP.
* extract — archive_fetch é chamado em cada URL legal distinta;
  falha graciosa quando fetch der 404/timeout.
* transform — ``CustoMandato`` por cargo + ``CustoComponente`` por linha
  + relacionamento; provenance carimbada com snapshot quando disponível.
* contrato de proveniência — todos os nós/rels têm os 5 campos required
  (source_id, source_record_id, source_url, ingested_at, run_id).
* totais — soma só componentes com ``incluir_no_total=True`` e
  ``valor_mensal != None`` (CEAP teto não infla; "não divulgado" não soma).
* idempotência — re-rodar com mesmas constantes produz mesmo grafo
  (validado indiretamente pela estabilidade dos componente_id).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.custo_mandato_br import (
    _CARGO_META,
    _COMPONENTS,
    _SOURCE_ID,
    CustoMandatoBrPipeline,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures — MockTransport que devolve HTML pra qualquer URL legal.
# ---------------------------------------------------------------------------


def _build_transport(
    *,
    fail_urls: frozenset[str] = frozenset(),
) -> httpx.MockTransport:
    """Retorna handler que devolve HTML pra todas URLs, exceto as listadas."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in fail_urls:
            return httpx.Response(404, content=b"<h1>not found</h1>")
        # Conteúdo único por URL pra garantir hash distinto e snapshots
        # separados (validamos contagem em test_archival_writes_snapshots).
        body = f"<html><body>fake legal page for {url}</body></html>".encode()
        return httpx.Response(
            200,
            content=body,
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
def pipeline(
    archival_root: Path,  # noqa: ARG001 — só ativa o env var
) -> CustoMandatoBrPipeline:
    transport = _build_transport()
    return CustoMandatoBrPipeline(
        driver=MagicMock(),
        data_dir="./data",
        http_client_factory=lambda: httpx.Client(
            transport=transport, follow_redirects=True,
        ),
    )


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert CustoMandatoBrPipeline.name == "custo_mandato_br"

    def test_source_id(self) -> None:
        assert CustoMandatoBrPipeline.source_id == _SOURCE_ID

    def test_mvp_cargos_cobertos(self) -> None:
        assert set(_COMPONENTS.keys()) == {
            "dep_federal", "senador", "dep_estadual_go", "governador_go",
        }

    def test_meta_alinhado_com_components(self) -> None:
        # Todo cargo em _COMPONENTS precisa ter metadata em _CARGO_META.
        assert set(_COMPONENTS.keys()) == set(_CARGO_META.keys())


# ---------------------------------------------------------------------------
# extract — archival
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_archives_each_unique_url(
        self,
        pipeline: CustoMandatoBrPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # Cada URL distinta vira um snapshot. Pelo menos 1 (registry url).
        snapshots = list((archival_root / _SOURCE_ID).rglob("*.html"))
        assert len(snapshots) >= 1
        # Todas URLs visitadas estão em _snapshot_by_url.
        assert all(uri for uri in pipeline._snapshot_by_url.values())

    def test_extract_failure_is_graceful(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        # Faz a URL do subsidio do dep federal falhar — pipeline não levanta,
        # snapshot fica None e componente entra sem cópia preservada.
        target = next(
            c["fonte_url"]
            for c in _COMPONENTS["dep_federal"]
            if c["componente_id"].endswith(":subsidio")
        )
        transport = _build_transport(fail_urls=frozenset({target}))
        pipeline = CustoMandatoBrPipeline(
            driver=MagicMock(),
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
        )
        pipeline.extract()
        assert pipeline._snapshot_by_url[target] is None
        # Outras URLs continuam com snapshot.
        assert any(uri for uri in pipeline._snapshot_by_url.values())


# ---------------------------------------------------------------------------
# transform — nodes + components + provenance
# ---------------------------------------------------------------------------


class TestTransform:
    def test_produces_one_cargo_per_mvp(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        cargos = {c["cargo"] for c in pipeline.cargos}
        assert cargos == {
            "dep_federal", "senador", "dep_estadual_go", "governador_go",
        }

    def test_componentes_count_matches_constants(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        esperado = sum(len(v) for v in _COMPONENTS.values())
        assert len(pipeline.componentes) == esperado

    def test_relacionamentos_count_matches_componentes(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.relacionamentos) == len(pipeline.componentes)

    def test_cargo_node_carries_provenance_and_snapshot(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for cargo_node in pipeline.cargos:
            for field in (
                "source_id", "source_record_id", "source_url",
                "ingested_at", "run_id",
            ):
                assert cargo_node.get(field), (
                    f"cargo {cargo_node['cargo']} missing {field}"
                )
            assert cargo_node["source_id"] == _SOURCE_ID
            assert cargo_node["source_url"].startswith("http")
            # Registry URL foi fetchada com sucesso → snapshot presente.
            assert cargo_node["source_snapshot_uri"]

    def test_componente_carries_per_componente_url(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Cada componente referencia a fonte legal específica dele —
        # não cola tudo na URL guarda-chuva do registry.
        urls_componentes = {c["fonte_url"] for c in pipeline.componentes}
        # Pelo menos 3 URLs distintas (subsídio, gabinete, CEAP, etc.)
        assert len(urls_componentes) >= 3

    def test_total_mensal_dep_federal_exclui_ceap_e_nao_divulgado(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        df = next(c for c in pipeline.cargos if c["cargo"] == "dep_federal")
        # subsidio (46366.19) + gabinete (165844.80) + auxilio_moradia (4253.00)
        # = 216463.99. CEAP (incluir_no_total=False) e saude_encargos
        # (valor=None) ficam de fora.
        assert df["custo_mensal_individual"] == pytest.approx(216463.99, rel=1e-4)

    def test_custo_anual_total_513_deputados(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        df = next(c for c in pipeline.cargos if c["cargo"] == "dep_federal")
        esperado = df["custo_mensal_individual"] * 12 * 513
        assert df["custo_anual_total"] == pytest.approx(esperado, rel=1e-6)

    def test_governador_go_carrega_subsidio_da_lei_estadual(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        # Governador GO carrega subsídio fixado pela Lei nº 17.254/2011
        # (GO) com reajustes posteriores. Vice-Governador entra como
        # componente extra com ``incluir_no_total=False`` (não soma).
        pipeline.extract()
        pipeline.transform()
        gov = next(
            c for c in pipeline.cargos if c["cargo"] == "governador_go"
        )
        assert gov["custo_mensal_individual"] > 0.0
        assert gov["custo_anual_total"] == pytest.approx(
            gov["custo_mensal_individual"] * 12, rel=1e-6,
        )
        # Vice-Governador é tracked como componente, mas não somado.
        vice = [
            c for c in pipeline.componentes
            if c["componente_id"] == "governador_go:vice_governador"
        ]
        assert len(vice) == 1
        assert vice[0].get("incluir_no_total") is False

    def test_componente_sem_valor_entra_no_grafo(
        self, pipeline: CustoMandatoBrPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # ``saude_encargos`` é o exemplo: valor_mensal=None mas componente
        # entra pra o PWA poder renderizar "não divulgado".
        opacos = [
            c for c in pipeline.componentes
            if c["componente_id"] == "dep_federal:saude_encargos"
        ]
        assert len(opacos) == 1
        assert opacos[0]["valor_mensal"] is None
        assert "não divulgado" in opacos[0]["valor_observacao"]


# ---------------------------------------------------------------------------
# load — chama Neo4jBatchLoader com 3 grupos (cargos + componentes + rels)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_calls_loader_for_all_three_groups(
        self,
        pipeline: CustoMandatoBrPipeline,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        called: list[tuple[str, ...]] = []

        class _StubLoader:
            def __init__(self, _driver: object) -> None:
                pass

            def load_nodes(
                self, label: str, rows: list[dict[str, Any]], key_field: str,
            ) -> int:
                called.append(("nodes", label, str(len(rows)), key_field))
                return len(rows)

            def load_relationships(self, **kwargs: Any) -> int:
                called.append(
                    ("rels", str(kwargs["rel_type"]), str(len(kwargs["rows"]))),
                )
                return len(kwargs["rows"])

        monkeypatch.setattr(
            "bracc_etl.pipelines.custo_mandato_br.Neo4jBatchLoader",
            _StubLoader,
        )
        pipeline.load()
        # 2 grupos de nodes + 1 grupo de rels.
        assert any(c[0] == "nodes" and c[1] == "CustoMandato" for c in called)
        assert any(c[0] == "nodes" and c[1] == "CustoComponente" for c in called)
        assert any(c[0] == "rels" and c[1] == "TEM_COMPONENTE" for c in called)

    def test_load_noop_when_nada_extracted(
        self,
        pipeline: CustoMandatoBrPipeline,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pipeline.cargos.clear()
        called: list[str] = []

        def _stub_loader(_driver: object) -> MagicMock:
            called.append("ctor")
            return MagicMock()

        monkeypatch.setattr(
            "bracc_etl.pipelines.custo_mandato_br.Neo4jBatchLoader",
            _stub_loader,
        )
        pipeline.load()
        assert called == []
