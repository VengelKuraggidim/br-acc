"""Tests for the ``custo_mandato_municipal_go`` pipeline.

Cobre a paridade com ``custo_mandato_br`` (metadata, extract archival,
transform, provenance) + invariantes específicas do escopo municipal GO:

* cargos MVP = {prefeito_goiania, vereador_goiania}
* esfera = "municipal" e uf = "GO" e municipio = "goiania"
* cap constitucional do vereador = 75% × subsídio dep estadual GO
  (CF Art. 29 VI)
* prefeito sem valor materializado (Lei Orgânica sem API) → total mensal
  e anual ficam zero — coerente com ``governador_go`` em ``custo_mandato_br``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.custo_mandato_municipal_go import (
    _CARGO_META,
    _COMPONENTS,
    _SOURCE_ID,
    _SUBSIDIO_DEP_ESTADUAL_GO,
    _VEREADOR_GOIANIA_CAP,
    CustoMandatoMunicipalGoPipeline,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_transport(
    *,
    fail_urls: frozenset[str] = frozenset(),
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in fail_urls:
            return httpx.Response(404, content=b"<h1>not found</h1>")
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
) -> CustoMandatoMunicipalGoPipeline:
    transport = _build_transport()
    return CustoMandatoMunicipalGoPipeline(
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
        assert CustoMandatoMunicipalGoPipeline.name == "custo_mandato_municipal_go"

    def test_source_id(self) -> None:
        assert CustoMandatoMunicipalGoPipeline.source_id == _SOURCE_ID

    def test_mvp_cargos_cobertos(self) -> None:
        assert set(_COMPONENTS.keys()) == {
            "prefeito_goiania", "vereador_goiania",
        }

    def test_meta_alinhado_com_components(self) -> None:
        assert set(_COMPONENTS.keys()) == set(_CARGO_META.keys())

    def test_cap_vereador_consistente_com_cf(self) -> None:
        # CF Art. 29 VI: vereador em município >500k hab → até 75% do
        # subsídio do dep estadual. Invariante: se o subsídio base mudar
        # em custo_mandato_br, o cap do vereador precisa ser re-derivado.
        assert _VEREADOR_GOIANIA_CAP == pytest.approx(
            _SUBSIDIO_DEP_ESTADUAL_GO * 0.75, rel=1e-9,
        )


# ---------------------------------------------------------------------------
# extract — archival
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_archives_each_unique_url(
        self,
        pipeline: CustoMandatoMunicipalGoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        snapshots = list((archival_root / _SOURCE_ID).rglob("*.html"))
        assert len(snapshots) >= 1
        assert all(uri for uri in pipeline._snapshot_by_url.values())

    def test_extract_failure_is_graceful(
        self,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        target = next(
            c["fonte_url"]
            for c in _COMPONENTS["vereador_goiania"]
            if c["componente_id"].endswith(":subsidio")
        )
        transport = _build_transport(fail_urls=frozenset({target}))
        pipeline = CustoMandatoMunicipalGoPipeline(
            driver=MagicMock(),
            http_client_factory=lambda: httpx.Client(
                transport=transport, follow_redirects=True,
            ),
        )
        pipeline.extract()
        assert pipeline._snapshot_by_url[target] is None
        assert any(uri for uri in pipeline._snapshot_by_url.values())


# ---------------------------------------------------------------------------
# transform — nodes + components + provenance
# ---------------------------------------------------------------------------


class TestTransform:
    def test_produces_one_cargo_per_mvp(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        cargos = {c["cargo"] for c in pipeline.cargos}
        assert cargos == {"prefeito_goiania", "vereador_goiania"}

    def test_componentes_count_matches_constants(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        esperado = sum(len(v) for v in _COMPONENTS.values())
        assert len(pipeline.componentes) == esperado

    def test_relacionamentos_count_matches_componentes(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.relacionamentos) == len(pipeline.componentes)

    def test_cargo_node_carries_provenance_and_snapshot(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
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
            assert cargo_node["source_snapshot_uri"]

    def test_esfera_e_municipio_carimbados(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        # Guard contra regressão do cargo-chave ser municipal.
        pipeline.extract()
        pipeline.transform()
        for cargo_node in pipeline.cargos:
            assert cargo_node["esfera"] == "municipal"
            assert cargo_node["uf"] == "GO"
            assert cargo_node["municipio"] == "goiania"

    def test_vereador_subsidio_bate_cap_constitucional(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        vereador = next(
            c for c in pipeline.cargos if c["cargo"] == "vereador_goiania"
        )
        # Só o subsídio entra no total (verba de gabinete está None).
        assert vereador["custo_mensal_individual"] == pytest.approx(
            _VEREADOR_GOIANIA_CAP, rel=1e-9,
        )

    def test_vereador_custo_anual_35_cadeiras(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        vereador = next(
            c for c in pipeline.cargos if c["cargo"] == "vereador_goiania"
        )
        esperado = vereador["custo_mensal_individual"] * 12 * 35
        assert vereador["custo_anual_total"] == pytest.approx(esperado, rel=1e-6)

    def test_prefeito_sem_valor_resulta_em_total_zero(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        # Lei Orgânica não tem API pública → subsídio fica None, total zera.
        # Mesmo padrão do governador_go em custo_mandato_br.
        pipeline.extract()
        pipeline.transform()
        prefeito = next(
            c for c in pipeline.cargos if c["cargo"] == "prefeito_goiania"
        )
        assert prefeito["custo_mensal_individual"] == 0.0
        assert prefeito["custo_anual_total"] == 0.0

    def test_componente_sem_valor_entra_no_grafo(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        opacos = [
            c for c in pipeline.componentes
            if c["componente_id"] == "prefeito_goiania:subsidio"
        ]
        assert len(opacos) == 1
        assert opacos[0]["valor_mensal"] is None
        assert opacos[0]["valor_observacao"]


# ---------------------------------------------------------------------------
# load — chama Neo4jBatchLoader com 3 grupos (cargos + componentes + rels)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_calls_loader_for_all_three_groups(
        self,
        pipeline: CustoMandatoMunicipalGoPipeline,
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
            "bracc_etl.pipelines.custo_mandato_municipal_go.Neo4jBatchLoader",
            _StubLoader,
        )
        pipeline.load()
        assert any(c[0] == "nodes" and c[1] == "CustoMandato" for c in called)
        assert any(c[0] == "nodes" and c[1] == "CustoComponente" for c in called)
        assert any(c[0] == "rels" and c[1] == "TEM_COMPONENTE" for c in called)

    def test_load_noop_when_nada_extracted(
        self,
        pipeline: CustoMandatoMunicipalGoPipeline,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pipeline.cargos.clear()
        called: list[str] = []

        def _stub_loader(_driver: object) -> MagicMock:
            called.append("ctor")
            return MagicMock()

        monkeypatch.setattr(
            "bracc_etl.pipelines.custo_mandato_municipal_go.Neo4jBatchLoader",
            _stub_loader,
        )
        pipeline.load()
        assert called == []
