"""Tests for the ``custo_mandato_municipal_go`` pipeline.

Cobre a paridade com ``custo_mandato_br`` (metadata, extract archival,
transform, provenance) + invariantes específicas do escopo municipal GO:

* cargos cobertos = top-10 cidades GO × {prefeito, vereador} = 20 cargos
  (Censo IBGE 2022). Goiânia é o cargo seed do MVP; expansão fase 2
  adiciona Aparecida, Anápolis, Rio Verde, Águas Lindas, Luziânia,
  Valparaíso, Trindade, Formosa, Senador Canedo.
* esfera = "municipal" e uf = "GO" e municipio = slug correspondente
* cap do vereador = % × subsídio dep estadual GO conforme CF Art. 29 VI
  (faixa por população: >500k → 75%; 300-500k → 60%; 100-300k → 50%)
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
    _GO_MUNICIPIOS,
    _SOURCE_ID,
    _SUBSIDIO_DEP_ESTADUAL_GO,
    _VEREADOR_GOIANIA_CAP,
    CustoMandatoMunicipalGoPipeline,
    _vereador_min_seats,
    _vereador_pct_tier,
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

    def test_cargos_cobertos_pareados_com_municipios(self) -> None:
        # Cada município gera 2 cargos (prefeito + vereador). _COMPONENTS
        # tem que bater com 2 × len(_GO_MUNICIPIOS).
        assert len(_COMPONENTS) == 2 * len(_GO_MUNICIPIOS)
        slugs = {m["slug"] for m in _GO_MUNICIPIOS}
        for slug in slugs:
            assert f"prefeito_{slug}" in _COMPONENTS
            assert f"vereador_{slug}" in _COMPONENTS

    def test_goiania_seed_preservada(self) -> None:
        # Goiânia foi o cargo seed do MVP — guard contra regressão dos
        # slugs/keys que o service e o registry referenciam.
        assert "prefeito_goiania" in _COMPONENTS
        assert "vereador_goiania" in _COMPONENTS

    def test_meta_alinhado_com_components(self) -> None:
        assert set(_COMPONENTS.keys()) == set(_CARGO_META.keys())

    def test_cap_vereador_goiania_consistente_com_cf(self) -> None:
        # CF Art. 29 VI: vereador em município >500k hab → até 75% do
        # subsídio do dep estadual. Invariante: se o subsídio base mudar
        # em custo_mandato_br, o cap do vereador precisa ser re-derivado.
        assert pytest.approx(
            _SUBSIDIO_DEP_ESTADUAL_GO * 0.75, rel=1e-9,
        ) == _VEREADOR_GOIANIA_CAP


class TestTierFormula:
    """CF Art. 29 VI — % do subsídio do dep estadual por faixa populacional."""

    @pytest.mark.parametrize(
        ("populacao", "pct_esperado"),
        [
            (5_000, 0.20),
            (10_000, 0.20),
            (10_001, 0.30),
            (50_000, 0.30),
            (50_001, 0.40),
            (100_000, 0.40),
            (100_001, 0.50),
            (300_000, 0.50),
            (300_001, 0.60),
            (500_000, 0.60),
            (500_001, 0.75),
            (1_437_237, 0.75),  # Goiânia
        ],
    )
    def test_pct_tier_em_cada_faixa(
        self, populacao: int, pct_esperado: float,
    ) -> None:
        assert _vereador_pct_tier(populacao) == pct_esperado

    @pytest.mark.parametrize(
        ("populacao", "min_esperado"),
        [
            (10_000, 9),
            (15_000, 9),
            (15_001, 11),
            (50_000, 13),
            (100_000, 17),  # 80k-120k → 17
            (300_000, 21),
            (591_418, 25),  # Aparecida — 450-600k → 25
        ],
    )
    def test_min_seats_em_cada_faixa(
        self, populacao: int, min_esperado: int,
    ) -> None:
        assert _vereador_min_seats(populacao) == min_esperado

    def test_anapolis_cap_eh_60_pct(self) -> None:
        # Anápolis (391k) cai na faixa 300-500k → 60% do dep estadual.
        anapolis = next(m for m in _GO_MUNICIPIOS if m["slug"] == "anapolis")
        assert _vereador_pct_tier(int(anapolis["populacao"])) == 0.60
        subsidio_node = next(
            c for c in _COMPONENTS["vereador_anapolis"]
            if c["componente_id"] == "vereador_anapolis:subsidio"
        )
        assert subsidio_node["valor_mensal"] == pytest.approx(
            _SUBSIDIO_DEP_ESTADUAL_GO * 0.60, rel=1e-9,
        )

    def test_rio_verde_cap_eh_50_pct(self) -> None:
        # Rio Verde (245k) cai na faixa 100-300k → 50%.
        rv = next(m for m in _GO_MUNICIPIOS if m["slug"] == "rio_verde")
        assert _vereador_pct_tier(int(rv["populacao"])) == 0.50
        subsidio_node = next(
            c for c in _COMPONENTS["vereador_rio_verde"]
            if c["componente_id"] == "vereador_rio_verde:subsidio"
        )
        assert subsidio_node["valor_mensal"] == pytest.approx(
            _SUBSIDIO_DEP_ESTADUAL_GO * 0.50, rel=1e-9,
        )

    def test_aparecida_cap_eh_75_pct(self) -> None:
        # Aparecida de Goiânia (591k) cai na faixa >500k → 75%.
        ap = next(
            m for m in _GO_MUNICIPIOS if m["slug"] == "aparecida_de_goiania"
        )
        assert _vereador_pct_tier(int(ap["populacao"])) == 0.75


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
    def test_produces_two_cargos_per_municipio(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        cargos = {c["cargo"] for c in pipeline.cargos}
        assert len(cargos) == 2 * len(_GO_MUNICIPIOS)
        for m in _GO_MUNICIPIOS:
            slug = m["slug"]
            assert f"prefeito_{slug}" in cargos
            assert f"vereador_{slug}" in cargos

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
        # Guard contra regressão do cargo-chave ser municipal. ``municipio``
        # tem que bater o slug do cargo.
        pipeline.extract()
        pipeline.transform()
        slugs_validos = {m["slug"] for m in _GO_MUNICIPIOS}
        for cargo_node in pipeline.cargos:
            assert cargo_node["esfera"] == "municipal"
            assert cargo_node["uf"] == "GO"
            assert cargo_node["municipio"] in slugs_validos
            assert cargo_node["cargo"].endswith(cargo_node["municipio"])

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
        # Lei Orgânica não tem API pública → subsídio fica None pra todas
        # as cidades, total zera. Mesmo padrão do governador_go em
        # custo_mandato_br.
        pipeline.extract()
        pipeline.transform()
        prefeitos = [
            c for c in pipeline.cargos if c["cargo"].startswith("prefeito_")
        ]
        assert len(prefeitos) == len(_GO_MUNICIPIOS)
        for prefeito in prefeitos:
            assert prefeito["custo_mensal_individual"] == 0.0, prefeito["cargo"]
            assert prefeito["custo_anual_total"] == 0.0, prefeito["cargo"]

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

    def test_anapolis_custo_anual_usa_min_seats_cf(
        self, pipeline: CustoMandatoMunicipalGoPipeline,
    ) -> None:
        # Anápolis (391k → faixa 300-450k) cai em CF Art. 29 IV mín 23
        # vereadores. Subsídio = 60% × 34.774,64.
        pipeline.extract()
        pipeline.transform()
        vereador = next(
            c for c in pipeline.cargos if c["cargo"] == "vereador_anapolis"
        )
        assert vereador["n_titulares"] == 23
        cap = _SUBSIDIO_DEP_ESTADUAL_GO * 0.60
        esperado_anual = cap * 12 * 23
        assert vereador["custo_anual_total"] == pytest.approx(
            esperado_anual, rel=1e-6,
        )


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
