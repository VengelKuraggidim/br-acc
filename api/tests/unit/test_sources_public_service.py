from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from bracc.services import sources_public_service
from bracc.services.sources_public_service import (
    _derive_live_badge,
    build_public_sources,
    build_public_sources_grouped,
    get_copy_path,
    load_public_copy,
)

if TYPE_CHECKING:
    from pathlib import Path

CSV_HEADER = (
    "source_id,name,category,tier,status,implementation_state,load_state,"
    "frequency,in_universe_v1,primary_url,pipeline_id,owner_agent,access_mode,"
    "notes,public_access_mode,discovery_status,last_seen_url,"
    "cadence_expected,cadence_observed,quality_status\n"
)


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    load_public_copy.cache_clear()


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRACC_SOURCES_COPY_PATH", raising=False)
    monkeypatch.delenv("BRACC_SOURCE_REGISTRY_PATH", raising=False)


def _write_copy(tmp_path: Path, sources: dict[str, dict]) -> Path:
    path = tmp_path / "copy.json"
    data = {
        "category_labels": {"identity": "Cadastro de empresas", "state": "Goiás — estado"},
        "sources": sources,
    }
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def _write_registry(tmp_path: Path, rows: list[str]) -> Path:
    path = tmp_path / "registry.csv"
    path.write_text(CSV_HEADER + "\n".join(rows) + "\n", encoding="utf-8")
    return path


class TestLoadPublicCopy:
    def test_returns_empty_when_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(tmp_path / "nao-existe.json"))
        data = load_public_copy()
        assert data == {"_meta": {}, "category_labels": {}, "sources": {}}

    def test_loads_json_content(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = _write_copy(
            tmp_path,
            {"cnpj": {"sigla_full": "CNPJ (Receita)", "o_que_e": "teste"}},
        )
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(path))
        data = load_public_copy()
        assert data["sources"]["cnpj"]["sigla_full"] == "CNPJ (Receita)"


class TestBuildPublicSources:
    def test_hidra_copy_no_registry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = _write_registry(
            tmp_path,
            [
                "cnpj,Receita CNPJ,identity,P0,loaded,implemented,loaded,monthly,true,"
                "https://example.com,cnpj,A,file,notes,,,,,,healthy",
            ],
        )
        copy = _write_copy(
            tmp_path,
            {
                "cnpj": {
                    "sigla_full": "CNPJ (Receita)",
                    "o_que_e": "É o cadastro.",
                    "o_que_pegamos": "Baixamos tudo.",
                    "por_que_importa": "Importa.",
                    "arquivos_exemplo": ["ZIP"],
                }
            },
        )
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(registry))
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(copy))
        result = build_public_sources()
        assert len(result) == 1
        assert result[0]["id"] == "cnpj"
        assert result[0]["sigla_full"] == "CNPJ (Receita)"
        assert result[0]["category_label"] == "Cadastro de empresas"
        assert result[0]["copy_disponivel"] is True
        assert result[0]["arquivos_exemplo"] == ["ZIP"]
        # Sem live => badge "sem_dados" por default
        assert result[0]["live"]["badge"] == "sem_dados"

    def test_inclui_tces_e_portais_de_outros_estados(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TCEs/portais não-GO entram no grafo pra cruzar conexões cross-estado.

        Ver memória project_go_scope_policy: escopo GO é das entidades-alvo,
        não do grafo. Políticos goianos podem ter contratos/sócios em outros
        estados — filtrar aqui perderia as conexões.
        """
        registry = _write_registry(
            tmp_path,
            [
                "tce_go,TCE Goiás,state,P0,loaded,implemented,loaded,daily,true,"
                "https://tce.go,tce_go,A,api,,,,,,,healthy",
                "tce_sp,TCE Sao Paulo,state,P0,loaded,implemented,loaded,daily,true,"
                "https://tce.sp,tce_sp,A,api,,,,,,,healthy",
                "state_portal_sp,SP portal,state,P0,loaded,implemented,loaded,daily,true,"
                "https://x,state_portal_sp,A,api,,,,,,,healthy",
                "state_portal_go,GO portal,state,P0,loaded,implemented,loaded,daily,true,"
                "https://x,state_portal_go,A,api,,,,,,,healthy",
            ],
        )
        copy = _write_copy(tmp_path, {})
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(registry))
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(copy))
        ids = {s["id"] for s in build_public_sources()}
        assert ids == {"tce_go", "tce_sp", "state_portal_sp", "state_portal_go"}

    def test_exclui_pipelines_de_enriquecimento(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = _write_registry(
            tmp_path,
            [
                "cnpj,Receita,identity,P0,loaded,implemented,loaded,monthly,true,"
                "https://x,cnpj,A,file,,,,,,,healthy",
                "entity_resolution_politicos_go,ER,enrichment,P0,loaded,implemented,"
                "loaded,weekly,true,https://x,er,A,api,,,,,,,healthy",
                "propagacao_fotos_person,Prop,enrichment,P0,loaded,implemented,"
                "loaded,weekly,true,https://x,pf,A,api,,,,,,,healthy",
            ],
        )
        copy = _write_copy(tmp_path, {})
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(registry))
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(copy))
        ids = {s["id"] for s in build_public_sources()}
        assert ids == {"cnpj"}

    def test_copy_ausente_retorna_name_como_sigla(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = _write_registry(
            tmp_path,
            [
                "foo,Foo Bar,identity,P0,loaded,implemented,loaded,monthly,true,"
                "https://x,foo,A,file,,,,,,,healthy",
            ],
        )
        copy = _write_copy(tmp_path, {})
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(registry))
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(copy))
        result = build_public_sources()
        assert result[0]["sigla_full"] == "Foo Bar"
        assert result[0]["copy_disponivel"] is False
        assert result[0]["o_que_e"] is None


class TestBuildPublicSourcesGrouped:
    def test_agrupa_por_category_label(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = _write_registry(
            tmp_path,
            [
                "cnpj,Receita,identity,P0,loaded,implemented,loaded,monthly,true,"
                "https://x,cnpj,A,file,,,,,,,healthy",
                "tse,TSE,electoral,P0,loaded,implemented,loaded,yearly,true,"
                "https://x,tse,A,file,,,,,,,healthy",
                "brasilapi_cnpj,API CNPJ,identity,P0,loaded,implemented,loaded,daily,true,"
                "https://x,api,A,api,,,,,,,healthy",
            ],
        )
        copy = _write_copy(tmp_path, {})
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(registry))
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(copy))
        grupos = build_public_sources_grouped()
        labels = {g["category_label"] for g in grupos}
        assert "Cadastro de empresas" in labels
        # Fonte fora do mapping cai em fallback title-case
        cadastro = next(g for g in grupos if g["category_label"] == "Cadastro de empresas")
        assert {s["id"] for s in cadastro["sources"]} == {"cnpj", "brasilapi_cnpj"}


class TestDeriveLiveBadge:
    def test_sem_dados_quando_sem_statuses(self) -> None:
        assert _derive_live_badge([], 0) == "sem_dados"

    def test_com_dados_quando_loaded_e_rows(self) -> None:
        assert _derive_live_badge(["loaded"], 1000) == "com_dados"
        assert _derive_live_badge(["loaded", "running"], 42) == "com_dados"

    def test_parcial_quando_loaded_mas_zero_rows(self) -> None:
        assert _derive_live_badge(["loaded"], 0) == "parcial"

    def test_parcial_quando_running(self) -> None:
        assert _derive_live_badge(["running"], 0) == "parcial"

    def test_falhou_quando_quality_fail_sem_loaded(self) -> None:
        assert _derive_live_badge(["quality_fail"], 0) == "falhou"

    def test_com_dados_vence_quality_fail(self) -> None:
        """Se uma run anterior carregou, falha posterior não apaga o badge."""
        assert _derive_live_badge(["loaded", "quality_fail"], 500) == "com_dados"


class TestBuildPublicSourcesComLive:
    def test_hidrata_live_status(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = _write_registry(
            tmp_path,
            [
                "cnpj,Receita,identity,P0,loaded,implemented,loaded,monthly,true,"
                "https://x,cnpj,A,file,,,,,,,healthy",
                "dou,DOU,gazette,P0,not_loaded,implemented,not_loaded,daily,true,"
                "https://x,dou,A,file,,,,,,,healthy",
            ],
        )
        copy = _write_copy(tmp_path, {})
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(registry))
        monkeypatch.setenv("BRACC_SOURCES_COPY_PATH", str(copy))
        live = {
            "cnpj": {
                "runs": 2,
                "last_run_at": "2026-04-18T00:00:00Z",
                "rows_loaded": 50000,
                "statuses": ["loaded"],
                "badge": "com_dados",
            }
        }
        result = build_public_sources(live)
        by_id = {s["id"]: s for s in result}
        assert by_id["cnpj"]["live"]["badge"] == "com_dados"
        assert by_id["cnpj"]["live"]["rows_loaded"] == 50000
        # dou sem entry em live => fallback
        assert by_id["dou"]["live"]["badge"] == "sem_dados"
        assert by_id["dou"]["live"]["runs"] == 0
        assert by_id["dou"]["declared_load_state"] == "not_loaded"


def test_production_copy_loads() -> None:
    """Sanity check do arquivo real enviado com o repo."""
    path = get_copy_path()
    if not path.exists():
        pytest.skip("Copy file não presente neste deploy")
    sources_public_service.load_public_copy.cache_clear()
    data = load_public_copy()
    assert "sources" in data
    assert "cnpj" in data["sources"]
    assert data["sources"]["cnpj"]["sigla_full"]
