"""Tests para bracc.services.custo_mandato_service + router custo-mandato.

Mocka ``execute_query_single`` (sem driver Neo4j real) para validar:

* shape do retorno: 4 cargos MVP suportados, todos com componentes
  ordenados;
* provenance do cargo + por componente é montada quando os campos
  required existem; ``None`` quando faltam;
* cargo válido sem nó no grafo levanta ``CargoNaoEncontradoError``;
* router rejeita cargo fora do enum (422) e devolve 404 quando o nó
  não existe;
* ``valor_mensal_fmt`` segue ``fmt_brl`` e ``None`` quando o valor é
  ``None`` (compat com PWA pra renderizar "não divulgado").
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bracc.services.custo_mandato_service import (
    CARGOS_SUPORTADOS,
    CargoNaoEncontradoError,
    obter_custo_mandato,
)

if TYPE_CHECKING:
    from httpx import AsyncClient


def _node(props: dict[str, Any]) -> dict[str, Any]:
    """Stub de node Neo4j — service usa ``dict(record["mandato"])`` direto."""
    return dict(props)


def _record(
    mandato: dict[str, Any] | None,
    componentes: list[dict[str, Any]],
) -> MagicMock:
    rec = MagicMock()
    data = {"mandato": mandato, "componentes": componentes}
    rec.__getitem__.side_effect = lambda key: data[key]
    rec.get.side_effect = lambda key, default=None: data.get(key, default)
    return rec


_PROVENANCE = {
    "source_id": "custo_mandato_br",
    "source_record_id": "dep_federal",
    "source_url": "https://www.camara.leg.br/transparencia/recursos-humanos/remuneracao",
    "ingested_at": "2026-04-19T12:00:00+00:00",
    "run_id": "custo_mandato_br_20260419120000",
    "source_snapshot_uri": "custo_mandato_br/2026-04/abc123def456.html",
}


def _fake_dep_federal_record() -> MagicMock:
    mandato = _node({
        "cargo": "dep_federal",
        "rotulo_humano": "Deputado(a) federal",
        "esfera": "federal",
        "uf": None,
        "n_titulares": 513,
        "custo_mensal_individual": 216463.99,
        "custo_anual_total": 216463.99 * 12 * 513,
        "equivalente_trabalhadores_min": 142,
        "salario_minimo_referencia": 1518.00,
        "salario_minimo_fonte": "https://www.planalto.gov.br/ccivil_03/_ato2023-2026/2025/decreto/d12342.htm",
        **_PROVENANCE,
    })
    componentes = [
        _node({
            "componente_id": "dep_federal:subsidio",
            "cargo": "dep_federal",
            "rotulo": "Subsídio mensal",
            "valor_mensal": 46366.19,
            "valor_observacao": "",
            "fonte_legal": "Decreto Legislativo nº 277/2024",
            "fonte_url": "https://www.congressonacional.leg.br/materias/166003",
            "incluir_no_total": True,
            "ordem": 0,
            **_PROVENANCE,
        }),
        _node({
            "componente_id": "dep_federal:saude_encargos",
            "cargo": "dep_federal",
            "rotulo": "Saúde + encargos",
            "valor_mensal": None,
            "valor_observacao": "não divulgado em formato consolidado pela Câmara",
            "fonte_legal": "—",
            "fonte_url": "https://www.camara.leg.br/transparencia/recursos-humanos/remuneracao",
            "incluir_no_total": True,
            "ordem": 4,
            **_PROVENANCE,
        }),
    ]
    return _record(mandato, componentes)


# ---------------------------------------------------------------------------
# service-level
# ---------------------------------------------------------------------------


class TestObterCustoMandato:
    def test_cargos_suportados_alinhado_com_pipeline(self) -> None:
        # MVP: dep_federal, senador, dep_estadual_go, governador_go.
        cargos = set(CARGOS_SUPORTADOS)
        assert cargos == {
            "dep_federal", "senador", "dep_estadual_go", "governador_go",
        }

    @pytest.mark.anyio
    async def test_monta_resposta_completa_pra_dep_federal(self) -> None:
        record = _fake_dep_federal_record()
        with patch(
            "bracc.services.custo_mandato_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=record,
        ):
            resultado = await obter_custo_mandato(
                MagicMock(), "dep_federal",
            )

        assert resultado.cargo == "dep_federal"
        assert resultado.n_titulares == 513
        assert resultado.custo_mensal_individual_fmt.startswith("R$")
        assert resultado.custo_anual_total_fmt.startswith("R$")
        assert len(resultado.componentes) == 2
        assert resultado.componentes[0].componente_id == "dep_federal:subsidio"
        # fmt_brl abrevia valores >= 1.000 — assertimos só o prefixo + sufixo.
        assert resultado.componentes[0].valor_mensal_fmt is not None
        assert resultado.componentes[0].valor_mensal_fmt.startswith("R$ ")
        assert "mil" in resultado.componentes[0].valor_mensal_fmt
        # Componente "não divulgado" → valor_mensal=None, fmt=None.
        assert resultado.componentes[1].valor_mensal is None
        assert resultado.componentes[1].valor_mensal_fmt is None
        # Provenance presente no topo + por componente.
        assert resultado.provenance is not None
        assert resultado.provenance.source_id == "custo_mandato_br"
        assert resultado.provenance.snapshot_url is not None
        assert resultado.componentes[0].provenance is not None

    @pytest.mark.anyio
    async def test_provenance_none_quando_faltam_campos(self) -> None:
        # Mandato sem source_url → provenance = None (graceful).
        mandato = _node({
            "cargo": "dep_federal", "rotulo_humano": "X", "esfera": "federal",
            "n_titulares": 513,
            "custo_mensal_individual": 0.0,
            "custo_anual_total": 0.0,
            "equivalente_trabalhadores_min": 0,
            "salario_minimo_referencia": 0.0,
            # Faltam source_*
        })
        record = _record(mandato, [])
        with patch(
            "bracc.services.custo_mandato_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=record,
        ):
            resultado = await obter_custo_mandato(MagicMock(), "dep_federal")
        assert resultado.provenance is None

    @pytest.mark.anyio
    async def test_cargo_sem_no_no_grafo_levanta_erro(self) -> None:
        with patch(
            "bracc.services.custo_mandato_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=None,
        ), pytest.raises(CargoNaoEncontradoError) as exc:
            await obter_custo_mandato(MagicMock(), "senador")
        assert "senador" in str(exc.value)
        assert "custo_mandato_br" in str(exc.value)

    @pytest.mark.anyio
    async def test_record_com_mandato_none_levanta_erro(self) -> None:
        # Driver pode devolver Record com mandato=None (OPTIONAL MATCH não
        # bate). Service trata como cargo não encontrado.
        record = _record(None, [])
        with patch(
            "bracc.services.custo_mandato_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=record,
        ), pytest.raises(CargoNaoEncontradoError):
            await obter_custo_mandato(MagicMock(), "dep_federal")


# ---------------------------------------------------------------------------
# router-level (HTTP)
# ---------------------------------------------------------------------------


class TestRouter:
    @pytest.mark.anyio
    async def test_get_dep_federal_200(self, client: AsyncClient) -> None:
        record = _fake_dep_federal_record()
        with patch(
            "bracc.services.custo_mandato_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=record,
        ):
            resp = await client.get("/custo-mandato/dep_federal")
        assert resp.status_code == 200
        body = resp.json()
        assert body["cargo"] == "dep_federal"
        assert body["n_titulares"] == 513
        assert len(body["componentes"]) == 2

    @pytest.mark.anyio
    async def test_cargo_fora_do_enum_422(self, client: AsyncClient) -> None:
        # FastAPI rejeita antes de tocar o serviço.
        resp = await client.get("/custo-mandato/prefeito")
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_cargo_valido_sem_no_404(self, client: AsyncClient) -> None:
        with patch(
            "bracc.services.custo_mandato_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=None,
        ):
            resp = await client.get("/custo-mandato/governador_go")
        assert resp.status_code == 404
        assert "governador_go" in resp.json()["detail"]

    @pytest.mark.anyio
    async def test_todos_cargos_mvp_aceitos_pelo_router(
        self, client: AsyncClient,
    ) -> None:
        # Smoke: cada cargo MVP atravessa o path validation.
        for cargo in sorted(CARGOS_SUPORTADOS):
            with patch(
                "bracc.services.custo_mandato_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=None,
            ):
                resp = await client.get(f"/custo-mandato/{cargo}")
            assert resp.status_code == 404, (
                f"cargo {cargo} should reach service (404 from missing node), "
                f"got {resp.status_code}"
            )
