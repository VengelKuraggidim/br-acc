"""Integration tests do endpoint ``GET /politico/{entity_id}``.

Após Fase 04.F o endpoint devolve o shape completo de :class:`PerfilPolitico`
(22 campos top-level), orquestrado por :mod:`bracc.services.perfil_service`.
Estes testes mockam ``obter_perfil`` no nível do router pra validar:

* happy path completo → shape PerfilPolitico integral;
* 404 quando a entidade não existe ou não é político;
* 502 quando o driver Neo4j quebra.

Testes da lógica do service (fixtures de sub-services, assembly detalhado)
ficam em ``tests/unit/test_perfil_service.py`` — aqui é só adapter/HTTP.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest

from bracc.models.perfil import (
    Emenda,
    PerfilPolitico,
    PoliticoResumo,
)
from bracc.services.perfil_service import DriverError, EntityNotFoundError

if TYPE_CHECKING:
    from httpx import AsyncClient


def _perfil_fixture() -> PerfilPolitico:
    """PerfilPolitico com os 22 campos preenchidos — happy path."""
    politico = PoliticoResumo(
        id="4:abc:1",
        nome="Deputado Exemplo",
        cpf=None,  # CPF pleno NUNCA aqui — mascarado upstream
        patrimonio=1_500_000.0,
        patrimonio_formatado="R$ 1.5 mil",
        is_pep=False,
        partido="ABC",
        cargo="Deputado(a) Federal",
        uf="GO",
    )
    emenda = Emenda(
        id="EM-2024-001",
        tipo="Emenda individual (feita por um unico parlamentar)",
        funcao="Saude publica",
        municipio="Goiania",
        uf="GO",
        valor_empenhado=100_000.0,
        valor_empenhado_fmt="R$ 100.0 mil",
        valor_pago=80_000.0,
        valor_pago_fmt="R$ 80.0 mil",
    )
    return PerfilPolitico(
        politico=politico,
        resumo="Deputado Exemplo e Deputado(a) Federal.",
        emendas=[emenda],
        total_emendas_valor=80_000.0,
        total_emendas_valor_fmt="R$ 80.0 mil",
        empresas=[],
        contratos=[],
        despesas_gabinete=[],
        total_despesas_gabinete=0.0,
        total_despesas_gabinete_fmt="R$ 0,00",
        comparacao_cidada=[],
        comparacao_cidada_resumo="",
        alertas=[{"tipo": "ok", "icone": "ok", "texto": "Tudo certo"}],
        fonte_emendas="bracc",
        descricao_conexoes="",
        doadores_empresa=[],
        doadores_pessoa=[],
        total_doacoes=0.0,
        total_doacoes_fmt="R$ 0,00",
        socios=[],
        familia=[],
        aviso_despesas="",
        validacao_tse=None,
    )


@pytest.mark.anyio
async def test_politico_happy_path_devolve_perfil_completo(
    client: AsyncClient,
) -> None:
    with patch(
        "bracc.routers.pwa_parity.perfil_service.obter_perfil",
        new_callable=AsyncMock,
        return_value=_perfil_fixture(),
    ):
        response = await client.get("/politico/4:abc:1")

    assert response.status_code == 200
    body = response.json()
    # Shape básico — 22 campos top-level presentes.
    assert body["politico"]["id"] == "4:abc:1"
    assert body["politico"]["nome"] == "Deputado Exemplo"
    assert body["politico"]["uf"] == "GO"
    assert body["politico"]["cargo"] == "Deputado(a) Federal"
    assert body["resumo"].startswith("Deputado Exemplo")
    assert len(body["emendas"]) == 1
    assert body["emendas"][0]["id"] == "EM-2024-001"
    assert body["total_emendas_valor"] == 80_000.0
    assert body["total_emendas_valor_fmt"].startswith("R$")
    assert body["empresas"] == []
    assert body["contratos"] == []
    assert body["despesas_gabinete"] == []
    assert body["total_despesas_gabinete"] == 0.0
    assert body["comparacao_cidada"] == []
    assert body["comparacao_cidada_resumo"] == ""
    assert body["alertas"] == [{"tipo": "ok", "icone": "ok", "texto": "Tudo certo"}]
    assert body["fonte_emendas"] == "bracc"
    assert body["descricao_conexoes"] == ""
    assert body["doadores_empresa"] == []
    assert body["doadores_pessoa"] == []
    assert body["total_doacoes"] == 0.0
    assert body["socios"] == []
    assert body["familia"] == []
    assert body["aviso_despesas"] == ""
    assert body["validacao_tse"] is None


@pytest.mark.anyio
async def test_politico_404_quando_nao_encontrado(
    client: AsyncClient,
) -> None:
    with patch(
        "bracc.routers.pwa_parity.perfil_service.obter_perfil",
        new_callable=AsyncMock,
        side_effect=EntityNotFoundError("Politico 'xxx' nao encontrado"),
    ):
        response = await client.get("/politico/xxx")

    assert response.status_code == 404
    assert "nao encontrado" in response.json()["detail"]


@pytest.mark.anyio
async def test_politico_502_em_driver_error(client: AsyncClient) -> None:
    with patch(
        "bracc.routers.pwa_parity.perfil_service.obter_perfil",
        new_callable=AsyncMock,
        side_effect=DriverError("neo4j down"),
    ):
        response = await client.get("/politico/4:abc:1")

    assert response.status_code == 502
    assert "neo4j down" in response.json()["detail"]
