"""Testes do modulo de APIs externas (apis_externas.py).

Usa respx para mockar chamadas HTTP — sem depender de APIs reais.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import respx
from httpx import Response

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from apis_externas import (
    CAMARA_API,
    agrupar_despesas_por_tipo,
    buscar_deputado_camara,
    buscar_despesas_deputado,
    converter_emendas_transparencia,
)


# === buscar_deputado_camara ===


@pytest.mark.asyncio
class TestBuscarDeputadoCamara:
    @respx.mock
    async def test_encontra_deputado_por_nome(self):
        respx.get(f"{CAMARA_API}/deputados").mock(
            return_value=Response(200, json={
                "dados": [{"id": 123, "nome": "Fulano", "siglaPartido": "PT"}],
            }),
        )
        respx.get(f"{CAMARA_API}/deputados/123").mock(
            return_value=Response(200, json={
                "dados": {
                    "id": 123,
                    "nomeCivil": "FULANO DE TAL",
                    "cpf": "11111111111",
                    "ultimoStatus": {"urlFoto": "https://foto.jpg"},
                },
            }),
        )

        result = await buscar_deputado_camara("FULANO DE TAL")
        assert result is not None
        assert result["id"] == 123

    @respx.mock
    async def test_nao_encontra_retorna_none(self):
        respx.get(f"{CAMARA_API}/deputados").mock(
            return_value=Response(200, json={"dados": []}),
        )

        result = await buscar_deputado_camara("NINGUEM")
        assert result is None

    @respx.mock
    async def test_match_por_cpf(self):
        respx.get(f"{CAMARA_API}/deputados").mock(
            return_value=Response(200, json={
                "dados": [
                    {"id": 1, "nome": "Dep A"},
                    {"id": 2, "nome": "Dep B"},
                ],
            }),
        )
        respx.get(f"{CAMARA_API}/deputados/1").mock(
            return_value=Response(200, json={
                "dados": {"id": 1, "cpf": "99999999999"},
            }),
        )
        respx.get(f"{CAMARA_API}/deputados/2").mock(
            return_value=Response(200, json={
                "dados": {"id": 2, "cpf": "11111111111"},
            }),
        )

        result = await buscar_deputado_camara("Dep", cpf="111.111.111-11")
        assert result is not None
        assert result["id"] == 2

    @respx.mock
    async def test_fallback_partes_do_nome(self):
        # Nome completo nao encontra
        respx.get(f"{CAMARA_API}/deputados", params={"nome": "JOAO SILVA"}).mock(
            return_value=Response(200, json={"dados": []}),
        )
        # Sobrenome encontra
        respx.get(f"{CAMARA_API}/deputados", params={"nome": "SILVA"}).mock(
            return_value=Response(200, json={
                "dados": [{"id": 5, "nome": "Silva"}],
            }),
        )
        respx.get(f"{CAMARA_API}/deputados/5").mock(
            return_value=Response(200, json={
                "dados": {"id": 5, "nomeCivil": "JOAO SILVA"},
            }),
        )

        result = await buscar_deputado_camara("JOAO SILVA")
        assert result is not None
        assert result["id"] == 5

    @respx.mock
    async def test_api_fora_retorna_none(self):
        respx.get(f"{CAMARA_API}/deputados").mock(
            return_value=Response(500),
        )

        result = await buscar_deputado_camara("FULANO")
        assert result is None

    @respx.mock
    async def test_fallback_sobrenome_nao_confunde_homonimo(self):
        # Nome completo nao encontra: cai no fallback por sobrenome.
        respx.get(
            f"{CAMARA_API}/deputados",
            params={"nome": "Clecio Antonio Alves", "siglaUf": "GO"},
        ).mock(return_value=Response(200, json={"dados": []}))
        # Sobrenome retorna UM unico deputado, mas nao e a pessoa buscada.
        respx.get(
            f"{CAMARA_API}/deputados",
            params={"nome": "Alves", "siglaUf": "GO"},
        ).mock(
            return_value=Response(200, json={
                "dados": [{"id": 999, "nome": "Silvye Alves"}],
            }),
        )
        respx.get(f"{CAMARA_API}/deputados/999").mock(
            return_value=Response(200, json={
                "dados": {
                    "id": 999,
                    "nomeCivil": "Silvye Maria Alves dos Santos",
                    "ultimoStatus": {
                        "nome": "Silvye Alves",
                        "urlFoto": "https://foto-silvye.jpg",
                    },
                },
            }),
        )

        result = await buscar_deputado_camara(
            "Clecio Antonio Alves", uf="GO",
        )
        # Nao pode devolver a Silvye so porque foi o unico resultado
        # do fallback por sobrenome.
        assert result is None


# === buscar_despesas_deputado ===


@pytest.mark.asyncio
class TestBuscarDespesasDeputado:
    @respx.mock
    async def test_busca_despesas_paginadas(self):
        respx.get(
            f"{CAMARA_API}/deputados/123/despesas",
            params={"ano": 2025, "pagina": 1, "itens": 100},
        ).mock(
            return_value=Response(200, json={
                "dados": [
                    {"tipoDespesa": "COMBUSTIVEL", "valorLiquido": 500.0},
                    {"tipoDespesa": "PASSAGEM", "valorLiquido": 1200.0},
                ],
            }),
        )
        respx.get(
            f"{CAMARA_API}/deputados/123/despesas",
            params={"ano": 2025, "pagina": 2, "itens": 100},
        ).mock(
            return_value=Response(200, json={"dados": []}),
        )
        respx.get(
            f"{CAMARA_API}/deputados/123/despesas",
            params={"ano": 2024, "pagina": 1, "itens": 100},
        ).mock(
            return_value=Response(200, json={"dados": []}),
        )

        result = await buscar_despesas_deputado(123, anos=[2025, 2024])
        assert len(result) == 2

    @respx.mock
    async def test_api_erro_retorna_vazio(self):
        respx.get(f"{CAMARA_API}/deputados/999/despesas").mock(
            return_value=Response(500),
        )

        result = await buscar_despesas_deputado(999, anos=[2025])
        assert result == []


# === agrupar_despesas_por_tipo ===


class TestAgruparDespesas:
    def test_agrupamento_basico(self):
        despesas = [
            {"tipoDespesa": "COMBUSTIVEL", "valorLiquido": 100},
            {"tipoDespesa": "COMBUSTIVEL", "valorLiquido": 200},
            {"tipoDespesa": "PASSAGEM", "valorLiquido": 500},
        ]
        resultado = agrupar_despesas_por_tipo(despesas)
        assert len(resultado) == 2
        # Ordenado por valor desc
        assert resultado[0]["tipo"] == "PASSAGEM"
        assert resultado[0]["total"] == 500
        assert resultado[1]["tipo"] == "COMBUSTIVEL"
        assert resultado[1]["total"] == 300

    def test_lista_vazia(self):
        assert agrupar_despesas_por_tipo([]) == []

    def test_valor_none_trata_como_zero(self):
        despesas = [
            {"tipoDespesa": "X", "valorLiquido": None},
            {"tipoDespesa": "X", "valorLiquido": 100},
        ]
        resultado = agrupar_despesas_por_tipo(despesas)
        assert resultado[0]["total"] == 100


# === converter_emendas_transparencia ===


class TestConverterEmendas:
    def test_conversao_basica(self):
        raw = [
            {
                "codigo": "123",
                "tipoEmenda": "Individual",
                "nomeAreaTematica": "Saude",
                "nomeLocalidadeGasto": "Goiania",
                "ufGasto": "GO",
                "valorEmpenhado": 1_000_000,
                "valorPago": 800_000,
            },
        ]
        resultado = converter_emendas_transparencia(raw)
        assert len(resultado) == 1
        assert resultado[0]["amendment_id"] == "123"
        assert resultado[0]["type"] == "Individual"
        assert resultado[0]["value_paid"] == 800_000

    def test_lista_vazia(self):
        assert converter_emendas_transparencia([]) == []

    def test_campos_ausentes(self):
        raw = [{}]
        resultado = converter_emendas_transparencia(raw)
        assert resultado[0]["value_paid"] == 0
        assert resultado[0]["value_committed"] == 0
