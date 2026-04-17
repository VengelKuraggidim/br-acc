"""Testes de integracao — verifica endpoints contra APIs reais.

Estes testes fazem chamadas reais ao backend (localhost:8001) e
a API da Camara. Rodam apenas se os servicos estiverem disponiveis.

Marcados com @pytest.mark.integracao para rodar separadamente.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BACKEND_URL = "http://localhost:8001"
CAMARA_API = "https://dadosabertos.camara.leg.br/api/v2"


def _backend_disponivel() -> bool:
    try:
        r = httpx.get(f"{BACKEND_URL}/status", timeout=5)
        return r.status_code == 200
    except httpx.HTTPError:
        return False


def _camara_disponivel() -> bool:
    try:
        r = httpx.get(f"{CAMARA_API}/deputados?nome=teste", timeout=10)
        return r.status_code == 200
    except httpx.HTTPError:
        return False


skip_sem_backend = pytest.mark.skipif(
    not _backend_disponivel(),
    reason="Backend nao esta rodando em localhost:8001",
)

skip_sem_camara = pytest.mark.skipif(
    not _camara_disponivel(),
    reason="API da Camara indisponivel",
)


# === Backend endpoints ===


@skip_sem_backend
class TestBackendEndpoints:
    def test_status_online(self):
        r = httpx.get(f"{BACKEND_URL}/status", timeout=10)
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "online"
        assert data["bracc_conectado"] is True

    def test_buscar_retorna_lista(self):
        r = httpx.get(
            f"{BACKEND_URL}/buscar-tudo",
            params={"q": "silva"},
            timeout=15,
        )
        assert r.status_code == 200
        data = r.json()
        assert "resultados" in data
        assert isinstance(data["resultados"], list)

    def test_buscar_query_curta_retorna_422(self):
        r = httpx.get(
            f"{BACKEND_URL}/buscar-tudo",
            params={"q": "a"},
            timeout=10,
        )
        assert r.status_code == 422

    def test_politico_nao_encontrado_retorna_404(self):
        r = httpx.get(
            f"{BACKEND_URL}/politico/nao_existe_id_falso",
            timeout=15,
        )
        assert r.status_code in (404, 502)


# === Perfil com dados em tempo real ===


@skip_sem_backend
class TestPerfilPoliticoTempoReal:
    """Testa o perfil completo com busca em tempo real."""

    def _buscar_primeiro_politico(self) -> str | None:
        r = httpx.get(
            f"{BACKEND_URL}/buscar",
            params={"nome": "lula"},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        dados = r.json()
        return dados[0]["id"] if dados else None

    def test_perfil_retorna_despesas_gabinete(self):
        entity_id = self._buscar_primeiro_politico()
        if not entity_id:
            pytest.skip("Nenhum politico encontrado na busca")

        r = httpx.get(
            f"{BACKEND_URL}/politico/{entity_id}",
            timeout=30,
        )
        assert r.status_code == 200
        data = r.json()

        # Campos basicos existem
        assert "politico" in data
        assert "alertas" in data
        assert "emendas" in data
        assert "despesas_gabinete" in data
        assert "fonte_emendas" in data

        # Politico tem nome
        assert data["politico"]["nome"]

    def test_perfil_foto_preenchida(self):
        entity_id = self._buscar_primeiro_politico()
        if not entity_id:
            pytest.skip("Nenhum politico encontrado")

        r = httpx.get(
            f"{BACKEND_URL}/politico/{entity_id}",
            timeout=30,
        )
        data = r.json()

        # Se e deputado, deve ter foto da API da Camara
        foto = data["politico"].get("foto_url")
        if foto:
            assert foto.startswith("http")

    def test_perfil_despesas_tem_formato_correto(self):
        entity_id = self._buscar_primeiro_politico()
        if not entity_id:
            pytest.skip("Nenhum politico encontrado")

        r = httpx.get(
            f"{BACKEND_URL}/politico/{entity_id}",
            timeout=30,
        )
        data = r.json()

        for desp in data.get("despesas_gabinete", []):
            assert "tipo" in desp
            assert "total" in desp
            assert "total_fmt" in desp
            assert isinstance(desp["total"], (int, float))
            assert "R$" in desp["total_fmt"]

    def test_perfil_nao_mistura_eleicao_com_emenda(self):
        entity_id = self._buscar_primeiro_politico()
        if not entity_id:
            pytest.skip("Nenhum politico encontrado")

        r = httpx.get(
            f"{BACKEND_URL}/politico/{entity_id}",
            timeout=30,
        )
        data = r.json()

        for emenda in data.get("emendas", []):
            # Emendas nao devem ter tipo "Eleicao"
            assert "eleicao" not in emenda["tipo"].lower(), (
                f"Eleicao misturada como emenda: {emenda}"
            )


# === API da Camara (real) ===


@skip_sem_camara
class TestCamaraApiReal:
    def test_busca_deputado_retorna_dados(self):
        r = httpx.get(
            f"{CAMARA_API}/deputados",
            params={"nome": "lula"},
            timeout=15,
        )
        assert r.status_code == 200
        dados = r.json().get("dados", [])
        assert len(dados) >= 1
        assert "id" in dados[0]
        assert "nome" in dados[0]

    def test_despesas_deputado_retorna_dados(self):
        # Primeiro encontra um deputado
        r = httpx.get(
            f"{CAMARA_API}/deputados",
            params={"nome": "lula"},
            timeout=15,
        )
        dep_id = r.json()["dados"][0]["id"]

        # Depois busca despesas
        r2 = httpx.get(
            f"{CAMARA_API}/deputados/{dep_id}/despesas",
            params={"ano": 2025, "itens": 5},
            timeout=15,
        )
        assert r2.status_code == 200
        dados = r2.json().get("dados", [])
        if dados:
            assert "tipoDespesa" in dados[0]
            assert "valorLiquido" in dados[0]
