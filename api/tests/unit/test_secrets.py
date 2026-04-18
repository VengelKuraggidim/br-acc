"""Tests de unidade pro helper load_secret (api side).

Cobre os caminhos negativos — helper não tem fallback pra env var, então
o caminho feliz exige Secret Manager real (fora do escopo unit).
"""

from __future__ import annotations

import pytest

from bracc.secrets import SecretNotFoundError, load_secret


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Limpa lru_cache do load_secret entre testes."""
    load_secret.cache_clear()


class TestGcpProjectIdObrigatorio:
    def test_levanta_sem_gcp_project_id(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("neo4j-password")

    def test_levanta_com_gcp_project_id_vazio(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("GCP_PROJECT_ID", "")
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("jwt-secret")

    def test_levanta_com_gcp_project_id_whitespace(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("GCP_PROJECT_ID", "   ")
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("transparencia-key")


class TestMensagemDeErro:
    def test_cita_prefixo_e_project_id_var(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(SecretNotFoundError) as exc:
            load_secret("neo4j-password")
        msg = str(exc.value)
        assert "GCP_PROJECT_ID" in msg
        assert "fiscal-cidadao-neo4j-password" in msg
        assert "gcloud auth application-default login" in msg
