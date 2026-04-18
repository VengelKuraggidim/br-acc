"""Tests de unidade pro helper load_secret (api side).

Cobre:
- Fallback pra env var local quando GCP_PROJECT_ID ausente.
- Erro claro quando secret não existe em lugar nenhum.
- Erro claro quando GCP_PROJECT_ID setado mas dep [gcp] não instalada.

NÃO testa path feliz do GCP real — isso exige Secret Manager + creds
e cai em teste integration (fora do escopo unit).
"""

from __future__ import annotations

import pytest

from bracc.secrets import SecretNotFoundError, load_secret


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Limpa lru_cache do load_secret entre testes."""
    load_secret.cache_clear()


class TestLocalFallback:
    def test_le_de_env_var_quando_gcp_project_id_vazio(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.setenv("MEU_SEGREDO", "valor-do-dev")
        assert load_secret("qualquer-coisa", env_fallback="MEU_SEGREDO") == "valor-do-dev"

    def test_aceita_gcp_project_id_vazio_ou_whitespace(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("GCP_PROJECT_ID", "   ")
        monkeypatch.setenv("FALLBACK_X", "xyz")
        assert load_secret("x", env_fallback="FALLBACK_X") == "xyz"

    def test_levanta_secretnotfound_quando_nenhum_dos_dois_setado(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("NAO_EXISTE_XYZ", raising=False)
        with pytest.raises(SecretNotFoundError, match="não encontrado"):
            load_secret("test", env_fallback="NAO_EXISTE_XYZ")

    def test_strip_whitespace_do_valor(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.setenv("COM_ESPACO", "  segredo  \n")
        assert load_secret("qualquer", env_fallback="COM_ESPACO") == "segredo"


class TestMensagensDeErro:
    def test_mensagem_cita_env_var_e_secret_name(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("SECRET_X", raising=False)
        with pytest.raises(SecretNotFoundError) as exc:
            load_secret("neo4j-password", env_fallback="SECRET_X")
        msg = str(exc.value)
        assert "SECRET_X" in msg
        assert "fiscal-cidadao-neo4j-password" in msg
        assert "GCP_PROJECT_ID" in msg
