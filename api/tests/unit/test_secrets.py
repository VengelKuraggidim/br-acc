"""Tests de unidade pro helper load_secret (api side).

Cobre o gate de fonte (BRACC_SECRETS_SOURCE), os 3 mitigadores de
defense-in-depth (whitelist APP_ENV, reject K_SERVICE, log critical),
o caminho env (happy + sad path) e os erros de configuração.

Nota: caminho gcp happy-path exige Secret Manager real (fora do
escopo unit). Testamos só o caminho de erro (GCP_PROJECT_ID ausente).
"""

from __future__ import annotations

import logging

import pytest

from bracc.secrets import SecretNotFoundError, load_secret


@pytest.fixture(autouse=True)
def _reset_cache_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Limpa lru_cache e zera env vars sensíveis entre testes."""
    load_secret.cache_clear()
    for var in (
        "BRACC_SECRETS_SOURCE",
        "GCP_PROJECT_ID",
        "APP_ENV",
        "K_SERVICE",
        "NEO4J_PASSWORD",
        "JWT_SECRET_KEY",
        "TRANSPARENCIA_API_KEY",
    ):
        monkeypatch.delenv(var, raising=False)


class TestSourceDefault:
    def test_sem_var_default_eh_gcp(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("NEO4J_PASSWORD", "irrelevante")
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("neo4j-password")

    def test_source_invalida_aborta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "vault")
        with pytest.raises(SecretNotFoundError, match="invalido.*vault"):
            load_secret("neo4j-password")


class TestEnvSourceHappyPath:
    def test_le_neo4j_password_do_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "dev")
        monkeypatch.setenv("NEO4J_PASSWORD", "senha-local-123")
        assert load_secret("neo4j-password") == "senha-local-123"

    def test_le_jwt_secret_do_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "test")
        monkeypatch.setenv("JWT_SECRET_KEY", "x" * 64)
        assert load_secret("jwt-secret") == "x" * 64

    def test_test_app_env_tambem_aceito(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "TEST")  # case-insensitive
        monkeypatch.setenv("NEO4J_PASSWORD", "x")
        assert load_secret("neo4j-password") == "x"


class TestEnvSourceMitigacoes:
    def test_aborta_em_app_env_production(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "production")
        monkeypatch.setenv("NEO4J_PASSWORD", "tentativa-bypass")
        with pytest.raises(SecretNotFoundError, match="APP_ENV.*'production'"):
            load_secret("neo4j-password")

    def test_aborta_em_app_env_staging(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "staging")
        monkeypatch.setenv("NEO4J_PASSWORD", "x")
        with pytest.raises(SecretNotFoundError, match="APP_ENV"):
            load_secret("neo4j-password")

    def test_aborta_sem_app_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("NEO4J_PASSWORD", "x")
        # APP_ENV ausente cai pra "" que não passa o gate
        with pytest.raises(SecretNotFoundError, match="APP_ENV"):
            load_secret("neo4j-password")

    def test_aborta_em_cloud_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "dev")
        monkeypatch.setenv("K_SERVICE", "fiscal-cidadao-api")
        monkeypatch.setenv("NEO4J_PASSWORD", "x")
        with pytest.raises(SecretNotFoundError, match="Cloud Run"):
            load_secret("neo4j-password")

    def test_log_critical_emitido(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "dev")
        monkeypatch.setenv("NEO4J_PASSWORD", "x")
        with caplog.at_level(logging.CRITICAL, logger="bracc.secrets"):
            load_secret("neo4j-password")
        assert any(
            "BRACC_SECRETS_SOURCE=env" in rec.message and rec.levelname == "CRITICAL"
            for rec in caplog.records
        )


class TestEnvSourceErrosDeConfiguracao:
    def test_secret_sem_mapping_aborta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "dev")
        with pytest.raises(SecretNotFoundError, match="sem mapeamento"):
            load_secret("nome-inventado")

    def test_env_var_ausente_aborta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "dev")
        # NEO4J_PASSWORD não setada
        with pytest.raises(SecretNotFoundError, match="NEO4J_PASSWORD ausente"):
            load_secret("neo4j-password")

    def test_env_var_vazia_aborta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "env")
        monkeypatch.setenv("APP_ENV", "dev")
        monkeypatch.setenv("NEO4J_PASSWORD", "")
        with pytest.raises(SecretNotFoundError, match="NEO4J_PASSWORD"):
            load_secret("neo4j-password")


class TestGcpSourceErrosDeConfiguracao:
    """Cobre o caminho gcp explícito (sem chamar Secret Manager real)."""

    def test_levanta_sem_gcp_project_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SECRETS_SOURCE", "gcp")
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("neo4j-password")

    def test_levanta_com_gcp_project_id_vazio(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GCP_PROJECT_ID", "")
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("jwt-secret")

    def test_levanta_com_gcp_project_id_whitespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GCP_PROJECT_ID", "   ")
        with pytest.raises(SecretNotFoundError, match="GCP_PROJECT_ID"):
            load_secret("transparencia-key")


class TestMensagemDeErro:
    def test_cita_prefixo_e_project_id_var_no_gcp(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(SecretNotFoundError) as exc:
            load_secret("neo4j-password")
        msg = str(exc.value)
        assert "GCP_PROJECT_ID" in msg
        assert "fiscal-cidadao-neo4j-password" in msg
        assert "gcloud auth application-default login" in msg

    def test_aponta_pra_bracc_secrets_source_env_no_gcp_error(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mensagem de erro guia o dev pra alternativa env-source."""
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(SecretNotFoundError) as exc:
            load_secret("neo4j-password")
        assert "BRACC_SECRETS_SOURCE=env" in str(exc.value)
