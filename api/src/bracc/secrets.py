"""Secret loading com fonte explícita (GCP Secret Manager ou env var).

A fonte é controlada por ``BRACC_SECRETS_SOURCE``:

- ``gcp`` (default): lê do Google Secret Manager. Único caminho aceito
  em produção. Requer ``GCP_PROJECT_ID`` + dep opcional ``[gcp]``.
- ``env``: lê de variáveis de ambiente locais. **Só** aceito em
  ``APP_ENV in {dev, test}`` E quando o runtime não é Cloud Run
  (``K_SERVICE`` ausente). Pra dev sem GCP e CI.

Defense in depth contra ativação acidental em prod (3 camadas):

1. **Default seguro**: sem ``BRACC_SECRETS_SOURCE`` setada, usa ``gcp``.
2. **Whitelist de APP_ENV**: ``env`` source rejeitada se
   ``APP_ENV`` não está em ``{dev, test}``.
3. **Reject Cloud Run**: presença de ``K_SERVICE`` (env injetada pelo
   runtime Cloud Run) com ``BRACC_SECRETS_SOURCE=env`` aborta o boot.

Toda inicialização com source=env emite ``logger.critical`` pra deixar
trilha em qualquer runtime que coletar logs estruturados.

Layout no GCP (convenção):

    projects/{GCP_PROJECT_ID}/secrets/fiscal-cidadao-{name}/versions/latest

Mapping nome lógico -> env var (pra source=env):

    neo4j-password   -> NEO4J_PASSWORD
    jwt-secret       -> JWT_SECRET_KEY
    transparencia-key -> TRANSPARENCIA_API_KEY

Secrets são cached em memory por processo (``@cache``); rotação só vale
após restart.

Requer dep opcional ``[gcp]`` (``google-cloud-secret-manager``) só
quando source=gcp.
"""

from __future__ import annotations

import logging
import os
from functools import cache

logger = logging.getLogger(__name__)

_SECRET_PREFIX = "fiscal-cidadao-"

_SECRET_ENV_MAP: dict[str, str] = {
    "neo4j-password": "NEO4J_PASSWORD",
    "jwt-secret": "JWT_SECRET_KEY",
    "transparencia-key": "TRANSPARENCIA_API_KEY",
}

_VALID_SOURCES = frozenset({"gcp", "env"})
_DEV_LIKE_APP_ENVS = frozenset({"dev", "test"})


class SecretNotFoundError(RuntimeError):
    """Raised quando um secret obrigatório não foi encontrado na fonte ativa."""


def _resolve_source() -> str:
    """Lê e valida ``BRACC_SECRETS_SOURCE``. Default ``gcp``.

    Raises
    ------
    SecretNotFoundError
        Se a variável estiver setada com valor desconhecido.
    """
    raw = os.environ.get("BRACC_SECRETS_SOURCE", "gcp").strip().lower()
    if raw not in _VALID_SOURCES:
        raise SecretNotFoundError(
            f"BRACC_SECRETS_SOURCE invalido: {raw!r}. "
            f"Valores aceitos: {sorted(_VALID_SOURCES)}."
        )
    return raw


def _enforce_env_source_gates() -> None:
    """Aborta se ``env`` source for usada fora de dev/test ou em Cloud Run.

    Camadas de defesa pra impedir ativação acidental em produção.

    Raises
    ------
    SecretNotFoundError
        Quando APP_ENV não é dev/test, ou K_SERVICE indica Cloud Run.
    """
    app_env = os.environ.get("APP_ENV", "").strip().lower()
    if app_env not in _DEV_LIKE_APP_ENVS:
        raise SecretNotFoundError(
            f"BRACC_SECRETS_SOURCE=env exige APP_ENV in {sorted(_DEV_LIKE_APP_ENVS)}; "
            f"APP_ENV atual={app_env!r}. Em prod, use BRACC_SECRETS_SOURCE=gcp."
        )
    k_service = os.environ.get("K_SERVICE", "").strip()
    if k_service:
        raise SecretNotFoundError(
            f"BRACC_SECRETS_SOURCE=env detectado em runtime Cloud Run "
            f"(K_SERVICE={k_service!r}). Bypass de Secret Manager bloqueado. "
            f"Use BRACC_SECRETS_SOURCE=gcp."
        )
    logger.critical(
        "[secrets] carregando via env var (BRACC_SECRETS_SOURCE=env, APP_ENV=%s). "
        "Bypass de Secret Manager — só esperado em dev/test.",
        app_env,
    )


@cache
def load_secret(name: str) -> str:
    """Busca secret pelo nome lógico na fonte configurada.

    Parameters
    ----------
    name:
        Sufixo do secret, sem o prefixo ``fiscal-cidadao-``.
        Ex.: ``"neo4j-password"`` resolve pra
        ``fiscal-cidadao-neo4j-password`` (gcp) ou ``NEO4J_PASSWORD`` (env).

    Returns
    -------
    str
        Valor do secret (sem trailing newline em gcp; sem strip em env).

    Raises
    ------
    SecretNotFoundError
        Quando a fonte ativa não consegue resolver o secret, ou quando
        as camadas de defesa rejeitam a config.
    RuntimeError
        Se source=gcp e dep opcional ``[gcp]`` não está instalada.
    """
    source = _resolve_source()
    if source == "env":
        return _load_from_env(name)
    return _load_from_gcp_with_project(name)


def _load_from_env(name: str) -> str:
    _enforce_env_source_gates()
    env_var = _SECRET_ENV_MAP.get(name)
    if env_var is None:
        raise SecretNotFoundError(
            f"Secret {name!r} sem mapeamento env var. "
            f"Adicione em _SECRET_ENV_MAP."
        )
    value = os.environ.get(env_var, "")
    if not value:
        raise SecretNotFoundError(
            f"BRACC_SECRETS_SOURCE=env mas {env_var} ausente ou vazio. "
            f"Setar em .env ou env do shell."
        )
    return value


def _load_from_gcp_with_project(name: str) -> str:
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    if not project_id:
        raise SecretNotFoundError(
            f"GCP_PROJECT_ID nao setado. Configure com:\n"
            f"  export GCP_PROJECT_ID=<seu-projeto-gcp>\n"
            f"  gcloud auth application-default login\n"
            f"Secret esperado: '{_SECRET_PREFIX}{name}'.\n"
            f"Pra dev sem GCP, use BRACC_SECRETS_SOURCE=env."
        )
    return _load_from_gcp(name, project_id)


def _load_from_gcp(name: str, project_id: str) -> str:
    """Busca de `projects/{project_id}/secrets/{prefix}{name}/versions/latest`."""
    try:
        from google.cloud import secretmanager
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-secret-manager nao instalado. "
            "Rode: uv sync --extra gcp"
        ) from exc

    client = secretmanager.SecretManagerServiceClient()
    secret_name = (
        f"projects/{project_id}/secrets/{_SECRET_PREFIX}{name}/versions/latest"
    )
    logger.info("[secrets] resolvendo %s via GCP Secret Manager", secret_name)
    response = client.access_secret_version(request={"name": secret_name})
    payload: bytes = response.payload.data
    return payload.decode("utf-8").strip()
