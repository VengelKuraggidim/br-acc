"""Secret loading with GCP Secret Manager + .env fallback.

Dual-mode lookup para credenciais sensíveis:

1. **Produção** (GCP): quando ``GCP_PROJECT_ID`` env var está setada,
   busca de ``projects/{id}/secrets/fiscal-cidadao-{name}/versions/latest``
   via ``google-cloud-secret-manager``. Dep opcional via extra ``[gcp]``.

2. **Dev local**: se ``GCP_PROJECT_ID`` ausente, lê direto de
   ``os.environ[env_var_fallback]`` (carregado de ``.env`` via
   ``python-dotenv`` ou shell).

Layout de secrets no GCP (convenção):

    fiscal-cidadao-neo4j-password
    fiscal-cidadao-transparencia-key
    fiscal-cidadao-jwt-secret

O prefixo ``fiscal-cidadao-`` é adicionado automaticamente por
:func:`load_secret`. Chamadas usam só o sufixo (``"neo4j-password"``).

Secrets são resolvidos **uma vez por processo** e cached em memory —
mudanças no Secret Manager só aparecem depois de restart. Aceitável porque
chaves rotacionam raramente e restart é barato.

Ver ``docs/secrets.md`` (futuro) e ``README.md`` pra configuração GCP.
"""

from __future__ import annotations

import logging
import os
from functools import cache

logger = logging.getLogger(__name__)

_SECRET_PREFIX = "fiscal-cidadao-"


class SecretNotFoundError(RuntimeError):
    """Raised quando um secret obrigatório não foi encontrado.

    Mensagem instruiu o caller sobre como configurar — tanto pro path
    GCP (Secret Manager + IAM) quanto pro path local (``.env``).
    """


@cache
def load_secret(name: str, *, env_fallback: str) -> str:
    """Busca secret pelo nome lógico, com fallback pra env var local.

    Parameters
    ----------
    name:
        Sufixo do secret no GCP, sem o prefixo ``fiscal-cidadao-``.
        Ex.: ``"neo4j-password"`` resolve pra
        ``fiscal-cidadao-neo4j-password`` no Secret Manager.
    env_fallback:
        Nome da env var usada em dev local. Ex.: ``"NEO4J_PASSWORD"``.

    Returns
    -------
    str
        Valor do secret (sem trailing newline).

    Raises
    ------
    SecretNotFoundError
        Quando nenhum dos dois paths retorna valor.
    RuntimeError
        Se ``GCP_PROJECT_ID`` está setado mas dep ``[gcp]`` não instalada.
    """
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    if project_id:
        return _load_from_gcp(name, project_id)
    value = os.environ.get(env_fallback, "").strip()
    if not value:
        raise SecretNotFoundError(
            f"Secret {name!r} não encontrado. Configure de um dos modos:\n"
            f"  - Dev local: export {env_fallback}=... (ou .env)\n"
            f"  - Produção:  export GCP_PROJECT_ID=... e crie o secret "
            f"'{_SECRET_PREFIX}{name}' no Secret Manager."
        )
    return value


def _load_from_gcp(name: str, project_id: str) -> str:
    """Busca de `projects/{project_id}/secrets/{prefix}{name}/versions/latest`."""
    try:
        from google.cloud import secretmanager  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "GCP_PROJECT_ID setado mas google-cloud-secret-manager ausente. "
            "Instale com: uv sync --extra gcp"
        ) from exc

    client = secretmanager.SecretManagerServiceClient()
    secret_name = (
        f"projects/{project_id}/secrets/{_SECRET_PREFIX}{name}/versions/latest"
    )
    logger.info("[secrets] resolvendo %s via GCP Secret Manager", secret_name)
    response = client.access_secret_version(request={"name": secret_name})
    payload: bytes = response.payload.data
    return payload.decode("utf-8").strip()
