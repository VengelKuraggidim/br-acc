"""Secret loading com GCP Secret Manager + .env fallback (ETL side).

Cópia intencional do helper em ``api/src/bracc/secrets.py`` — ``api/`` e
``etl/`` são pacotes Python independentes (pyproject.toml separados) e
não compartilham código. Manter em sync manualmente quando mudar a
semântica do contrato (raramente).

Ver ``api/src/bracc/secrets.py`` pro docstring completo.
"""

from __future__ import annotations

import logging
import os
from functools import cache

logger = logging.getLogger(__name__)

_SECRET_PREFIX = "fiscal-cidadao-"


class SecretNotFoundError(RuntimeError):
    """Raised quando um secret obrigatório não foi encontrado."""


@cache
def load_secret(name: str, *, env_fallback: str) -> str:
    """Busca secret pelo nome lógico, fallback pra env var local.

    Ver ``api/src/bracc/secrets.py::load_secret`` pro docstring completo.
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
    """Busca via ``google-cloud-secret-manager`` (dep opcional ``[gcp]``)."""
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
