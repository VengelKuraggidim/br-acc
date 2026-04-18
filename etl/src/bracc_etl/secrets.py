"""Secret loading via GCP Secret Manager (ETL side, caminho único).

Cópia intencional do helper em ``api/src/bracc/secrets.py`` — ``api/`` e
``etl/`` são pacotes Python independentes. Manter em sync manualmente.

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
def load_secret(name: str) -> str:
    """Busca secret pelo nome lógico no GCP Secret Manager.

    Ver ``api/src/bracc/secrets.py::load_secret`` pro contrato completo.
    """
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    if not project_id:
        raise SecretNotFoundError(
            f"GCP_PROJECT_ID nao setado. Configure com:\n"
            f"  export GCP_PROJECT_ID=<seu-projeto-gcp>\n"
            f"  gcloud auth application-default login\n"
            f"Secret esperado: '{_SECRET_PREFIX}{name}'."
        )
    return _load_from_gcp(name, project_id)


def _load_from_gcp(name: str, project_id: str) -> str:
    """Busca via ``google-cloud-secret-manager`` (dep opcional ``[gcp]``)."""
    try:
        from google.cloud import secretmanager  # type: ignore[import-not-found]
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
