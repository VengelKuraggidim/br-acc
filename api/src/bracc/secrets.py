"""Secret loading via GCP Secret Manager (caminho único).

**Sem fallback pra env var.** Dois caminhos de leitura criam pontos cegos:
se uma env var vazar em prod por acidente, o app pula o Secret Manager
silenciosamente e usa o valor errado. Aqui só existe um caminho — se
não está no Secret Manager, falha explícita.

Layout de secrets (convenção):

    projects/{GCP_PROJECT_ID}/secrets/fiscal-cidadao-{name}/versions/latest

Nomes usados hoje: ``neo4j-password``, ``jwt-secret``, ``transparencia-key``.
O prefixo ``fiscal-cidadao-`` é adicionado automaticamente por
:func:`load_secret`.

Dev local: definir ``GCP_PROJECT_ID`` (não é segredo — só o ID do projeto
GCP) e rodar ``gcloud auth application-default login`` uma vez. O
``.env`` não deve conter secrets.

Secrets são cached em memory por processo (``@cache``) — mudanças no
Secret Manager só aparecem depois de restart. Aceitável porque rotação
é rara e restart é barato.

Requer dep opcional ``[gcp]`` (``google-cloud-secret-manager``).
"""

from __future__ import annotations

import logging
import os
from functools import cache

logger = logging.getLogger(__name__)

_SECRET_PREFIX = "fiscal-cidadao-"


class SecretNotFoundError(RuntimeError):
    """Raised quando um secret obrigatório não foi encontrado no Secret Manager."""


@cache
def load_secret(name: str) -> str:
    """Busca secret pelo nome lógico no GCP Secret Manager.

    Parameters
    ----------
    name:
        Sufixo do secret, sem o prefixo ``fiscal-cidadao-``.
        Ex.: ``"neo4j-password"`` resolve pra
        ``fiscal-cidadao-neo4j-password``.

    Returns
    -------
    str
        Valor do secret (sem trailing newline).

    Raises
    ------
    SecretNotFoundError
        Quando ``GCP_PROJECT_ID`` ausente ou secret inexistente no
        Secret Manager.
    RuntimeError
        Se dep opcional ``[gcp]`` não está instalada.
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
    """Busca de `projects/{project_id}/secrets/{prefix}{name}/versions/latest`."""
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
