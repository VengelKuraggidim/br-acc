"""Archival layer — preserve raw copies of external sources at ingestion time.

Proveniência hoje grava ``source_url`` + ``ingested_at``. Isso basta pra
apontar pra onde o dado estava, mas não sobrevive a portais que trocam URL
ou tiram páginas do ar (e governos brasileiros fazem isso com frequência).
Esta camada grava um **snapshot content-addressed** do payload bruto — HTML,
JSON, PDF, CSV, screenshot — no momento em que o pipeline leu a fonte, e
devolve um URI relativo que pipelines podem carimbar na proveniência via
``source_snapshot_uri``.

## Layout de storage

Raiz configurável via ``BRACC_ARCHIVAL_ROOT`` (default ``./archival/``,
relativo ao cwd do pipeline).

Layout dentro da raiz::

    {source_id}/{YYYY-MM}/{hash12}.{ext}

- ``source_id``: igual ao do contrato de proveniência.
- ``YYYY-MM``: mês do ``ingested_at`` — buckets mensais pra evitar
  diretórios gigantes.
- ``hash12``: ``sha256(content)[:12]`` hex. Content-addressed, então
  o mesmo payload sempre cai no mesmo caminho → idempotente.
- ``ext``: derivado de ``content_type`` (ver ``_CONTENT_TYPE_EXTENSIONS``).
  Desconhecido vira ``.bin``.

## API

- :func:`archive_fetch` — grava e devolve o URI relativo.
- :func:`restore_snapshot` — lê um URI de volta (debugging/tests).

## Extensão futura (GCS / S3 / IPFS)

Hoje só disco local. Fernando roda o projeto em GCP (Asgard Studio), então o
próximo passo natural é plugar um adapter que envia o mesmo blob content-
addressed pra um bucket GCS com o mesmo layout, mantendo URIs relativos
iguais em dev e prod. O hook esperado é subclasse/adapter: reescrever
``_write_bytes`` / ``_read_bytes`` pra apontar pro bucket e preservar o
contrato de "mesmo content → mesma URI → idempotente". Não reinventar o
formato da URI pra não quebrar dados já gravados no grafo.

## Retrofit nos pipelines legados

Os 10 pipelines GO atuais (folha_go, pncp_go, alego, …) **não** chamam esta
camada ainda. ``source_snapshot_uri`` é opt-in no contrato de proveniência
— pipelines existentes continuam funcionando sem mudança. O retrofit é
tracked como fase separada; ver ``todo-list-prompts/high_priority/`` pra o
plano por pipeline.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Content-type → extensão. Ampliar conforme pipelines novos chegam.
# Quando o content-type não bate em nada, cai em ``.bin`` e o payload
# continua preservado (só não ganha hint no nome).
_CONTENT_TYPE_EXTENSIONS: dict[str, str] = {
    "text/html": ".html",
    "application/json": ".json",
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "text/csv": ".csv",
    "text/plain": ".txt",
}

_DEFAULT_ROOT = "./archival/"
_HASH_PREFIX_LEN = 12


def _archival_root() -> Path:
    """Return the archival storage root, honoring ``BRACC_ARCHIVAL_ROOT``.

    Relative paths are resolved against the current working directory of the
    pipeline (the Makefile targets ``cd`` into ``etl/`` before running).
    """
    raw = os.environ.get("BRACC_ARCHIVAL_ROOT", _DEFAULT_ROOT)
    return Path(raw).expanduser()


def _extension_for(content_type: str) -> str:
    # Tolerar charset/boundaries: ``application/json; charset=utf-8``
    primary = content_type.split(";", 1)[0].strip().lower() if content_type else ""
    return _CONTENT_TYPE_EXTENSIONS.get(primary, ".bin")


def _month_bucket(run_id: str) -> str:
    """Derive ``YYYY-MM`` bucket from ``run_id`` (``{source}_YYYYMMDDHHMMSS``).

    Falls back to ``unknown`` if the run_id does not carry a recognizable
    timestamp — keeps the archive writable even for ad-hoc manual runs.
    """
    # run_id shape: "{source_id}_YYYYMMDDHHMMSS". We just need the first
    # 6 digits of the timestamp suffix.
    suffix = run_id.rsplit("_", 1)[-1] if "_" in run_id else run_id
    if len(suffix) >= 6 and suffix[:6].isdigit():
        return f"{suffix[:4]}-{suffix[4:6]}"
    return "unknown"


def _relative_uri(source_id: str, bucket: str, hash12: str, ext: str) -> str:
    """Build the relative URI. POSIX-style (portable across OS and GCS)."""
    return f"{source_id}/{bucket}/{hash12}{ext}"


def _resolve_absolute(uri: str) -> Path:
    return _archival_root() / uri


def archive_fetch(
    url: str,  # noqa: ARG001 — kept in signature for future adapters (GCS metadata, indexing)
    content: bytes,
    content_type: str,
    run_id: str,
    source_id: str,
) -> str:
    """Grava snapshot imutável, retorna URI relativo ao root.

    Content-addressed: o caminho deriva puramente de ``sha256(content)``,
    então chamar com o mesmo payload sempre devolve a mesma URI e **não
    re-escreve** o arquivo em disco. Safe pra chamar múltiplas vezes no
    mesmo fetch (retries, resume, etc.).

    Parameters
    ----------
    url:
        URL da fonte (apenas pra logging/auditoria; não entra no caminho
        do arquivo — o que manda é o hash do conteúdo).
    content:
        Bytes literais do payload. Não decodificar antes: o ponto do
        archival é preservar a forma bruta que o servidor devolveu.
    content_type:
        HTTP ``Content-Type`` (ex.: ``application/json; charset=utf-8``).
        Usado só pra escolher a extensão do arquivo — o conteúdo é salvo
        cru independente do valor.
    run_id:
        Identificador do run de ingestão, no formato canônico
        ``{source_id}_YYYYMMDDHHMMSS``. Usado pra derivar o bucket mensal.
    source_id:
        Chave da fonte no ``docs/source_registry_br_v1.csv``.

    Returns
    -------
    str
        URI relativa (POSIX) no formato ``{source_id}/{YYYY-MM}/{hash12}.{ext}``.
        Guardar essa string no campo ``source_snapshot_uri`` do bloco de
        proveniência do row que esta fetch produziu.

    Notes
    -----
    Hook futuro: subclasse ou adapter pra GCS/S3/IPFS. Preservar idempotência
    (mesmo content → mesma URI) pra não quebrar dados já no grafo.
    """
    if not source_id:
        raise ValueError("archive_fetch: source_id is required")
    if not run_id:
        raise ValueError("archive_fetch: run_id is required")

    digest = hashlib.sha256(content).hexdigest()[:_HASH_PREFIX_LEN]
    ext = _extension_for(content_type)
    bucket = _month_bucket(run_id)
    uri = _relative_uri(source_id, bucket, digest, ext)
    absolute = _resolve_absolute(uri)

    if absolute.exists():
        # Idempotência: mesmo content = mesmo caminho. Não re-escreve.
        logger.debug(
            "[archival] hit for %s (source_id=%s bucket=%s hash=%s)",
            url,
            source_id,
            bucket,
            digest,
        )
        return uri

    absolute.parent.mkdir(parents=True, exist_ok=True)
    # Escrita atômica: grava em .tmp e faz rename. Protege contra leitores
    # que pegam o arquivo a meio caminho de ser escrito (ex.: ctrl-c no
    # pipeline enquanto está fazendo fsync).
    tmp_path = absolute.with_suffix(absolute.suffix + ".tmp")
    tmp_path.write_bytes(content)
    tmp_path.replace(absolute)
    logger.info(
        "[archival] wrote %s (%d bytes) for %s",
        uri,
        len(content),
        url,
    )
    return uri


def restore_snapshot(uri: str) -> bytes:
    """Return raw bytes for a previously-archived URI.

    Útil pra debugging, testes de regressão, ou pra re-processar um
    pipeline sem voltar no servidor da fonte (ex.: fonte saiu do ar).

    Raises
    ------
    FileNotFoundError
        Se o URI não existe sob o archival root atual (ex.: URI foi
        gerada com outro ``BRACC_ARCHIVAL_ROOT``).
    """
    absolute = _resolve_absolute(uri)
    if not absolute.exists():
        raise FileNotFoundError(
            f"archival snapshot not found: {uri} (looked in {absolute})",
        )
    return absolute.read_bytes()
