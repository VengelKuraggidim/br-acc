"""Pipeline-side helpers for the provenance contract.

See ``docs/provenance.md`` for the full contract. This module holds the
runtime helpers that pipelines and the loader call:

- :func:`primary_url_for` — look up ``primary_url`` from the source registry.
- :func:`provenance_mode` — read ``BRACC_PROVENANCE_MODE`` (warn/strict/off).
- :func:`missing_provenance_fields` — return the list of required provenance
  fields a row is missing (ignoring the nullable ``source_record_id``).
"""

from __future__ import annotations

import csv
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.schemas.provenance import PROVENANCE_FIELDS

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

# ``source_snapshot_uri`` é opt-in (pipelines novos devem popular via
# ``bracc_etl.archival.archive_fetch``, legados continuam válidos sem ele).
# ``source_record_id`` pode ser vazio quando a fonte não expõe id natural.
_NULLABLE_PROVENANCE_FIELDS: frozenset[str] = frozenset(
    {"source_record_id", "source_snapshot_uri"},
)
_REQUIRED_PROVENANCE_FIELDS: tuple[str, ...] = tuple(
    f for f in PROVENANCE_FIELDS if f not in _NULLABLE_PROVENANCE_FIELDS
)


def provenance_mode() -> str:
    """Read ``BRACC_PROVENANCE_MODE`` env var.

    Valid values: ``warn`` (default), ``strict``, ``off``. Anything else is
    treated as ``warn`` with a logged warning.
    """
    raw = os.environ.get("BRACC_PROVENANCE_MODE", "warn").strip().lower()
    if raw in {"warn", "strict", "off"}:
        return raw
    logger.warning("Unknown BRACC_PROVENANCE_MODE=%r, defaulting to warn", raw)
    return "warn"


def _registry_path() -> Path:
    env_path = os.environ.get("BRACC_SOURCE_REGISTRY_PATH")
    if env_path:
        return Path(env_path)
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "docs" / "source_registry_br_v1.csv"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "source_registry_br_v1.csv not found; set BRACC_SOURCE_REGISTRY_PATH",
    )


@lru_cache(maxsize=1)
def _primary_urls() -> dict[str, str]:
    try:
        path = _registry_path()
    except FileNotFoundError as exc:
        logger.warning("Source registry not found: %s", exc)
        return {}
    out: dict[str, str] = {}
    with path.open(encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            sid = (row.get("source_id") or "").strip()
            url = (row.get("primary_url") or "").strip()
            if sid:
                out[sid] = url
    return out


def primary_url_for(source_id: str) -> str:
    """Return the ``primary_url`` for a source_id, or empty string if absent."""
    return _primary_urls().get(source_id, "")


def missing_provenance_fields(row: dict[str, Any]) -> list[str]:
    """Return names of required provenance fields that are missing or empty.

    ``source_record_id`` is nullable per contract, so its absence is not
    flagged. ``source_url`` must additionally start with ``http``.
    """
    missing = [f for f in _REQUIRED_PROVENANCE_FIELDS if not row.get(f)]
    url = row.get("source_url")
    if url and isinstance(url, str) and not url.startswith("http") and "source_url" not in missing:
        missing.append("source_url")
    return missing


def enforce_provenance(
    rows: Iterable[dict[str, Any]],
    *,
    context: str,
) -> None:
    """Validate rows against the provenance contract.

    Behavior depends on :func:`provenance_mode`:

    - ``off``: no-op.
    - ``warn`` (default): log a warning naming up to 3 offenders.
    - ``strict``: raise ``ValueError`` on the first batch with any missing field.
    """
    mode = provenance_mode()
    if mode == "off":
        return
    offenders: list[tuple[int, list[str]]] = []
    total = 0
    for i, row in enumerate(rows):
        total += 1
        missing = missing_provenance_fields(row)
        if missing:
            offenders.append((i, missing))
    if not offenders:
        return
    sample = offenders[:3]
    summary = "; ".join(f"row {i}: missing {fields}" for i, fields in sample)
    msg = (
        f"[provenance:{context}] {len(offenders)}/{total} rows violate contract "
        f"(sample: {summary})"
    )
    if mode == "strict":
        raise ValueError(msg)
    logger.warning(msg)


# Kept for easier testing from outside the module.
def _reset_cache_for_tests() -> None:
    _primary_urls.cache_clear()
