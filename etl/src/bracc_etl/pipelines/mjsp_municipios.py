"""ETL pipeline for the MJSP/SINESP municipal homicide series.

The Brazilian Ministry of Justice and Public Security (MJSP) consolidates
municipal-level public-security indicators reported by every state's
SINESP coordinator and republishes them on the open-data portal at
``dados.mj.gov.br``. The dataset
``sistema-nacional-de-estatisticas-de-seguranca-publica`` exposes a
single municipal XLSX (``indicadoressegurancapublicamunic.xlsx``) with
one sheet per UF and the schema
``Cód_IBGE | Município | Sigla UF | Região | Mês/Ano | Vítimas``.

**Coverage caveat**: this XLSX carries only one indicator —
**Homicídio Doloso** (per the official data dictionary, MJSP Portaria
229/2018). The other 14 naturezas published by SSP-GO (estupro, roubos,
furtos, feminicídio, etc.) are *not* available at municipal granularity
in this federal feed. They remain state-level only via the existing
``ssp_go`` pipeline; closing that gap is tracked as a manual LAI request
in ``todo-list-prompts/high_priority/debitos/ssp-go-granularidade-municipio.md``.

What this pipeline writes:

- ``GoSecurityStat`` nodes (same label as ``ssp_go``) with
  ``crime_type='HOMICIDIO DOLOSO'`` and one row per ``(municipality × mês)``.
  ``stat_id`` already includes ``cod_ibge`` so municipal rows coexist
  with the existing state-level rows (``cod_ibge=5200000``) without
  collision.
- UF filter: only rows with ``Sigla UF == 'GO'`` are loaded — the rest
  of the country is out of scope for Fiscal Cidadão's GO focus.

Data source: https://dados.mj.gov.br/dataset/sistema-nacional-de-estatisticas-de-seguranca-publica
"""

from __future__ import annotations

import hashlib
import logging
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# CKAN package + resource IDs. The package id is the human slug; the
# municipal XLSX has a stable resource UUID even though MJSP rotates
# the download URL prefix once in a while. Resolving via CKAN
# ``package_show`` is the canonical way to get the latest URL — kept as
# the discovery primitive so a URL change at MJSP doesn't require a
# code change.
_CKAN_PKG_URL = (
    "https://dados.mj.gov.br/api/3/action/package_show"
    "?id=sistema-nacional-de-estatisticas-de-seguranca-publica"
)
_MUNICIPAL_RESOURCE_NAME = "Dados Nacionais de Segurança Pública - Municípios"
# Hard-coded fallback URL for offline/CKAN-down scenarios. Stable as of
# 2026-04-27. If MJSP changes it, ``_resolve_municipal_url`` falls back
# to this so the pipeline can still run from a cached XLSX.
_FALLBACK_XLSX_URL = (
    "http://dados.mj.gov.br/dataset/210b9ae2-21fc-4986-89c6-2006eb4db247"
    "/resource/03af7ce2-174e-4ebd-b085-384503cfb40f"
    "/download/indicadoressegurancapublicamunic.xlsx"
)
_XLSX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# UF filter: Fiscal Cidadão GO only. Changing this means the loader
# starts pulling other states; that's a new débito, not a flag.
_TARGET_UF = "GO"

# Indicator label hard-coded into every row: the MJSP municipal XLSX
# only publishes Homicídio Doloso (data dictionary, Portaria MJSP
# 229/2018). If MJSP starts publishing other indicators in the same
# file, the parser needs an extra column in the header check below.
_CRIME_TYPE = normalize_name("HOMICIDIO DOLOSO")

_LOCAL_XLSX_FILENAME = "indicadoressegurancapublicamunic.xlsx"


def _resolve_municipal_url(client: httpx.Client) -> str:
    """Return the current download URL for the municipal XLSX.

    Asks CKAN ``package_show`` first; falls back to the hard-coded URL
    when CKAN is unreachable or the resource is gone. The fallback path
    keeps the pipeline alive during MJSP outages — the file fetched is
    still authoritative once it arrives.
    """
    try:
        resp = client.get(_CKAN_PKG_URL)
        resp.raise_for_status()
        resources = resp.json().get("result", {}).get("resources", [])
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "[mjsp_municipios] CKAN package_show failed (%s); "
            "falling back to hard-coded URL", exc,
        )
        return _FALLBACK_XLSX_URL

    for res in resources:
        if res.get("name") == _MUNICIPAL_RESOURCE_NAME and res.get("url"):
            return str(res["url"])
    logger.warning(
        "[mjsp_municipios] resource %r not found in CKAN listing; "
        "falling back to hard-coded URL",
        _MUNICIPAL_RESOURCE_NAME,
    )
    return _FALLBACK_XLSX_URL


def _download_xlsx(
    client: httpx.Client,
    url: str,
    target: Path,
) -> Path | None:
    """Stream the municipal XLSX to ``target``; return path or ``None``."""
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[mjsp_municipios] failed to download %s: %s", url, exc)
        return None
    target.write_bytes(resp.content)
    logger.info(
        "[mjsp_municipios] wrote %s (%d bytes)", target, len(resp.content),
    )
    return target


def fetch_to_disk(
    output_dir: Path | str,
    limit: int | None = None,  # noqa: ARG001 — kept for CLI symmetry; XLSX is single-file
) -> list[Path]:
    """Download the municipal XLSX into ``output_dir``.

    The MJSP municipal feed is a single XLSX (no per-year split, no
    pagination), so ``limit`` is accepted but ignored — kept in the
    signature so the CLI wrapper is symmetric with other ``fetch_to_disk``
    helpers and a future patch can add e.g. UF-level XLSX without a
    breaking signature change.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        url = _resolve_municipal_url(client)
        target = output_dir / _LOCAL_XLSX_FILENAME
        result = _download_xlsx(client, url, target)
        if result is not None:
            written.append(result)
    return written


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _format_period(value: Any) -> str:
    """Coerce the ``Mês/Ano`` cell into ``YYYY-MM``.

    The XLSX serializes the column as a real Excel date, so pandas
    parses it as ``Timestamp``. Strings (when the column is read with
    ``dtype=str``) come in two shapes:

    - ``'2018-01-01 00:00:00'`` — pandas's str() of a Timestamp.
    - ``'YYYY-MM-DD'`` / ``'MM/YYYY'`` — happens when a row was edited
      manually upstream.

    We only care about year-month: extract the first 7 chars of the ISO
    representation. Empty / unparseable values become ``''`` so the
    caller can drop the row.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m")
    text = str(value).strip()
    if not text:
        return ""
    # ISO-ish: first 7 chars are YYYY-MM in any of the common shapes.
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    # ``MM/YYYY`` fallback.
    if len(text) == 7 and text[2] == "/":
        return f"{text[3:]}-{text[:2]}"
    # Last-ditch: try pandas to parse, return empty on failure.
    try:
        ts = pd.to_datetime(text, errors="raise")
        return ts.strftime("%Y-%m")
    except (ValueError, TypeError):
        return ""


class MjspMunicipiosPipeline(Pipeline):
    """Pipeline for the MJSP municipal homicide-doloso XLSX (UF=GO slice)."""

    name = "mjsp_municipios"
    source_id = "mjsp_municipios"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        *,
        archive_xlsx: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs,
        )
        self._raw_stats: pd.DataFrame = pd.DataFrame()
        self.stats: list[dict[str, Any]] = []
        # Opt-out switch para o fetch online. Fixtures offline (sem
        # mock de HTTP) desativam para não bater rede.
        self._archive_xlsx_enabled = archive_xlsx
        self._snapshot_uri: str | None = None

    def _fetch_archive_online(self) -> tuple[bytes | None, str | None]:
        """Baixa e arquiva o XLSX municipal; devolve (bytes, snapshot_uri).

        Falhas de HTTP são logadas e engolidas (extract cai pro path
        offline com o XLSX cacheado em ``data/mjsp_municipios/``).
        """
        try:
            with httpx.Client(timeout=120, follow_redirects=True) as client:
                url = _resolve_municipal_url(client)
                resp = client.get(url)
                resp.raise_for_status()
                content = resp.content
                content_type = resp.headers.get(
                    "content-type", _XLSX_CONTENT_TYPE,
                )
                uri = archive_fetch(
                    url=url,
                    content=content,
                    content_type=content_type,
                    run_id=self.run_id,
                    source_id=self.source_id,
                )
                logger.info(
                    "[mjsp_municipios] archived municipal XLSX -> %s", uri,
                )
                return content, uri
        except httpx.HTTPError as exc:
            logger.warning(
                "[mjsp_municipios] online fetch failed: %s", exc,
            )
            return None, None

    def _read_uf_sheet(self, content: bytes) -> pd.DataFrame:
        """Read the GO sheet from the XLSX, returning the GO slice only.

        The XLSX has one sheet per UF (``AC``, ``AL``, ..., ``GO``, ...).
        We read only the GO sheet to keep memory low — the UF column is
        redundantly stamped on every row, but we drop other states up
        front anyway.
        """
        try:
            df = pd.read_excel(
                BytesIO(content),
                sheet_name=_TARGET_UF,
                dtype=str,
                keep_default_na=False,
            )
        except (ValueError, KeyError) as exc:
            logger.warning(
                "[mjsp_municipios] could not read sheet %r: %s",
                _TARGET_UF, exc,
            )
            return pd.DataFrame()
        return df

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "mjsp_municipios"
        content: bytes | None = None
        if self._archive_xlsx_enabled:
            content, self._snapshot_uri = self._fetch_archive_online()

        if content is None:
            cached = src_dir / _LOCAL_XLSX_FILENAME
            if cached.exists() and cached.stat().st_size > 0:
                content = cached.read_bytes()
                logger.info(
                    "[mjsp_municipios] using cached XLSX %s", cached,
                )

        if content is None:
            logger.warning(
                "[mjsp_municipios] no XLSX available "
                "(online fetch failed and no cache in %s)", src_dir,
            )
            return

        self._raw_stats = self._read_uf_sheet(content)
        if self.limit:
            self._raw_stats = self._raw_stats.head(self.limit)
        self.rows_in = len(self._raw_stats)

    def transform(self) -> None:
        if self._raw_stats.empty:
            return

        for _, row in self._raw_stats.iterrows():
            uf = str(row.get("Sigla UF", "")).strip().upper()
            if uf != _TARGET_UF:
                # Defensive: the GO sheet should only contain GO, but
                # we double-check so a future schema drift doesn't leak
                # other states.
                continue
            cod_ibge = str(row.get("Cód_IBGE", "")).strip()
            municipality_raw = str(row.get("Município", "")).strip()
            municipality = normalize_name(municipality_raw)
            periodo = _format_period(row.get("Mês/Ano", ""))
            count_raw = str(row.get("Vítimas", "")).strip()
            try:
                count = int(float(count_raw)) if count_raw else 0
            except (TypeError, ValueError):
                count = 0

            if not cod_ibge or not municipality or not periodo:
                continue

            stat_id = _hash_id(cod_ibge, municipality, _CRIME_TYPE, periodo)
            stat_record_id = f"{cod_ibge}|{_CRIME_TYPE}|{periodo}"
            self.stats.append(self.attach_provenance(
                {
                    "stat_id": stat_id,
                    "cod_ibge": cod_ibge,
                    "municipality": municipality,
                    "crime_type": _CRIME_TYPE,
                    "period": periodo,
                    "count": count,
                    "uf": _TARGET_UF,
                    "source": "mjsp_municipios",
                },
                record_id=stat_record_id,
                snapshot_uri=self._snapshot_uri,
            ))

        self.stats = deduplicate_rows(self.stats, ["stat_id"])
        self.rows_loaded = len(self.stats)

    def load(self) -> None:
        if not self.stats:
            logger.warning("[mjsp_municipios] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes("GoSecurityStat", self.stats, key_field="stat_id")
