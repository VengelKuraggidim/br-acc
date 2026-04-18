from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    strip_document,
)

logger = logging.getLogger(__name__)

# Tesouro Nacional SICONFI APIs (no auth required).
SICONFI_API_BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
# DCA-Anexo I-C is the balance-sheet annex containing per-account amounts.
# Other annexes (I-A, I-B) summarise expenditure / revenue at higher level —
# the pipeline transform aggregates by ``conta`` so I-C is the right grain.
SICONFI_DEFAULT_ANNEX = "DCA-Anexo I-C"
SICONFI_REQUEST_TIMEOUT = 60.0
SICONFI_REQUEST_DELAY = 0.3


def _fetch_entes(client: httpx.Client) -> list[dict[str, Any]]:
    """Return the full list of SICONFI entities (states + municipalities)."""
    url = f"{SICONFI_API_BASE}/entes"
    all_items: list[dict[str, Any]] = []
    offset = 0
    page_size = 5000
    while True:
        resp = client.get(
            url, params={"offset": offset, "limit": page_size},
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", []) if isinstance(data, dict) else data
        if not items:
            break
        all_items.extend(items)
        if len(items) < page_size:
            break
        offset += page_size
        time.sleep(SICONFI_REQUEST_DELAY)
    return all_items


def _fetch_dca(
    client: httpx.Client,
    cod_ibge: str,
    year: int,
    annex: str,
) -> list[dict[str, Any]]:
    """Fetch DCA rows for a single (entity, year)."""
    url = f"{SICONFI_API_BASE}/dca"
    params = {
        "an_exercicio": str(year),
        "no_anexo": annex,
        "id_ente": str(cod_ibge),
    }
    resp = client.get(url, params=params)
    if resp.status_code in (404, 204):
        return []
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", []) if isinstance(data, dict) else data
    return list(items) if items else []


def fetch_to_disk(
    output_dir: Path,
    *,
    date: str | None = None,
    exercicios: list[int] | None = None,
    entes: list[str] | None = None,
    annex: str = SICONFI_DEFAULT_ANNEX,
    limit: int | None = None,
    states_only: bool = False,
) -> list[Path]:
    """Download SICONFI DCA balance-sheet rows and persist them as CSVs.

    The :class:`SiconfiPipeline` extract step globs ``dca_*.csv`` under
    ``data/siconfi/``; ``fetch_to_disk`` writes one ``dca_<year>.csv`` per
    requested ``exercicio`` containing all entities for that year.

    Parameters
    ----------
    output_dir:
        Destination directory (created if missing).
    date:
        Accepted for API symmetry with other ``fetch_to_disk`` callers; if
        provided as ``YYYY-MM-DD``/``YYYYMMDD`` the year portion is added to
        ``exercicios`` when no explicit list was passed.
    exercicios:
        Years to fetch. Defaults to the previous calendar year (SICONFI lags
        ~12 months for full-year balance sheets).
    entes:
        Optional list of IBGE codes to filter to. ``None`` (default) iterates
        every state + municipality returned by ``/entes``.
    annex:
        DCA annex name. Defaults to ``DCA-Anexo I-C`` (per-account balances).
    limit:
        If set, cap the number of entities iterated per year. Useful for
        smoke tests (full national run is ~5,570 entities × N years).
    states_only:
        Restrict to UF-level entities (``esfera == "E"``); roughly 27 rows
        per year.

    Returns
    -------
    Sorted list of CSV paths written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if exercicios is None and date:
        token = date.replace("-", "")
        if len(token) >= 4:
            try:
                exercicios = [int(token[:4])]
            except ValueError:
                exercicios = None
    if not exercicios:
        # Default to the previous calendar year — SICONFI typically only has
        # complete annual data ~6-12 months after the year ends.
        from datetime import datetime  # local import: avoid module-load DTZ
        exercicios = [datetime.now().year - 1]  # noqa: DTZ005

    written: list[Path] = []
    headers = {"User-Agent": "BR-ACC-ETL/1.0 (siconfi)"}

    with httpx.Client(timeout=SICONFI_REQUEST_TIMEOUT, headers=headers) as client:
        all_entes = _fetch_entes(client)
        logger.info("[siconfi] fetched %d entidades", len(all_entes))

        if states_only:
            target_entes = [e for e in all_entes if e.get("esfera") == "E"]
        else:
            target_entes = list(all_entes)

        if entes:
            keep = {str(e) for e in entes}
            target_entes = [
                e for e in target_entes if str(e.get("cod_ibge", "")) in keep
            ]

        if limit is not None:
            target_entes = target_entes[:limit]

        logger.info(
            "[siconfi] querying %d entidades (%s, annex=%s)",
            len(target_entes),
            "states only" if states_only else "all spheres",
            annex,
        )

        for year in exercicios:
            year_rows: list[dict[str, Any]] = []
            failed = 0
            for idx, ente in enumerate(target_entes, 1):
                cod_ibge = str(ente.get("cod_ibge", "")).strip()
                if not cod_ibge:
                    continue
                try:
                    rows = _fetch_dca(client, cod_ibge, year, annex)
                except httpx.HTTPError as exc:
                    failed += 1
                    logger.debug(
                        "[siconfi] DCA failure for %s/%d: %s", cod_ibge, year, exc,
                    )
                    rows = []
                if rows:
                    # Stamp ente metadata onto each row so downstream extract
                    # has the IBGE code and human name without a separate join.
                    for r in rows:
                        r.setdefault("cod_ibge", cod_ibge)
                        r.setdefault(
                            "instituicao",
                            ente.get("ente") or ente.get("nome", ""),
                        )
                    year_rows.extend(rows)
                time.sleep(SICONFI_REQUEST_DELAY)
                if idx % 100 == 0:
                    logger.info(
                        "[siconfi] year=%d: %d/%d entes processed (%d failed, %d rows)",
                        year, idx, len(target_entes), failed, len(year_rows),
                    )

            year_path = output_dir / f"dca_{year}.csv"
            _write_csv(year_path, year_rows)
            written.append(year_path)
            logger.info(
                "[siconfi] year=%d: wrote %d rows -> %s (%d failed)",
                year, len(year_rows), year_path, failed,
            )

    return sorted(written)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write ``rows`` as a UTF-8 CSV; empty file when ``rows`` is empty."""
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                fieldnames.append(str(k))
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


class SiconfiPipeline(Pipeline):
    """ETL pipeline for SICONFI (municipal/state finance declarations).

    Data source: Tesouro Nacional API (apidatalake.tesouro.gov.br).
    Loads MunicipalFinance nodes linked to municipalities (Company nodes by CNPJ).
    """

    name = "siconfi"
    source_id = "siconfi"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: list[dict[str, Any]] = []
        self.finances: list[dict[str, Any]] = []
        self.municipality_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        siconfi_dir = Path(self.data_dir) / "siconfi"
        all_records: list[dict[str, Any]] = []

        # Read CSV files produced by download_siconfi.py
        csv_files = sorted(siconfi_dir.glob("dca_*.csv"))
        for csv_file in csv_files:
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            all_records.extend(df.to_dict("records"))  # type: ignore[arg-type]
            logger.info("  Loaded %d records from %s", len(df), csv_file.name)

        # Fallback: also try JSON if present (original API format)
        for json_file in sorted(siconfi_dir.glob("*.json")):
            with open(json_file, encoding="utf-8") as f:
                data = json.load(f)
                items = data if isinstance(data, list) else data.get("items", [])
                all_records.extend(items)

        if self.limit:
            all_records = all_records[: self.limit]

        self._raw = all_records
        logger.info("Extracted %d SICONFI records", len(self._raw))

    def transform(self) -> None:
        finances: list[dict[str, Any]] = []
        municipality_rels: list[dict[str, Any]] = []

        for row in self._raw:
            cod_ibge = str(row.get("cod_ibge", "")).strip()
            if not cod_ibge:
                continue

            # CSV uses "instituicao", API JSON uses "ente"
            ente = normalize_name(
                str(row.get("instituicao", "") or row.get("ente", ""))
            )
            exercicio = str(row.get("exercicio", "")).strip()
            conta = str(row.get("conta", "")).strip()
            coluna = str(row.get("coluna", "")).strip()
            valor = row.get("valor")

            if valor is None or valor == "":
                continue

            try:
                amount = float(str(valor).replace(",", "."))
            except (ValueError, TypeError):
                continue

            # CNPJ may be in API JSON but not in CSV downloads
            cnpj_raw = str(row.get("cnpj", "")).strip()
            cnpj_digits = strip_document(cnpj_raw)
            cnpj_formatted = format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""

            id_source = f"{cod_ibge}_{exercicio}_{conta}_{coluna}"
            finance_id = hashlib.sha256(id_source.encode()).hexdigest()[:16]

            finances.append({
                "finance_id": finance_id,
                "cod_ibge": cod_ibge,
                "municipality": ente,
                "year": exercicio,
                "account": conta,
                "column": coluna,
                "amount": amount,
                "source": "siconfi",
            })

            if cnpj_formatted:
                municipality_rels.append({
                    "cnpj": cnpj_formatted,
                    "finance_id": finance_id,
                    "municipality": ente,
                })

        self.finances = deduplicate_rows(finances, ["finance_id"])
        self.municipality_rels = municipality_rels
        logger.info(
            "Transformed %d finance records, %d municipality links",
            len(self.finances),
            len(self.municipality_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.finances:
            loader.load_nodes("MunicipalFinance", self.finances, key_field="finance_id")

        if self.municipality_rels:
            # Ensure Company nodes exist for municipalities
            muni_nodes = deduplicate_rows(
                [
                    {"cnpj": r["cnpj"], "razao_social": r["municipality"]}
                    for r in self.municipality_rels
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", muni_nodes, key_field="cnpj")

            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.cnpj}) "
                "MATCH (f:MunicipalFinance {finance_id: row.finance_id}) "
                "MERGE (c)-[:DECLAROU_FINANCA]->(f)"
            )
            loader.run_query_with_retry(query, self.municipality_rels)
