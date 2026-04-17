from __future__ import annotations

import csv
import hashlib
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
from bracc_etl.transforms import deduplicate_rows, normalize_name

logger = logging.getLogger(__name__)

API_BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/"
GOIAS_UF_CODE = "52"
REQUEST_TIMEOUT = 30.0
REQUEST_DELAY = 0.3
# RREO uses nr_periodo=6 (annual summary, 6th bimester)
RREO_YEARS = range(2021, 2025)
RREO_ANEXO = "RREO-Anexo 01"

# Summary-account keywords kept when persisting RREO rows to disk. Mirrors the
# filter applied by TcmGoPipeline._extract_finbra_api so that the CSVs written
# by fetch_to_disk can be consumed verbatim by the ETL step without an extra
# transform.
RREO_SUMMARY_KEYWORDS = (
    "RECEITAS (EXCETO INTRA",
    "RECEITA CORRENTE",
    "RECEITA TRIBUTÁRIA",
    "RECEITA DE TRANSFERÊNCIA",
    "DESPESAS (EXCETO INTRA",
    "DESPESAS CORRENTES",
    "DESPESAS DE CAPITAL",
    "DESPESA TOTAL COM PESSOAL",
)

# Canonical cumulative-realized column names in SICONFI RREO Anexo 01.
# The endpoint returns ~7 rows per (ente, ano, conta), one per "coluna"
# (previsao inicial, previsao atualizada, no bimestre, ate o bimestre,
# saldo, %). We keep only the annual cumulative realized value to avoid
# summing semantically different figures.
REVENUE_COLUMNS = {"até o bimestre (c)", "ate o bimestre (c)"}
EXPENDITURE_COLUMNS = {
    "despesas liquidadas até o bimestre (h)",
    "despesas liquidadas ate o bimestre (h)",
}
# FINBRA CSV fallback uses a single pre-aggregated "Valor" column.
FINBRA_COLUMN = "valor"


# ----------------------------------------------------------------------
# Module-level HTTP helpers (shared by the pipeline and fetch_to_disk).
# ----------------------------------------------------------------------

def _fetch_entes_goias(client: httpx.Client) -> list[dict[str, Any]]:
    """Return all Goias entities from the SICONFI entes endpoint."""
    url = f"{API_BASE}entes"
    resp = client.get(url)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", []) if isinstance(data, dict) else data
    return [
        r for r in items
        if str(r.get("cod_ibge", "")).startswith(GOIAS_UF_CODE)
    ]


def _fetch_rreo_for_muni_year(
    client: httpx.Client, cod_ibge: str, year: int
) -> list[dict[str, Any]]:
    """Fetch summary RREO Anexo 01 rows for a single municipality/year.

    Returns only the top-level summary accounts (keeping the same filter
    the ETL pipeline applies), annotated with ``an_exercicio``. Raises
    ``httpx.HTTPError`` on transport/HTTP errors other than 404 (which
    returns an empty list, matching the pipeline's graceful skip).
    """
    url = f"{API_BASE}rreo"
    params = {
        "an_exercicio": str(year),
        "nr_periodo": "6",
        "co_tipo_demonstrativo": "RREO",
        "no_anexo": RREO_ANEXO,
        "id_ente": cod_ibge,
    }
    resp = client.get(url, params=params)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", []) if isinstance(data, dict) else data
    kept: list[dict[str, Any]] = []
    for item in items:
        conta = str(item.get("conta", ""))
        if any(kw in conta.upper() for kw in RREO_SUMMARY_KEYWORDS):
            item["an_exercicio"] = str(year)
            kept.append(item)
    return kept


def fetch_to_disk(
    output_dir: Path,
    limit_municipios: int | None = None,
    years: list[int] | None = None,
) -> list[Path]:
    """Download TCM-GO (SICONFI RREO) raw data to ``output_dir``.

    Writes two kinds of CSV that the TcmGoPipeline already knows how to
    ingest via its local-CSV fallback path:

    * ``entes.csv`` — list of Goias municipalities (from the SICONFI
      ``entes`` endpoint), filtered to UF code 52.
    * ``finbra_rreo_<year>.csv`` — one file per requested year with the
      summary RREO Anexo 01 rows for every fetched municipality.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    limit_municipios:
        If set, only the first N Goias municipalities are queried. Useful
        for smoke tests.
    years:
        Explicit list of ``an_exercicio`` values. Defaults to the module
        constant ``RREO_YEARS`` (2021..2024 inclusive).

    Returns
    -------
    List of absolute paths to every CSV written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    years_list = list(years) if years else list(RREO_YEARS)
    written: list[Path] = []

    with httpx.Client(
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": "BR-ACC-ETL/1.0 (tcm_go)"},
    ) as client:
        entes = _fetch_entes_goias(client)
        logger.info("Fetched %d Goias entes from SICONFI", len(entes))
        if limit_municipios is not None:
            entes = entes[:limit_municipios]
            logger.info("Limiting to first %d municipalities", len(entes))

        entes_path = output_dir / "entes.csv"
        _write_csv(entes_path, entes)
        written.append(entes_path.resolve())
        logger.info("Wrote %d entes -> %s", len(entes), entes_path)

        for year in years_list:
            year_rows: list[dict[str, Any]] = []
            total = len(entes)
            fetched = 0
            failed = 0
            for idx, muni in enumerate(entes, 1):
                cod_ibge = str(muni.get("cod_ibge", ""))
                if not cod_ibge:
                    continue
                try:
                    rows = _fetch_rreo_for_muni_year(client, cod_ibge, year)
                    year_rows.extend(rows)
                    fetched += 1
                except httpx.HTTPError as exc:
                    failed += 1
                    logger.debug(
                        "RREO failure for %s/%d: %s", cod_ibge, year, exc
                    )
                time.sleep(REQUEST_DELAY)
                if idx % 50 == 0:
                    logger.info(
                        "  Year %d: %d/%d fetched (%d failed, %d rows)",
                        year, idx, total, failed, len(year_rows),
                    )

            year_path = output_dir / f"finbra_rreo_{year}.csv"
            _write_csv(year_path, year_rows)
            written.append(year_path.resolve())
            logger.info(
                "Year %d: wrote %d rows (%d munis fetched, %d failed) -> %s",
                year, len(year_rows), fetched, failed, year_path,
            )

    return written


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write ``rows`` as CSV to ``path``.

    If ``rows`` is empty, writes an empty file (no header) so downstream
    globbing and contract checks still see the artefact.
    """
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
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


class TcmGoPipeline(Pipeline):
    """ETL pipeline for Goias municipal finance data from SICONFI/Tesouro Nacional.

    Fetches fiscal data (revenues and expenditures) for all 246 Goias
    municipalities via the SICONFI API, with local CSV fallback.
    """

    name = "tcm_go"
    source_id = "tcm_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._municipalities: list[dict[str, Any]] = []
        self._raw_fiscal: list[dict[str, Any]] = []
        self.municipalities: list[dict[str, Any]] = []
        self.revenues: list[dict[str, Any]] = []
        self.expenditures: list[dict[str, Any]] = []
        self.revenue_rels: list[dict[str, Any]] = []
        self.expenditure_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    def extract(self) -> None:
        tcm_dir = Path(self.data_dir) / "tcm_go"

        # Try local CSV files first
        entes_loaded = self._extract_entes_csv(tcm_dir)
        fiscal_loaded = self._extract_finbra_csv(tcm_dir)

        # Fall back to API when local files are absent or empty
        if not entes_loaded:
            self._extract_entes_api()
        if not fiscal_loaded:
            self._extract_finbra_api()

        if self.limit:
            self._raw_fiscal = self._raw_fiscal[: self.limit]

        self.rows_in = len(self._raw_fiscal)
        logger.info(
            "Extracted %d municipalities, %d fiscal records for Goias",
            len(self._municipalities),
            len(self._raw_fiscal),
        )

    def _extract_entes_csv(self, tcm_dir: Path) -> bool:
        csv_path = tcm_dir / "entes.csv"
        if not csv_path.exists():
            return False
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        records: list[dict[str, Any]] = [
            {str(k): v for k, v in r.items()} for r in df.to_dict("records")
        ]
        # Filter to Goias
        self._municipalities = [
            r for r in records if str(r.get("cod_ibge", "")).startswith(GOIAS_UF_CODE)
        ]
        logger.info("Loaded %d Goias entes from CSV", len(self._municipalities))
        return len(self._municipalities) > 0

    def _extract_finbra_csv(self, tcm_dir: Path) -> bool:
        csv_files = sorted(tcm_dir.glob("finbra*.csv"))
        if not csv_files:
            return False
        records: list[dict[str, Any]] = []
        for csv_file in csv_files:
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            records.extend(
                {str(k): v for k, v in r.items()} for r in df.to_dict("records")
            )
            logger.info("  Loaded %d records from %s", len(df), csv_file.name)
        # Filter to Goias municipalities
        self._raw_fiscal = [
            r for r in records if str(r.get("cod_ibge", "")).startswith(GOIAS_UF_CODE)
        ]
        logger.info("Loaded %d Goias fiscal records from CSV", len(self._raw_fiscal))
        return len(self._raw_fiscal) > 0

    def _extract_entes_api(self) -> None:
        """Fetch list of Goias municipalities from SICONFI entes endpoint."""
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
                self._municipalities = _fetch_entes_goias(client)
                logger.info(
                    "Fetched %d Goias municipalities from API", len(self._municipalities)
                )
        except httpx.HTTPError as exc:
            logger.warning("Failed to fetch entes from API: %s", exc)

    def _extract_finbra_api(self) -> None:
        """Fetch fiscal data for Goias municipalities from SICONFI RREO endpoint.

        Uses RREO (Resumo da Execucao Orcamentaria) Anexo 01 which contains
        revenue and expenditure summaries per municipality per year.
        The 6th bimester (nr_periodo=6) gives the annual totals.
        """
        if not self._municipalities:
            logger.warning("No municipalities to fetch fiscal data for")
            return

        records: list[dict[str, Any]] = []
        total_munis = len(self._municipalities)

        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            for year in RREO_YEARS:
                fetched_year = 0
                for muni in self._municipalities:
                    cod_ibge = str(muni.get("cod_ibge", ""))
                    if not cod_ibge:
                        continue
                    try:
                        rows = _fetch_rreo_for_muni_year(client, cod_ibge, year)
                        records.extend(rows)
                        fetched_year += 1
                    except httpx.HTTPError as exc:
                        logger.debug(
                            "Failed RREO for %s/%d: %s", cod_ibge, year, exc
                        )
                    time.sleep(REQUEST_DELAY)

                    # Limit for testing
                    if self.limit and len(records) >= self.limit:
                        break

                logger.info(
                    "  Year %d: fetched %d/%d municipalities",
                    year, fetched_year, total_munis,
                )
                if self.limit and len(records) >= self.limit:
                    break

        self._raw_fiscal = records
        logger.info("Fetched %d fiscal records from RREO API", len(self._raw_fiscal))

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        municipalities: list[dict[str, Any]] = []
        revenues: list[dict[str, Any]] = []
        expenditures: list[dict[str, Any]] = []
        revenue_rels: list[dict[str, Any]] = []
        expenditure_rels: list[dict[str, Any]] = []

        # Build municipality nodes from entes data
        for row in self._municipalities:
            cod_ibge = str(row.get("cod_ibge", "")).strip()
            if not cod_ibge or not cod_ibge.startswith(GOIAS_UF_CODE):
                continue
            name = normalize_name(
                str(row.get("ente", "") or row.get("instituicao", "") or row.get("nome", ""))
            )
            population = row.get("populacao", "")
            municipalities.append({
                "municipality_id": cod_ibge,
                "name": name,
                "uf": "GO",
                "population": str(population).strip() if population else "",
                "source": "tcm_go",
            })

        # Process fiscal records into revenues and expenditures
        for row in self._raw_fiscal:
            cod_ibge = str(row.get("cod_ibge", "")).strip()
            if not cod_ibge or not cod_ibge.startswith(GOIAS_UF_CODE):
                continue

            conta = str(row.get("conta", "")).strip()
            coluna = str(row.get("coluna", "") or row.get("rotulo", "")).strip()
            coluna_norm = coluna.lower()
            descricao = conta
            exercicio = str(
                row.get("exercicio", "")
                or row.get("an_exercicio", "")
            ).strip()
            valor = row.get("valor")

            if valor is None or valor == "":
                continue
            try:
                amount = float(str(valor).replace(",", "."))
            except (ValueError, TypeError):
                continue

            is_revenue = self._is_revenue(conta)
            # Drop rows that are not the cumulative-realized value.
            # Without this, we would sum previsao + realizada + saldo + %
            # and produce nonsense totals.
            if is_revenue:
                if coluna_norm not in REVENUE_COLUMNS and coluna_norm != FINBRA_COLUMN:
                    continue
            else:
                if coluna_norm not in EXPENDITURE_COLUMNS and coluna_norm != FINBRA_COLUMN:
                    continue

            id_source = f"{cod_ibge}_{exercicio}_{conta}_{coluna}"
            stable_id = hashlib.sha256(id_source.encode()).hexdigest()[:16]

            if is_revenue:
                revenues.append({
                    "revenue_id": stable_id,
                    "municipality_id": cod_ibge,
                    "year": exercicio,
                    "account": conta,
                    "column": coluna,
                    "description": descricao,
                    "amount": amount,
                    "source": "tcm_go",
                })
                revenue_rels.append({
                    "source_key": cod_ibge,
                    "target_key": stable_id,
                })
            else:
                expenditures.append({
                    "expenditure_id": stable_id,
                    "municipality_id": cod_ibge,
                    "year": exercicio,
                    "account": conta,
                    "column": coluna,
                    "description": descricao,
                    "amount": amount,
                    "source": "tcm_go",
                })
                expenditure_rels.append({
                    "source_key": cod_ibge,
                    "target_key": stable_id,
                })

        self.municipalities = deduplicate_rows(municipalities, ["municipality_id"])
        self.revenues = deduplicate_rows(revenues, ["revenue_id"])
        self.expenditures = deduplicate_rows(expenditures, ["expenditure_id"])
        self.revenue_rels = revenue_rels
        self.expenditure_rels = expenditure_rels

        logger.info(
            "Transformed %d municipalities, %d revenues, %d expenditures",
            len(self.municipalities),
            len(self.revenues),
            len(self.expenditures),
        )

    @staticmethod
    def _is_revenue(conta: str) -> bool:
        """Classify an account as revenue based on its name."""
        conta_lower = conta.lower()
        revenue_keywords = (
            "receita",
            "arrecada",
            "tribut",
            "transfer",
            "fpm",
            "icms",
            "iptu",
            "iss",
            "revenue",
        )
        return any(kw in conta_lower for kw in revenue_keywords)

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.municipalities:
            loader.load_nodes(
                "GoMunicipality", self.municipalities, key_field="municipality_id"
            )

        if self.revenues:
            loader.load_nodes(
                "MunicipalRevenue", self.revenues, key_field="revenue_id"
            )

        if self.expenditures:
            loader.load_nodes(
                "MunicipalExpenditure", self.expenditures, key_field="expenditure_id"
            )

        if self.revenue_rels:
            loader.load_relationships(
                rel_type="ARRECADOU",
                rows=self.revenue_rels,
                source_label="GoMunicipality",
                source_key="municipality_id",
                target_label="MunicipalRevenue",
                target_key="revenue_id",
            )

        if self.expenditure_rels:
            loader.load_relationships(
                rel_type="GASTOU",
                rows=self.expenditure_rels,
                source_label="GoMunicipality",
                source_key="municipality_id",
                target_label="MunicipalExpenditure",
                target_key="expenditure_id",
            )

        self.rows_loaded = len(self.revenues) + len(self.expenditures)
