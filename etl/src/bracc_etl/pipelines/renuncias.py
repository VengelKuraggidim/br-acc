from __future__ import annotations

import hashlib
import logging
import zipfile
from datetime import date
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
    parse_brl_amount,
    strip_document,
)

logger = logging.getLogger(__name__)

# Note: Portal da Transparencia slug is ``renuncias`` (NOT
# ``renuncias-fiscais`` -- that variant returns HTTP 500). Mode is
# ``ANO``, upstream coverage 2015-current.
_RENUNCIAS_BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/renuncias"
_RENUNCIAS_USER_AGENT = "br-acc/bracc-etl download_renuncias (httpx)"
_RENUNCIAS_HTTP_TIMEOUT = 300.0


def _default_renuncias_years() -> list[int]:
    """Return the last 3 completed calendar years (inclusive of current)."""
    current = date.today().year
    return list(range(current - 2, current + 1))


def fetch_to_disk(
    output_dir: Path | str,
    years: list[int] | None = None,
    *,
    skip_existing: bool = True,
    timeout: float = _RENUNCIAS_HTTP_TIMEOUT,
) -> list[Path]:
    """Download Portal da Transparencia ``renuncias`` yearly ZIPs to disk.

    Each ``/download-de-dados/renuncias/<YYYY>`` request 302s to a
    yearly ``<YYYY>_RenunciasFiscais.zip`` on the CGU dadosabertos
    bucket. ``RenunciasPipeline.extract`` globs ``*Ren*.csv`` minus
    ``*PorBen*.csv``, so this wrapper extracts only the files that
    survive that filter.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    requested_years = sorted({int(y) for y in (years or _default_renuncias_years())})
    written: list[Path] = []

    headers = {"User-Agent": _RENUNCIAS_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        for year in requested_years:
            existing = [
                p for p in output_dir.glob(f"{year}_*.csv")
                if ("Ren" in p.name or "ren" in p.name) and "PorBen" not in p.name
            ]
            if skip_existing and existing:
                logger.info(
                    "[renuncias] skipping year %s (found %d CSV(s))",
                    year, len(existing),
                )
                written.extend(existing)
                continue

            url = f"{_RENUNCIAS_BASE_URL}/{year}"
            zip_path = raw_dir / f"renuncias_{year}.zip"

            if not (skip_existing and zip_path.exists() and zip_path.stat().st_size > 0):
                logger.info("[renuncias] downloading %s -> %s", url, zip_path.name)
                try:
                    with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        total = resp.headers.get("content-length")
                        logger.info(
                            "[renuncias] %s size: %s bytes",
                            zip_path.name, total or "unknown",
                        )
                        with zip_path.open("wb") as fh:
                            for chunk in resp.iter_bytes(chunk_size=1 << 16):
                                if chunk:
                                    fh.write(chunk)
                except httpx.HTTPError as exc:
                    logger.warning(
                        "[renuncias] download failed for %s: %s", year, exc,
                    )
                    continue
            else:
                logger.info("[renuncias] reusing cached zip %s", zip_path.name)

            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    for info in zf.infolist():
                        name = Path(info.filename).name
                        if not name.lower().endswith(".csv"):
                            continue
                        if "Ren" not in name and "ren" not in name:
                            continue
                        if "PorBen" in name:
                            continue
                        dst_path = output_dir / name
                        with zf.open(info) as src, dst_path.open("wb") as dst:
                            while True:
                                block = src.read(1 << 20)
                                if not block:
                                    break
                                dst.write(block)
                        logger.info(
                            "[renuncias] extracted %s (%.2f MB)",
                            dst_path.name,
                            dst_path.stat().st_size / 1024 / 1024,
                        )
                        written.append(dst_path)
            except zipfile.BadZipFile:
                logger.warning(
                    "[renuncias] bad zip %s -- deleting for re-download",
                    zip_path.name,
                )
                zip_path.unlink(missing_ok=True)
                continue

    return sorted(set(written))


class RenunciasPipeline(Pipeline):
    """ETL pipeline for Renúncias Fiscais (tax waivers/exemptions).

    Data source: Portal da Transparência CSV downloads.
    Loads TaxWaiver nodes linked to Company nodes by CNPJ.
    """

    name = "renuncias"
    source_id = "renuncias"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.waivers: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        data_dir = Path(self.data_dir) / "renuncias"
        frames: list[pd.DataFrame] = []

        # Only process RenúnciasFiscais files (have amounts); skip
        # EmpresasHabilitadas and EmpresasImunesOuIsentas (no values).
        for csv_file in sorted(data_dir.glob("*.csv")):
            fname = csv_file.name
            if ("Ren" not in fname and "ren" not in fname) or "PorBen" in fname:
                continue
            df = pd.read_csv(
                csv_file,
                dtype=str,
                delimiter=";",
                encoding="latin-1",
                keep_default_na=False,
            )
            frames.append(df)

        if frames:
            self._raw = pd.concat(frames, ignore_index=True)
        else:
            self._raw = pd.DataFrame()

        if self.limit:
            self._raw = self._raw.head(self.limit)

        logger.info("Extracted %d renuncias records", len(self._raw))

    def transform(self) -> None:
        waivers: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            cnpj_raw = str(row.get("CNPJ", "")).strip().strip('"')
            digits = strip_document(cnpj_raw)

            if len(digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_raw)
            name = normalize_name(str(
                row.get("Razão Social", row.get("Raz\xe3o Social", ""))
            ))
            tributo = str(row.get("Tributo", row.get("TRIBUTO", ""))).strip()
            tipo = str(
                row.get("Tipo Renúncia", row.get("Tipo Ren\xfancia", ""))
                or row.get("Benefício Fiscal", row.get("Benef\xedcio Fiscal", ""))
            ).strip()
            ano = str(
                row.get("Ano-calendário", row.get("Ano-calend\xe1rio", ""))
                or row.get("ANO", "")
            ).strip()

            valor_raw = str(
                row.get("Valor Renúncia Fiscal (R$)",
                         row.get("Valor Ren\xfancia Fiscal (R$)", "0"))
            )
            amount = parse_brl_amount(valor_raw, default=None)
            if amount is None or amount <= 0:
                continue

            id_source = f"{digits}_{ano}_{tributo}_{tipo}"
            waiver_id = hashlib.sha256(id_source.encode()).hexdigest()[:16]

            waivers.append({
                "waiver_id": waiver_id,
                "cnpj": cnpj_formatted,
                "beneficiary_name": name,
                "tax_type": tributo,
                "waiver_type": tipo,
                "year": ano,
                "amount": amount,
                "source": "renuncias_fiscais",
            })

            company_rels.append({
                "cnpj": cnpj_formatted,
                "waiver_id": waiver_id,
                "company_name": name,
            })

        self.waivers = deduplicate_rows(waivers, ["waiver_id"])
        self.company_rels = company_rels
        logger.info(
            "Transformed %d waivers, %d company links",
            len(self.waivers),
            len(self.company_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.waivers:
            loader.load_nodes("TaxWaiver", self.waivers, key_field="waiver_id")

        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MERGE (c:Company {cnpj: row.cnpj}) "
                "ON CREATE SET c.razao_social = row.company_name "
                "WITH c, row "
                "MATCH (w:TaxWaiver {waiver_id: row.waiver_id}) "
                "MERGE (c)-[:RECEBEU_RENUNCIA]->(w)"
            )
            loader.run_query_with_retry(query, self.company_rels)
