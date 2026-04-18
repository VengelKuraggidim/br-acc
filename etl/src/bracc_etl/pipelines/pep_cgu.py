from __future__ import annotations

import io
import logging
import zipfile
from datetime import UTC, datetime
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
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Module-level fetch_to_disk: CGU Portal da Transparência monthly PEP dump.
# --------------------------------------------------------------------------
#
# The Portal da Transparência exposes a monthly ZIP of the official PEP
# (Pessoas Expostas Politicamente) registry at:
#
#     https://portaldatransparencia.gov.br/download-de-dados/pep/<YYYYMM>
#
# which 302-redirects to a CloudFront-backed ZIP, e.g.
# ``https://dadosabertos-download.cgu.gov.br/.../202602_PEP.zip``. Each
# archive holds a single ``YYYYMM_PEP.csv`` (~16 MB, latin-1, ``;`` delim,
# ~130k rows nationally). ``fetch_to_disk`` downloads, unzips and copies
# the inner CSV to ``<output_dir>/pep.csv`` — the exact path + dialect
# ``PepCguPipeline.extract`` reads.

_PEP_BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/pep"
_PEP_HTTP_TIMEOUT = 180.0
# How many months to walk back when --month is not given and the current
# month is not yet published.
_PEP_MAX_MONTH_FALLBACK = 12


def _previous_month(yyyymm: str) -> str:
    """Return YYYYMM for the month before ``yyyymm`` (e.g. '202602' -> '202601')."""
    year = int(yyyymm[:4])
    month = int(yyyymm[4:])
    month -= 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}{month:02d}"


def _current_yyyymm() -> str:
    now = datetime.now(tz=UTC)
    return f"{now.year:04d}{now.month:02d}"


def fetch_to_disk(
    output_dir: Path | str,
    month: str | None = None,
    timeout: float = _PEP_HTTP_TIMEOUT,
    max_fallback: int = _PEP_MAX_MONTH_FALLBACK,
) -> list[Path]:
    """Download the CGU PEP monthly dump and write ``pep.csv``.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing. Writes
        ``<output_dir>/pep.csv`` (same path ``PepCguPipeline.extract`` reads).
    month:
        Month in ``YYYYMM`` form (e.g. ``"202602"``). When omitted, the
        function starts at the current UTC month and walks backwards up
        to ``max_fallback`` months until it finds a published ZIP. Portal
        da Transparência is typically 1-2 months behind the current date.
    timeout:
        HTTP timeout per request.
    max_fallback:
        How many months to try before giving up when ``month`` is ``None``.

    Returns
    -------
    List with one absolute path (the extracted ``pep.csv``).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "pep.csv"

    candidates: list[str]
    if month:
        token = month.strip().replace("-", "").replace("/", "")
        if len(token) != 6 or not token.isdigit():
            raise ValueError(
                f"month must be YYYYMM (6 digits); got {month!r}",
            )
        candidates = [token]
    else:
        # Walk back from current month; the Portal typically lags ~1-2 months.
        current = _current_yyyymm()
        candidates = [current]
        for _ in range(max_fallback):
            candidates.append(_previous_month(candidates[-1]))

    last_exc: Exception | None = None
    with httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "br-acc/bracc-etl download_pep_cgu (httpx)"},
        timeout=timeout,
    ) as client:
        for yyyymm in candidates:
            url = f"{_PEP_BASE_URL}/{yyyymm}"
            logger.info("[pep_cgu.fetch_to_disk] trying %s", url)
            try:
                # Stream into a memory buffer — the ZIP is ~2 MB.
                buf = io.BytesIO()
                with client.stream("GET", url) as resp:
                    if resp.status_code >= 400:
                        logger.info(
                            "[pep_cgu.fetch_to_disk] %s returned HTTP %d",
                            url, resp.status_code,
                        )
                        continue
                    for chunk in resp.iter_bytes(chunk_size=1 << 16):
                        if chunk:
                            buf.write(chunk)
                buf.seek(0)
                # Validate that we got a ZIP (CloudFront sometimes responds
                # with an HTML 403/XML error body with 2xx status when the
                # month isn't published yet).
                try:
                    zf = zipfile.ZipFile(buf)
                except zipfile.BadZipFile as exc:
                    logger.info(
                        "[pep_cgu.fetch_to_disk] %s: not a zip (%s)", url, exc,
                    )
                    continue
                break
            except httpx.HTTPError as exc:
                last_exc = exc
                continue
        else:
            raise RuntimeError(
                "pep_cgu.fetch_to_disk could not locate a published "
                f"PEP ZIP after trying {len(candidates)} month(s) "
                f"starting at {candidates[0]}: last error={last_exc!r}"
            )

    # Extract the inner CSV member (named ``YYYYMM_PEP.csv``) to pep.csv.
    csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_members:
        raise RuntimeError(
            f"PEP ZIP {yyyymm} contains no CSV member: {zf.namelist()}"
        )
    if len(csv_members) > 1:
        logger.warning(
            "[pep_cgu.fetch_to_disk] ZIP has %d CSVs; picking first: %s",
            len(csv_members), csv_members[0],
        )
    member = csv_members[0]
    with zf.open(member) as src, out_path.open("wb") as dst:
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            dst.write(chunk)
    zf.close()

    logger.info(
        "[pep_cgu.fetch_to_disk] wrote %s (%.2f MB) from %s [%s]",
        out_path, out_path.stat().st_size / 1024 / 1024, member, yyyymm,
    )
    return [out_path.resolve()]

# Government CSV columns may appear in different cases.
# Map UPPER CASE (as downloaded) -> canonical mixed-case for the pipeline.
_COLUMN_ALIASES: dict[str, str] = {
    # Canonical (space-delimited with accents)
    "CPF": "CPF",
    "NOME": "Nome",
    "SIGLA FUNCAO": "Sigla Função",
    "SIGLA FUNÇÃO": "Sigla Função",
    "DESCRICAO FUNCAO": "Descrição Função",
    "DESCRIÇÃO FUNÇÃO": "Descrição Função",
    "NIVEL FUNCAO": "Nível Função",
    "NÍVEL FUNÇÃO": "Nível Função",
    "NOME ORGAO": "Nome Órgão",
    "NOME ÓRGÃO": "Nome Órgão",
    "DATA INICIO EXERCICIO": "Data Início Exercício",
    "DATA INÍCIO EXERCÍCIO": "Data Início Exercício",
    "DATA FIM EXERCICIO": "Data Fim Exercício",
    "DATA FIM EXERCÍCIO": "Data Fim Exercício",
    "DATA FIM CARENCIA": "Data Fim Carência",
    "DATA FIM CARÊNCIA": "Data Fim Carência",
    # Underscore-delimited format (government CSV as of 2025)
    "NOME_PEP": "Nome",
    "SIGLA_FUNCAO": "Sigla Função",
    "SIGLA_FUNÇÃO": "Sigla Função",
    "DESCRICAO_FUNCAO": "Descrição Função",
    "DESCRIÇÃO_FUNÇÃO": "Descrição Função",
    "NIVEL_FUNCAO": "Nível Função",
    "NÍVEL_FUNÇÃO": "Nível Função",
    "NOME_ORGAO": "Nome Órgão",
    "NOME_ÓRGÃO": "Nome Órgão",
    "DATA_INICIO_EXERCICIO": "Data Início Exercício",
    "DATA_INÍCIO_EXERCÍCIO": "Data Início Exercício",
    "DATA_FIM_EXERCICIO": "Data Fim Exercício",
    "DATA_FIM_EXERCÍCIO": "Data Fim Exercício",
    "DATA_FIM_CARENCIA": "Data Fim Carência",
    "DATA_FIM_CARÊNCIA": "Data Fim Carência",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: try exact match, then case-insensitive alias."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        col_upper = col.strip().upper()
        if col_upper in _COLUMN_ALIASES:
            rename_map[col] = _COLUMN_ALIASES[col_upper]
        elif col.strip() in _COLUMN_ALIASES.values():
            rename_map[col] = col.strip()
    return df.rename(columns=rename_map)


class PepCguPipeline(Pipeline):
    """ETL pipeline for CGU PEP List (official PEP registry)."""

    name = "pep_cgu"
    source_id = "cgu_pep"

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
        self.pep_records: list[dict[str, Any]] = []
        self.person_links: list[dict[str, Any]] = []

    def extract(self) -> None:
        pep_dir = Path(self.data_dir) / "pep_cgu"
        csv_path = pep_dir / "pep.csv"
        if not csv_path.exists():
            msg = f"PEP CSV not found: {csv_path}"
            raise FileNotFoundError(msg)
        self._raw = pd.read_csv(
            csv_path,
            dtype=str,
            delimiter=";",
            encoding="latin-1",
            keep_default_na=False,
        )
        self._raw = _normalize_columns(self._raw)
        logger.info("[pep_cgu] Extracted %d PEP records", len(self._raw))

    def transform(self) -> None:
        records: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []

        for idx, row in self._raw.iterrows():
            cpf_raw = str(row.get("CPF", "")).strip()
            digits = strip_document(cpf_raw)

            nome = normalize_name(str(row.get("Nome", "")))
            if not nome:
                continue

            # Use full CPF when available, else keep masked format
            cpf_formatted = format_cpf(cpf_raw) if len(digits) == 11 else cpf_raw

            sigla = str(row.get("Sigla Função", "")).strip()
            descricao = str(row.get("Descrição Função", "")).strip()
            nivel = str(row.get("Nível Função", "")).strip()
            orgao = str(row.get("Nome Órgão", "")).strip()
            data_inicio = parse_date(str(row.get("Data Início Exercício", "")))
            data_fim = parse_date(str(row.get("Data Fim Exercício", "")))
            data_carencia = parse_date(str(row.get("Data Fim Carência", "")))

            pep_id = f"pep_{digits}_{idx}"

            records.append({
                "pep_id": pep_id,
                "cpf": cpf_formatted,
                "name": nome,
                "role": sigla,
                "role_description": descricao,
                "level": nivel,
                "org": orgao,
                "start_date": data_inicio,
                "end_date": data_fim,
                "grace_end_date": data_carencia,
                "source": "cgu_pep",
            })

            # Only link to Person nodes when we have a full CPF
            if len(digits) == 11:
                links.append({
                    "source_key": cpf_formatted,
                    "target_key": pep_id,
                })

            if self.limit and len(records) >= self.limit:
                break

        self.pep_records = deduplicate_rows(records, ["pep_id"])
        self.person_links = links
        logger.info(
            "[pep_cgu] Transformed %d PEP records, %d person links",
            len(self.pep_records),
            len(self.person_links),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.pep_records:
            loaded = loader.load_nodes("PEPRecord", self.pep_records, key_field="pep_id")
            logger.info("[pep_cgu] Loaded %d PEPRecord nodes", loaded)

        if self.person_links:
            query = (
                "UNWIND $rows AS row "
                "MERGE (p:Person {cpf: row.source_key}) "
                "ON CREATE SET p.name = '' "
                "WITH p, row "
                "MATCH (pep:PEPRecord {pep_id: row.target_key}) "
                "MERGE (p)-[:PEP_REGISTRADA]->(pep)"
            )
            loaded = loader.run_query_with_retry(query, self.person_links)
            logger.info("[pep_cgu] Loaded %d PEP_REGISTRADA relationships", loaded)
