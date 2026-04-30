from __future__ import annotations

import hashlib
import logging
import sys
import tempfile
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows

logger = logging.getLogger(__name__)

# UF IBGE numeric code -> sigla (subset; MTPS uses these codes in the
# microdata. Matches rais.py::UF_CODE_MAP exactly).
_UF_CODE_TO_SIGLA: dict[str, str] = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}

# CAGED upstream FTP. Layout: NOVO CAGED/<YYYY>/<YYYYMM>/CAGEDMOV<YYYYMM>.7z
# (plus CAGEDFOR = late, CAGEDEXC = exclusions; we ingest only MOV).
_CAGED_FTP_BASE = "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED"

# CAGED tipo_movimentacao: 1 = admission, 2 = dismissal
_MOVEMENT_TYPES: dict[str, str] = {
    "1": "admissao",
    "2": "desligamento",
    "3": "desligamento",  # some codes map to sub-types
}

# Chunk size for streaming CSV reads (100K rows per chunk)
_READ_CHUNK_SIZE = 100_000


def _generate_stats_id(
    year: str,
    month: str,
    uf: str,
    municipality_code: str,
    cnae_subclass: str,
    cbo_code: str,
    movement_type: str,
) -> str:
    """Deterministic id for aggregate CAGED buckets."""
    raw = "|".join([
        year,
        month.zfill(2),
        uf,
        municipality_code,
        cnae_subclass,
        cbo_code,
        movement_type,
    ])
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _build_movement_date(ano: str, mes: str) -> str:
    """Build YYYY-MM date string from year and month columns."""
    month = mes.zfill(2)
    return f"{ano}-{month}"


def _parse_salary(raw: str) -> float | None:
    """Parse salary value to float. Handles both dot-decimal and comma-decimal."""
    cleaned = raw.strip().replace("\u2212", "-")  # unicode minus
    if not cleaned or cleaned == "-":
        return None
    # Brazilian format: 1.500,50 -> dot as thousands, comma as decimal
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        val = float(cleaned)
        return val if val >= 0 else None
    except ValueError:
        return None


# ── fetch_to_disk: download .7z mensal, extrair, remapear pro layout ──
# que CagedPipeline.extract() consome ────────────────────────────────


# Mapeamento de cabeçalhos PDET (.txt) → colunas que o pipeline lê.
# Os nomes upstream vêm em UTF-8 sem acento na maioria dos meses, mas a
# normalização (NFKD + lowercase + sem espaço) é defensiva para o caso
# de PDET republicar com acentuação. Layout fonte:
# https://pdet.mte.gov.br/microdados-novo-caged
_CAGED_COLUMN_MAP: dict[str, str] = {
    "competenciamov": "_competencia",
    "regiao": "_regiao",
    "uf": "sigla_uf",  # numeric code → será convertido pra sigla
    "municipio": "id_municipio",
    "secao": "cnae_2_secao",
    "subclasse": "cnae_2_subclasse",
    "saldomovimentacao": "saldo_movimentacao",
    "cbo2002ocupacao": "cbo_2002",
    "categoria": "categoria",
    "graudeinstrucao": "grau_instrucao",
    "idade": "idade",
    "horascontratuais": "horas_contratuais",
    "racacor": "raca_cor",
    "sexo": "sexo",
    "tipoempregador": "tipo_empregador",
    "tipoestabelecimento": "tipo_estabelecimento",
    "tipomovimentacao": "tipo_movimentacao",
    "tipodedeficiencia": "tipo_deficiencia",
    "indtrabintermitente": "indicador_trabalho_intermitente",
    "indtrabparcial": "indicador_trabalho_parcial",
    "salario": "salario_mensal",
    "cnpjraiz": "cnpj_raiz",
}


def _slugify_column(raw: str) -> str:
    """Normalize a PDET column header for mapping (lowercase, no accents)."""
    nfkd = unicodedata.normalize("NFKD", raw)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return "".join(c for c in ascii_only.lower() if c.isalnum())


def _parse_caged_archive(
    archive_path: Path,
    *,
    year: int,
    month: int,
    limit: int | None,
) -> pd.DataFrame:
    """Extract a CAGEDMOV .7z archive and return a remapped DataFrame.

    The PDET .7z always contains exactly one ``.txt`` named like the
    archive (e.g. ``CAGEDMOV202401.txt``). We extract to a tempdir, read
    with ``sep=";"`` UTF-8, then project onto the column layout
    ``CagedPipeline.extract()`` expects. UF numeric → sigla conversion
    happens here so the downstream pipeline doesn't need to care.
    """
    # Lazy import: scripts/ is sibling to src/ at repo level
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    from _download_utils import extract_7z_archive  # type: ignore[import-not-found]

    with tempfile.TemporaryDirectory(prefix="caged_extract_") as tmp:
        tmp_dir = Path(tmp)
        extracted = extract_7z_archive(archive_path, tmp_dir)
        txt_files = [p for p in extracted if p.suffix.lower() == ".txt"]
        if not txt_files:
            raise RuntimeError(
                f"No .txt found inside {archive_path.name} after extraction "
                f"(got: {[p.name for p in extracted]})",
            )
        txt = txt_files[0]
        logger.info("[caged.fetch_to_disk] reading %s", txt.name)

        df = pd.read_csv(
            txt,
            sep=";",
            encoding="utf-8",
            dtype=str,
            keep_default_na=False,
            nrows=limit,
        )

    # Remap headers via slugified key.
    rename: dict[str, str] = {}
    for col in df.columns:
        slug = _slugify_column(col)
        if slug in _CAGED_COLUMN_MAP:
            rename[col] = _CAGED_COLUMN_MAP[slug]
    df = df.rename(columns=rename)

    # Convert UF numeric IBGE → sigla. Unknown codes pass through
    # untouched (so callers can spot upstream surprises).
    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].map(
            lambda v: _UF_CODE_TO_SIGLA.get(v.strip(), v.strip()),
        )

    # Synthesize ano/mes — pipeline expects them as separate columns.
    # Prefer the upstream ``_competencia`` (YYYYMM) when present, fall
    # back to the requested year/month so the file remains internally
    # consistent even if the upstream column is missing.
    if "_competencia" in df.columns:
        comp = df["_competencia"].astype(str).str.strip()
        df["ano"] = comp.str[:4]
        df["mes"] = comp.str[4:6]
        df = df.drop(columns=["_competencia"])
    else:
        df["ano"] = str(year)
        df["mes"] = f"{month:02d}"

    # Drop helper-only columns we don't propagate.
    df = df.drop(columns=[c for c in ("_regiao",) if c in df.columns])

    return df


def fetch_to_disk(
    output_dir: Path | str,
    *,
    year: int,
    months: list[int] | None = None,
    limit: int | None = None,
    skip_existing: bool = True,
    timeout: int = 600,
) -> list[Path]:
    """Download Novo CAGED monthly microdata and write CSVs the pipeline reads.

    For each requested month, downloads ``CAGEDMOV<YYYYMM>.7z`` from the
    PDET FTP, extracts the single ``.txt`` inside via the system ``7z``
    binary, remaps PDET column names to the layout
    ``CagedPipeline.extract()`` consumes, and writes
    ``caged_<YYYYMM>.csv`` (UTF-8, comma-separated) to ``output_dir``.

    The CAGED MOV archive for a single month is ~50 MB compressed and
    expands to ~500 MB of plaintext. Use ``--limit`` for smoke runs.

    Args:
        output_dir: Destination directory (created if missing). Usually
            ``data/caged``.
        year: Calendar year (e.g. ``2024``). Required — there's no
            sensible default for a 12 GB/year dataset.
        months: Optional list of 1..12 to restrict which months to fetch.
            Defaults to all 12 months.
        limit: When set, truncate each month's CSV to the first N rows
            (after read). Useful for smoke tests.
        skip_existing: When True (default), reuse archives already on
            disk under ``output_dir/raw/`` instead of re-downloading.
        timeout: Per-file HTTP timeout in seconds.

    Returns:
        List of written CSV paths (one per successfully processed month).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    months_to_fetch = sorted(months) if months else list(range(1, 13))

    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    from _download_utils import download_ftp_file

    written: list[Path] = []
    for month in months_to_fetch:
        yyyymm = f"{year}{month:02d}"
        archive_name = f"CAGEDMOV{yyyymm}.7z"
        url = f"{_CAGED_FTP_BASE}/{year}/{yyyymm}/{archive_name}"
        archive_path = raw_dir / archive_name

        if skip_existing and archive_path.exists() and archive_path.stat().st_size > 0:
            logger.info("[caged.fetch_to_disk] skip existing %s", archive_name)
        else:
            logger.info("[caged.fetch_to_disk] downloading %s", url)
            if not download_ftp_file(url, archive_path, timeout=timeout):
                logger.warning(
                    "[caged.fetch_to_disk] download failed for %s — skipping month",
                    archive_name,
                )
                continue

        try:
            df = _parse_caged_archive(
                archive_path, year=year, month=month, limit=limit,
            )
        except RuntimeError as exc:
            logger.warning(
                "[caged.fetch_to_disk] extract failed for %s: %s — skipping month",
                archive_name, exc,
            )
            continue

        out_path = output_dir / f"caged_{yyyymm}.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        logger.info(
            "[caged.fetch_to_disk] wrote %s (%d rows)", out_path.name, len(df),
        )
        written.append(out_path.resolve())

    return written


class CagedPipeline(Pipeline):
    """ETL pipeline for CAGED labor movement data (aggregate-only mode).

    Public CAGED data is treated as aggregate labor signal. This pipeline
    intentionally avoids Person/Company linkage and only writes LaborStats nodes.
    """

    name = "caged"
    source_id = "caged"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._csv_files: list[Path] = []

    def extract(self) -> None:
        caged_dir = Path(self.data_dir) / "caged"
        self._csv_files = sorted(caged_dir.glob("caged_*.csv"))
        if not self._csv_files:
            logger.warning("No caged_*.csv files found in %s", caged_dir)

    def transform(self) -> None:
        pass  # Transform happens per chunk in load()

    def _transform_chunk(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Transform a DataFrame chunk into aggregate LaborStats rows."""
        if df.empty:
            return []

        work = df.copy()

        def _col(name: str) -> pd.Series[Any]:
            if name in work.columns:
                return work[name]
            return pd.Series([""] * len(work), index=work.index, dtype="string")

        work["ano"] = _col("ano").astype(str).str.strip()
        work["mes"] = _col("mes").astype(str).str.strip()
        work = work[(work["ano"] != "") & (work["mes"] != "")]
        if work.empty:
            return []

        work["sigla_uf"] = _col("sigla_uf").astype(str).str.strip()
        work["id_municipio"] = _col("id_municipio").astype(str).str.strip()
        work["cnae_2_subclasse"] = _col("cnae_2_subclasse").astype(str).str.strip()
        work["cbo_2002"] = _col("cbo_2002").astype(str).str.strip()
        work["movement_type"] = _col("tipo_movimentacao").astype(str).str.strip().map(
            lambda v: _MOVEMENT_TYPES.get(v, v),
        )
        work["salary"] = _col("salario_mensal").astype(str).map(_parse_salary)
        work["movement_count"] = 1
        work["admissions"] = (work["movement_type"] == "admissao").astype(int)
        work["dismissals"] = (work["movement_type"] == "desligamento").astype(int)

        group_cols = [
            "ano",
            "mes",
            "sigla_uf",
            "id_municipio",
            "cnae_2_subclasse",
            "cbo_2002",
            "movement_type",
        ]
        grouped = (
            work.groupby(group_cols, dropna=False)
            .agg(
                total_movements=("movement_count", "sum"),
                admissions=("admissions", "sum"),
                dismissals=("dismissals", "sum"),
                avg_salary=("salary", "mean"),
            )
            .reset_index()
        )

        rows: list[dict[str, Any]] = []
        for _, row in grouped.iterrows():
            year = str(row["ano"]).strip()
            month = str(row["mes"]).strip().zfill(2)
            uf = str(row["sigla_uf"]).strip()
            municipality_code = str(row["id_municipio"]).strip()
            cnae_subclass = str(row["cnae_2_subclasse"]).strip()
            cbo_code = str(row["cbo_2002"]).strip()
            movement_type = str(row["movement_type"]).strip()

            stats_id = _generate_stats_id(
                year,
                month,
                uf,
                municipality_code,
                cnae_subclass,
                cbo_code,
                movement_type,
            )
            admissions = int(row["admissions"])
            dismissals = int(row["dismissals"])

            item: dict[str, Any] = {
                "stats_id": stats_id,
                "year": year,
                "month": month,
                "movement_date": _build_movement_date(year, month),
                "movement_type": movement_type,
                "uf": uf,
                "municipality_code": municipality_code,
                "cnae_subclass": cnae_subclass,
                "cbo_code": cbo_code,
                "total_movements": int(row["total_movements"]),
                "admissions": admissions,
                "dismissals": dismissals,
                "net_balance": admissions - dismissals,
                "identity_quality": "aggregate",
                "source": "caged",
            }
            if pd.notna(row["avg_salary"]):
                item["avg_salary"] = float(row["avg_salary"])
            rows.append(item)

        return deduplicate_rows(rows, ["stats_id"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        for csv_file in self._csv_files:
            logger.info("Processing %s ...", csv_file.name)
            reader = pd.read_csv(
                csv_file,
                dtype=str,
                keep_default_na=False,
                chunksize=_READ_CHUNK_SIZE,
                nrows=self.limit,
            )
            for chunk in reader:
                stats_rows = self._transform_chunk(chunk)
                if stats_rows:
                    loader.load_nodes("LaborStats", stats_rows, key_field="stats_id")
            logger.info("Finished %s", csv_file.name)
