from __future__ import annotations

import logging
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# TSE 2024 masks ALL candidate CPFs as "-4". After strip_document → "4",
# format_cpf → "4" — every candidate MERGEs into one ghost node.
# We use SQ_CANDIDATO (unique sequential ID per candidate per election) instead.
_MASKED_CPF_SENTINEL = "-4"

# URL canônica do dataset TSE pro attach_provenance. Pipeline-wide
# constante porque o TSE não expõe deep-link estável por registro
# individual — todo row carimba o mesmo ``source_url`` (página agregada
# de dados abertos). ``source_id`` é canônico (``tse``, alinhado com
# ``docs/source_registry_br_v1.csv``).
_TSE_DATASET_URL = "https://dadosabertos.tse.jus.br/"


class TSEPipeline(Pipeline):
    """Electoral data pipeline — candidates and campaign donations."""

    name = "tse"
    source_id = "tse"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.candidates: list[dict[str, Any]] = []
        self.donations: list[dict[str, Any]] = []
        self.elections: list[dict[str, Any]] = []

    def extract(self) -> None:
        tse_dir = Path(self.data_dir) / "tse"
        if not tse_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, tse_dir)
            self._raw_candidatos = pd.DataFrame()
            self._raw_doacoes = pd.DataFrame()
            return
        candidatos_path = tse_dir / "candidatos.csv"
        doacoes_path = tse_dir / "doacoes.csv"
        if not candidatos_path.exists() or not doacoes_path.exists():
            logger.warning("[%s] Required CSV files not found in %s", self.name, tse_dir)
            self._raw_candidatos = pd.DataFrame()
            self._raw_doacoes = pd.DataFrame()
            return
        self._raw_candidatos = pd.read_csv(
            candidatos_path, encoding="latin-1", dtype=str,
            nrows=self.limit,
        )
        self._raw_doacoes = pd.read_csv(
            doacoes_path, encoding="latin-1", dtype=str,
            nrows=self.limit,
        )
        self.rows_in = len(self._raw_candidatos) + len(self._raw_doacoes)

    def transform(self) -> None:
        self._transform_candidates()
        self._transform_donations()

    def _transform_candidates(self) -> None:
        candidates: list[dict[str, Any]] = []
        elections: list[dict[str, Any]] = []

        for _, row in self._raw_candidatos.iterrows():
            sq = str(row["sq_candidato"]).strip()
            raw_cpf = str(row["cpf"]).strip()
            name = normalize_name(str(row["nome"]))
            ano = int(row["ano"])
            cargo = normalize_name(str(row["cargo"]))
            uf = str(row["uf"]).strip().upper()
            municipio = normalize_name(str(row.get("municipio", "")))
            partido = str(row.get("partido", "")).strip().upper()

            # Only store CPF if it's a real value (not the TSE "-4" mask)
            cpf = None
            if raw_cpf != _MASKED_CPF_SENTINEL:
                cpf = format_cpf(strip_document(raw_cpf))

            candidate: dict[str, Any] = {
                "sq_candidato": sq,
                "name": name,
                "partido": partido,
                "uf": uf,
            }
            if cpf:
                candidate["cpf"] = cpf

            candidates.append(candidate)
            elections.append({
                "year": ano,
                "cargo": cargo,
                "uf": uf,
                "municipio": municipio,
                "candidate_sq": sq,
            })

        self.candidates = deduplicate_rows(candidates, ["sq_candidato"])
        self.elections = deduplicate_rows(
            elections, ["year", "cargo", "uf", "municipio", "candidate_sq"]
        )

    def _transform_donations(self) -> None:
        donations: list[dict[str, Any]] = []

        for _, row in self._raw_doacoes.iterrows():
            candidate_sq = str(row["sq_candidato"]).strip()
            donor_doc = strip_document(str(row["cpf_cnpj_doador"]))
            donor_name = normalize_name(str(row["nome_doador"]))
            valor = float(str(row["valor"]).replace(",", "."))
            ano = int(row["ano"])

            # Data da doação (DT_RECEITA / Data da receita / variantes early).
            # Coluna opcional em fixtures e anos antigos — ``parse_date``
            # devolve "" quando ausente ou não parseável, e o Neo4j aceita
            # string vazia na rel sem quebrar o grafo.
            raw_data = row.get("data_doacao", "")
            if pd.isna(raw_data):
                raw_data = ""
            donated_at = parse_date(str(raw_data).strip())

            is_company = len(donor_doc) == 14
            donor_doc_fmt = format_cnpj(donor_doc)
            if not is_company:
                donor_doc_fmt = format_cpf(donor_doc)

            donations.append({
                "candidate_sq": candidate_sq,
                "donor_doc": donor_doc_fmt,
                "donor_name": donor_name,
                "donor_is_company": is_company,
                "valor": valor,
                "year": ano,
                "donated_at": donated_at,
            })

        self.donations = donations

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # Split candidates: CPF-keyed (dedup by CPF) vs sq_candidato-only
        cpf_candidates = [c for c in self.candidates if c.get("cpf")]
        nocpf_candidates = [c for c in self.candidates if not c.get("cpf")]

        # Merge by CPF, also store sq_candidato as a list for cross-referencing
        if cpf_candidates:
            cpf_deduped = deduplicate_rows(cpf_candidates, ["cpf"])
            cpf_deduped = [
                self.attach_provenance(c, record_id=c.get("sq_candidato", c["cpf"]),
                                       record_url=_TSE_DATASET_URL)
                for c in cpf_deduped
            ]
            loader.load_nodes("Person", cpf_deduped, key_field="cpf")

        # For candidates without CPF, merge by sq_candidato
        if nocpf_candidates:
            nocpf_stamped = [
                self.attach_provenance(c, record_id=c["sq_candidato"],
                                       record_url=_TSE_DATASET_URL)
                for c in nocpf_candidates
            ]
            loader.load_nodes("Person", nocpf_stamped, key_field="sq_candidato")

        # Build sq_candidato→cpf lookup for linking
        sq_to_cpf: dict[str, str] = {}
        for c in self.candidates:
            if c.get("cpf"):
                sq_to_cpf[c["sq_candidato"]] = c["cpf"]

        # Map sq_candidato to Person node via Cypher SET for CANDIDATO_EM linking
        sq_cpf_rows = [{"sq": sq, "cpf": cpf} for sq, cpf in sq_to_cpf.items()]
        if sq_cpf_rows:
            loader.run_query(
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.cpf}) "
                "SET p.sq_candidato = row.sq",
                sq_cpf_rows,
            )

        # Election nodes
        election_nodes = deduplicate_rows(
            [
                {"year": e["year"], "cargo": e["cargo"], "uf": e["uf"], "municipio": e["municipio"]}
                for e in self.elections
            ],
            ["year", "cargo", "uf", "municipio"],
        )
        if election_nodes:
            loader.run_query(
                "UNWIND $rows AS row "
                "MERGE (e:Election {year: row.year, cargo: row.cargo, "
                "uf: row.uf, municipio: row.municipio})",
                election_nodes,
            )

        # CANDIDATO_EM relationships — find person by CPF first, fallback to sq_candidato
        candidato_rels = []
        for e in self.elections:
            rel: dict[str, Any] = {
                "target_year": e["year"],
                "target_cargo": e["cargo"],
                "target_uf": e["uf"],
                "target_municipio": e["municipio"],
            }
            cpf = sq_to_cpf.get(e["candidate_sq"])
            if cpf:
                rel["cpf"] = cpf
                rel["sq"] = ""
            else:
                rel["cpf"] = ""
                rel["sq"] = e["candidate_sq"]
            candidato_rels.append(rel)

        if candidato_rels:
            loader.run_query(
                "UNWIND $rows AS row "
                "OPTIONAL MATCH (p1:Person {cpf: row.cpf}) WHERE row.cpf <> '' "
                "OPTIONAL MATCH (p2:Person {sq_candidato: row.sq}) WHERE row.sq <> '' "
                "WITH coalesce(p1, p2) AS p, row "
                "WHERE p IS NOT NULL "
                "MATCH (e:Election {year: row.target_year, cargo: row.target_cargo, "
                "uf: row.target_uf, municipio: row.target_municipio}) "
                "MERGE (p)-[:CANDIDATO_EM]->(e)",
                candidato_rels,
            )

        # Donor nodes and DOOU relationships
        person_donors = [
            {"cpf": d["donor_doc"], "name": d["donor_name"]}
            for d in self.donations
            if not d["donor_is_company"]
        ]
        company_donors = [
            {"cnpj": d["donor_doc"], "name": d["donor_name"], "razao_social": d["donor_name"]}
            for d in self.donations
            if d["donor_is_company"]
        ]

        if person_donors:
            person_donors_deduped = deduplicate_rows(person_donors, ["cpf"])
            person_donors_deduped = [
                self.attach_provenance(p, record_id=p["cpf"],
                                       record_url=_TSE_DATASET_URL)
                for p in person_donors_deduped
            ]
            loader.load_nodes("Person", person_donors_deduped, key_field="cpf")
        if company_donors:
            company_donors_deduped = deduplicate_rows(company_donors, ["cnpj"])
            company_donors_deduped = [
                self.attach_provenance(c, record_id=c["cnpj"],
                                       record_url=_TSE_DATASET_URL)
                for c in company_donors_deduped
            ]
            loader.load_nodes(
                "Company", company_donors_deduped, key_field="cnpj",
            )

        # DOOU from Person donors → candidate
        person_donation_rels = []
        for d in self.donations:
            if d["donor_is_company"]:
                continue
            target_cpf = sq_to_cpf.get(d["candidate_sq"], "")
            target_key = target_cpf or d["candidate_sq"]
            rel = self.attach_provenance(
                {
                    "source_key": d["donor_doc"],
                    "target_cpf": target_cpf,
                    "target_sq": d["candidate_sq"] if not target_cpf else "",
                    "valor": d["valor"],
                    "year": d["year"],
                    "donated_at": d["donated_at"],
                },
                record_id=f"{d['year']}:{d['donor_doc']}:{target_key}",
                record_url=_TSE_DATASET_URL,
            )
            person_donation_rels.append(rel)
        if person_donation_rels:
            # DOOU MERGE key é ``(year)`` — múltiplas doações do mesmo doador
            # pro mesmo candidato no mesmo ano colapsam em 1 rel só. Por isso
            # ``r.donated_at`` é ``last-write-wins`` neste pipeline (bulk
            # histórico). O pipeline ``tse_prestacao_contas_go`` mantém
            # granularidade por doação via ``donation_id``.
            loader.run_query(
                "UNWIND $rows AS row "
                "MATCH (d:Person {cpf: row.source_key}) "
                "OPTIONAL MATCH (c1:Person {cpf: row.target_cpf}) WHERE row.target_cpf <> '' "
                "OPTIONAL MATCH (c2:Person {sq_candidato: row.target_sq}) "
                "WHERE row.target_sq <> '' "
                "WITH d, coalesce(c1, c2) AS c, row "
                "WHERE c IS NOT NULL "
                "MERGE (d)-[r:DOOU {year: row.year}]->(c) "
                "SET r.valor = row.valor, "
                "    r.donated_at = row.donated_at, "
                "    r.source_id = row.source_id, "
                "    r.source_record_id = row.source_record_id, "
                "    r.source_url = row.source_url, "
                "    r.ingested_at = row.ingested_at, "
                "    r.run_id = row.run_id",
                person_donation_rels,
            )

        # DOOU from Company donors → candidate
        company_donation_rels = []
        for d in self.donations:
            if not d["donor_is_company"]:
                continue
            target_cpf = sq_to_cpf.get(d["candidate_sq"], "")
            target_key = target_cpf or d["candidate_sq"]
            rel = self.attach_provenance(
                {
                    "source_key": d["donor_doc"],
                    "target_cpf": target_cpf,
                    "target_sq": d["candidate_sq"] if not target_cpf else "",
                    "valor": d["valor"],
                    "year": d["year"],
                    "donated_at": d["donated_at"],
                },
                record_id=f"{d['year']}:{d['donor_doc']}:{target_key}",
                record_url=_TSE_DATASET_URL,
            )
            company_donation_rels.append(rel)
        if company_donation_rels:
            loader.run_query(
                "UNWIND $rows AS row "
                "MATCH (d:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (c1:Person {cpf: row.target_cpf}) WHERE row.target_cpf <> '' "
                "OPTIONAL MATCH (c2:Person {sq_candidato: row.target_sq}) "
                "WHERE row.target_sq <> '' "
                "WITH d, coalesce(c1, c2) AS c, row "
                "WHERE c IS NOT NULL "
                "MERGE (d)-[r:DOOU {year: row.year}]->(c) "
                "SET r.valor = row.valor, "
                "    r.donated_at = row.donated_at, "
                "    r.source_id = row.source_id, "
                "    r.source_record_id = row.source_record_id, "
                "    r.source_url = row.source_url, "
                "    r.ingested_at = row.ingested_at, "
                "    r.run_id = row.run_id",
                company_donation_rels,
            )

        self.rows_loaded = (
            len(self.candidates) + len(self.elections) + len(self.donations)
        )


# ────────────────────────────────────────────────────────────────────
# Acquisition helper — UF-scoped CDN download for Fiscal Cidadao
# ────────────────────────────────────────────────────────────────────
#
# The TSE CDN publishes per-state CSVs inside year-scoped ZIPs, which lets us
# download only the slice we need (e.g. UF=GO) instead of the ~GB full-country
# dump. The Fiscal Cidadao fork's bootstrap contract uses this helper via
# ``scripts/download_tse.py`` so the ``tse`` pipeline can run without a manual
# file_manifest step.
#
# Columns and per-year donation URL logic mirror ``etl/scripts/download_tse.py``
# (the all-UF historical downloader); this is a narrowed copy that filters
# per-state files instead of concatenating the full country.

_TSE_CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele"

_CANDIDATO_COLS = {
    "SQ_CANDIDATO": "sq_candidato",
    "NR_CPF_CANDIDATO": "cpf",
    "NM_CANDIDATO": "nome",
    "DS_CARGO": "cargo",
    "SG_UF": "uf",
    "NM_UE": "municipio",
    "ANO_ELEICAO": "ano",
    "SG_PARTIDO": "partido",
    "NR_CANDIDATO": "nr_candidato",
}

_DOACAO_COLS_NEW = {
    "SQ_CANDIDATO": "sq_candidato",
    "NR_CPF_CNPJ_DOADOR": "cpf_cnpj_doador",
    "NM_DOADOR": "nome_doador",
    "VR_RECEITA": "valor",
    "DT_RECEITA": "data_doacao",
    "AA_ELEICAO": "ano",
    "NM_CANDIDATO": "nome_candidato",
    "SG_PARTIDO": "partido",
    "NR_CANDIDATO": "nr_candidato",
    "SG_UF": "uf",
    "SG_UE": "sg_ue",
}

_DOACAO_COLS_LEGACY = {
    "Sequencial Candidato": "sq_candidato",
    "CPF/CNPJ do doador": "cpf_cnpj_doador",
    "Nome do doador": "nome_doador",
    "Valor receita": "valor",
    "Data da receita": "data_doacao",
    "Nome candidato": "nome_candidato",
    "UF": "uf",
    "Sigla  UE": "sg_ue",
    "Sigla UE": "sg_ue",
}

_DOACAO_COLS_EARLY_VARIANTS: dict[str, list[str]] = {
    "sq_candidato": ["SEQUENCIAL_CANDIDATO"],
    "cpf_cnpj_doador": [
        "CD_CPF_CNPJ_DOADOR", "CD_CPF_CGC", "CD_CPF_CGC_DOA", "NUMERO_CPF_CGC_DOADOR",
    ],
    "nome_doador": ["NM_DOADOR", "NO_DOADOR", "NOME_DOADOR"],
    "valor": ["VR_RECEITA", "VALOR_RECEITA"],
    "data_doacao": ["DT_RECEITA", "DT_DOACAO", "DATA_RECEITA", "DATA_DOACAO"],
    "nome_candidato": ["NM_CANDIDATO", "NO_CAND", "NOME_CANDIDATO"],
    "partido": ["SG_PARTIDO", "SG_PART", "SIGLA_PARTIDO"],
    "uf": ["SG_UF", "SIGLA_UF", "UF"],
}


def _donation_url(year: int) -> str:
    """Map election year → correct TSE CDN URL for donation data."""
    if year >= 2018:
        return (
            f"{_TSE_CDN}/prestacao_contas/"
            f"prestacao_de_contas_eleitorais_candidatos_{year}.zip"
        )
    if year in (2012, 2014):
        return f"{_TSE_CDN}/prestacao_contas/prestacao_final_{year}.zip"
    return f"{_TSE_CDN}/prestacao_contas/prestacao_contas_{year}.zip"


def _download_zip(url: str, dest: Path, *, timeout: float = 600.0) -> Path | None:
    """Download a ZIP; returns path on success or None on HTTP / bad-ZIP failure.

    The TSE CDN sometimes returns HTTP 200 with a short HTML 404 body for missing
    year ZIPs, so we validate via ``zipfile.is_zipfile`` after download.
    """
    import httpx

    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=timeout) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=65_536):
                    fh.write(chunk)
    except httpx.HTTPError as exc:
        logger.warning("[tse] HTTP error downloading %s: %s", url, exc)
        if dest.exists():
            dest.unlink()
        return None

    if not zipfile.is_zipfile(dest):
        logger.warning(
            "[tse] URL %s did not return a valid ZIP (likely missing year)", url,
        )
        dest.unlink(missing_ok=True)
        return None
    return dest


def _extract_uf_files(
    zip_path: Path, extract_dir: Path, *, uf: str, prefixes: tuple[str, ...],
) -> list[Path]:
    """Extract only per-UF CSV/TXT members matching ``*_<UF>.csv|.txt``.

    ``prefixes`` restricts which filename prefixes we accept (defensive — TSE
    ZIPs occasionally mix layouts). Returns paths to the extracted files.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    uf_upper = uf.upper()
    selected: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            name = Path(info.filename).name.lower()
            if not any(name.startswith(p) for p in prefixes):
                continue
            if not (name.endswith(".csv") or name.endswith(".txt")):
                continue
            # Require the UF to appear in the filename (per-state file).
            if f"_{uf_upper.lower()}." not in name:
                continue
            zf.extract(info, extract_dir)
            selected.append(extract_dir / info.filename)
    return selected


def _extract_any_receitas(zip_path: Path, extract_dir: Path) -> list[Path]:
    """Extract all receitas files (any UF). Used for year formats that don't
    split per-UF at the filename level (e.g. 2002–2008 nested layouts).
    Filtering by UF column happens post-read.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    selected: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = Path(info.filename).name.lower()
            if (
                name == "receitacandidato.csv"
                or name == "receitascandidatos.txt"
                or name.startswith("receitas_candidatos")
            ):
                zf.extract(info, extract_dir)
                selected.append(extract_dir / info.filename)
    return selected


def _read_tse_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path, sep=";", encoding="latin-1", dtype=str, keep_default_na=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[tse] skip %s: %s", path.name, exc)
        return pd.DataFrame()


def _remap_donations(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Remap donation columns to pipeline-expected names across all TSE eras."""
    if df.empty:
        return df

    cols = set(df.columns)
    if "SQ_CANDIDATO" in cols:
        col_map = dict(_DOACAO_COLS_NEW)
    elif "Sequencial Candidato" in cols:
        col_map = dict(_DOACAO_COLS_LEGACY)
        # Legacy UF column name varies; try common variants.
        for candidate in ("UF", "Sigla UF", "Sigla  UF"):
            if candidate in cols and candidate not in col_map:
                col_map[candidate] = "uf"
                break
    else:
        # Early format: match first available variant per target field.
        col_map = {}
        for target, variants in _DOACAO_COLS_EARLY_VARIANTS.items():
            for variant in variants:
                if variant in cols:
                    col_map[variant] = target
                    break
        if not col_map:
            logger.warning(
                "[tse] year %d: unknown donation schema, columns=%s",
                year, list(df.columns)[:8],
            )
            return pd.DataFrame()

    available = {src: dst for src, dst in col_map.items() if src in df.columns}
    mapped = df[list(available.keys())].rename(columns=available)

    if "ano" not in mapped.columns:
        mapped["ano"] = str(year)
    return mapped


def fetch_to_disk(
    output_dir: Path,
    *,
    uf: str = "GO",
    years: list[int] | None = None,
    timeout: float = 600.0,
    skip_existing: bool = True,
) -> list[Path]:
    """Download TSE candidate + donation data filtered to one UF.

    Writes pipeline-ready ``candidatos.csv`` and ``doacoes.csv`` under
    ``output_dir``. ``years`` defaults to a GO-relevant historical set covering
    Marconi Perillo-era elections (1998–2022). Raw per-year ZIPs land under
    ``output_dir/raw/`` for idempotent re-runs.

    Returns the list of output CSV paths actually written (0, 1, or 2 files).
    """
    uf_upper = uf.upper()
    years = years or [1998, 2002, 2006, 2010, 2014, 2018, 2022]
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    # ── Candidates ────────────────────────────────────────────────────
    cand_frames: list[pd.DataFrame] = []
    for year in years:
        zip_path = raw_dir / f"consulta_cand_{year}.zip"
        if not (skip_existing and zip_path.exists() and zipfile.is_zipfile(zip_path)):
            url = f"{_TSE_CDN}/consulta_cand/consulta_cand_{year}.zip"
            logger.info("[tse] downloading candidates %s", url)
            if _download_zip(url, zip_path, timeout=timeout) is None:
                continue

        extract_dir = raw_dir / f"consulta_cand_{year}_extracted"
        files = _extract_uf_files(
            zip_path, extract_dir, uf=uf_upper, prefixes=("consulta_cand_",),
        )
        for f in files:
            df = _read_tse_csv(f)
            if df.empty:
                continue
            cand_frames.append(df)

    if cand_frames:
        full = pd.concat(cand_frames, ignore_index=True)
        available = {src: dst for src, dst in _CANDIDATO_COLS.items() if src in full.columns}
        mapped = full[list(available.keys())].rename(columns=available)
        # Defensive UF filter (per-state filenames already scope this, but
        # some years ship BRASIL.csv alongside — we exclude those via filename).
        if "uf" in mapped.columns:
            mapped = mapped[mapped["uf"].str.upper() == uf_upper]
        out = output_dir / "candidatos.csv"
        mapped.to_csv(out, index=False, encoding="latin-1")
        logger.info("[tse] wrote %d candidate rows → %s", len(mapped), out)
        written.append(out)

    # ── Donations ─────────────────────────────────────────────────────
    don_frames: list[pd.DataFrame] = []
    for year in years:
        zip_path = raw_dir / f"doacoes_{year}.zip"
        if not (skip_existing and zip_path.exists() and zipfile.is_zipfile(zip_path)):
            url = _donation_url(year)
            logger.info("[tse] downloading donations %s", url)
            if _download_zip(url, zip_path, timeout=timeout) is None:
                continue

        extract_dir = raw_dir / f"doacoes_{year}_extracted"
        # Try per-UF filename slice first (2012+); if empty, fall back to
        # all-receitas extraction and filter on the UF column (2002–2010).
        files = _extract_uf_files(
            zip_path, extract_dir, uf=uf_upper, prefixes=("receitas_candidatos",),
        )
        if not files:
            files = _extract_any_receitas(zip_path, extract_dir)

        frames: list[pd.DataFrame] = [_read_tse_csv(f) for f in files]
        frames = [df for df in frames if not df.empty]
        if not frames:
            continue

        year_df = pd.concat(frames, ignore_index=True)
        mapped = _remap_donations(year_df, year)
        if mapped.empty:
            continue
        if "uf" in mapped.columns:
            mapped = mapped[mapped["uf"].str.upper() == uf_upper]
        # sg_ue is a secondary signal (municipal elections carry SG_UE=<MUN>
        # not <UF>); we keep rows where sg_ue is empty/UF-matching to avoid
        # dropping state-level donations.
        don_frames.append(mapped)

    if don_frames:
        combined = pd.concat(don_frames, ignore_index=True)
        out = output_dir / "doacoes.csv"
        combined.to_csv(out, index=False, encoding="latin-1")
        logger.info("[tse] wrote %d donation rows → %s", len(combined), out)
        written.append(out)

    return written
