from __future__ import annotations

import csv
import logging
import zipfile
from datetime import date as _date
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
    parse_brl_flexible,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# ── Download / fetch_to_disk (for scripts/download_transferegov.py) ──────
#
# Despite the module name, the upstream feed for ``TransferegovPipeline``
# is NOT ``/download-de-dados/transferencias/`` — that endpoint has a
# different schema. The actual feed is the same consolidated
# ``/download-de-dados/emendas-parlamentares/<YYYYMMDD>`` ZIP that
# ``siop`` and ``tesouro_emendas`` download from, but transferegov is
# the only consumer of the auxiliary ``_Convenios.csv`` and
# ``_PorFavorecido.csv`` slices that the other two pipelines skip.
#
# The Portal accepts any date token in the URL syntactically and always
# 302-redirects to the latest consolidated ZIP on
# ``dadosabertos-download.cgu.gov.br``. We therefore default ``date`` to
# today's UTC date and treat it as a cache-busting key only.
#
# All three CSVs (``EmendasParlamentares.csv`` ~43 MB,
# ``EmendasParlamentares_Convenios.csv`` ~24 MB,
# ``EmendasParlamentares_PorFavorecido.csv`` ~167 MB) are extracted
# verbatim from the ZIP (latin-1, semicolon-delimited) — no column
# remap, since ``TransferegovPipeline.extract`` already reads the
# Portuguese accented headers directly.
_TRANSFEREGOV_DOWNLOAD_BASE = (
    "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares"
)
_TRANSFEREGOV_USER_AGENT = "br-acc/bracc-etl download_transferegov (httpx)"
_TRANSFEREGOV_HTTP_TIMEOUT = 600.0

# CSV members the pipeline reads. Order matters: the main file is read
# first so its presence can short-circuit a malformed ZIP early.
_TRANSFEREGOV_MEMBERS: tuple[str, ...] = (
    "EmendasParlamentares.csv",
    "EmendasParlamentares_Convenios.csv",
    "EmendasParlamentares_PorFavorecido.csv",
)
# Member -> output filename. Identity mapping today, but kept explicit so
# a future Portal rename (e.g. ``EMENDAS_PARLAMENTARES.CSV``) can be
# absorbed without churning ``TransferegovPipeline.extract``.
_TRANSFEREGOV_OUT_NAMES: dict[str, str] = {m: m for m in _TRANSFEREGOV_MEMBERS}


def _truncate_csv_to_limit(src_path: Path, limit: int) -> int:
    """Truncate a latin-1 CSV in-place to header + ``limit`` data rows.

    Returns the number of *data* rows kept. Used to make the 167 MB
    ``_PorFavorecido.csv`` smoke-test friendly without changing the
    pipeline's read semantics.
    """
    tmp_path = src_path.with_suffix(src_path.suffix + ".trunc")
    written_rows = 0
    with src_path.open("r", encoding="latin-1", newline="") as src, \
            tmp_path.open("w", encoding="latin-1", newline="") as dst:
        reader = csv.reader(src, delimiter=";")
        writer = csv.writer(dst, delimiter=";")
        try:
            header = next(reader)
        except StopIteration:
            tmp_path.unlink(missing_ok=True)
            return 0
        writer.writerow(header)
        for row in reader:
            writer.writerow(row)
            written_rows += 1
            if written_rows >= limit:
                break
    tmp_path.replace(src_path)
    return written_rows


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,
    limit: int | None = None,
    timeout: float = _TRANSFEREGOV_HTTP_TIMEOUT,
) -> list[Path]:
    """Download Portal-emendas ZIP and extract all 3 CSVs to ``output_dir``.

    Writes ``EmendasParlamentares.csv``, ``EmendasParlamentares_Convenios.csv``,
    and ``EmendasParlamentares_PorFavorecido.csv`` (latin-1, semicolon-delim)
    into ``output_dir`` — the exact filenames+dialect
    ``TransferegovPipeline.extract`` consumes.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    date:
        Optional ``YYYYMMDD`` cache key. The Portal endpoint accepts any
        date token syntactically and always serves the latest consolidated
        ZIP, so this only affects the raw-zip cache filename. Defaults to
        today (UTC).
    limit:
        When set, truncates each CSV to the first N data rows after
        extraction. Useful for smoke tests against the 234 MB unpacked
        archive (in particular ``_PorFavorecido.csv`` at 167 MB).
    timeout:
        Per-request HTTP timeout in seconds.

    Returns
    -------
    Sorted list of output CSV paths (always 3 entries on a successful
    download).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    date_token = date or _date.today().strftime("%Y%m%d")
    url = f"{_TRANSFEREGOV_DOWNLOAD_BASE}/{date_token}"
    zip_path = raw_dir / f"emendas_parlamentares_{date_token}.zip"

    headers = {"User-Agent": _TRANSFEREGOV_USER_AGENT}
    if not (zip_path.exists() and zip_path.stat().st_size > 0):
        logger.info(
            "[transferegov.fetch_to_disk] downloading %s -> %s",
            url, zip_path.name,
        )
        with httpx.Client(
            follow_redirects=True,
            headers=headers,
            timeout=timeout,
            verify=False,
        ) as client, client.stream("GET", url) as resp:
            resp.raise_for_status()
            with zip_path.open("wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    if chunk:
                        fh.write(chunk)
    else:
        logger.info(
            "[transferegov.fetch_to_disk] reusing cached zip %s",
            zip_path.name,
        )

    written: list[Path] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = set(zf.namelist())
            for member in _TRANSFEREGOV_MEMBERS:
                if member not in members:
                    logger.warning(
                        "[transferegov.fetch_to_disk] %s missing from %s",
                        member, zip_path.name,
                    )
                    continue
                out_name = _TRANSFEREGOV_OUT_NAMES[member]
                out_path = output_dir / out_name
                with zf.open(member) as src, out_path.open("wb") as dst:
                    while True:
                        block = src.read(1 << 20)
                        if not block:
                            break
                        dst.write(block)
                if limit is not None:
                    kept = _truncate_csv_to_limit(out_path, limit)
                    logger.info(
                        "[transferegov.fetch_to_disk] truncated %s to %d rows",
                        out_name, kept,
                    )
                size_mb = out_path.stat().st_size / 1024 / 1024
                logger.info(
                    "[transferegov.fetch_to_disk] wrote %s (%.2f MB)",
                    out_path, size_mb,
                )
                written.append(out_path)
    except zipfile.BadZipFile:
        logger.warning(
            "[transferegov.fetch_to_disk] bad zip %s -- deleting",
            zip_path.name,
        )
        zip_path.unlink(missing_ok=True)
        return []

    return sorted(written)


class TransferegovPipeline(Pipeline):
    """ETL pipeline for TransfereGov parliamentary amendments data.

    Sources: Portal da Transparência emendas parlamentares bulk download.
    Three CSV files:
    - EmendasParlamentares.csv: amendments with authors, functions, municipalities
    - EmendasParlamentares_PorFavorecido.csv: who received the money (companies/persons)
    - EmendasParlamentares_Convenios.csv: convênios linked to amendments
    """

    name = "transferegov"
    source_id = "transferegov"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_emendas: pd.DataFrame = pd.DataFrame()
        self._raw_favorecidos: pd.DataFrame = pd.DataFrame()
        self._raw_convenios: pd.DataFrame = pd.DataFrame()
        self.amendments: list[dict[str, Any]] = []
        self.authors: list[dict[str, Any]] = []
        self.author_rels: list[dict[str, Any]] = []
        self.favorecido_companies: list[dict[str, Any]] = []
        self.favorecido_persons: list[dict[str, Any]] = []
        self.favorecido_rels: list[dict[str, Any]] = []
        self.convenios: list[dict[str, Any]] = []
        self.convenio_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "transferegov"
        self._raw_emendas = pd.read_csv(
            src_dir / "EmendasParlamentares.csv",
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
        )
        self._raw_favorecidos = pd.read_csv(
            src_dir / "EmendasParlamentares_PorFavorecido.csv",
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
        )
        self._raw_convenios = pd.read_csv(
            src_dir / "EmendasParlamentares_Convenios.csv",
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
        )

    def transform(self) -> None:
        self._transform_amendments()
        self._transform_favorecidos()
        self._transform_convenios()

    def _transform_amendments(self) -> None:
        """Transform main amendments file: Amendment nodes + Person authors."""
        amendments: list[dict[str, Any]] = []
        authors: list[dict[str, Any]] = []
        author_rels: list[dict[str, Any]] = []

        # Group by amendment code to aggregate values
        grouped = self._raw_emendas.groupby("Código da Emenda")

        for code, group in grouped:
            code_str = str(code).strip()
            if not code_str or code_str == "Sem informação":
                continue

            first = group.iloc[0]
            author_code = str(first["Código do Autor da Emenda"]).strip()
            author_name = normalize_name(str(first["Nome do Autor da Emenda"]))
            emenda_type = str(first["Tipo de Emenda"]).strip()
            function_name = normalize_name(str(first["Nome Função"]))
            municipality = str(first["Município"]).strip()
            uf = str(first["UF"]).strip()

            # Sum values across all rows for this amendment
            value_empenhado = sum(
                parse_brl_flexible(str(r["Valor Empenhado"]))
                for _, r in group.iterrows()
            )
            value_pago = sum(
                parse_brl_flexible(str(r["Valor Pago"]))
                for _, r in group.iterrows()
            )

            amendments.append({
                "amendment_id": code_str,
                "type": emenda_type,
                "function": function_name,
                "municipality": municipality,
                "uf": uf,
                "value_committed": value_empenhado,
                "value_paid": value_pago,
            })

            # Author relationship
            if author_code and author_code != "S/I":
                authors.append({
                    "author_key": author_code,
                    "name": author_name,
                })
                author_rels.append({
                    "source_key": author_code,
                    "target_key": code_str,
                })

        self.amendments = deduplicate_rows(amendments, ["amendment_id"])
        self.authors = deduplicate_rows(authors, ["author_key"])
        self.author_rels = author_rels

    def _transform_favorecidos(self) -> None:
        """Transform favorecidos: companies/persons receiving amendment funds."""
        companies: list[dict[str, Any]] = []
        persons: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for _, row in self._raw_favorecidos.iterrows():
            emenda_code = str(row["Código da Emenda"]).strip()
            if not emenda_code or emenda_code == "Sem informação":
                continue

            doc_raw = str(row["Código do Favorecido"]).strip()
            digits = strip_document(doc_raw)
            tipo = str(row["Tipo Favorecido"]).strip()
            nome = normalize_name(str(row["Favorecido"]))
            valor = parse_brl_flexible(str(row["Valor Recebido"]))
            municipio = str(row["Município Favorecido"]).strip()
            uf = str(row["UF Favorecido"]).strip()

            if tipo == "Pessoa Jurídica" and len(digits) == 14:
                cnpj = format_cnpj(doc_raw)
                companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome,
                })
                rels.append({
                    "amendment_id": emenda_code,
                    "doc": cnpj,
                    "entity_type": "Company",
                    "doc_field": "cnpj",
                    "value": valor,
                    "municipality": municipio,
                    "uf": uf,
                })
            elif tipo == "Pessoa Fisica" and len(digits) == 11:
                # Individual CPFs — we don't store raw CPFs for non-PEPs,
                # but we still create Person nodes for graph linkage
                from bracc_etl.transforms import format_cpf

                cpf = format_cpf(doc_raw)
                persons.append({
                    "cpf": cpf,
                    "name": nome,
                })
                rels.append({
                    "amendment_id": emenda_code,
                    "doc": cpf,
                    "entity_type": "Person",
                    "doc_field": "cpf",
                    "value": valor,
                    "municipality": municipio,
                    "uf": uf,
                })
            # Skip Unidade Gestora, Inscrição Genérica, Inválido

        self.favorecido_companies = deduplicate_rows(companies, ["cnpj"])
        self.favorecido_persons = deduplicate_rows(persons, ["cpf"])
        self.favorecido_rels = rels

    def _transform_convenios(self) -> None:
        """Transform convênios linked to amendments."""
        convenios: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for _, row in self._raw_convenios.iterrows():
            emenda_code = str(row["Código da Emenda"]).strip()
            if not emenda_code or emenda_code == "Sem informação":
                continue

            numero = str(row["Número Convênio"]).strip()
            if not numero:
                continue

            convenente = normalize_name(str(row["Convenente"]))
            objeto = normalize_name(str(row["Objeto Convênio"]))
            valor = parse_brl_flexible(str(row["Valor Convênio"]))
            data_pub = parse_date(str(row["Data Publicação Convênio"]))
            funcao = normalize_name(str(row["Nome Função"]))

            convenios.append({
                "convenio_id": numero,
                "convenente": convenente,
                "object": objeto,
                "value": valor,
                "date_published": data_pub,
                "function": funcao,
            })

            rels.append({
                "source_key": emenda_code,
                "target_key": numero,
            })

        self.convenios = deduplicate_rows(convenios, ["convenio_id"])
        self.convenio_rels = rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # 1. Amendment nodes
        if self.amendments:
            loader.load_nodes("Amendment", self.amendments, key_field="amendment_id")

        # 2. Person nodes for authors (keyed by author_key for entity resolution)
        if self.authors:
            loader.load_nodes("Person", self.authors, key_field="author_key")

        # 3. Person -[:AUTOR_EMENDA]-> Amendment
        if self.author_rels:
            loader.load_relationships(
                rel_type="AUTOR_EMENDA",
                rows=self.author_rels,
                source_label="Person",
                source_key="author_key",
                target_label="Amendment",
                target_key="amendment_id",
            )

        # 4. Company nodes for favorecidos
        if self.favorecido_companies:
            loader.load_nodes(
                "Company", self.favorecido_companies, key_field="cnpj"
            )

        # 5. Person nodes for favorecidos
        if self.favorecido_persons:
            loader.load_nodes(
                "Person", self.favorecido_persons, key_field="cpf"
            )

        # 6. Amendment -[:BENEFICIOU]-> Company/Person
        if self.favorecido_rels:
            company_rels = [
                r for r in self.favorecido_rels if r["entity_type"] == "Company"
            ]
            person_rels = [
                r for r in self.favorecido_rels if r["entity_type"] == "Person"
            ]

            if company_rels:
                query = (
                    "UNWIND $rows AS row "
                    "MATCH (a:Amendment {amendment_id: row.amendment_id}) "
                    "MATCH (c:Company {cnpj: row.doc}) "
                    "MERGE (a)-[r:BENEFICIOU]->(c) "
                    "SET r.value = row.value, "
                    "r.municipality = row.municipality, "
                    "r.uf = row.uf"
                )
                loader.run_query(query, company_rels)

            if person_rels:
                query = (
                    "UNWIND $rows AS row "
                    "MATCH (a:Amendment {amendment_id: row.amendment_id}) "
                    "MATCH (p:Person {cpf: row.doc}) "
                    "MERGE (a)-[r:BENEFICIOU]->(p) "
                    "SET r.value = row.value, "
                    "r.municipality = row.municipality, "
                    "r.uf = row.uf"
                )
                loader.run_query(query, person_rels)

        # 7. Convenio nodes
        if self.convenios:
            loader.load_nodes("Convenio", self.convenios, key_field="convenio_id")

        # 8. Amendment -[:GEROU_CONVENIO]-> Convenio
        if self.convenio_rels:
            loader.load_relationships(
                rel_type="GEROU_CONVENIO",
                rows=self.convenio_rels,
                source_label="Amendment",
                source_key="amendment_id",
                target_label="Convenio",
                target_key="convenio_id",
            )
