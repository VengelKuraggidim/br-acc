"""ETL pipeline for Camara dos Deputados CEAP expense data.

Ingests CEAP (Cota para o Exercicio da Atividade Parlamentar) expenses.
Creates Expense nodes linked to Person (deputy) via GASTOU
and to Company (supplier) via FORNECEU.

The module also exposes :func:`fetch_to_disk`, a thin wrapper around the
Camara dos Deputados open-data API v2 used by the GO-scoped bootstrap
contract to materialize JSON snapshots of the five canonical endpoints
(deputados, CEAP despesas, proposicoes, votacoes, orgaos) under
``data/camara/`` without requiring the CEAP CSV annual dumps.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_brl_amount,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


_API_BASE = "https://dadosabertos.camara.leg.br/api/v2"
_HTTP_TIMEOUT = 30.0
_PAGE_SIZE = 100
# Cap per-deputy sub-resource pages to avoid hammering the API in smoke
# runs; ``limit`` in :func:`fetch_to_disk` further trims each endpoint.
_MAX_PAGES_PER_DEPUTY = 50
_MAX_PAGES_GLOBAL = 10
_DEFAULT_HEADERS = {"Accept": "application/json"}

# Annual CEAP bulk CSV (the dataset CamaraPipeline.extract() globs for).
# Public, no auth. Served as a ZIP because the raw ``.csv`` endpoint
# returns a file padded with ~12 MB of null bytes before the actual
# content (upstream CDN bug, observed 2026-04). Each archive contains
# a single ``Ano-<year>.csv`` (~80 MB). Years go back to 2008; default
# window targets the Marconi-era senate overlap.
_CEAP_ZIP_URL = "https://www.camara.leg.br/cotas/Ano-{year}.csv.zip"
_DEFAULT_CEAP_YEARS = tuple(range(2019, 2027))


def _make_expense_id(deputy_id: str, date: str, supplier_doc: str, value: str) -> str:
    """Generate a stable expense ID from key fields."""
    raw = f"camara_{deputy_id}_{date}_{supplier_doc}_{value}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _paginate(
    client: httpx.Client,
    path: str,
    params: dict[str, str | int],
    *,
    max_pages: int,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Walk through a Camara v2 paginated endpoint, returning ``dados`` rows.

    Stops at ``max_pages`` (safety net) or once ``limit`` records have been
    accumulated. Non-200 responses and JSON decode errors are logged and
    treated as end-of-stream so the caller still gets whatever pages
    succeeded before the failure.
    """
    rows: list[dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        query = dict(params)
        query.setdefault("itens", _PAGE_SIZE)
        query["pagina"] = page
        url = f"{_API_BASE}{path}"
        try:
            resp = client.get(url, params=query, headers=_DEFAULT_HEADERS)
        except httpx.HTTPError as exc:
            logger.warning("[camara] HTTP error on %s page %d: %s", path, page, exc)
            break
        if resp.status_code != 200:
            logger.warning(
                "[camara] non-200 on %s page %d: %s", path, page, resp.status_code,
            )
            break
        try:
            payload = resp.json()
        except json.JSONDecodeError as exc:
            logger.warning("[camara] JSON decode error on %s page %d: %s", path, page, exc)
            break
        batch = payload.get("dados") or []
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(r for r in batch if isinstance(r, dict))
        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        # Stop when we've consumed every page available.
        if len(batch) < int(query.get("itens", _PAGE_SIZE)):
            break
        page += 1
    return rows


def _write_json(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        json.dumps({"dados": records}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _download_ceap_csv(
    client: httpx.Client,
    year: int,
    output_dir: Path,
    *,
    uf: str | None = None,
) -> Path | None:
    """Download the annual CEAP CSV for ``year`` and write it to ``output_dir``.

    Fetches the ZIP archive (the plain ``.csv`` endpoint has a CDN-side
    null-byte padding bug) and extracts the single ``Ano-<year>.csv``.
    When ``uf`` is set, the file is filtered client-side on the ``sgUF``
    column so the on-disk slice matches the scope the bootstrap contract
    expects.

    Returns the written path on success, ``None`` on HTTP failure or when
    the filter yields zero rows.
    """
    import io
    import zipfile

    url = _CEAP_ZIP_URL.format(year=year)
    try:
        resp = client.get(url)
    except httpx.HTTPError as exc:
        logger.warning("[camara] HTTP error on CEAP %d: %s", year, exc)
        return None
    if resp.status_code != 200:
        logger.warning("[camara] non-200 on CEAP %d: %s", year, resp.status_code)
        return None

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        logger.warning("[camara] CEAP %d: bad zip archive", year)
        return None
    inner_name = next(
        (n for n in zf.namelist() if n.lower().endswith(".csv")),
        None,
    )
    if inner_name is None:
        logger.warning(
            "[camara] CEAP %d: no CSV in archive (members=%s)",
            year, zf.namelist(),
        )
        return None
    csv_bytes = zf.read(inner_name)

    suffix = f"_{uf}" if uf else ""
    out_path = output_dir / f"Ano-{year}{suffix}.csv"
    if not uf:
        out_path.write_bytes(csv_bytes)
        logger.info(
            "[camara] wrote %s (%.1f MB)", out_path.name, len(csv_bytes) / 1_048_576,
        )
        return out_path

    # Client-side UF filter. CEAP CSVs are ';'-delimited, UTF-8 with BOM.
    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        sep=";",
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
    )
    before = len(df)
    uf_series = df.get("sgUF")
    if not isinstance(uf_series, pd.Series):
        uf_series = pd.Series([""] * len(df), index=df.index)
    df = df[uf_series.astype(str).str.upper() == uf.upper()]
    if df.empty:
        logger.warning(
            "[camara] CEAP %d: zero rows after UF=%s filter (had %d)",
            year, uf, before,
        )
        return None
    df.to_csv(out_path, sep=";", index=False, encoding="utf-8-sig")
    logger.info(
        "[camara] wrote %s (%d/%d rows, UF=%s)",
        out_path.name, len(df), before, uf,
    )
    return out_path


def fetch_to_disk(
    output_dir: Path,
    uf: str | None = None,
    limit: int | None = None,
    years: list[int] | tuple[int, ...] | None = None,
) -> list[Path]:
    """Download Camara dos Deputados CEAP CSVs + v2 API snapshots.

    Produces two complementary outputs under ``output_dir``:

    * **Annual CEAP CSVs** (``Ano-{year}[_UF].csv``) — the primary input
      consumed by :class:`CamaraPipeline.extract`, which globs
      ``data/camara/*.csv``. Sourced from
      ``https://www.camara.leg.br/cotas/Ano-<year>.csv`` (public, no
      auth). When ``uf`` is set the CSV is filtered client-side on the
      ``sgUF`` column so only the matching UF rows are written.
    * **v2 API JSON snapshots** — ``deputados[_UF].json``,
      ``despesas[_UF].json``, ``proposicoes[_UF].json``, ``votacoes``,
      ``orgaos``. Sidecar metadata (proposicoes, votacoes, orgaos) that
      enriches the graph beyond what the CEAP CSV alone exposes. Hits
      ``/deputados`` (optionally filtered by ``siglaUf=<uf>``),
      ``/deputados/{id}/despesas``, ``/deputados/{id}/proposicoes``,
      ``/votacoes``, ``/orgaos``.

    Args:
        output_dir: Directory where CSVs + JSONs are written. Created if
            missing.
        uf: Two-letter UF filter. Applied to the CEAP CSV (client-side
            ``sgUF`` filter) and to ``/deputados`` (server-side
            ``siglaUf`` param). When set, per-deputy sub-resources are
            scoped to the matching deputies. ``None`` keeps the full
            national dataset in both outputs.
        limit: Optional record cap applied per JSON endpoint. Useful for
            smoke tests; ``None`` means no cap. Does **not** apply to the
            CEAP CSV (which is downloaded whole and filtered).
        years: Years to include in the CEAP CSV download. ``None``
            defaults to 2019-2026 (current Marconi-era window).

    Returns:
        List of file paths written to disk (sorted). Endpoints that
        returned zero rows are skipped so the caller can tell success
        apart from empty responses.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    uf_norm = uf.strip().upper() if uf else None
    suffix = f"_{uf_norm}" if uf_norm else ""
    ceap_years = tuple(years) if years else _DEFAULT_CEAP_YEARS

    written: list[Path] = []

    # --- Annual CEAP CSVs (primary input for CamaraPipeline.extract) ---
    with httpx.Client(
        timeout=httpx.Timeout(120.0, connect=30.0),
        follow_redirects=True,
    ) as csv_client:
        for year in ceap_years:
            path = _download_ceap_csv(
                csv_client, year, output_dir, uf=uf_norm,
            )
            if path is not None:
                written.append(path)

    with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
        # --- /deputados (filtered by UF when provided) ---
        dep_params: dict[str, str | int] = {"ordem": "ASC", "ordenarPor": "nome"}
        if uf_norm:
            dep_params["siglaUf"] = uf_norm
        deputies = _paginate(
            client, "/deputados", dep_params,
            max_pages=_MAX_PAGES_GLOBAL, limit=limit,
        )
        if deputies:
            path = output_dir / f"deputados{suffix}.json"
            _write_json(path, deputies)
            written.append(path)
            logger.info(
                "[camara] wrote %s (%d deputies, uf=%s)",
                path.name, len(deputies), uf_norm or "ALL",
            )
        else:
            logger.warning("[camara] no deputies returned for uf=%s", uf_norm or "ALL")

        deputy_ids = [
            str(d.get("id")) for d in deputies if d.get("id") is not None
        ]

        # --- /deputados/{id}/despesas (CEAP) ---
        expenses: list[dict[str, Any]] = []
        for dep_id in deputy_ids:
            if limit is not None and len(expenses) >= limit:
                break
            remaining = None if limit is None else max(0, limit - len(expenses))
            if remaining == 0:
                break
            dep_expenses = _paginate(
                client,
                f"/deputados/{dep_id}/despesas",
                {"ordem": "DESC", "ordenarPor": "ano"},
                max_pages=_MAX_PAGES_PER_DEPUTY,
                limit=remaining,
            )
            # Tag with deputy id so downstream consumers can link without
            # re-querying /deputados.
            for row in dep_expenses:
                row["_deputy_id"] = dep_id
            expenses.extend(dep_expenses)
        if expenses:
            path = output_dir / f"despesas{suffix}.json"
            _write_json(path, expenses[: limit] if limit is not None else expenses)
            written.append(path)
            logger.info("[camara] wrote %s (%d despesas)", path.name, len(expenses))

        # --- /deputados/{id}/proposicoes (proposals authored) ---
        proposicoes: list[dict[str, Any]] = []
        for dep_id in deputy_ids:
            if limit is not None and len(proposicoes) >= limit:
                break
            remaining = None if limit is None else max(0, limit - len(proposicoes))
            if remaining == 0:
                break
            # The /proposicoes endpoint accepts idDeputadoAutor as filter.
            props = _paginate(
                client,
                "/proposicoes",
                {"idDeputadoAutor": dep_id, "ordem": "DESC", "ordenarPor": "id"},
                max_pages=_MAX_PAGES_PER_DEPUTY,
                limit=remaining,
            )
            for row in props:
                row["_deputy_id"] = dep_id
            proposicoes.extend(props)
        if proposicoes:
            path = output_dir / f"proposicoes{suffix}.json"
            _write_json(
                path,
                proposicoes[: limit] if limit is not None else proposicoes,
            )
            written.append(path)
            logger.info(
                "[camara] wrote %s (%d proposicoes)", path.name, len(proposicoes),
            )

        # --- /votacoes (global — not UF-scoped by the API) ---
        votacoes = _paginate(
            client, "/votacoes",
            {"ordem": "DESC", "ordenarPor": "dataHoraRegistro"},
            max_pages=_MAX_PAGES_GLOBAL, limit=limit,
        )
        if votacoes:
            path = output_dir / f"votacoes{suffix}.json"
            _write_json(path, votacoes)
            written.append(path)
            logger.info("[camara] wrote %s (%d votacoes)", path.name, len(votacoes))

        # --- /orgaos (committees / global) ---
        orgaos = _paginate(
            client, "/orgaos",
            {"ordem": "ASC", "ordenarPor": "id"},
            max_pages=_MAX_PAGES_GLOBAL, limit=limit,
        )
        if orgaos:
            path = output_dir / f"orgaos{suffix}.json"
            _write_json(path, orgaos)
            written.append(path)
            logger.info("[camara] wrote %s (%d orgaos)", path.name, len(orgaos))

    return sorted(written)


class CamaraPipeline(Pipeline):
    """ETL pipeline for Camara dos Deputados CEAP expenses."""

    name = "camara"
    source_id = "camara"

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
        self.expenses: list[dict[str, Any]] = []
        self.deputies: list[dict[str, Any]] = []
        self.deputies_by_id: list[dict[str, Any]] = []
        self.suppliers: list[dict[str, Any]] = []
        self.gastou_rels: list[dict[str, Any]] = []
        self.gastou_by_deputy_id_rels: list[dict[str, Any]] = []
        self.forneceu_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        camara_dir = Path(self.data_dir) / "camara"
        csv_files = sorted(camara_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", camara_dir)
            return

        frames: list[pd.DataFrame] = []
        for f in csv_files:
            df = pd.read_csv(
                f,
                sep=";",
                dtype=str,
                encoding="utf-8-sig",
                keep_default_na=False,
            )
            frames.append(df)
            logger.info("  Loaded %d rows from %s", len(df), f.name)

        self._raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        logger.info("Total raw rows: %d", len(self._raw))

    def transform(self) -> None:
        if self._raw.empty:
            return

        expenses: list[dict[str, Any]] = []
        deputies_map: dict[str, dict[str, Any]] = {}
        deputies_by_id_map: dict[str, dict[str, Any]] = {}
        suppliers_map: dict[str, dict[str, Any]] = {}
        gastou: list[dict[str, Any]] = []
        gastou_by_deputy_id: list[dict[str, Any]] = []
        forneceu: list[dict[str, Any]] = []
        skipped = 0

        for _, row in self._raw.iterrows():
            deputy_name = normalize_name(str(row.get("txNomeParlamentar", "")))
            deputy_cpf_raw = str(row.get("cpf", "")).strip()
            deputy_id = str(row.get("nuDeputadoId", "")).strip()
            uf = str(row.get("sgUF", "")).strip()
            partido = str(row.get("sgPartido", "")).strip()

            supplier_doc_raw = str(row.get("txtCNPJCPF", ""))
            supplier_digits = strip_document(supplier_doc_raw)
            supplier_name = normalize_name(str(row.get("txtFornecedor", "")))

            # Must have supplier document and deputy ID
            if not supplier_digits or not deputy_id:
                skipped += 1
                continue

            # Format supplier document
            if len(supplier_digits) == 14:
                supplier_doc = format_cnpj(supplier_doc_raw)
            elif len(supplier_digits) == 11:
                supplier_doc = format_cpf(supplier_doc_raw)
            else:
                skipped += 1
                continue

            expense_type = str(row.get("txtDescricao", "")).strip()
            date = parse_date(str(row.get("datEmissao", "")))
            value = parse_brl_amount(row.get("vlrLiquido", ""))

            expense_id = _make_expense_id(deputy_id, date, supplier_doc, str(value))

            expenses.append({
                "expense_id": expense_id,
                "deputy_id": deputy_id,
                "type": expense_type,
                "supplier_doc": supplier_doc,
                "value": value,
                "date": date,
                "description": expense_type,
                "source": "camara",
            })

            # Track deputy — prefer CPF, fall back to deputy_id
            deputy_cpf_digits = strip_document(deputy_cpf_raw)
            if len(deputy_cpf_digits) == 11:
                deputy_cpf = format_cpf(deputy_cpf_raw)
                deputies_map[deputy_cpf] = {
                    "cpf": deputy_cpf,
                    "name": deputy_name,
                    "deputy_id": deputy_id,
                    "uf": uf,
                    "partido": partido,
                }
                gastou.append({
                    "source_key": deputy_cpf,
                    "target_key": expense_id,
                })
            elif deputy_id:
                deputies_by_id_map[deputy_id] = {
                    "deputy_id": deputy_id,
                    "name": deputy_name,
                    "uf": uf,
                    "partido": partido,
                }
                gastou_by_deputy_id.append({
                    "deputy_id": deputy_id,
                    "target_key": expense_id,
                })

            # Track supplier
            if len(supplier_digits) == 14:
                suppliers_map[supplier_doc] = {
                    "cnpj": supplier_doc,
                    "razao_social": supplier_name,
                }
                forneceu.append({
                    "source_key": supplier_doc,
                    "target_key": expense_id,
                })
            elif len(supplier_digits) == 11:
                # Individual supplier (CPF)
                suppliers_map[supplier_doc] = {
                    "cpf": supplier_doc,
                    "name": supplier_name,
                }
                forneceu.append({
                    "source_key": supplier_doc,
                    "target_key": expense_id,
                })

        self.expenses = deduplicate_rows(expenses, ["expense_id"])
        self.deputies = list(deputies_map.values())
        self.deputies_by_id = list(deputies_by_id_map.values())
        self.suppliers = list(suppliers_map.values())
        self.gastou_rels = gastou
        self.gastou_by_deputy_id_rels = gastou_by_deputy_id
        self.forneceu_rels = forneceu

        if self.limit:
            self.expenses = self.expenses[: self.limit]

        logger.info(
            "Transformed: %d expenses, %d deputies (CPF) + %d (deputy_id), "
            "%d suppliers (skipped %d)",
            len(self.expenses),
            len(self.deputies),
            len(self.deputies_by_id),
            len(self.suppliers),
            skipped,
        )

    def load(self) -> None:
        if not self.expenses:
            logger.warning("No expenses to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load Expense nodes (include deputy_id for linkage)
        expense_nodes = [
            {
                "expense_id": e["expense_id"],
                "deputy_id": e["deputy_id"],
                "type": e["type"],
                "value": e["value"],
                "date": e["date"],
                "description": e["description"],
                "source": e["source"],
            }
            for e in self.expenses
        ]
        count = loader.load_nodes("Expense", expense_nodes, key_field="expense_id")
        logger.info("Loaded %d Expense nodes", count)

        # Load/merge Person nodes for deputies (CPF-based)
        if self.deputies:
            count = loader.load_nodes("Person", self.deputies, key_field="cpf")
            logger.info("Merged %d deputy Person nodes (CPF)", count)

        # Load/merge Person nodes for deputies without CPF (deputy_id-based)
        if self.deputies_by_id:
            query = (
                "UNWIND $rows AS row "
                "MERGE (p:Person {deputy_id: row.deputy_id}) "
                "SET p.name = row.name, p.uf = row.uf, p.partido = row.partido"
            )
            count = loader.run_query(query, self.deputies_by_id)
            logger.info("Merged %d deputy Person nodes (deputy_id)", count)

        # Load/merge Company nodes for CNPJ suppliers
        company_suppliers = [s for s in self.suppliers if "cnpj" in s]
        if company_suppliers:
            count = loader.load_nodes("Company", company_suppliers, key_field="cnpj")
            logger.info("Merged %d supplier Company nodes", count)

        # Load/merge Person nodes for CPF suppliers
        person_suppliers = [s for s in self.suppliers if "cpf" in s]
        if person_suppliers:
            count = loader.load_nodes("Person", person_suppliers, key_field="cpf")
            logger.info("Merged %d supplier Person nodes", count)

        # GASTOU: Person -> Expense (CPF-based)
        if self.gastou_rels:
            count = loader.load_relationships(
                rel_type="GASTOU",
                rows=self.gastou_rels,
                source_label="Person",
                source_key="cpf",
                target_label="Expense",
                target_key="expense_id",
            )
            logger.info("Created %d GASTOU relationships (CPF)", count)

        # GASTOU: Person -> Expense (deputy_id-based, for CPF-less deputies)
        if self.gastou_by_deputy_id_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {deputy_id: row.deputy_id}) "
                "MATCH (e:Expense {expense_id: row.target_key}) "
                "MERGE (p)-[:GASTOU]->(e)"
            )
            count = loader.run_query(query, self.gastou_by_deputy_id_rels)
            logger.info("Created %d GASTOU relationships (deputy_id)", count)

        # FORNECEU: Company/Person -> Expense
        if self.forneceu_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (e:Expense {expense_id: row.target_key}) "
                "OPTIONAL MATCH (c:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (p:Person {cpf: row.source_key}) "
                "WITH e, coalesce(c, p) AS supplier "
                "WHERE supplier IS NOT NULL "
                "MERGE (supplier)-[:FORNECEU]->(e)"
            )
            count = loader.run_query(query, self.forneceu_rels)
            logger.info("Created %d FORNECEU relationships", count)
