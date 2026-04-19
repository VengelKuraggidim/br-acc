"""ETL pipeline for Senado Federal CEAPS expense data.

Ingests CEAPS (Cota para o Exercicio da Atividade Parlamentar dos Senadores)
expenses. Creates Expense nodes linked to Person (senator) via GASTOU
and to Company (supplier) via FORNECEU.

Senator identity enrichment: loads parlamentares.json (from Dados Abertos API)
to map parliamentary names to CPFs for deterministic matching.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

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


def _make_expense_id(senator_name: str, date: str, supplier_doc: str, value: str) -> str:
    """Generate a stable expense ID from key fields."""
    raw = f"senado_{senator_name}_{date}_{supplier_doc}_{value}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class SenadoPipeline(Pipeline):
    """ETL pipeline for Senado Federal CEAPS expenses."""

    name = "senado"
    source_id = "senado"

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
        self._senator_lookup: dict[str, dict[str, str]] = {}
        self.expenses: list[dict[str, Any]] = []
        self.suppliers: list[dict[str, Any]] = []
        self.gastou_rels: list[dict[str, Any]] = []
        self.gastou_by_name_rels: list[dict[str, Any]] = []
        self.forneceu_rels: list[dict[str, Any]] = []

    def _load_senator_lookup(self) -> dict[str, dict[str, str]]:
        """Load senator identity lookup from parlamentares.json.

        Returns a dict mapping normalized parliamentary name to senator info
        (cpf, codigo, nome_completo).
        """
        lookup_path = Path(self.data_dir) / "senado" / "parlamentares.json"
        if not lookup_path.exists():
            logger.info("No parlamentares.json found — senator CPF enrichment disabled")
            return {}

        with open(lookup_path, encoding="utf-8") as f:
            senators = json.load(f)

        lookup: dict[str, dict[str, str]] = {}
        for s in senators:
            nome = normalize_name(s.get("nome_parlamentar", ""))
            if nome:
                lookup[nome] = {
                    "cpf": s.get("cpf", ""),
                    "codigo": s.get("codigo", ""),
                    "nome_completo": s.get("nome_completo", ""),
                }
            # Also index by full civil name for broader matching
            nome_completo = normalize_name(s.get("nome_completo", ""))
            if nome_completo and nome_completo != nome:
                lookup[nome_completo] = {
                    "cpf": s.get("cpf", ""),
                    "codigo": s.get("codigo", ""),
                    "nome_completo": s.get("nome_completo", ""),
                }

        logger.info(
            "Loaded senator lookup: %d entries (%d with CPF)",
            len(lookup),
            sum(1 for v in lookup.values() if v["cpf"]),
        )
        return lookup

    def extract(self) -> None:
        senado_dir = Path(self.data_dir) / "senado"

        # Load senator identity lookup for CPF enrichment
        self._senator_lookup = self._load_senator_lookup()

        csv_files = sorted(senado_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", senado_dir)
            return

        frames: list[pd.DataFrame] = []
        for f in csv_files:
            df = pd.read_csv(
                f,
                sep=";",
                dtype=str,
                encoding="latin-1",
                keep_default_na=False,
                skiprows=1,
            )
            frames.append(df)
            logger.info("  Loaded %d rows from %s", len(df), f.name)

        self._raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self.rows_in = len(self._raw)
        logger.info("Total raw rows: %d", len(self._raw))

    def transform(self) -> None:
        if self._raw.empty:
            return

        expenses: list[dict[str, Any]] = []
        suppliers_map: dict[str, dict[str, Any]] = {}
        gastou: list[dict[str, Any]] = []
        gastou_by_name: list[dict[str, Any]] = []
        forneceu: list[dict[str, Any]] = []
        skipped = 0

        for _, row in self._raw.iterrows():
            senator_name = normalize_name(str(row.get("SENADOR", "")))
            expense_type = str(row.get("TIPO_DESPESA", "")).strip()

            supplier_doc_raw = str(row.get("CNPJ_CPF", ""))
            supplier_digits = strip_document(supplier_doc_raw)
            supplier_name = normalize_name(str(row.get("FORNECEDOR", "")))

            if not supplier_digits:
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

            date = parse_date(str(row.get("DATA", "")))
            value = parse_brl_amount(row.get("VALOR_REEMBOLSADO", ""))
            documento = str(row.get("DOCUMENTO", "")).strip()
            detalhamento = str(row.get("DETALHAMENTO", "")).strip()

            expense_id = _make_expense_id(senator_name, date, supplier_doc, str(value))

            expenses.append({
                "expense_id": expense_id,
                "senator_name": senator_name,
                "type": expense_type,
                "supplier_doc": supplier_doc,
                "value": value,
                "date": date,
                "description": detalhamento or expense_type,
                "documento": documento,
                "source": "senado",
            })

            # Track senator -> expense (CPF-first, name fallback)
            senator_info = self._senator_lookup.get(senator_name, {})
            senator_cpf_raw = senator_info.get("cpf", "")
            senator_cpf_digits = strip_document(senator_cpf_raw)
            if len(senator_cpf_digits) == 11:
                senator_cpf = format_cpf(senator_cpf_raw)
                gastou.append({
                    "source_key": senator_cpf,
                    "target_key": expense_id,
                })
            elif senator_name:
                gastou_by_name.append({
                    "senator_name": senator_name,
                    "target_key": expense_id,
                })

            # Track supplier
            if len(supplier_digits) == 14:
                suppliers_map[supplier_doc] = {
                    "cnpj": supplier_doc,
                    "razao_social": supplier_name,
                }
            elif len(supplier_digits) == 11:
                suppliers_map[supplier_doc] = {
                    "cpf": supplier_doc,
                    "name": supplier_name,
                }

            forneceu.append({
                "source_key": supplier_doc,
                "target_key": expense_id,
            })

        self.expenses = deduplicate_rows(expenses, ["expense_id"])
        self.suppliers = list(suppliers_map.values())
        self.gastou_rels = gastou
        self.gastou_by_name_rels = gastou_by_name
        self.forneceu_rels = forneceu

        if self.limit:
            self.expenses = self.expenses[: self.limit]

        logger.info(
            "Transformed: %d expenses, %d suppliers, "
            "%d GASTOU (CPF) + %d GASTOU (name) (skipped %d)",
            len(self.expenses),
            len(self.suppliers),
            len(self.gastou_rels),
            len(self.gastou_by_name_rels),
            skipped,
        )

    def load(self) -> None:
        if not self.expenses:
            logger.warning("No expenses to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load Expense nodes
        expense_nodes = [
            {
                "expense_id": e["expense_id"],
                "type": e["type"],
                "value": e["value"],
                "date": e["date"],
                "description": e["description"],
                "source": e["source"],
            }
            for e in self.expenses
        ]
        count = loader.load_nodes("Expense", expense_nodes, key_field="expense_id")
        self.rows_loaded += count
        logger.info("Loaded %d Expense nodes", count)

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

        # GASTOU: Person (senator) -> Expense
        # Tier 1: CPF-based (from senator lookup enrichment)
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

        # Tier 2: Name-based (no CANDIDATO_EM filter — matches suplentes and
        # pre-2002 senators who lack TSE candidacy records)
        if self.gastou_by_name_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (e:Expense {expense_id: row.target_key}) "
                "MATCH (p:Person {name: row.senator_name}) "
                "MERGE (p)-[:GASTOU]->(e)"
            )
            count = loader.run_query(query, self.gastou_by_name_rels)
            logger.info("Created %d GASTOU relationships (name)", count)

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


# ────────────────────────────────────────────────────────────────────
# Acquisition helper — UF-scoped Senado download for Fiscal Cidadao
# ────────────────────────────────────────────────────────────────────
#
# Sources:
#   - Senado Dados Abertos API (https://legis.senado.leg.br/dadosabertos)
#     endpoints:
#       * /senador/lista/legislatura/{N}?uf={UF}  — roster per legislature
#       * /senador/{codigo}                        — identificacao + dados basicos
#       * /senador/{codigo}/mandatos               — mandate history
#   - CEAPS per-year CSV dump at
#     https://www.senado.leg.br/transparencia/LAI/verba/despesa_ceaps_{YYYY}.csv
#     (fed into ``SenadoPipeline.extract`` via the SENADOR column).
#
# This helper collects senators whose mandate UF matches the requested UF
# (default GO) across every legislature from 48 (1987-1991) to the current
# one. CEAPS CSVs are downloaded full and then client-side filtered to rows
# whose SENADOR column matches any collected GO senator — CEAPS itself has no
# UF column, so a senator-name join is the only available filter.

_SENADO_API_BASE = "https://legis.senado.leg.br/dadosabertos"
_CEAPS_CSV_BASE = "https://www.senado.leg.br/transparencia/LAI/verba/despesa_ceaps_{year}.csv"
_DEFAULT_LEGISLATURES = tuple(range(48, 58))  # 48 (1987) through 57 (2027)
# CEAPS dumps start in 2008; we default to Marconi-era windows (covers GO
# senators since legislatura 53). Callers can override with --years.
_DEFAULT_CEAPS_YEARS = tuple(range(2008, 2027))


def _http_get_json(url: str, *, timeout: float = 60.0, retries: int = 3) -> dict[str, Any] | None:
    """GET a JSON payload from the Senado API with basic retry/backoff."""
    import time as _time

    import httpx

    headers = {"Accept": "application/json"}
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                resp = client.get(url, headers=headers)
            if resp.status_code == 200:
                try:
                    payload: dict[str, Any] = resp.json()
                    return payload
                except ValueError as exc:  # malformed JSON
                    logger.warning("[senado] non-JSON response from %s: %s", url, exc)
                    return None
            if resp.status_code == 404:
                return None
            logger.warning(
                "[senado] HTTP %d for %s (attempt %d)",
                resp.status_code, url, attempt + 1,
            )
        except httpx.HTTPError as exc:
            last_exc = exc
            logger.warning("[senado] HTTP error on %s (attempt %d): %s", url, attempt + 1, exc)
        _time.sleep(2**attempt)
    if last_exc is not None:
        logger.warning("[senado] giving up on %s after %d retries", url, retries)
    return None


def _http_download(url: str, dest: Path, *, timeout: float = 600.0) -> Path | None:
    """Stream ``url`` into ``dest``. Returns the path on success, else None."""
    import httpx

    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=timeout) as resp:
            if resp.status_code != 200:
                logger.warning("[senado] HTTP %d for %s", resp.status_code, url)
                return None
            with open(dest, "wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=65_536):
                    fh.write(chunk)
    except httpx.HTTPError as exc:
        logger.warning("[senado] HTTP error %s: %s", url, exc)
        dest.unlink(missing_ok=True)
        return None
    return dest


def _parse_senator_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the nested ListaParlamentarLegislatura payload to plain dicts."""
    try:
        parlamentares = payload["ListaParlamentarLegislatura"]["Parlamentares"]["Parlamentar"]
    except (KeyError, TypeError):
        return []
    if isinstance(parlamentares, dict):
        parlamentares = [parlamentares]

    out: list[dict[str, Any]] = []
    for entry in parlamentares:
        ident = entry.get("IdentificacaoParlamentar", {}) or {}
        mandatos_raw = entry.get("Mandatos", {}).get("Mandato", [])
        if isinstance(mandatos_raw, dict):
            mandatos_raw = [mandatos_raw]
        mandate_ufs = {
            str(m.get("UfParlamentar", "")).strip().upper()
            for m in mandatos_raw
            if isinstance(m, dict)
        }
        out.append({
            "codigo": str(ident.get("CodigoParlamentar", "")).strip(),
            "nome_parlamentar": str(ident.get("NomeParlamentar", "")).strip(),
            "nome_completo": str(ident.get("NomeCompletoParlamentar", "")).strip(),
            "uf": str(ident.get("UfParlamentar", "")).strip().upper(),
            "partido": str(ident.get("SiglaPartidoParlamentar", "")).strip(),
            "sexo": str(ident.get("SexoParlamentar", "")).strip(),
            "mandate_ufs": sorted(u for u in mandate_ufs if u),
        })
    return out


def _parse_senator_detail(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract the enriched senator fields from the DetalheParlamentar payload."""
    try:
        p = payload["DetalheParlamentar"]["Parlamentar"]
    except (KeyError, TypeError):
        return {}
    ident = p.get("IdentificacaoParlamentar", {}) or {}
    dados = p.get("DadosBasicosParlamentar", {}) or {}
    return {
        "codigo": str(ident.get("CodigoParlamentar", "")).strip(),
        "nome_parlamentar": str(ident.get("NomeParlamentar", "")).strip(),
        "nome_completo": str(ident.get("NomeCompletoParlamentar", "")).strip(),
        "uf": str(ident.get("UfParlamentar", "")).strip().upper(),
        "partido": str(ident.get("SiglaPartidoParlamentar", "")).strip(),
        "sexo": str(ident.get("SexoParlamentar", "")).strip(),
        # CPF is no longer published by Dados Abertos — keep the key for forward
        # compatibility but expect "" in most cases.
        "cpf": str(dados.get("CpfParlamentar", "")).strip(),
        "data_nascimento": str(dados.get("DataNascimento", "")).strip(),
        "naturalidade": str(dados.get("Naturalidade", "")).strip(),
        "uf_naturalidade": str(dados.get("UfNaturalidade", "")).strip().upper(),
    }


def _parse_mandates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        mandatos = payload["MandatoParlamentar"]["Parlamentar"]["Mandatos"]["Mandato"]
    except (KeyError, TypeError):
        return []
    if isinstance(mandatos, dict):
        mandatos = [mandatos]
    out: list[dict[str, Any]] = []
    for m in mandatos:
        if not isinstance(m, dict):
            continue
        first_leg = m.get("PrimeiraLegislaturaDoMandato") or {}
        second_leg = m.get("SegundaLegislaturaDoMandato") or {}
        out.append({
            "codigo_mandato": str(m.get("CodigoMandato", "")).strip(),
            "uf": str(m.get("UfParlamentar", "")).strip().upper(),
            "participacao": str(m.get("DescricaoParticipacao", "")).strip(),
            "primeira_legislatura": str(first_leg.get("NumeroLegislatura", "")).strip(),
            "data_inicio": str(first_leg.get("DataInicio", "")).strip(),
            "data_fim": str(second_leg.get("DataFim") or first_leg.get("DataFim") or "").strip(),
        })
    return out


def fetch_to_disk(
    output_dir: Path,
    *,
    uf: str = "GO",
    limit: int | None = None,
    legislaturas: list[int] | None = None,
    years: list[int] | None = None,
    timeout: float = 60.0,
    fetch_details: bool = True,
    fetch_ceaps: bool = True,
    skip_existing: bool = True,
) -> list[Path]:
    """Download Senado data scoped to senators whose mandate UF matches ``uf``.

    Writes, under ``output_dir``:
      * ``parlamentares.json``  — enriched roster (matches the format
        ``SenadoPipeline._load_senator_lookup`` already consumes).
      * ``senadores_{uf}.json``  — raw per-legislature roster dump.
      * ``mandatos_{uf}.json``   — mandate history per senator.
      * ``despesa_ceaps_{YYYY}_{UF}.csv`` — CEAPS rows where SENADOR matches a
        roster entry (skipped when ``fetch_ceaps=False``).
      * ``raw/despesa_ceaps_{YYYY}.csv`` — cached full-year CSV (for reuse).

    ``limit`` caps the number of enriched senators probed (useful for smoke
    runs). ``legislaturas`` defaults to 48..current; ``years`` defaults to
    2008..current for CEAPS.
    """
    import time as _time

    uf_upper = uf.upper()
    legs = legislaturas or list(_DEFAULT_LEGISLATURES)
    ceaps_years = years or list(_DEFAULT_CEAPS_YEARS)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    # ------------------------------------------------------------------
    # 1) Collect UF-scoped senators across legislatures.
    # ------------------------------------------------------------------
    per_leg: dict[int, list[dict[str, Any]]] = {}
    unique: dict[str, dict[str, Any]] = {}  # keyed by codigo
    for leg in legs:
        url = f"{_SENADO_API_BASE}/senador/lista/legislatura/{leg}?uf={uf_upper}"
        logger.info("[senado] fetching legislature %d (uf=%s)", leg, uf_upper)
        payload = _http_get_json(url, timeout=timeout)
        if not payload:
            continue
        rows = _parse_senator_list(payload)
        # Defensive client-side filter (API already filters via ?uf=): keep a
        # senator only when the mandate roster or current UF matches.
        rows = [
            r for r in rows
            if r["uf"] == uf_upper or uf_upper in r["mandate_ufs"]
        ]
        per_leg[leg] = rows
        for r in rows:
            cod = r["codigo"]
            if not cod:
                continue
            existing = unique.setdefault(cod, {**r, "legislaturas": []})
            if leg not in existing["legislaturas"]:
                existing["legislaturas"].append(leg)
        _time.sleep(0.3)  # polite to the API

    if not unique:
        logger.warning("[senado] no senators found for uf=%s in legislatures %s", uf_upper, legs)
        return written

    logger.info("[senado] collected %d unique %s senators", len(unique), uf_upper)

    # Deterministic order (by codigo), so smoke runs with ``--limit`` are
    # reproducible and the roster used for CEAPS matching is stable.
    def _codigo_sort_key(s: dict[str, Any]) -> int:
        cod = str(s.get("codigo", ""))
        return int(cod) if cod.isdigit() else 0

    senators = sorted(unique.values(), key=_codigo_sort_key)

    # ``limit`` caps the per-senator enrichment (detail + mandatos API calls)
    # but NOT the roster used for CEAPS matching — otherwise a smoke run with
    # ``--limit 10`` would silently drop Marconi/Caiado/etc. from the filter.
    roster_for_enrichment = senators[:limit] if (limit and limit > 0) else senators

    # ------------------------------------------------------------------
    # 2) Enrich each senator via the detail endpoint + fetch mandates.
    # ------------------------------------------------------------------
    enriched: list[dict[str, Any]] = []
    mandates_all: list[dict[str, Any]] = []
    for idx, sen in enumerate(roster_for_enrichment, 1):
        codigo = sen["codigo"]
        entry: dict[str, Any] = {
            "codigo": codigo,
            "nome_parlamentar": sen.get("nome_parlamentar", ""),
            "nome_completo": sen.get("nome_completo", ""),
            "uf": sen.get("uf", uf_upper),
            "partido": sen.get("partido", ""),
            "legislaturas": sen.get("legislaturas", []),
            "cpf": "",
        }
        if fetch_details:
            detail = _http_get_json(
                f"{_SENADO_API_BASE}/senador/{codigo}", timeout=timeout,
            )
            if detail:
                parsed = _parse_senator_detail(detail)
                entry.update({k: v for k, v in parsed.items() if v})
            _time.sleep(0.25)

            mand = _http_get_json(
                f"{_SENADO_API_BASE}/senador/{codigo}/mandatos", timeout=timeout,
            )
            if mand:
                for m in _parse_mandates(mand):
                    mandates_all.append({"codigo": codigo, **m})
            _time.sleep(0.25)

        enriched.append(entry)
        if idx % 20 == 0:
            logger.info(
                "[senado] enriched %d/%d senators", idx, len(roster_for_enrichment),
            )

    # Senators that were collected but not enriched (because of ``limit``)
    # still contribute their un-enriched names to the roster used for CEAPS
    # filtering below — their nome_parlamentar/nome_completo already comes
    # from the lista/legislatura payload.
    enriched_codigos = {e["codigo"] for e in enriched}
    full_roster = enriched + [
        s for s in senators if s["codigo"] not in enriched_codigos
    ]

    # ------------------------------------------------------------------
    # 3) Persist senator metadata files.
    # ------------------------------------------------------------------
    parlamentares_path = output_dir / "parlamentares.json"
    parlamentares_path.write_text(
        json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    written.append(parlamentares_path)
    logger.info("[senado] wrote %d senators → %s", len(enriched), parlamentares_path)

    senadores_uf_path = output_dir / f"senadores_{uf_upper.lower()}.json"
    senadores_uf_path.write_text(
        json.dumps(per_leg, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    written.append(senadores_uf_path)

    if mandates_all:
        mandates_path = output_dir / f"mandatos_{uf_upper.lower()}.json"
        mandates_path.write_text(
            json.dumps(mandates_all, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        written.append(mandates_path)

    # ------------------------------------------------------------------
    # 4) Optionally download + filter CEAPS CSVs to the UF senator roster.
    # ------------------------------------------------------------------
    if not fetch_ceaps:
        return written

    # Build set of normalized senator names (both parliamentary + civil)
    # from the FULL roster so ``--limit`` doesn't shrink CEAPS matches.
    roster_names: set[str] = set()
    for s in full_roster:
        for key in ("nome_parlamentar", "nome_completo"):
            nm = normalize_name(str(s.get(key, "")))
            if nm:
                roster_names.add(nm)
    if not roster_names:
        logger.warning("[senado] empty roster — skipping CEAPS filter")
        return written

    for year in ceaps_years:
        raw_csv = raw_dir / f"despesa_ceaps_{year}.csv"
        if not (skip_existing and raw_csv.exists() and raw_csv.stat().st_size > 0):
            url = _CEAPS_CSV_BASE.format(year=year)
            logger.info("[senado] downloading %s", url)
            if _http_download(url, raw_csv, timeout=timeout) is None:
                continue

        try:
            df = pd.read_csv(
                raw_csv,
                sep=";",
                dtype=str,
                encoding="latin-1",
                keep_default_na=False,
                skiprows=1,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[senado] skip %s: %s", raw_csv.name, exc)
            continue

        if df.empty or "SENADOR" not in df.columns:
            logger.info("[senado] %s empty or no SENADOR column", raw_csv.name)
            continue

        norm_col = df["SENADOR"].map(lambda v: normalize_name(str(v)))
        mask = norm_col.isin(roster_names)
        filtered = df[mask]
        if filtered.empty:
            logger.info(
                "[senado] year=%d uf=%s: no CEAPS rows matched roster",
                year, uf_upper,
            )
            continue

        out_path = output_dir / f"despesa_ceaps_{year}_{uf_upper.lower()}.csv"
        # Write with ';' sep + latin-1 to stay compatible with the pipeline's
        # reader (see ``SenadoPipeline.extract``). We prepend the original
        # "ULTIMA ATUALIZACAO" preamble so the pipeline's ``skiprows=1`` still
        # works.
        with open(out_path, "w", encoding="latin-1", newline="") as fh:
            fh.write("ULTIMA ATUALIZACAO;FISCAL_CIDADAO_FILTERED\n")
            filtered.to_csv(fh, sep=";", index=False, encoding="latin-1")
        written.append(out_path)
        logger.info(
            "[senado] year=%d uf=%s rows=%d → %s",
            year, uf_upper, len(filtered), out_path.name,
        )

    return written
