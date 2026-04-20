"""ETL pipeline for PNCP (Portal Nacional de Contratações Públicas) data.

Ingests federal procurement contracts from the PNCP API JSON files.
Creates Contract nodes linked to Company nodes via VENCEU relationships.
Distinct from Transparência convênios — these are procurement contracts
(licitações, pregões, dispensas, inexigibilidades).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    cap_contract_value,
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    strip_document,
)

if TYPE_CHECKING:
    import io
    from collections.abc import Iterator

    from neo4j import Driver

logger = logging.getLogger(__name__)

_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_MAX_FUTURE_DAYS = 365

_STREAM_CHUNK = 1 << 16  # 64 KiB


def _stream_json_array(
    fh: io.TextIOBase, chunk_size: int = _STREAM_CHUNK,
) -> Iterator[Any]:
    """Yield elements from a text stream containing a JSON array.

    Uses ``json.JSONDecoder.raw_decode`` over a sliding buffer so files
    of arbitrary size can be iterated without loading them whole. The
    ``comprasnet`` consolidated year files peak at 3+ GB and parsing
    them via ``json.loads`` OOM-killed the pipeline (17 GB RSS on
    2026-04-19 before the kernel intervened).
    """
    decoder = json.JSONDecoder()
    buf = ""
    started = False

    def _refill() -> bool:
        nonlocal buf
        chunk = fh.read(chunk_size)
        if not chunk:
            return False
        buf += chunk
        return True

    while not started:
        buf = buf.lstrip()
        if buf.startswith("["):
            buf = buf[1:]
            started = True
            break
        if buf:
            raise ValueError(f"Expected JSON array, got {buf[:20]!r}")
        if not _refill():
            return

    while True:
        buf = buf.lstrip()
        while buf.startswith(","):
            buf = buf[1:].lstrip()
        if buf.startswith("]"):
            return
        if not buf:
            if not _refill():
                return
            continue
        try:
            obj, idx = decoder.raw_decode(buf)
        except json.JSONDecodeError:
            if not _refill():
                decoder.raw_decode(buf)
                return
            continue
        yield obj
        buf = buf[idx:]


def _sanitize_iso_date(raw_value: str) -> str:
    """Return ISO date if valid and not absurdly in the future, else empty."""
    candidate = raw_value.strip()[:10]
    if not _ISO_DATE_RE.fullmatch(candidate):
        return ""
    try:
        parsed = date.fromisoformat(candidate)
    except ValueError:
        return ""
    if parsed > date.today() + timedelta(days=_MAX_FUTURE_DAYS):
        return ""
    return candidate

# Map PNCP modalidade IDs to short labels
_MODALIDADE_MAP: dict[int, str] = {
    1: "leilao_eletronico",
    3: "concurso",
    5: "concorrencia",
    6: "pregao_eletronico",
    8: "dispensa",
    9: "inexigibilidade",
    11: "pre_qualificacao",
}


class ComprasnetPipeline(Pipeline):
    """ETL pipeline for PNCP federal procurement contracts."""

    name = "comprasnet"
    source_id = "comprasnet"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.contracts: list[dict[str, Any]] = []
        self.contract_nodes: list[dict[str, Any]] = []
        self.company_nodes: list[dict[str, Any]] = []
        self.venceu_rels: list[dict[str, Any]] = []
        self.referente_a_rels: list[dict[str, Any]] = []
        self._year_files: list[Path] = []

    def extract(self) -> None:
        """Enumerate per-year consolidated files; defer parsing to run().

        Parsing happens one year at a time in :meth:`run` so the 3+ GB
        2025 file (and equivalents in later years) never sits in memory
        at the same time as another year's records.
        """
        src_dir = Path(self.data_dir) / "comprasnet"
        self._year_files = sorted(src_dir.glob("*_contratos.json"))
        if not self._year_files:
            logger.warning("No PNCP JSON files found in %s", src_dir)

    def _extract_year(self, path: Path) -> None:
        """Stream a single consolidated year into ``self._raw_records``."""
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for rec in _stream_json_array(fh):
                records.append(rec)
        self._raw_records = records
        self.rows_in += len(records)
        logger.info("  Loaded %d records from %s", len(records), path.name)

    def _reset_year_state(self) -> None:
        """Drop per-year working sets so the next year starts clean."""
        self._raw_records = []
        self.contracts = []
        self.contract_nodes = []
        self.company_nodes = []
        self.venceu_rels = []
        self.referente_a_rels = []

    def transform(self) -> None:
        if not hasattr(self, "_raw_records"):
            return

        contracts: list[dict[str, Any]] = []
        skipped_no_cnpj = 0
        skipped_no_value = 0

        for rec in self._raw_records:
            # Extract supplier CNPJ
            ni_fornecedor = str(rec.get("niFornecedor", "")).strip()
            cnpj_digits = strip_document(ni_fornecedor)

            # Only process companies (PJ with 14-digit CNPJ)
            tipo_pessoa = str(rec.get("tipoPessoa", "")).strip()
            if tipo_pessoa != "PJ" or len(cnpj_digits) != 14:
                skipped_no_cnpj += 1
                continue

            # Skip zero-value contracts
            valor = rec.get("valorGlobal") or rec.get("valorInicial") or 0
            if not valor or float(valor) <= 0:
                skipped_no_value += 1
                continue

            cnpj = format_cnpj(ni_fornecedor)

            # Build stable contract ID from PNCP control number
            numero_controle = str(
                rec.get("numeroControlePNCP", "")
            ).strip()
            if not numero_controle:
                # Fallback: compose from org CNPJ + sequence
                org_cnpj = strip_document(
                    str(rec.get("orgaoEntidade", {}).get("cnpj", ""))
                )
                seq = rec.get("sequencialContrato", "")
                ano = rec.get("anoContrato", "")
                numero_controle = f"{org_cnpj}-{seq}-{ano}"

            bid_reference = str(
                rec.get("numeroControlePncpCompra")
                or rec.get("numeroControlePNCPCompra")
                or ""
            ).strip()

            # Extract contracting org info
            org = rec.get("orgaoEntidade", {})
            org_name = normalize_name(
                str(org.get("razaoSocial", ""))
            )

            # Contract type (Empenho, Contrato, etc.)
            tipo_contrato = rec.get("tipoContrato", {})
            tipo_nome = str(tipo_contrato.get("nome", "")) if tipo_contrato else ""

            # Dates
            data_assinatura = _sanitize_iso_date(str(rec.get("dataAssinatura", "")))
            data_fim = _sanitize_iso_date(str(rec.get("dataVigenciaFim", "")))

            # Supplier name
            razao_social = normalize_name(
                str(rec.get("nomeRazaoSocialFornecedor", ""))
            )

            contracts.append({
                "contract_id": numero_controle,
                "bid_id": bid_reference,
                "object": normalize_name(
                    str(rec.get("objetoContrato", ""))
                ),
                "value": cap_contract_value(float(valor)),
                "contracting_org": org_name,
                "date": data_assinatura,
                "date_end": data_fim,
                "cnpj": cnpj,
                "razao_social": razao_social,
                "tipo_contrato": tipo_nome,
                "source": "comprasnet",
            })

        self.contracts = deduplicate_rows(contracts, ["contract_id"])

        logger.info(
            "Transformed: %d contracts (skipped %d no-CNPJ, %d zero-value)",
            len(self.contracts),
            skipped_no_cnpj,
            skipped_no_value,
        )

        if self.limit:
            self.contracts = self.contracts[: self.limit]

        # Build provenance-stamped node and relationship collections.
        contract_nodes: list[dict[str, Any]] = []
        company_nodes: list[dict[str, Any]] = []
        venceu_rels: list[dict[str, Any]] = []
        referente_a_rels: list[dict[str, Any]] = []
        for c in self.contracts:
            contract_id = c["contract_id"]
            contract_nodes.append(self.attach_provenance(
                {
                    "contract_id": contract_id,
                    "object": c["object"],
                    "value": c["value"],
                    "contracting_org": c["contracting_org"],
                    "date": c["date"],
                    "date_end": c["date_end"],
                    "tipo_contrato": c["tipo_contrato"],
                    "source": c["source"],
                },
                record_id=contract_id,
            ))
            company_nodes.append(self.attach_provenance(
                {"cnpj": c["cnpj"], "razao_social": c["razao_social"]},
                record_id=c["cnpj"],
            ))
            venceu_rels.append(self.attach_provenance(
                {"source_key": c["cnpj"], "target_key": contract_id},
                record_id=contract_id,
            ))
            if c.get("bid_id"):
                referente_a_rels.append(self.attach_provenance(
                    {"source_key": contract_id, "target_key": c["bid_id"]},
                    record_id=contract_id,
                ))

        self.contract_nodes = contract_nodes
        self.company_nodes = deduplicate_rows(company_nodes, ["cnpj"])
        self.venceu_rels = venceu_rels
        self.referente_a_rels = referente_a_rels

    def load(self) -> None:
        if not self.contract_nodes:
            logger.warning("No contracts to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        count = loader.load_nodes(
            "Contract", self.contract_nodes, key_field="contract_id",
        )
        self.rows_loaded += count
        logger.info("Loaded %d Contract nodes", count)

        count = loader.load_nodes(
            "Company", self.company_nodes, key_field="cnpj",
        )
        logger.info("Merged %d Company nodes", count)

        count = loader.load_relationships(
            rel_type="VENCEU",
            rows=self.venceu_rels,
            source_label="Company",
            source_key="cnpj",
            target_label="Contract",
            target_key="contract_id",
        )
        logger.info("Created %d VENCEU relationships", count)

        count = loader.load_relationships(
            rel_type="REFERENTE_A",
            rows=self.referente_a_rels,
            source_label="Contract",
            source_key="contract_id",
            target_label="Bid",
            target_key="bid_id",
        )
        logger.info("Created %d REFERENTE_A relationships", count)

    def run(self) -> None:
        """Process each year file independently to bound peak memory.

        The base ``run()`` would call extract → transform → load once,
        which required ``_raw_records`` to hold every year's records at
        once. With 2019-2026 that crossed 17 GB RSS and was OOM-killed.
        This override runs the same three-phase DAG per consolidated
        year file, flushing to Neo4j and clearing state between years.
        The ``IngestionRun`` node still reflects a single logical run
        spanning all years.
        """
        started_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._upsert_ingestion_run(status="running", started_at=started_at)
        try:
            logger.info("[%s] Starting extraction...", self.name)
            self.extract()
            if not self._year_files:
                logger.info("[%s] No input files; nothing to do.", self.name)
            else:
                original_limit = self.limit
                limit_remaining: int | None = original_limit
                for year_file in self._year_files:
                    if limit_remaining is not None and limit_remaining <= 0:
                        break
                    logger.info(
                        "[%s] === Processing %s ===", self.name, year_file.name,
                    )
                    self._extract_year(year_file)
                    if limit_remaining is not None:
                        self.limit = limit_remaining
                    logger.info("[%s] Starting transformation...", self.name)
                    self.transform()
                    if limit_remaining is not None:
                        limit_remaining -= len(self.contracts)
                    logger.info("[%s] Starting load...", self.name)
                    self.load()
                    self._reset_year_state()
                self.limit = original_limit
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="loaded",
                started_at=started_at,
                finished_at=finished_at,
            )
            logger.info("[%s] Pipeline complete.", self.name)
        except Exception as exc:
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="quality_fail",
                started_at=started_at,
                finished_at=finished_at,
                error=str(exc)[:1000],
            )
            raise
