from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    strip_document,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Module-level constants + fetch_to_disk (public bulk download).
# --------------------------------------------------------------------------
#
# OpenSanctions publishes its collections as FollowTheMoney JSONL bulk
# feeds under https://data.opensanctions.org/datasets/latest/<dataset>/.
# No authentication required.
#
# The `default` collection aggregates every dataset (~2.7 GB) and is too
# heavy to ship in a smoke-test / demo bootstrap. Since OpenSanctionsPipeline
# hard-filters to Brazilian-connected entities (_is_brazilian_entity), the
# pragmatic default for the br/acc fork is the Brazil-scoped `br_pep`
# dataset (~110 MB, 250k entities). Callers can override with --dataset
# to pull any other OpenSanctions collection (e.g. `peps`, `sanctions`,
# `default`).

OPENSANCTIONS_BASE = "https://data.opensanctions.org/datasets/latest"
OPENSANCTIONS_DEFAULT_DATASET = "br_pep"
# Upper bound on entities written when caller passes no explicit --limit.
# Keeps smoke-runs snappy while still exercising the FtM JSONL parser at
# realistic volume; operators who want the full dataset pass --limit 0.
OPENSANCTIONS_DEFAULT_LIMIT = 50_000


def _dataset_ftm_url(dataset: str, base: str = OPENSANCTIONS_BASE) -> str:
    return f"{base}/{dataset}/entities.ftm.json"


def fetch_to_disk(
    output_dir: Path,
    dataset: str = OPENSANCTIONS_DEFAULT_DATASET,
    limit: int | None = OPENSANCTIONS_DEFAULT_LIMIT,
    url: str | None = None,
    timeout: float = 300.0,
    base: str = OPENSANCTIONS_BASE,
) -> list[Path]:
    """Download OpenSanctions FtM JSONL to ``output_dir/entities.ftm.json``.

    OpenSanctionsPipeline.extract() reads one JSON object per line, so
    the file is streamed line-by-line; a ``limit`` caps the number of
    lines kept (applied before any entity-level filtering so caller
    always sees deterministic byte counts).

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    dataset:
        OpenSanctions dataset slug. Defaults to ``br_pep`` (Brazilian
        PEPs only, ~110 MB) since the pipeline discards non-Brazilian
        entities. Pass ``default``, ``peps``, etc. for wider scopes.
    limit:
        Max number of JSONL lines to write. ``None`` or ``0`` keeps the
        full stream (multi-GB for the aggregate ``default`` dataset).
        Defaults to 50 000 lines — enough to smoke-test the pipeline
        while keeping disk/network modest.
    url:
        Explicit URL override. When set, supersedes ``dataset`` / ``base``.
    timeout:
        HTTP timeout in seconds.
    base:
        Override OpenSanctions base URL (default: OPENSANCTIONS_BASE).

    Returns
    -------
    List with the absolute path of the JSONL file written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ftm_path = output_dir / "entities.ftm.json"
    source_url = url or _dataset_ftm_url(dataset, base=base)

    # Treat 0 / negative limit as "no cap" for CLI ergonomics.
    effective_limit: int | None = limit if limit and limit > 0 else None

    logger.info(
        "[opensanctions.fetch_to_disk] GET %s (dataset=%s, limit=%s)",
        source_url, dataset, effective_limit,
    )

    written_lines = 0
    bytes_written = 0
    with httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "br-acc/bracc-etl download_opensanctions (httpx)"},
    ) as client, client.stream("GET", source_url, timeout=timeout) as resp:
        resp.raise_for_status()
        with open(ftm_path, "wb") as fh:
            buf = b""
            done = False
            for chunk in resp.iter_bytes(chunk_size=65_536):
                if not chunk:
                    continue
                buf += chunk
                # Only split on newlines so partial JSON objects never hit disk.
                while True:
                    nl = buf.find(b"\n")
                    if nl < 0:
                        break
                    line = buf[: nl + 1]
                    buf = buf[nl + 1 :]
                    fh.write(line)
                    bytes_written += len(line)
                    written_lines += 1
                    if (
                        effective_limit is not None
                        and written_lines >= effective_limit
                    ):
                        done = True
                        break
                if done:
                    break
            # Flush any trailing partial line only when we are NOT in limit
            # mode — a half-line would break JSONL parsing downstream.
            if not done and buf:
                fh.write(buf)
                bytes_written += len(buf)
                if not buf.endswith(b"\n"):
                    written_lines += 1

    logger.info(
        "[opensanctions.fetch_to_disk] wrote %d lines (%d bytes) to %s",
        written_lines, bytes_written, ftm_path,
    )

    return [ftm_path.resolve()]

# Brazilian-related terms for filtering
BRAZIL_COUNTRY_CODES = {"br", "bra", "brazil", "brasil"}
BRAZIL_POSITION_TERMS = {
    "brasil", "brazil", "brasileiro", "brasileira",
    "deputado", "senador", "governador", "prefeito", "vereador",
    "ministro", "secretario", "presidente da republica",
}

# Confidence threshold for CPF-based matching (name matching in link_global_peps.cypher)
EXACT_CPF_MATCH = 1.0


def _is_brazilian_entity(entity: dict[str, Any]) -> bool:
    """Check if a FtM entity has Brazilian connections."""
    props = entity.get("properties", {})

    # Check country field
    countries = props.get("country", [])
    for c in countries:
        if c.lower() in BRAZIL_COUNTRY_CODES:
            return True

    # Check nationality
    nationalities = props.get("nationality", [])
    for n in nationalities:
        if n.lower() in BRAZIL_COUNTRY_CODES:
            return True

    # Check position for Brazilian government roles
    positions = props.get("position", [])
    for pos in positions:
        pos_lower = pos.lower()
        for term in BRAZIL_POSITION_TERMS:
            if term in pos_lower:
                return True

    return False


def _extract_cpf(entity: dict[str, Any]) -> str | None:
    """Extract CPF from FtM taxNumber property."""
    props = entity.get("properties", {})
    tax_numbers = props.get("taxNumber", [])
    for tn in tax_numbers:
        digits = strip_document(tn)
        if len(digits) == 11:
            return format_cpf(digits)
    return None


class OpenSanctionsPipeline(Pipeline):
    """ETL pipeline for OpenSanctions PEP data (FollowTheMoney format)."""

    name = "opensanctions"
    source_id = "opensanctions"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_entities: list[dict[str, Any]] = []
        self.global_peps: list[dict[str, Any]] = []
        self.pep_match_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        data_dir = Path(self.data_dir) / "opensanctions"
        ftm_path = data_dir / "entities.ftm.json"

        if not ftm_path.exists():
            logger.warning("[opensanctions] entities.ftm.json not found at %s", ftm_path)
            return

        entities: list[dict[str, Any]] = []
        with open(ftm_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entity = json.loads(line)
                    entities.append(entity)
                except json.JSONDecodeError:
                    continue

        self._raw_entities = entities
        logger.info("[opensanctions] Extracted %d raw entities", len(self._raw_entities))

    def _transform_peps(self) -> list[dict[str, Any]]:
        """Filter and transform Brazilian-connected PEP entities."""
        peps: list[dict[str, Any]] = []

        for entity in self._raw_entities:
            schema = entity.get("schema", "")
            if schema != "Person":
                continue

            if not _is_brazilian_entity(entity):
                continue

            entity_id = entity.get("id", "").strip()
            if not entity_id:
                continue

            props = entity.get("properties", {})
            names = props.get("name", [])
            if not names:
                continue

            primary_name = names[0]
            countries = props.get("country", [])
            positions = props.get("position", [])
            start_dates = props.get("startDate", [])
            end_dates = props.get("endDate", [])
            datasets = entity.get("datasets", [])

            cpf = _extract_cpf(entity)

            peps.append({
                "pep_id": f"os_{entity_id}",
                "name": normalize_name(primary_name),
                "original_name": primary_name,
                "country": countries[0] if countries else "",
                "position": positions[0] if positions else "",
                "all_positions": "; ".join(positions) if positions else "",
                "start_date": start_dates[0] if start_dates else "",
                "end_date": end_dates[0] if end_dates else "",
                "datasets": "; ".join(datasets) if datasets else "",
                "cpf": cpf or "",
                "source": "opensanctions",
            })

        return peps

    def _build_cpf_match_rels(self) -> list[dict[str, Any]]:
        """Build GLOBAL_PEP_MATCH relationships based on CPF."""
        rels: list[dict[str, Any]] = []
        for pep in self.global_peps:
            cpf = pep.get("cpf", "")
            if not cpf:
                continue
            rels.append({
                "source_key": cpf,
                "target_key": pep["pep_id"],
                "match_type": "cpf_exact",
                "confidence": EXACT_CPF_MATCH,
            })
        return rels

    def transform(self) -> None:
        self.global_peps = deduplicate_rows(self._transform_peps(), ["pep_id"])
        self.pep_match_rels = self._build_cpf_match_rels()

        logger.info(
            "[opensanctions] Transformed %d GlobalPEP nodes, %d CPF match relationships",
            len(self.global_peps),
            len(self.pep_match_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.global_peps:
            loaded = loader.load_nodes("GlobalPEP", self.global_peps, key_field="pep_id")
            logger.info("[opensanctions] Loaded %d GlobalPEP nodes", loaded)

        if self.pep_match_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (g:GlobalPEP {pep_id: row.target_key}) "
                "MERGE (p)-[r:GLOBAL_PEP_MATCH]->(g) "
                "SET r.match_type = row.match_type, "
                "    r.confidence = row.confidence"
            )
            loaded = loader.run_query_with_retry(query, self.pep_match_rels)
            logger.info("[opensanctions] Loaded %d GLOBAL_PEP_MATCH relationships", loaded)
