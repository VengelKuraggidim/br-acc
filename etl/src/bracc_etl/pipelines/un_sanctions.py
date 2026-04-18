from __future__ import annotations

import hashlib
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
)

logger = logging.getLogger(__name__)

VALID_ENTITY_TYPES = {"individual", "entity"}

# --------------------------------------------------------------------------
# Module-level constants + fetch_to_disk (public bulk feed).
# --------------------------------------------------------------------------
#
# The UN Security Council publishes its Consolidated Sanctions List as a
# single XML file at scsanctions.un.org. The endpoint is open (no auth,
# no token) and stable; it is refreshed whenever the 1267, 1988, 1718,
# 1970 and related committees update the list.
#
# Schema (truncated)::
#
#   <CONSOLIDATED_LIST dateGenerated="...">
#     <INDIVIDUALS>
#       <INDIVIDUAL>
#         <DATAID/>
#         <FIRST_NAME/><SECOND_NAME/><THIRD_NAME/><FOURTH_NAME/>
#         <UN_LIST_TYPE/>
#         <REFERENCE_NUMBER/>
#         <LISTED_ON/>
#         <NATIONALITY><VALUE/></NATIONALITY>
#         <INDIVIDUAL_ALIAS><ALIAS_NAME/></INDIVIDUAL_ALIAS>
#         ...
#       </INDIVIDUAL>
#     </INDIVIDUALS>
#     <ENTITIES>
#       <ENTITY>
#         <FIRST_NAME/>        <!-- the entity name lives here too -->
#         <UN_LIST_TYPE/><REFERENCE_NUMBER/><LISTED_ON/>
#         <ENTITY_ALIAS><ALIAS_NAME/></ENTITY_ALIAS>
#         ...
#       </ENTITY>
#     </ENTITIES>
#   </CONSOLIDATED_LIST>
#
# ``UnSanctionsPipeline.extract()`` reads a flat JSON array with the
# fields below, so ``fetch_to_disk`` projects the XML onto that shape::
#
#   { "reference_number", "entity_type" ("individual"|"entity"),
#     "name", "aliases" [], "listed_date", "un_list_type",
#     "nationality" }

UN_CONSOLIDATED_URL = (
    "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
)


def _generate_sanction_id(reference_number: str, name: str) -> str:
    """Generate a deterministic 16-char hex ID from reference_number + name."""
    raw = f"{reference_number}|{name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _text(node: ET.Element | None, path: str = "") -> str:
    """Safe ``.find(path).text`` that always returns a stripped string."""
    if node is None:
        return ""
    target = node.find(path) if path else node
    if target is None or target.text is None:
        return ""
    return target.text.strip()


def _join_name_parts(node: ET.Element, *tags: str) -> str:
    """Concatenate the given child tags into a single space-separated name."""
    parts = [_text(node.find(t)) for t in tags]
    return " ".join(p for p in parts if p).strip()


def _extract_aliases(node: ET.Element, alias_tag: str) -> list[str]:
    """Collect non-empty ALIAS_NAME values from the alias blocks."""
    out: list[str] = []
    for alias in node.findall(alias_tag):
        name = _text(alias.find("ALIAS_NAME"))
        if name:
            out.append(name)
    return out


def _individual_record(node: ET.Element) -> dict[str, Any] | None:
    reference_number = _text(node.find("REFERENCE_NUMBER"))
    if not reference_number:
        return None
    name = _join_name_parts(
        node, "FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME",
    )
    if not name:
        return None
    return {
        "reference_number": reference_number,
        "entity_type": "individual",
        "name": name,
        "aliases": _extract_aliases(node, "INDIVIDUAL_ALIAS"),
        "listed_date": _text(node.find("LISTED_ON")),
        "un_list_type": _text(node.find("UN_LIST_TYPE")),
        "nationality": _text(node.find("NATIONALITY/VALUE")),
    }


def _entity_record(node: ET.Element) -> dict[str, Any] | None:
    reference_number = _text(node.find("REFERENCE_NUMBER"))
    if not reference_number:
        return None
    # ENTITIES use FIRST_NAME as the official entity name; no multi-part
    # split like individuals have.
    name = _text(node.find("FIRST_NAME"))
    if not name:
        return None
    return {
        "reference_number": reference_number,
        "entity_type": "entity",
        "name": name,
        "aliases": _extract_aliases(node, "ENTITY_ALIAS"),
        "listed_date": _text(node.find("LISTED_ON")),
        "un_list_type": _text(node.find("UN_LIST_TYPE")),
        "nationality": "",
    }


def fetch_to_disk(
    output_dir: Path,
    url: str = UN_CONSOLIDATED_URL,
    limit: int | None = None,
    timeout: float = 60.0,
) -> list[Path]:
    """Download the UN Consolidated Sanctions List and project it to JSON.

    Writes two files to ``output_dir``:

    * ``un_sanctions.xml`` — raw upstream XML (kept verbatim for audit).
    * ``un_sanctions.json`` — flat JSON array in the schema
      :class:`UnSanctionsPipeline` expects.

    Parameters
    ----------
    output_dir:
        Destination. Created if missing.
    url:
        Override for the upstream XML endpoint (default: UN SC bulk feed).
    limit:
        If set, truncate the combined individual+entity list to the first
        N records. Applied after the XML is parsed so the raw XML is
        always written in full.
    timeout:
        httpx request timeout in seconds.

    Returns
    -------
    List of absolute paths written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("[un_sanctions.fetch_to_disk] GET %s", url)
    with httpx.Client(
        follow_redirects=True,
        headers={
            "User-Agent": "br-acc/bracc-etl download_un_sanctions (httpx)",
        },
    ) as client:
        resp = client.get(url, timeout=timeout)
        resp.raise_for_status()
        xml_bytes = resp.content

    xml_path = output_dir / "un_sanctions.xml"
    xml_path.write_bytes(xml_bytes)
    logger.info(
        "[un_sanctions.fetch_to_disk] wrote %s (%d bytes)",
        xml_path, len(xml_bytes),
    )

    root = ET.fromstring(xml_bytes)

    records: list[dict[str, Any]] = []
    individuals_node = root.find("INDIVIDUALS")
    if individuals_node is not None:
        for ind in individuals_node.findall("INDIVIDUAL"):
            rec = _individual_record(ind)
            if rec is not None:
                records.append(rec)
    entities_node = root.find("ENTITIES")
    if entities_node is not None:
        for ent in entities_node.findall("ENTITY"):
            rec = _entity_record(ent)
            if rec is not None:
                records.append(rec)

    n_individuals = sum(1 for r in records if r["entity_type"] == "individual")
    n_entities = sum(1 for r in records if r["entity_type"] == "entity")
    logger.info(
        "[un_sanctions.fetch_to_disk] parsed %d individuals + %d entities "
        "= %d total records",
        n_individuals, n_entities, len(records),
    )

    if limit is not None:
        records = records[:limit]
        logger.info(
            "[un_sanctions.fetch_to_disk] truncated to --limit=%d records",
            limit,
        )

    json_path = output_dir / "un_sanctions.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    logger.info(
        "[un_sanctions.fetch_to_disk] wrote %s (%d records)",
        json_path, len(records),
    )

    return [xml_path.resolve(), json_path.resolve()]


class UnSanctionsPipeline(Pipeline):
    """ETL pipeline for UN Security Council consolidated sanctions list.

    Loads all INDIVIDUAL and ENTITY entries as InternationalSanction nodes.
    Name-based matching to existing Person/Company nodes is attempted
    via MERGE on normalized name.
    """

    name = "un_sanctions"
    source_id = "un_sanctions"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: list[dict[str, Any]] = []
        self.sanctions: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        un_dir = Path(self.data_dir) / "un_sanctions"
        json_path = un_dir / "un_sanctions.json"

        if not json_path.exists():
            logger.warning("[un_sanctions] un_sanctions.json not found at %s", json_path)
            return

        logger.info("[un_sanctions] Reading %s", json_path)
        with open(json_path, encoding="utf-8") as f:
            self._raw = json.load(f)

        if self.limit:
            self._raw = self._raw[: self.limit]

        logger.info("[un_sanctions] Extracted %d entries", len(self._raw))

    def transform(self) -> None:
        sanctions: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for entry in self._raw:
            reference_number = str(entry.get("reference_number", "")).strip()
            if not reference_number:
                continue

            entity_type = str(entry.get("entity_type", "")).strip().lower()
            if entity_type not in VALID_ENTITY_TYPES:
                continue

            name_raw = str(entry.get("name", "")).strip()
            if not name_raw:
                continue

            name_normalized = normalize_name(name_raw)
            sanction_id = _generate_sanction_id(reference_number, name_raw)

            sanction: dict[str, Any] = {
                "sanction_id": sanction_id,
                "name": name_normalized,
                "original_name": name_raw,
                "entity_type": entity_type,
                "reference_number": reference_number,
                "listed_date": str(entry.get("listed_date", "")).strip(),
                "un_list_type": str(entry.get("un_list_type", "")).strip(),
                "nationality": str(entry.get("nationality", "")).strip(),
                "source": "un_sanctions",
                "source_list": "UN",
            }

            aliases = entry.get("aliases", [])
            if aliases:
                sanction["aliases"] = "|".join(str(a) for a in aliases)

            sanctions.append(sanction)

            # Build name-match relationships
            if entity_type == "individual":
                person_rels.append({
                    "source_key": sanction_id,
                    "target_key": name_normalized,
                })
            elif entity_type == "entity":
                company_rels.append({
                    "source_key": sanction_id,
                    "target_key": name_normalized,
                })

        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])

        # Filter rels to only include sanctions that survived dedup
        valid_ids = {s["sanction_id"] for s in self.sanctions}
        self.person_rels = [r for r in person_rels if r["source_key"] in valid_ids]
        self.company_rels = [r for r in company_rels if r["source_key"] in valid_ids]

        logger.info(
            "[un_sanctions] Transformed %d InternationalSanction nodes "
            "(%d person matches, %d company matches)",
            len(self.sanctions),
            len(self.person_rels),
            len(self.company_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.sanctions:
            loaded = loader.load_nodes(
                "InternationalSanction", self.sanctions, key_field="sanction_id"
            )
            logger.info("[un_sanctions] Loaded %d InternationalSanction nodes", loaded)

        if self.person_rels:
            person_query = (
                "UNWIND $rows AS row "
                "MATCH (s:InternationalSanction {sanction_id: row.source_key}) "
                "MATCH (p:Person {name: row.target_key}) "
                "MERGE (p)-[r:UN_SANCTIONED]->(s) "
                "SET r.matched_by = 'name'"
            )
            loaded = loader.run_query_with_retry(person_query, self.person_rels)
            logger.info("[un_sanctions] Created %d Person UN_SANCTIONED rels", loaded)

        if self.company_rels:
            company_query = (
                "UNWIND $rows AS row "
                "MATCH (s:InternationalSanction {sanction_id: row.source_key}) "
                "MATCH (c:Company {razao_social: row.target_key}) "
                "MERGE (c)-[r:UN_SANCTIONED]->(s) "
                "SET r.matched_by = 'name'"
            )
            loaded = loader.run_query_with_retry(company_query, self.company_rels)
            logger.info("[un_sanctions] Created %d Company UN_SANCTIONED rels", loaded)
