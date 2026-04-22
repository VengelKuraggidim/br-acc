import logging
from pathlib import Path
from typing import Any

from neo4j import AsyncDriver, AsyncSession, Record

from bracc.config import settings

logger = logging.getLogger(__name__)

QUERIES_DIR = Path(__file__).parent.parent / "queries"
# Gated federal-scope queries. Resolved as a fallback after QUERIES_DIR so
# the gated routers (bracc._federal.routers.*) can keep using
# execute_query(session, "public_company_lookup", ...) unchanged when
# ENABLE_FEDERAL_ROUTES=true mounts them. See docs/_federal_gating.md.
_FEDERAL_QUERIES_DIR = Path(__file__).parent.parent / "_federal" / "queries"


class CypherLoader:
    """Loads and caches .cypher query files."""

    _cache: dict[str, str] = {}

    @classmethod
    def load(cls, name: str) -> str:
        if name not in cls._cache:
            path = QUERIES_DIR / f"{name}.cypher"
            if not path.exists():
                federal_path = _FEDERAL_QUERIES_DIR / f"{name}.cypher"
                if federal_path.exists():
                    path = federal_path
                else:
                    msg = f"Query file not found: {path}"
                    raise FileNotFoundError(msg)
            cls._cache[name] = path.read_text().strip()
        return cls._cache[name]

    @classmethod
    def clear_cache(cls) -> None:
        cls._cache.clear()


async def execute_query(
    session: AsyncSession,
    query_name: str,
    parameters: dict[str, Any] | None = None,
    timeout: float = 15,
) -> list[Record]:
    """Execute a named .cypher query with parameter binding."""
    cypher = CypherLoader.load(query_name)
    result = await session.run(cypher, parameters or {}, timeout=timeout)
    return [record async for record in result]


async def execute_query_single(
    session: AsyncSession,
    query_name: str,
    parameters: dict[str, Any] | None = None,
    timeout: float = 15,
) -> Record | None:
    """Execute a named query and return a single record."""
    cypher = CypherLoader.load(query_name)
    result = await session.run(cypher, parameters or {}, timeout=timeout)
    return await result.single()


def sanitize_props(
    props: dict[str, Any],
) -> dict[str, str | float | int | bool | None]:
    """Flatten Neo4j node/rel properties to JSON-safe scalar values.

    Neo4j can return lists, dicts, and temporal types in node properties.
    This converts them to strings so the API contract
    (dict[str, str | float | int | bool | None]) is honoured.
    """
    clean: dict[str, str | float | int | bool | None] = {}
    for k, v in props.items():
        if v is None or isinstance(v, (str, int, float, bool)):
            clean[k] = v
        elif isinstance(v, list):
            clean[k] = ", ".join(str(item) for item in v)
        else:
            # Neo4j Date, DateTime, Duration, dict, etc.
            clean[k] = str(v)
    return clean


_ENTITY_SEARCH_ANALYZER = "standard-folding"


async def _migrate_entity_search_analyzer(session: AsyncSession) -> None:
    """Drop entity_search if its analyzer diverges from the expected one.

    CREATE FULLTEXT INDEX ... IF NOT EXISTS skips when the index already
    exists, even if the analyzer config is outdated. Dropping first lets the
    schema_init CREATE recreate it with the current analyzer. Neo4j rebuilds
    fulltext indexes from existing nodes, so no data is lost.
    """
    result = await session.run(
        "SHOW FULLTEXT INDEXES YIELD name, options "
        "WHERE name = 'entity_search' RETURN options AS options"
    )
    record = await result.single()
    if record is None:
        return
    options = record["options"] or {}
    index_config = options.get("indexConfig", {}) if isinstance(options, dict) else {}
    current = index_config.get("fulltext.analyzer")
    if current == _ENTITY_SEARCH_ANALYZER:
        return
    logger.info(
        "Rebuilding entity_search fulltext index: analyzer %r -> %r",
        current, _ENTITY_SEARCH_ANALYZER,
    )
    await session.run("DROP INDEX entity_search")


async def ensure_schema(driver: AsyncDriver) -> None:
    """Run schema_init.cypher statements on startup. All use IF NOT EXISTS so idempotent."""
    raw = CypherLoader.load("schema_init")
    # Strip ``//`` comment lines BEFORE splitting on ``;`` — comments podem
    # conter ``;`` ("duplicates; promote to") e quebrariam o split nativo,
    # gerando pseudo-statements como "promote to\nCREATE INDEX ..." que
    # o Neo4j rejeita com CypherSyntaxError.
    code_lines = [
        ln for ln in raw.splitlines() if not ln.strip().startswith("//")
    ]
    code = "\n".join(code_lines)
    statements = [s.strip() for s in code.split(";") if s.strip()]
    async with driver.session(database=settings.neo4j_database) as session:
        await _migrate_entity_search_analyzer(session)
        for stmt in statements:
            await session.run(stmt)
    logger.info("Schema bootstrap complete: %d statements executed", len(statements))
