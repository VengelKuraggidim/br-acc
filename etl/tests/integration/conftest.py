from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

try:
    from neo4j import Driver, GraphDatabase
    from testcontainers.neo4j import Neo4jContainer  # type: ignore[import-untyped]

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False
    Neo4jContainer = None
    Driver = None  # type: ignore[assignment,misc]


@pytest.fixture(scope="session")
def neo4j_container() -> Generator[Neo4jContainer]:
    if not HAS_TESTCONTAINERS:
        pytest.skip("testcontainers not installed")
    container = Neo4jContainer("neo4j:5-community")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def neo4j_driver(neo4j_container: Neo4jContainer) -> Generator[Driver]:
    uri = neo4j_container.get_connection_url()
    driver = GraphDatabase.driver(uri, auth=("neo4j", neo4j_container.password))
    # Apply schema
    schema_path = Path(__file__).parent.parent.parent.parent / "infra" / "neo4j" / "init.cypher"
    if schema_path.exists():
        with driver.session() as session:
            for statement in schema_path.read_text().split(";"):
                stmt = statement.strip()
                if stmt and not stmt.startswith("//"):
                    session.run(stmt)
    yield driver
    driver.close()
