"""Autouse marker for the gated federal test tree.

Every test under ``api/tests/_federal/`` is automatically marked
``federal`` so the default ``make test-api`` run (which deselects
``federal``) skips them. Run them explicitly with
``make test-api-federal``.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(
    config: pytest.Config,  # noqa: ARG001 - pytest contract
    items: list[pytest.Item],
) -> None:
    marker = pytest.mark.federal
    for item in items:
        if "_federal/" in str(item.fspath).replace("\\", "/"):
            item.add_marker(marker)
