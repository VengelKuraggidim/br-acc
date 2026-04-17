from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from unittest.mock import MagicMock


def mock_driver(pipeline_or_driver: object) -> MagicMock:
    """Return the driver mock as ``MagicMock`` to silence strict-typed attribute access.

    Pipelines store ``driver: neo4j.Driver``; tests hand them a ``MagicMock``. Every
    follow-up access (``.session.called``, ``.session.return_value.__enter__``, etc.)
    would trip mypy on the strict-typed ``Driver`` interface. Reach through this
    helper once per test and the chain is untyped ``MagicMock`` the rest of the way.
    Accepts either a pipeline (has ``.driver``) or a bare driver mock.
    """
    target = cast("Any", pipeline_or_driver)
    if hasattr(target, "driver") and not hasattr(target, "session"):
        target = target.driver
    return cast("MagicMock", target)


def mock_session(pipeline_or_driver: object) -> MagicMock:
    """Return the MagicMock that stands in for an active Neo4j session.

    Shortcut for the very common pattern
    ``driver.session(...).__enter__()`` in tests: reach through this helper instead
    of threading ``cast(MagicMock, …)`` at every call site.
    """
    session_enter = mock_driver(pipeline_or_driver).session.return_value.__enter__
    return cast("MagicMock", session_enter.return_value)
