"""Shared row-access helpers for pipeline transform stages.

Brazilian public data sources often ship the same field under several column
names across snapshots (e.g. ``CODIGO`` vs ``codigo`` vs ``cod``). Pipelines
routinely defined a private ``_pick`` / ``_get`` helper that returns the first
non-empty value among a list of candidate keys. This module centralises that
helper so the behavior is consistent (stripping, NaN-like filtering) and the
LOC cost of a new pipeline does not include redefining the utility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

_NAN_LIKE = frozenset({"nan", "none"})


def row_pick(row: pd.Series, *keys: str) -> str:
    """Return the first non-empty, non-NaN-like string value from ``row``.

    Tries each key in order; coerces the value to ``str``, strips whitespace,
    and skips empties plus sentinel strings ``"nan"`` / ``"none"`` (case
    insensitive) that pandas may emit when a source column contains missing
    values. Returns ``""`` if no candidate matches.

    Examples:
        >>> import pandas as pd
        >>> row_pick(pd.Series({"a": "", "b": "hit"}), "a", "b")
        'hit'
        >>> row_pick(pd.Series({"a": "nan", "b": "real"}), "a", "b")
        'real'
        >>> row_pick(pd.Series({"x": ""}), "a", "b")
        ''
    """
    for key in keys:
        value = str(row.get(key, "")).strip()
        if value and value.lower() not in _NAN_LIKE:
            return value
    return ""
