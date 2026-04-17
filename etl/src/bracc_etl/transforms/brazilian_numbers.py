"""Parse numeric values from Brazilian government data sources.

Four parsers cover the shapes that appear across pipelines. Pick the one that
matches the upstream format exactly — migrating to the wrong variant can silently
change aggregate values (e.g. treating ``"1234.56"`` as ``123456`` because dots
were assumed to be thousand-separators).

- :func:`parse_brl_amount` — strict Brazilian format ``"1.234.567,89"``. Always
  strips dots and converts the decimal comma to a dot.
- :func:`parse_numeric_comma` — decimal-comma format without thousand-separators
  (e.g. ``"1234,56"``). Converts comma to dot; keeps dots.
- :func:`parse_brl_flexible` — lenient: strips an optional ``R$`` prefix and
  whitespace, treats dots as thousands only when a comma is also present.
- :func:`parse_number_smart` — heuristic that inspects the rightmost separator to
  disambiguate Brazilian (``"1.234,56"``) from en-style (``"1,234.56"``) input.
"""

from __future__ import annotations

import re
from typing import Any

_FLEXIBLE_STRIP_RE = re.compile(r"[R$\s]")
_SMART_KEEP_RE = re.compile(r"[^0-9,.-]")


def parse_brl_amount[T](value: Any, *, default: T = 0.0) -> float | T:  # type: ignore[assignment]
    """Parse strict Brazilian numeric format.

    Always strips thousand-separator dots and converts the decimal comma to a
    dot. Returns ``default`` for ``None``, empty strings, and parse failures.

    Examples:
        >>> parse_brl_amount("1.234.567,89")
        1234567.89
        >>> parse_brl_amount("") is None  # with default=None
        False
        >>> parse_brl_amount("abc", default=None) is None
        True
    """
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    cleaned = text.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def parse_numeric_comma[T](value: Any, *, default: T = 0.0) -> float | T:  # type: ignore[assignment]
    """Parse decimal-comma format without thousand-separators.

    Converts comma to dot. Does NOT strip dots — use this for sources like
    TSE Bens or PGFN where values are shaped like ``"1234,56"`` or ``"1234.56"``.

    Examples:
        >>> parse_numeric_comma("1234,56")
        1234.56
        >>> parse_numeric_comma("1234.56")
        1234.56
    """
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    cleaned = text.replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def parse_brl_flexible[T](value: Any, *, default: T = 0.0) -> float | T:  # type: ignore[assignment]
    """Parse Brazilian monetary string tolerant of ``R$`` prefix.

    Strips currency symbol and whitespace. If a comma is present, treats dots as
    thousand-separators and comma as decimal; otherwise parses as-is (so plain
    ``"1234.56"`` still works).

    Examples:
        >>> parse_brl_flexible("R$ 1.234,56")
        1234.56
        >>> parse_brl_flexible("1234.56")
        1234.56
    """
    if not value:
        return default
    cleaned = _FLEXIBLE_STRIP_RE.sub("", str(value).strip())
    if not cleaned:
        return default
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def parse_number_smart[T](value: Any, *, default: T = 0.0) -> float | T:  # type: ignore[assignment]
    """Smart-detect numeric format by inspecting the rightmost separator.

    - ``"1.234,56"`` (BR): strips dots, comma→dot → ``1234.56``
    - ``"1234,56"``: comma→dot → ``1234.56``
    - ``"1234.56"``: unchanged → ``1234.56``

    Note: en-style thousand-separators with decimal dot (``"1,234.56"``) are NOT
    supported and will return ``default`` — the parser preserves the semantics
    of the pipelines it replaces (mides/folha_go/camara_goiania), which
    prioritize Brazilian input.

    Non-numeric characters (except ``,.-``) are stripped first, so
    ``"R$ 1.234,56"`` parses correctly.
    """
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    text = _SMART_KEEP_RE.sub("", text)
    if not text:
        return default
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except (ValueError, TypeError):
        return default
