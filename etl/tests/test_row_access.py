from __future__ import annotations

import pandas as pd

from bracc_etl.transforms import row_pick


class TestRowPick:
    def test_returns_first_non_empty(self) -> None:
        row = pd.Series({"a": "", "b": "hit", "c": "later"})
        assert row_pick(row, "a", "b", "c") == "hit"

    def test_all_missing_returns_empty(self) -> None:
        row = pd.Series({"a": ""})
        assert row_pick(row, "a", "b", "c") == ""

    def test_strips_whitespace(self) -> None:
        row = pd.Series({"a": "   ", "b": "  value  "})
        assert row_pick(row, "a", "b") == "value"

    def test_unknown_key_is_skipped(self) -> None:
        row = pd.Series({"present": "yes"})
        assert row_pick(row, "absent", "present") == "yes"

    def test_skips_nan_literal(self) -> None:
        row = pd.Series({"a": "nan", "b": "real"})
        assert row_pick(row, "a", "b") == "real"

    def test_skips_nan_case_insensitive(self) -> None:
        row = pd.Series({"a": "NaN", "b": "NONE", "c": "value"})
        assert row_pick(row, "a", "b", "c") == "value"

    def test_non_string_values_are_coerced(self) -> None:
        # pandas may yield non-str values in untyped frames.
        row = pd.Series({"a": 42, "b": "later"})
        assert row_pick(row, "a", "b") == "42"

    def test_empty_keys_returns_empty(self) -> None:
        assert row_pick(pd.Series({})) == ""
