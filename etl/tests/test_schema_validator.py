from __future__ import annotations

import logging

import pandas as pd
import pandera.pandas as pa
import pytest

from bracc_etl.schemas.validator import (
    _get_validation_mode,
    validate_dataframe,
    validate_dataframe_sampled,
)


@pytest.fixture
def simple_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "id": pa.Column(str, pa.Check.str_length(min_value=1)),
            "value": pa.Column(int, pa.Check.ge(0)),
        },
    )


@pytest.fixture
def valid_df() -> pd.DataFrame:
    return pd.DataFrame({"id": ["a", "b", "c"], "value": [1, 2, 3]})


@pytest.fixture
def invalid_df() -> pd.DataFrame:
    # value=-1 fails ge(0); id="" fails str_length min=1.
    return pd.DataFrame({"id": ["a", "", "c"], "value": [1, -1, 3]})


class TestGetValidationMode:
    def test_default_is_warn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("BRACC_SCHEMA_VALIDATION", raising=False)
        assert _get_validation_mode() == "warn"

    @pytest.mark.parametrize("value", ["strict", "STRICT", " Strict "])
    def test_strict_is_case_and_whitespace_sensitive(
        self, value: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", value)
        # Implementation does .lower() but not .strip(); document actual
        # behavior so regressions are explicit.
        expected = value.lower()
        assert _get_validation_mode() == expected

    def test_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "off")
        assert _get_validation_mode() == "off"


class TestValidateDataframeOff:
    def test_off_skips_validation_entirely(
        self,
        simple_schema: pa.DataFrameSchema,
        invalid_df: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # In "off" mode, even an invalid df should pass straight through.
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "off")
        result = validate_dataframe(invalid_df, simple_schema, "test_source")
        assert result is invalid_df


class TestValidateDataframeWarn:
    def test_valid_df_returns_validated(
        self,
        simple_schema: pa.DataFrameSchema,
        valid_df: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "warn")
        with caplog.at_level(logging.INFO):
            result = validate_dataframe(valid_df, simple_schema, "test_source")
        assert result.equals(valid_df)
        assert any("passed" in rec.message.lower() for rec in caplog.records)

    def test_invalid_df_logs_warning_and_returns_original(
        self,
        simple_schema: pa.DataFrameSchema,
        invalid_df: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "warn")
        with caplog.at_level(logging.WARNING):
            result = validate_dataframe(invalid_df, simple_schema, "test_source")
        # Warn mode returns original df unchanged.
        assert result is invalid_df
        assert any("failures" in rec.message.lower() for rec in caplog.records)


class TestValidateDataframeStrict:
    def test_valid_df_passes(
        self,
        simple_schema: pa.DataFrameSchema,
        valid_df: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "strict")
        result = validate_dataframe(valid_df, simple_schema, "test_source")
        assert result.equals(valid_df)

    def test_invalid_df_raises(
        self,
        simple_schema: pa.DataFrameSchema,
        invalid_df: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import pandera.errors as pa_errors

        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "strict")
        with pytest.raises(pa_errors.SchemaErrors):
            validate_dataframe(invalid_df, simple_schema, "test_source")


class TestValidateDataframeSampled:
    def test_small_df_validates_in_full(
        self,
        simple_schema: pa.DataFrameSchema,
        valid_df: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "warn")
        result = validate_dataframe_sampled(
            valid_df, simple_schema, "test_source", sample_size=100,
        )
        assert result.equals(valid_df)

    def test_large_df_samples_but_returns_full(
        self,
        simple_schema: pa.DataFrameSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Build a 1000-row valid df; sample 100 and still return all 1000.
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "warn")
        df = pd.DataFrame({
            "id": [f"id_{i}" for i in range(1000)],
            "value": list(range(1000)),
        })
        result = validate_dataframe_sampled(
            df, simple_schema, "test_source", sample_size=100,
        )
        assert len(result) == 1000
        assert result.equals(df)

    def test_sample_failure_warned_but_full_df_returned(
        self,
        simple_schema: pa.DataFrameSchema,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("BRACC_SCHEMA_VALIDATION", "warn")
        # 1000 rows, half invalid (id=""); sample of 100 will see some failures.
        df = pd.DataFrame({
            "id": [f"id_{i}" if i % 2 else "" for i in range(1000)],
            "value": list(range(1000)),
        })
        with caplog.at_level(logging.WARNING):
            result = validate_dataframe_sampled(
                df, simple_schema, "test_source", sample_size=100,
            )
        assert len(result) == 1000  # Full df returned despite failures
        # Sample name should show up in logs with the annotation.
        assert any("sample=100" in rec.message for rec in caplog.records)
