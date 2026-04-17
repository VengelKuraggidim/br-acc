from __future__ import annotations

import logging
import re
from unittest.mock import MagicMock

import pytest

from bracc_etl.base import Pipeline


class DummyPipeline(Pipeline):
    name = "dummy"
    source_id = "test"

    def __init__(self) -> None:
        super().__init__(driver=MagicMock(), data_dir="./data")
        self.extracted = False
        self.transformed = False
        self.loaded = False

    def extract(self) -> None:
        self.extracted = True

    def transform(self) -> None:
        self.transformed = True

    def load(self) -> None:
        self.loaded = True


class FailingExtract(DummyPipeline):
    def extract(self) -> None:
        msg = "boom"
        raise RuntimeError(msg)


class FailingTransform(DummyPipeline):
    def transform(self) -> None:
        msg = "transform boom"
        raise RuntimeError(msg)


def test_pipeline_run_executes_all_stages() -> None:
    pipeline = DummyPipeline()
    pipeline.run()
    assert pipeline.extracted
    assert pipeline.transformed
    assert pipeline.loaded


def test_run_id_matches_source_id_with_timestamp() -> None:
    pipeline = DummyPipeline()
    # Format: "<source_id>_YYYYMMDDHHMMSS"
    assert re.fullmatch(r"test_\d{14}", pipeline.run_id) is not None


def test_run_id_falls_back_to_name_when_source_id_missing() -> None:
    class NoSourceId(Pipeline):
        name = "without_source_id"

        def extract(self) -> None: ...
        def transform(self) -> None: ...
        def load(self) -> None: ...

    pipeline = NoSourceId(driver=MagicMock(), data_dir="./data")
    # Without a source_id, it should still key off name.
    assert pipeline.run_id.startswith("without_source_id_")


def test_extract_failure_propagates_and_marks_quality_fail() -> None:
    pipeline = FailingExtract()
    with pytest.raises(RuntimeError, match="boom"):
        pipeline.run()
    # After extract raises, transform and load must not have been called.
    assert pipeline.transformed is False
    assert pipeline.loaded is False


def test_transform_failure_propagates_after_extract() -> None:
    pipeline = FailingTransform()
    with pytest.raises(RuntimeError, match="transform boom"):
        pipeline.run()
    assert pipeline.extracted is True
    assert pipeline.loaded is False


def test_default_neo4j_database_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEO4J_DATABASE", "mydb")
    pipeline = DummyPipeline()
    assert pipeline.neo4j_database == "mydb"


def test_neo4j_database_explicit_arg_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEO4J_DATABASE", "envdb")

    class _P(Pipeline):
        name = "p"
        source_id = "p"

        def extract(self) -> None: ...
        def transform(self) -> None: ...
        def load(self) -> None: ...

    pipeline = _P(driver=MagicMock(), data_dir="./data", neo4j_database="arg_db")
    assert pipeline.neo4j_database == "arg_db"


def test_driver_error_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    driver = MagicMock()
    driver.session.side_effect = RuntimeError("neo4j offline")

    class _P(Pipeline):
        name = "p"
        source_id = "p"

        def extract(self) -> None: ...
        def transform(self) -> None: ...
        def load(self) -> None: ...

    pipeline = _P(driver=driver, data_dir="./data")
    # Should not crash even though every IngestionRun write fails.
    with caplog.at_level(logging.WARNING):
        pipeline.run()
    assert any(
        "failed to persist IngestionRun" in rec.message
        for rec in caplog.records
    )
