"""Tests pro pipeline ``propagacao_fotos_person``.

Cobre:

* metadata + registry wiring;
* load — Cypher UNWIND devolve contagens por label, stats acumulam;
* load — no-op quando nenhum label-fonte tem foto (query devolve vazio);
* load — falha de sessão Neo4j é loggada, não propaga exceção.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from bracc_etl.pipelines.propagacao_fotos_person import (
    _SOURCE_ID,
    _SOURCE_LABELS,
    PropagacaoFotosPersonPipeline,
)
from bracc_etl.runner import PIPELINES

if TYPE_CHECKING:
    pass


def _mock_driver_with_records(records: list[dict[str, Any]]) -> MagicMock:
    """Driver Neo4j mockado com `session.run` devolvendo ``records``."""
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value

    def make_record(d: dict[str, Any]) -> MagicMock:
        rec = MagicMock()
        rec.get.side_effect = lambda k, _default=None, _d=d: _d.get(k, _default)
        return rec

    session.run.return_value = iter([make_record(r) for r in records])
    return driver


def _make_pipeline(driver: MagicMock) -> PropagacaoFotosPersonPipeline:
    return PropagacaoFotosPersonPipeline(driver=driver, data_dir="./data")


def test_metadata_and_registry_wiring() -> None:
    assert _SOURCE_ID == "propagacao_fotos_person"
    assert set(_SOURCE_LABELS) == {
        "FederalLegislator",
        "StateLegislator",
        "Senator",
    }
    assert PIPELINES["propagacao_fotos_person"] is PropagacaoFotosPersonPipeline
    p = _make_pipeline(driver=MagicMock())
    assert p.name == _SOURCE_ID
    assert p.source_id == _SOURCE_ID


def test_load_accumulates_stats_by_label() -> None:
    driver = _mock_driver_with_records([
        {"label": "FederalLegislator", "propagated": 7},
        {"label": "Senator", "propagated": 3},
        {"label": "StateLegislator", "propagated": 0},
    ])
    p = _make_pipeline(driver=driver)
    p.extract()
    p.transform()
    p.load()
    assert p._stats == {
        "FederalLegislator": 7,
        "Senator": 3,
        "StateLegislator": 0,
    }
    assert p.rows_loaded == 10


def test_load_is_no_op_when_no_source_has_photo() -> None:
    driver = _mock_driver_with_records([])  # query retorna zero linhas
    p = _make_pipeline(driver=driver)
    p.load()
    assert p.rows_loaded == 0
    assert all(v == 0 for v in p._stats.values())


def test_load_swallows_session_failure_without_propagating() -> None:
    driver = MagicMock()
    driver.session.return_value.__enter__.side_effect = RuntimeError("neo4j down")
    p = _make_pipeline(driver=driver)
    p.load()  # não lança
    assert p.rows_loaded == 0


def test_load_ignores_unknown_label_in_record() -> None:
    """Se a query Cypher devolver label fora do set hardcoded, não
    quebra; apenas não contabiliza (defesa em profundidade contra
    drift de schema)."""
    driver = _mock_driver_with_records([
        {"label": "FederalLegislator", "propagated": 5},
        {"label": "LabelInexistente", "propagated": 99},
    ])
    p = _make_pipeline(driver=driver)
    p.load()
    assert p._stats["FederalLegislator"] == 5
    assert "LabelInexistente" not in p._stats
    assert p.rows_loaded == 5
