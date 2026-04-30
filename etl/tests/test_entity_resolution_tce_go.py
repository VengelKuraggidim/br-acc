"""Tests do pipeline ``entity_resolution_tce_go``.

Cobre:

* helpers puros — ``_normalize_name``, ``_digits_only``;
* Tier 1 happy path — Person stub com CPF que bate com membro de cluster;
* Tier 1 sem match — stub com CPF que não bate em nenhum cluster vai
  pro audit ``no_match``;
* Tier 2 (opt-in) happy path — match por nome quando CPF não bate;
* Tier 2 ambiguidade — múltiplos clusters com mesmo display_name = audit + skip;
* Tier 2 default OFF — stub não-matched por CPF NÃO tenta nome;
* Idempotência — segunda rodada não duplica edges (MERGE no Cypher final);
* Provenance — todo row carrega os 5 campos obrigatórios.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.entity_resolution_tce_go import (
    _SOURCE_ID,
    EntityResolutionTceGoPipeline,
    _digits_only,
    _normalize_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_primary_url_cache() -> Iterator[None]:
    from bracc_etl.provenance import _reset_cache_for_tests

    _reset_cache_for_tests()
    yield
    _reset_cache_for_tests()


def _row_to_record(row: dict[str, Any]) -> MagicMock:
    record = MagicMock()
    record.keys.return_value = list(row.keys())
    record.__iter__ = lambda _self: iter(row.keys())
    record.__getitem__ = lambda _self, key: row[key]
    record.data.return_value = row
    return record


def _build_driver(
    discovery_rows: list[dict[str, Any]],
) -> tuple[MagicMock, list[tuple[str, dict[str, Any]]]]:
    """Mock driver que serve _DISCOVERY_QUERY e captura writes."""
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value
    calls: list[tuple[str, dict[str, Any]]] = []

    def run(query: str, params: dict[str, Any] | None = None) -> MagicMock:
        calls.append((query, params or {}))
        result = MagicMock()
        # Discovery: 3 UNION ALLs com kind ∈ {stub, cluster_cpf, cluster_name}.
        if "UNION ALL" in query and "tce_go_irregulares" in query:
            result.__iter__ = lambda _self: iter(
                [_row_to_record(r) for r in discovery_rows],
            )
        else:
            result.__iter__ = lambda _self: iter([])
        # consume() é chamado pelo loader; precisa devolver algo truthy.
        result.consume.return_value = MagicMock()
        return result

    session.run.side_effect = run
    return driver, calls


def _make_pipeline(
    discovery_rows: list[dict[str, Any]],
    tmp_path: Path,
    *,
    enable_name_tier: bool = False,
) -> tuple[EntityResolutionTceGoPipeline, MagicMock, list[tuple[str, dict[str, Any]]]]:
    driver, calls = _build_driver(discovery_rows)
    pipeline = EntityResolutionTceGoPipeline(
        driver=driver,
        data_dir=str(tmp_path),
        enable_name_tier=enable_name_tier,
    )
    pipeline.run_id = f"{_SOURCE_ID}_20260429120000"
    return pipeline, driver, calls


def _stub(element_id: str, cpf: str, name: str) -> dict[str, Any]:
    return {
        "kind": "stub",
        "element_id": element_id,
        "cpf": cpf,
        "name": name,
        "canonical_id": None,
        "display_name": None,
    }


def _cluster_cpf(
    src_element_id: str, cpf: str, canonical_id: str, display_name: str,
) -> dict[str, Any]:
    return {
        "kind": "cluster_cpf",
        "element_id": src_element_id,
        "cpf": cpf,
        "name": display_name,
        "canonical_id": canonical_id,
        "display_name": display_name,
    }


def _cluster_name(canonical_id: str, display_name: str) -> dict[str, Any]:
    return {
        "kind": "cluster_name",
        "element_id": None,
        "cpf": None,
        "name": display_name,
        "canonical_id": canonical_id,
        "display_name": display_name,
    }


# ---------------------------------------------------------------------------
# Helpers puros
# ---------------------------------------------------------------------------


class TestDigitsOnly:
    def test_cpf_formatted(self) -> None:
        assert _digits_only("218.405.711-87") == "21840571187"

    def test_none(self) -> None:
        assert _digits_only(None) == ""

    def test_empty(self) -> None:
        assert _digits_only("") == ""


class TestNormalizeName:
    def test_upper_sem_acento(self) -> None:
        assert _normalize_name("José Ferreira") == "JOSE FERREIRA"

    def test_pontuacao(self) -> None:
        assert _normalize_name("Dr. João  Silva") == "DR JOAO SILVA"


# ---------------------------------------------------------------------------
# Pipeline end-to-end (driver mockado)
# ---------------------------------------------------------------------------


class TestTier1CpfExact:
    def test_match_simples(self, tmp_path: Path) -> None:
        # Stub TCE-GO com CPF que bate com Person já clusterizado pelo
        # entity_resolution_politicos_go (cluster ancorado em Senator).
        rows = [
            _stub("stub1", "218.405.711-87", "JORGE KAJURU"),
            _cluster_cpf(
                "person_n2", "218.405.711-87",
                "canon_senado_5895", "JORGE KAJURU REIS DA COSTA NASSER",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        assert len(edges) == 1
        edge = edges[0]
        assert edge["canonical_id"] == "canon_senado_5895"
        assert edge["target_element_id"] == "stub1"
        assert edge["method"] == "tce_go_cpf_exact"
        assert edge["confidence"] == pytest.approx(1.0)
        assert edge["target_label"] == "Person"
        # Provenance fields.
        for field in (
            "source_id", "source_record_id", "source_url",
            "ingested_at", "run_id",
        ):
            assert edge[field], f"missing provenance field {field}"

    def test_cpf_format_diferente_normaliza(self, tmp_path: Path) -> None:
        # Stub com CPF formatado XXX.XXX.XXX-XX, cluster com CPF em
        # outro formato (digits puros) — _digits_only normaliza ambos.
        rows = [
            _stub("stub1", "111.222.333-44", "MARIA SILVA"),
            _cluster_cpf(
                "person_a", "11122233344",
                "canon_cpf_11122233344", "MARIA SILVA SANTOS",
            ),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path)
        pipeline.run()

        assert len(pipeline.represents_rels) == 1
        assert pipeline.represents_rels[0]["canonical_id"] == "canon_cpf_11122233344"

    def test_sem_match_vai_pro_audit_no_match(self, tmp_path: Path) -> None:
        # Stub sem CPF clusterizado — servidor que nunca foi político.
        # Esperado: zero edges, audit entry no_match.
        rows = [
            _stub("stub1", "999.888.777-66", "FULANO DE TAL"),
            _cluster_cpf(
                "person_a", "111.222.333-44",
                "canon_senado_5895", "OUTRO POLITICO",
            ),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path)
        pipeline.run()

        assert pipeline.represents_rels == []
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        no_match = [e for e in entries if e.get("type") == "no_match"]
        assert len(no_match) == 1
        assert no_match[0]["stub_element_id"] == "stub1"
        # Summary entry sempre presente.
        assert any(e.get("type") == "summary" for e in entries)

    def test_cpf_collision_vira_audit(self, tmp_path: Path) -> None:
        # 2 clusters distintos reivindicando o mesmo CPF — estado inválido
        # do entity_resolution_politicos_go, mas defendemos com audit + skip.
        rows = [
            _stub("stub1", "218.405.711-87", "JORGE KAJURU"),
            _cluster_cpf(
                "person_a", "218.405.711-87",
                "canon_senado_5895", "JORGE KAJURU",
            ),
            _cluster_cpf(
                "person_b", "218.405.711-87",
                "canon_camara_999", "OUTRO NOME",
            ),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path)
        pipeline.run()

        assert pipeline.represents_rels == []
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        assert any(e.get("type") == "cluster_cpf_collision" for e in entries)


class TestTier2NameOptIn:
    def test_default_off_nao_tenta_nome(self, tmp_path: Path) -> None:
        # CPF não bate. Nome bate exato com display_name. Mas
        # enable_name_tier=False (default) → stub fica órfão.
        rows = [
            _stub("stub1", "999.888.777-66", "JORGE KAJURU"),
            _cluster_name("canon_senado_5895", "JORGE KAJURU"),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path, enable_name_tier=False)
        pipeline.run()

        assert pipeline.represents_rels == []

    def test_opt_in_match_unico(self, tmp_path: Path) -> None:
        rows = [
            _stub("stub1", "999.888.777-66", "JORGE KAJURU"),
            _cluster_name("canon_senado_5895", "JORGE KAJURU"),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path, enable_name_tier=True)
        pipeline.run()

        assert len(pipeline.represents_rels) == 1
        edge = pipeline.represents_rels[0]
        assert edge["canonical_id"] == "canon_senado_5895"
        assert edge["method"] == "tce_go_name_exact"
        assert edge["confidence"] == pytest.approx(0.7)

    def test_opt_in_ambiguidade_skipa(self, tmp_path: Path) -> None:
        # 2 clusters com display_name idêntico (homonímia) → audit + skip.
        rows = [
            _stub("stub1", "999.888.777-66", "JOAO SILVA"),
            _cluster_name("canon_camara_1", "JOAO SILVA"),
            _cluster_name("canon_camara_2", "JOAO SILVA"),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path, enable_name_tier=True)
        pipeline.run()

        assert pipeline.represents_rels == []
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        ambiguous = [e for e in entries if e.get("type") == "name_ambiguous"]
        assert len(ambiguous) == 1
        assert sorted(ambiguous[0]["candidate_canonical_ids"]) == [
            "canon_camara_1", "canon_camara_2",
        ]

    def test_cpf_match_tem_prioridade_sobre_nome(self, tmp_path: Path) -> None:
        # Tier 1 deve ganhar mesmo com Tier 2 disponível.
        rows = [
            _stub("stub1", "218.405.711-87", "JORGE KAJURU"),
            _cluster_cpf(
                "person_n2", "218.405.711-87",
                "canon_senado_5895", "JORGE KAJURU REIS DA COSTA NASSER",
            ),
            # Outro cluster com display_name parecido — não deve ser usado.
            _cluster_name("canon_outro", "JORGE KAJURU"),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path, enable_name_tier=True)
        pipeline.run()

        assert len(pipeline.represents_rels) == 1
        edge = pipeline.represents_rels[0]
        assert edge["method"] == "tce_go_cpf_exact"
        assert edge["canonical_id"] == "canon_senado_5895"


class TestIdempotenciaPath:
    def test_segunda_rodada_emite_mesmas_arestas(self, tmp_path: Path) -> None:
        # Cypher final usa MERGE no rel — re-rodar não duplica no grafo.
        # Aqui validamos que o pipeline emite o mesmo set de edges
        # determinístico (mesmo run produzido pelo input idêntico, modulo
        # provenance fields que mudam por run_id).
        rows = [
            _stub("stub1", "218.405.711-87", "KAJURU"),
            _cluster_cpf(
                "person_n2", "218.405.711-87",
                "canon_senado_5895", "JORGE KAJURU",
            ),
        ]
        p1, _, _ = _make_pipeline(rows, tmp_path)
        p1.run()
        p2, _, _ = _make_pipeline(rows, tmp_path)
        p2.run()

        # Mesmo target + canonical = mesma aresta lógica (MERGE
        # idempotente no Cypher).
        assert (
            p1.represents_rels[0]["target_element_id"]
            == p2.represents_rels[0]["target_element_id"]
        )
        assert (
            p1.represents_rels[0]["canonical_id"]
            == p2.represents_rels[0]["canonical_id"]
        )


class TestStats:
    def test_summary_no_audit_log(self, tmp_path: Path) -> None:
        rows = [
            # 1 match CPF
            _stub("stub_match", "111.111.111-11", "MATCH CPF"),
            _cluster_cpf(
                "p1", "111.111.111-11", "canon_a", "POLITICO A",
            ),
            # 1 sem match
            _stub("stub_orphan", "222.222.222-22", "FULANO ORFAO"),
        ]
        pipeline, _, _ = _make_pipeline(rows, tmp_path)
        pipeline.run()

        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        first_line = audit_files[0].read_text(encoding="utf-8").splitlines()[0]
        summary = json.loads(first_line)
        assert summary["type"] == "summary"
        assert summary["stubs_total"] == 2
        assert summary["matched_cpf"] == 1
        assert summary["matched_name"] == 0
        assert summary["unmatched"] == 1
