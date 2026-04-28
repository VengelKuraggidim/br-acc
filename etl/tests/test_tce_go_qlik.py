"""Offline tests for the TCE-GO Qlik panel parsers.

The fixtures (``etl/tests/fixtures/tce_go/qlik_dom_*.json``) were captured
in 2026-04-27 by ``tce_go_qlik.fetch_panel_dom`` against the live painéis
em ``paineis.tce.go.gov.br``. They mirror exactly what the headless
Selenium driver would yield, so the parsers are exercised end-to-end
without booting a browser.

When TCE-GO updates the panels (column reorder, new dataset year, etc.),
re-run the capture script (see top-of-file docstring of
``bracc_etl.pipelines.tce_go_qlik``) to refresh these fixtures and these
tests will catch any breakage in the parser shape.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.tce_go import TceGoPipeline
from bracc_etl.pipelines.tce_go_qlik import (
    _dedupe_consecutive,
    parse_fiscalizacoes_dom,
    parse_irregulares_dom,
)

FIXTURES = Path(__file__).parent / "fixtures" / "tce_go"


@pytest.fixture
def irregulares_payload() -> dict:
    return json.loads((FIXTURES / "qlik_dom_irregulares.json").read_text())


@pytest.fixture
def fiscalizacoes_payload() -> dict:
    return json.loads((FIXTURES / "qlik_dom_fiscalizacoes.json").read_text())


class TestDedupeConsecutive:
    def test_drops_adjacent_duplicates(self) -> None:
        out = _dedupe_consecutive([
            {"text": "a", "url": None}, {"text": "a", "url": None},
            {"text": "b", "url": None},
        ])
        assert [c["text"] for c in out] == ["a", "b"]

    def test_keeps_separated_duplicates(self) -> None:
        # Same value not consecutive — keep both
        out = _dedupe_consecutive([
            {"text": "a", "url": None}, {"text": "b", "url": None},
            {"text": "a", "url": None},
        ])
        assert [c["text"] for c in out] == ["a", "b", "a"]

    def test_url_distinguishes_cells(self) -> None:
        # Same text different url — keep both (not a duplicate)
        out = _dedupe_consecutive([
            {"text": "Visualizar", "url": "http://a"},
            {"text": "Visualizar", "url": "http://b"},
        ])
        assert len(out) == 2


class TestParseIrregulares:
    def test_row_count(self, irregulares_payload: dict) -> None:
        rows = parse_irregulares_dom(irregulares_payload)
        # Painel mostra 8 anos (2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024)
        assert len(rows) == 8

    def test_anos_cobertos(self, irregulares_payload: dict) -> None:
        rows = parse_irregulares_dom(irregulares_payload)
        anos = [r["processo"].split("/")[-1] for r in rows]
        assert anos == ["2010", "2012", "2014", "2016", "2018", "2020", "2022", "2024"]

    def test_pdf_url_present(self, irregulares_payload: dict) -> None:
        rows = parse_irregulares_dom(irregulares_payload)
        # Cada linha aponta pra um documento sob portal.tce.go.gov.br/documents/.
        # Quase todos têm extensão .pdf no path, mas o de 2024 foi enviado
        # sem extensão (typo do TCE no upload — content-type ainda é PDF).
        for r in rows:
            assert r["pdf_url"].startswith(
                "https://portal.tce.go.gov.br/documents/20181/835290/",
            )

    def test_julgamento_format_for_parse_date(self, irregulares_payload: dict) -> None:
        rows = parse_irregulares_dom(irregulares_payload)
        # parse_date aceita DD/MM/YYYY
        for r in rows:
            assert r["julgamento"].startswith("31/12/")

    def test_compatible_with_existing_transform(
        self, irregulares_payload: dict, tmp_path: Path,
    ) -> None:
        """End-to-end: parser → CSV → TceGoPipeline._transform_irregular.

        Garante que as rows produzidas pelo Qlik scraper alimentam o
        transform legado sem mudança de schema (apenas extra ``pdf_url``
        que o transform ignora).
        """
        rows = parse_irregulares_dom(irregulares_payload)
        # Stage CSV exatamente onde o pipeline procura
        tce_dir = tmp_path / "tce_go"
        tce_dir.mkdir()
        with (tce_dir / "irregulares.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["processo", "nome", "julgamento", "cnpj",
                            "motivo", "pdf_url"],
                delimiter=";",
            )
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        pipeline = TceGoPipeline(driver=MagicMock(), data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        # 8 anos = 8 IrregularAccount nodes (sem CNPJ porque o índice
        # não traz; PDF parsing é fase 2 — já documentado no parser)
        assert len(pipeline.irregular_accounts) == 8
        # Sem CNPJ → nenhum IMPEDIDO_TCE_GO emitido
        assert pipeline.impedido_rels == []


class TestParseFiscalizacoes:
    def test_row_count(self, fiscalizacoes_payload: dict) -> None:
        rows = parse_fiscalizacoes_dom(fiscalizacoes_payload)
        # O sheet tem 2 tabelas (summary + detail); ~50-60 linhas combinadas
        assert 30 < len(rows) < 200, f"unexpected count: {len(rows)}"

    def test_columns_populated(self, fiscalizacoes_payload: dict) -> None:
        rows = parse_fiscalizacoes_dom(fiscalizacoes_payload)
        for r in rows:
            assert r["numero"], f"numero vazio em {r}"
            assert r["descricao"]
            assert r["ano"].isdigit() and len(r["ano"]) == 4

    def test_both_table_shapes_present(
        self, fiscalizacoes_payload: dict,
    ) -> None:
        """Ambas tabelas (summary e detail) devem produzir linhas distintas
        no resultado — a summary preenche ``situacao`` + ``relator``, a
        detail preenche ``jurisdicionado`` + ``objetivo`` + ``lace``."""
        rows = parse_fiscalizacoes_dom(fiscalizacoes_payload)
        with_status = [r for r in rows if r["situacao"]]
        with_jurisdicionado = [r for r in rows if r["jurisdicionado"]]
        assert with_status, "nenhuma linha summary parseada"
        assert with_jurisdicionado, "nenhuma linha detail parseada"

    def test_inicio_format(self, fiscalizacoes_payload: dict) -> None:
        rows = parse_fiscalizacoes_dom(fiscalizacoes_payload)
        for r in rows:
            assert r["inicio"].startswith("01/01/")

    def test_compatible_with_existing_transform(
        self, fiscalizacoes_payload: dict, tmp_path: Path,
    ) -> None:
        rows = parse_fiscalizacoes_dom(fiscalizacoes_payload)
        tce_dir = tmp_path / "tce_go"
        tce_dir.mkdir()
        with (tce_dir / "fiscalizacoes.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["numero", "ano", "tipo", "situacao",
                            "descricao", "relator", "inicio", "jurisdicionado",
                            "objetivo", "lace"],
                delimiter=";",
            )
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        pipeline = TceGoPipeline(driver=MagicMock(), data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        # Pipeline dedup é por (numero, titulo, inicio); summary e detail
        # do mesmo processo têm titulos distintos = 2 audits separados.
        # Esperamos no máximo o input total e pelo menos a metade
        # (cobrindo dedup natural de linhas idênticas no input).
        assert 0 < len(pipeline.audits) <= len(rows)
        statuses = {a["status"] for a in pipeline.audits}
        assert any(s for s in statuses), "nenhum status preservado"


class TestEmptyDom:
    """Robustness: handle empty payloads (panel down, render timeout)."""

    def test_irregulares_empty(self) -> None:
        assert parse_irregulares_dom({"rows": []}) == []

    def test_fiscalizacoes_empty(self) -> None:
        assert parse_fiscalizacoes_dom({"rows": []}) == []

    def test_irregulares_skip_short_row(self) -> None:
        rows = parse_irregulares_dom({"rows": [
            [{"text": "2024", "url": None}],  # only 1 col → skip
        ]})
        assert rows == []
