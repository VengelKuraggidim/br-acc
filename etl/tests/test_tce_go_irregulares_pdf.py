"""Offline tests for the TCE-GO irregulares PDF parser.

3 PDFs reais ficam em ``etl/tests/fixtures/tce_go/`` cobrindo os 3
sub-formatos do acervo:

- ``ano_2010_*.pdf`` — sem CPF (apenas Acórdão + Nome + Cargo)
- ``ano_2014_*.pdf`` — CPF completo + Processo + Cargo
- ``ano_2022_*.pdf`` — CPF mascarado por LGPD (``836.XXX.XXX-34``)

Cobertura ponta-a-ponta: parser → CSV → ``TceGoPipeline._transform_irregular``
→ verificação de :Person + IMPEDIDO_TCE_GO emitidos.
"""

from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.tce_go import TceGoPipeline
from bracc_etl.pipelines.tce_go_irregulares_pdf import (
    parse_irregulares_pdf,
    parse_pdf_file,
)

FIXTURES = Path(__file__).parent / "fixtures" / "tce_go"


@pytest.fixture
def pdf_2010() -> Path:
    return FIXTURES / "ano_2010_f47d356f.pdf"


@pytest.fixture
def pdf_2014() -> Path:
    return FIXTURES / "ano_2014_5294070d.pdf"


@pytest.fixture
def pdf_2022() -> Path:
    return FIXTURES / "ano_2022_505c6d14.pdf"


class TestFormat2010NoCpf:
    """Cluster A: tabela antiga sem coluna de CPF."""

    def test_extracts_18_servidores(self, pdf_2010: Path) -> None:
        rows = parse_pdf_file(pdf_2010, "2010")
        assert len(rows) == 18

    def test_no_cpf_when_source_lacks(self, pdf_2010: Path) -> None:
        rows = parse_pdf_file(pdf_2010, "2010")
        assert all(r["cpf"] == "" for r in rows)
        assert all(r["cpf_masked"] is False for r in rows)

    def test_processo_is_acordao(self, pdf_2010: Path) -> None:
        rows = parse_pdf_file(pdf_2010, "2010")
        for r in rows:
            assert r["processo"].startswith("Acórdão "), r

    def test_known_servidor(self, pdf_2010: Path) -> None:
        rows = parse_pdf_file(pdf_2010, "2010")
        nomes = {r["nome"] for r in rows}
        assert "Antônio Luiz Pereira da Costa" in nomes
        assert "Joaquim da Silva Mourão" in nomes


class TestFormat2014FullCpf:
    """Cluster B (representante 2014): CPF completo + processo numerado."""

    def test_extracts_servidores_with_cpf(self, pdf_2014: Path) -> None:
        rows = parse_pdf_file(pdf_2014, "2014")
        assert len(rows) >= 8
        # Todos têm CPF visível (não mascarado)
        assert all(r["cpf"] for r in rows)
        assert all(r["cpf_masked"] is False for r in rows)

    def test_cpf_format(self, pdf_2014: Path) -> None:
        import re
        cpf_re = re.compile(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")
        for r in parse_pdf_file(pdf_2014, "2014"):
            assert cpf_re.match(r["cpf"]), r["cpf"]

    def test_processo_numeric(self, pdf_2014: Path) -> None:
        for r in parse_pdf_file(pdf_2014, "2014"):
            assert r["processo"].isdigit()


class TestFormat2022MaskedCpf:
    """Cluster B com LGPD masking (representante 2022)."""

    def test_extracts_servidores(self, pdf_2022: Path) -> None:
        rows = parse_pdf_file(pdf_2022, "2022")
        assert len(rows) >= 20

    def test_cpf_masked_flag_set(self, pdf_2022: Path) -> None:
        rows = parse_pdf_file(pdf_2022, "2022")
        # 2022 mascara TUDO; cpf_masked deve ser True em todos
        assert all(r["cpf_masked"] is True for r in rows)

    def test_cpf_preserves_visible_digits(self, pdf_2022: Path) -> None:
        rows = parse_pdf_file(pdf_2022, "2022")
        for r in rows:
            # Format esperado: "836.XXX.XXX-34" (primeiro + último blocos visíveis)
            assert "X" in r["cpf"], r["cpf"]
            assert r["cpf"][:3].isdigit()
            assert r["cpf"][-2:].isdigit()


class TestRobustness:
    def test_empty_text(self) -> None:
        assert parse_irregulares_pdf("", "2024") == []

    def test_text_without_cpf_or_acordao(self) -> None:
        assert parse_irregulares_pdf("garbage text\nfoo bar", "2020") == []


class TestEndToEndWithPipeline:
    """Parser PDF → CSV no schema esperado → TceGoPipeline.transform()."""

    def test_2014_emits_person_nodes_and_impedido_rels(
        self, pdf_2014: Path, tmp_path: Path,
    ) -> None:
        rows = parse_pdf_file(pdf_2014, "2014")
        # Stage CSV no shape que o orquestrador escreve
        tce_dir = tmp_path / "tce_go"
        tce_dir.mkdir()
        with (tce_dir / "irregulares.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["nome", "cpf", "cpf_masked", "processo",
                            "cargo", "julgamento", "ano", "pdf_url"],
                delimiter=";",
            )
            writer.writeheader()
            for r in rows:
                writer.writerow({**r, "pdf_url": "http://example/pdf"})

        pipeline = TceGoPipeline(driver=MagicMock(), data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()

        # Cada servidor com CPF gera um IrregularAccount + um Person stub
        # + um IMPEDIDO_TCE_GO de :Person -> :TceGoIrregularAccount
        assert len(pipeline.irregular_accounts) == len(rows)
        assert len(pipeline.persons) == len(rows)
        assert len(pipeline.impedido_rels_person) == len(rows)
        # Sem CNPJ no PDF de servidores → 0 rels via :Company
        assert pipeline.impedido_rels == []

    def test_2010_no_cpf_no_person(
        self, pdf_2010: Path, tmp_path: Path,
    ) -> None:
        rows = parse_pdf_file(pdf_2010, "2010")
        tce_dir = tmp_path / "tce_go"
        tce_dir.mkdir()
        with (tce_dir / "irregulares.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["nome", "cpf", "cpf_masked", "processo",
                            "cargo", "julgamento", "ano", "pdf_url"],
                delimiter=";",
            )
            writer.writeheader()
            for r in rows:
                writer.writerow({**r, "pdf_url": "http://example/pdf"})

        pipeline = TceGoPipeline(driver=MagicMock(), data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        # 2010 não tem CPF → IrregularAccount nodes existem, mas
        # nenhum Person stub e nenhum IMPEDIDO rel é emitido.
        assert len(pipeline.irregular_accounts) == len(rows)
        assert pipeline.persons == []
        assert pipeline.impedido_rels_person == []
        assert pipeline.impedido_rels == []

    def test_2022_masked_cpf_skips_person(
        self, pdf_2022: Path, tmp_path: Path,
    ) -> None:
        """CPF mascarado (``836.XXX.XXX-34``) tem 5 dígitos visíveis +
        6 X — strip_document filtra X, sobram 5 dígitos, NEM CPF NEM
        CNPJ — nenhuma rel IMPEDIDO é emitida (esperado: dado não é
        chave única confiável)."""
        rows = parse_pdf_file(pdf_2022, "2022")
        tce_dir = tmp_path / "tce_go"
        tce_dir.mkdir()
        with (tce_dir / "irregulares.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["nome", "cpf", "cpf_masked", "processo",
                            "cargo", "julgamento", "ano", "pdf_url"],
                delimiter=";",
            )
            writer.writeheader()
            for r in rows:
                writer.writerow({**r, "pdf_url": "http://example/pdf"})

        pipeline = TceGoPipeline(driver=MagicMock(), data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        # IrregularAccount nodes criados (nome + processo dão a info útil)
        assert len(pipeline.irregular_accounts) == len(rows)
        # CPF mascarado não vira Person (faltam 6 dígitos)
        assert pipeline.persons == []
        assert pipeline.impedido_rels_person == []
