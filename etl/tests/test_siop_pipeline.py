from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bracc_etl.pipelines.siop import SiopPipeline, _classify_amendment_type
from tests._mock_helpers import mock_session

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SiopPipeline:
    driver = MagicMock()
    pipeline = SiopPipeline(driver=driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _load_fixture_data(pipeline: SiopPipeline) -> None:
    """Load CSV fixture directly into the pipeline's raw DataFrame."""
    pipeline._raw = pd.read_csv(
        FIXTURES / "siop" / "emendas.csv",
        dtype=str,
        sep=";",
        keep_default_na=False,
    )


# ── Metadata ─────────────────────────────────────────────────────────


class TestSiopPipelineMetadata:
    def test_name(self) -> None:
        assert SiopPipeline.name == "siop"

    def test_source_id(self) -> None:
        assert SiopPipeline.source_id == "siop"


# ── Amendment Type Classification ────────────────────────────────────


class TestClassifyAmendmentType:
    def test_individual(self) -> None:
        assert _classify_amendment_type("Individual") == "individual"

    def test_bancada(self) -> None:
        assert _classify_amendment_type("Bancada") == "bancada"

    def test_comissao(self) -> None:
        assert _classify_amendment_type("Comissão") == "comissao"

    def test_comissao_ascii(self) -> None:
        assert _classify_amendment_type("Comissao") == "comissao"

    def test_relator(self) -> None:
        assert _classify_amendment_type("Relator") == "relator"

    def test_relator_case(self) -> None:
        assert _classify_amendment_type("RELATOR") == "relator"

    def test_unknown_type(self) -> None:
        assert _classify_amendment_type("  Especial  ") == "Especial"

    def test_empty(self) -> None:
        assert _classify_amendment_type("") == ""


# ── Extract ──────────────────────────────────────────────────────────


class TestSiopExtract:
    def test_extract_reads_csvs(self, tmp_path: Path) -> None:
        """Extract reads all CSVs from the siop directory."""
        siop_dir = tmp_path / "siop"
        siop_dir.mkdir()

        # Write a minimal CSV
        csv_content = (
            "ANO;CÓDIGO EMENDA;NÚMERO EMENDA;TIPO EMENDA;AUTOR EMENDA;"
            "CPF/CNPJ AUTOR;LOCALIDADE;CÓDIGO FUNÇÃO;NOME FUNÇÃO;"
            "CÓDIGO SUBFUNÇÃO;NOME SUBFUNÇÃO;CÓDIGO PROGRAMA;NOME PROGRAMA;"
            "CÓDIGO AÇÃO;NOME AÇÃO;VALOR EMPENHADO;VALOR LIQUIDADO;VALOR PAGO\n"
            "2023;99990001;1;Individual;TESTE;12345678901;Local;10;Saude;"
            "301;Basica;5019;Programa;219A;Acao;100,00;80,00;50,00\n"
        )
        (siop_dir / "emendas_2023.csv").write_text(csv_content, encoding="latin-1")

        driver = MagicMock()
        pipeline = SiopPipeline(driver=driver, data_dir=str(tmp_path))
        pipeline.extract()

        assert len(pipeline._raw) == 1

    def test_extract_multiple_files(self, tmp_path: Path) -> None:
        """Extract concatenates multiple yearly CSVs."""
        siop_dir = tmp_path / "siop"
        siop_dir.mkdir()

        header = (
            "ANO;CÓDIGO EMENDA;NÚMERO EMENDA;TIPO EMENDA;AUTOR EMENDA;"
            "CPF/CNPJ AUTOR;LOCALIDADE;CÓDIGO FUNÇÃO;NOME FUNÇÃO;"
            "CÓDIGO SUBFUNÇÃO;NOME SUBFUNÇÃO;CÓDIGO PROGRAMA;NOME PROGRAMA;"
            "CÓDIGO AÇÃO;NOME AÇÃO;VALOR EMPENHADO;VALOR LIQUIDADO;VALOR PAGO\n"
        )
        row_2023 = (
            "2023;99990001;1;Individual;A;12345678901;L;10;S;301;B;5019;P;219A;Ac;100,00;80,00;50,00\n"
        )
        row_2024 = (
            "2024;99990002;2;Bancada;B;;L2;12;E;362;M;5012;P2;20RN;Ac2;200,00;160,00;100,00\n"
        )

        (siop_dir / "emendas_2023.csv").write_text(header + row_2023, encoding="latin-1")
        (siop_dir / "emendas_2024.csv").write_text(header + row_2024, encoding="latin-1")

        driver = MagicMock()
        pipeline = SiopPipeline(driver=driver, data_dir=str(tmp_path))
        pipeline.extract()

        assert len(pipeline._raw) == 2

    def test_extract_no_files(self, tmp_path: Path) -> None:
        """Extract gracefully handles empty directory."""
        siop_dir = tmp_path / "siop"
        siop_dir.mkdir()

        driver = MagicMock()
        pipeline = SiopPipeline(driver=driver, data_dir=str(tmp_path))
        pipeline.extract()

        assert pipeline._raw.empty


# ── Transform ────────────────────────────────────────────────────────


class TestSiopTransform:
    def test_produces_amendments(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 5 rows in fixture with 4 distinct CÓDIGO EMENDA values
        # (first two rows share code 26590009)
        assert len(pipeline.amendments) == 4

    def test_amendment_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        a = pipeline.amendments[0]
        expected_fields = {
            "amendment_id", "amendment_code", "amendment_number", "year",
            "amendment_type", "author_name", "locality", "function",
            "program", "program_code", "action", "action_code",
            "amount_committed", "amount_settled", "amount_paid", "source",
        }
        assert expected_fields.issubset(set(a.keys()))
        assert a["source"] == "siop"

    def test_amendment_id_format(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for a in pipeline.amendments:
            assert a["amendment_id"].startswith("siop_")

    def test_aggregates_values(self) -> None:
        """Two rows with code 26590009 should sum their monetary values."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        amd = next(a for a in pipeline.amendments if a["amendment_code"] == "26590009")
        # 1,500,000 + 500,000 = 2,000,000
        assert amd["amount_committed"] == pytest.approx(2_000_000.0)
        # 1,200,000 + 400,000 = 1,600,000
        assert amd["amount_settled"] == pytest.approx(1_600_000.0)
        # 750,000 + 250,000 = 1,000,000
        assert amd["amount_paid"] == pytest.approx(1_000_000.0)

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        author_names = {a["author_name"] for a in pipeline.amendments}
        assert "DEPUTADO JOAO SILVA" in author_names
        assert "SENADORA MARIA SOUZA" in author_names

    def test_classifies_amendment_types(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        types = {a["amendment_type"] for a in pipeline.amendments}
        assert "individual" in types
        assert "bancada" in types
        assert "comissao" in types
        assert "relator" in types

    def test_extracts_program_action(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        amd = next(a for a in pipeline.amendments if a["amendment_code"] == "26590009")
        assert "SAUDE" in amd["function"] or "ATENCAO" in amd["program"]
        assert amd["program_code"] == "5019"
        assert amd["action_code"] == "219A"

    def test_produces_authors_with_cpf(self) -> None:
        """Only authors with valid 11-digit CPFs become Person nodes."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # Fixture has 2 authors with CPFs: 52998224725 and 11144477735
        assert len(pipeline.authors) == 2

    def test_formats_author_cpf(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cpfs = {a["cpf"] for a in pipeline.authors}
        assert "529.982.247-25" in cpfs
        assert "111.444.777-35" in cpfs

    def test_produces_author_rels(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # 2 authors with CPFs => 2 AUTOR_EMENDA rels
        assert len(pipeline.author_rels) == 2

    def test_author_rel_structure(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        for rel in pipeline.author_rels:
            assert "source_key" in rel
            assert "target_key" in rel
            assert rel["target_key"].startswith("siop_")

    def test_skips_authors_without_cpf(self) -> None:
        """Bancada/comissao amendments lack CPFs and should not create Person nodes."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        author_cpf_digits = {
            a["cpf"].replace(".", "").replace("-", "") for a in pipeline.authors
        }
        # No empty CPFs should appear
        assert "" not in author_cpf_digits

    def test_year_preserved(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        years = {a["year"] for a in pipeline.amendments}
        assert "2023" in years
        assert "2024" in years

    def test_empty_dataframe(self) -> None:
        """Transform on empty data produces no results."""
        pipeline = _make_pipeline()
        pipeline._raw = pd.DataFrame()
        pipeline.transform()

        assert pipeline.amendments == []
        assert pipeline.authors == []
        assert pipeline.author_rels == []


# ── Load ─────────────────────────────────────────────────────────────


class TestSiopLoad:
    def test_load_creates_amendment_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        amendment_calls = [
            call for call in run_calls
            if "MERGE (n:Amendment" in str(call)
        ]
        assert len(amendment_calls) >= 1

    def test_load_creates_person_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        person_calls = [
            call for call in run_calls
            if "MERGE (n:Person" in str(call)
        ]
        assert len(person_calls) >= 1

    def test_load_creates_autor_emenda_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            call for call in run_calls
            if "AUTOR_EMENDA" in str(call)
        ]
        assert len(rel_calls) >= 1

    def test_load_empty_data(self) -> None:
        """Load with no data does not call Neo4j."""
        pipeline = _make_pipeline()
        pipeline.load()

        session_mock = mock_session(pipeline)
        assert session_mock.run.call_count == 0

    def test_load_cypher_uses_param_binding(self) -> None:
        """Verify Cypher uses $rows parameter binding, never string interpolation."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = mock_session(pipeline)
        for call in session_mock.run.call_args_list:
            query = str(call[0][0])
            if "AUTOR_EMENDA" in query:
                assert "$rows" in query or "row." in query
