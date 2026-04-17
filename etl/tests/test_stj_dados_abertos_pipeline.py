from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl.pipelines.stj_dados_abertos import StjPipeline, _generate_case_id

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> StjPipeline:
    return StjPipeline(
        driver=MagicMock(), data_dir=str(data_dir or FIXTURES),
    )


class TestMetadata:
    def test_name(self) -> None:
        assert StjPipeline.name == "stj_dados_abertos"

    def test_source_id(self) -> None:
        assert StjPipeline.source_id == "stj_dados_abertos"


class TestGenerateCaseId:
    def test_deterministic(self) -> None:
        a = _generate_case_id("HC", "123456", "2024")
        b = _generate_case_id("HC", "123456", "2024")
        assert a == b

    def test_length_16(self) -> None:
        assert len(_generate_case_id("HC", "123456", "2024")) == 16

    def test_distinct_inputs_differ(self) -> None:
        a = _generate_case_id("HC", "123456", "2024")
        b = _generate_case_id("REsp", "123456", "2024")
        c = _generate_case_id("HC", "654321", "2024")
        d = _generate_case_id("HC", "123456", "2025")
        assert len({a, b, c, d}) == 4


class TestExtract:
    def test_raises_when_csv_missing(self, tmp_path: Path) -> None:
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        with pytest.raises(FileNotFoundError):
            pipeline.extract()

    def test_reads_fixture_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        # Fixture has 6 rows including 1 duplicate and 2 intentionally invalid.
        assert len(pipeline._raw) == 6


class TestTransform:
    def test_skips_rows_without_class(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        classes = {c["case_class"] for c in pipeline.cases}
        assert "" not in classes

    def test_skips_rows_without_number(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        numbers = {c["case_number"] for c in pipeline.cases}
        assert "" not in numbers

    def test_dedupes_by_case_id(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 3 unique case_ids (HC 123456 / REsp 789012 / AgRg 555666);
        # duplicate HC row collapses; 2 invalid rows (missing class / number)
        # are skipped.
        assert len(pipeline.cases) == 3

    def test_court_always_stj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert all(c["court"] == "STJ" for c in pipeline.cases)

    def test_source_field(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert all(c["source"] == "stj_dados_abertos" for c in pipeline.cases)

    def test_rapporteur_rel_only_when_name_present(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # AgRg 555666 has empty relator → no rel. HC 123456 rel appears twice
        # pre-dedup (the duplicate source row), but rapporteur_rels is not
        # run through deduplicate_rows.
        rels_by_target = {r["target_key"] for r in pipeline.rapporteur_rels}
        # 2 distinct targets have rapporteurs (HC + REsp, not AgRg).
        assert len(rels_by_target) == 2

    def test_rapporteur_name_normalized(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # normalize_name uppercases, strips accents etc. All present values
        # should be uppercased-ish in result.
        rapporteurs = {c["rapporteur"] for c in pipeline.cases if c["rapporteur"]}
        for name in rapporteurs:
            assert name == name.strip()
            assert name != ""

    def test_respects_limit(self) -> None:
        driver = MagicMock()
        pipeline = StjPipeline(
            driver=driver, data_dir=str(FIXTURES), limit=1,
        )
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.cases) == 1

    def test_case_id_stability_across_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        expected_hc = _generate_case_id("HC", "123456", "2024")
        hc_cases = [c for c in pipeline.cases if c["case_class"] == "HC"]
        assert hc_cases
        assert hc_cases[0]["case_id"] == expected_hc


class TestLoad:
    def test_empty_state_does_not_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.cases = []
        pipeline.rapporteur_rels = []
        pipeline.load()  # no exception

    def test_populated_state_runs_without_error(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()  # MagicMock driver accepts any cypher calls
