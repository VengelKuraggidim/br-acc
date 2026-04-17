from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bracc_etl.pipelines.datajud import DatajudPipeline, _pick, _stable_id

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None, **kwargs: object) -> DatajudPipeline:
    return DatajudPipeline(
        driver=MagicMock(),
        data_dir=data_dir or str(FIXTURES),
        **kwargs,  # type: ignore[arg-type]
    )


class TestDatajudMetadata:
    def test_name(self) -> None:
        assert DatajudPipeline.name == "datajud"

    def test_source_id(self) -> None:
        assert DatajudPipeline.source_id == "datajud"


class TestStableId:
    def test_deterministic(self) -> None:
        assert _stable_id("a", "b", "c") == _stable_id("a", "b", "c")

    def test_different_inputs_produce_different_hashes(self) -> None:
        assert _stable_id("a", "b") != _stable_id("a", "c")

    def test_respects_length(self) -> None:
        assert len(_stable_id("x", length=8)) == 8
        assert len(_stable_id("x")) == 24  # default

    def test_separator_prevents_collisions(self) -> None:
        # ("ab", "c") and ("a", "bc") must not hash identically.
        assert _stable_id("ab", "c") != _stable_id("a", "bc")


class TestPick:
    def test_returns_first_non_empty(self) -> None:
        row = pd.Series({"a": "", "b": "hit", "c": "later"})
        assert _pick(row, "a", "b", "c") == "hit"

    def test_returns_empty_when_all_missing(self) -> None:
        row = pd.Series({"a": "", "b": ""})
        assert _pick(row, "a", "b", "c") == ""

    def test_strips_whitespace(self) -> None:
        row = pd.Series({"a": "   ", "b": "  value  "})
        assert _pick(row, "a", "b") == "value"


class TestDatajudTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.cases) == 2
        assert len(pipeline.persons) == 1
        assert len(pipeline.companies) == 2

    def test_party_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.person_case_rels) == 1
        assert len(pipeline.company_case_rels) == 2

    def test_case_fields_mapped(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        case_ids = {c["judicial_case_id"] for c in pipeline.cases}
        assert case_ids == {"jc-1", "jc-2"}
        sample = next(c for c in pipeline.cases if c["judicial_case_id"] == "jc-1")
        assert sample["court"] == "TJSP"
        assert sample["source"] == "datajud"

    def test_transform_falls_back_to_stable_id_when_id_missing(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_cases = pd.DataFrame(
            [
                {
                    "judicial_case_id": "",
                    "case_number": "0001234-56.2026.8.26.0001",
                    "court": "TJSP",
                    "class": "",
                    "subject": "",
                    "filed_at": "2026-02-01",
                    "status": "",
                    "source_url": "",
                }
            ]
        )
        pipeline.transform()
        assert len(pipeline.cases) == 1
        # Not empty, not the original empty string
        generated = pipeline.cases[0]["judicial_case_id"]
        assert generated != ""
        assert len(generated) == 24  # default stable_id length

    def test_transform_parties_skips_row_without_case_id(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_parties = pd.DataFrame(
            [
                {
                    "judicial_case_id": "",
                    "party_name": "Nobody",
                    "party_cpf": "12345678909",
                    "party_cnpj": "",
                    "role": "AUTOR",
                }
            ]
        )
        pipeline.transform()
        assert pipeline.persons == []
        assert pipeline.person_case_rels == []

    def test_transform_parties_skips_short_documents(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_parties = pd.DataFrame(
            [
                {
                    "judicial_case_id": "jc-1",
                    "party_name": "Short",
                    "party_cpf": "123",  # < 11 digits
                    "party_cnpj": "",
                    "role": "AUTOR",
                }
            ]
        )
        pipeline.transform()
        assert pipeline.persons == []
        assert pipeline.companies == []


class TestDatajudExtract:
    def test_missing_data_dir_yields_empty_frames(self, tmp_path: Path) -> None:
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        assert pipeline._raw_cases.empty
        assert pipeline._raw_parties.empty

    def test_dry_run_manifest_is_read_without_raising(self, tmp_path: Path) -> None:
        manifest_dir = tmp_path / "datajud"
        manifest_dir.mkdir()
        (manifest_dir / "dry_run_manifest.json").write_text(
            json.dumps({"message": "credentials missing"}), encoding="utf-8"
        )

        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        # Manifest presence doesn't produce rows, but the branch must not raise.
        assert pipeline._raw_cases.empty

    def test_malformed_dry_run_manifest_is_tolerated(self, tmp_path: Path) -> None:
        manifest_dir = tmp_path / "datajud"
        manifest_dir.mkdir()
        (manifest_dir / "dry_run_manifest.json").write_text(
            "{not valid json", encoding="utf-8"
        )

        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        # JSONDecodeError path falls through with a warning — must not raise.
        assert pipeline._raw_cases.empty

    def test_limit_truncates_dataframes(self) -> None:
        pipeline = _make_pipeline(limit=1)
        pipeline.extract()
        assert len(pipeline._raw_cases) <= 1
        assert len(pipeline._raw_parties) <= 1


class TestDatajudLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_with_empty_collections_short_circuits(self) -> None:
        pipeline = _make_pipeline()
        # Do NOT call extract/transform; all lists stay empty.
        pipeline.load()
        # No session calls made because every branch is guarded by truthiness.
