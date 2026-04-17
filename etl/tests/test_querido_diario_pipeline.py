from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.querido_diario import (
    QueridoDiarioPipeline,
    _sha256_text,
    _stable_id,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> QueridoDiarioPipeline:
    return QueridoDiarioPipeline(
        driver=MagicMock(), data_dir=data_dir or str(FIXTURES)
    )


class TestQueridoDiarioMetadata:
    def test_name(self) -> None:
        assert QueridoDiarioPipeline.name == "querido_diario"

    def test_source_id(self) -> None:
        assert QueridoDiarioPipeline.source_id == "querido_diario"


class TestQueridoDiarioTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.acts) == 2
        assert len(pipeline.company_mentions) == 1

    def test_extracts_cnpj_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        mention = pipeline.company_mentions[0]
        assert mention["cnpj"] == "11.222.333/0001-81"
        assert mention["method"] == "text_cnpj_extract"
        assert "extract_span" in mention

    def test_sets_text_status_for_available_text(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        statuses = {row["municipal_gazette_act_id"]: row["text_status"] for row in pipeline.acts}
        assert statuses["qd-1"] == "available"
        assert statuses["qd-2"] == "available"

    def test_does_not_extract_mentions_when_text_forbidden(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline._raw_acts.append({
            "act_id": "qd-3",
            "municipality_name": "Belo Horizonte",
            "municipality_code": "3106200",
            "uf": "MG",
            "date": "2026-02-23",
            "title": "DIARIO OFICIAL",
            "text": "",
            "text_status": "forbidden",
            "txt_url": "s3://bucket/path/file.txt",
            "source_url": "https://qd/3",
            "edition": "125",
        })
        pipeline.transform()

        qd3 = next(row for row in pipeline.acts if row["municipal_gazette_act_id"] == "qd-3")
        assert qd3["text_status"] == "forbidden"
        # Only qd-1 from fixture has CNPJ mention.
        assert len(pipeline.company_mentions) == 1


class TestQueridoDiarioLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()


class TestHelpers:
    def test_stable_id_deterministic(self) -> None:
        assert _stable_id("a", "b") == _stable_id("a", "b")

    def test_stable_id_default_length_is_24(self) -> None:
        assert len(_stable_id("a")) == 24

    def test_sha256_text_deterministic(self) -> None:
        assert _sha256_text("hello") == _sha256_text("hello")

    def test_sha256_text_differs_per_input(self) -> None:
        assert _sha256_text("a") != _sha256_text("b")

    def test_sha256_text_returns_64_hex_chars(self) -> None:
        digest = _sha256_text("anything")
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)


class TestExtract:
    def _write_csv(self, data_dir: Path, content: str) -> None:
        (data_dir / "acts.csv").write_text(content, encoding="utf-8")

    def test_missing_data_dir_returns_silently(self, tmp_path: Path) -> None:
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        assert pipeline._raw_acts == []

    def test_jsonl_records_are_collected(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "querido_diario"
        data_dir.mkdir()
        records = [
            {"act_id": "j-1", "municipality_name": "Recife", "uf": "PE",
             "title": "Titulo 1", "text": "Conteudo 1", "date": "2026-02-01"},
            {"act_id": "j-2", "municipality_name": "Recife", "uf": "PE",
             "title": "Titulo 2", "text": "Conteudo 2", "date": "2026-02-02"},
        ]
        lines = "\n".join(json.dumps(r) for r in records)
        (data_dir / "acts.jsonl").write_text(lines, encoding="utf-8")

        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        ids = {r["act_id"] for r in pipeline._raw_acts}
        assert ids == {"j-1", "j-2"}

    def test_malformed_jsonl_lines_are_skipped(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "querido_diario"
        data_dir.mkdir()
        (data_dir / "acts.jsonl").write_text(
            '{"act_id":"j-1","title":"ok","text":"t","date":"2026-02-01"}\n'
            "{not valid json}\n"
            '{"act_id":"j-2","title":"ok2","text":"t2","date":"2026-02-02"}\n',
            encoding="utf-8",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        assert {r["act_id"] for r in pipeline._raw_acts} == {"j-1", "j-2"}

    def test_json_list_format(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "querido_diario"
        data_dir.mkdir()
        (data_dir / "acts.json").write_text(
            json.dumps(
                [
                    {"act_id": "l-1", "title": "ok", "text": "t", "date": "2026-02-01"},
                ]
            ),
            encoding="utf-8",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        assert {r["act_id"] for r in pipeline._raw_acts} == {"l-1"}

    def test_json_envelope_with_acts_key(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "querido_diario"
        data_dir.mkdir()
        (data_dir / "acts.json").write_text(
            json.dumps(
                {
                    "run": "x",
                    "acts": [
                        {"act_id": "d-1", "title": "ok", "text": "t",
                         "date": "2026-02-01"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        assert {r["act_id"] for r in pipeline._raw_acts} == {"d-1"}

    def test_malformed_json_file_is_tolerated(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "querido_diario"
        data_dir.mkdir()
        (data_dir / "acts.json").write_text("{this is broken", encoding="utf-8")
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        assert pipeline._raw_acts == []

    def test_limit_truncates_records(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "querido_diario"
        data_dir.mkdir()
        (data_dir / "acts.csv").write_text(
            "act_id,title,text,date\n"
            "a,T,X,2026-02-01\n"
            "b,T,X,2026-02-02\n"
            "c,T,X,2026-02-03\n",
            encoding="utf-8",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.limit = 2
        pipeline.extract()
        assert len(pipeline._raw_acts) == 2


class TestTransformEdges:
    def test_skips_row_with_neither_text_nor_title(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_acts = [
            {"act_id": "skip-me", "title": "", "text": "", "date": "2026-02-01"},
            {"act_id": "keep-me", "title": "With Title", "text": "", "date": "2026-02-02"},
        ]
        pipeline.transform()
        ids = {a["municipal_gazette_act_id"] for a in pipeline.acts}
        assert ids == {"keep-me"}

    def test_text_status_forbidden_from_s3_url(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_acts = [
            {"act_id": "s3-row", "title": "T", "text": "",
             "txt_url": "s3://bucket/path/file.txt", "date": "2026-02-01"},
        ]
        pipeline.transform()
        assert pipeline.acts[0]["text_status"] == "forbidden"

    def test_text_status_missing_when_no_text_no_s3(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_acts = [
            {"act_id": "no-text", "title": "T", "text": "", "txt_url": "",
             "date": "2026-02-01"},
        ]
        pipeline.transform()
        assert pipeline.acts[0]["text_status"] == "missing"

    def test_explicit_text_status_is_respected(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_acts = [
            {"act_id": "flagged", "title": "T", "text": "body here",
             "text_status": "forbidden", "date": "2026-02-01"},
        ]
        pipeline.transform()
        assert pipeline.acts[0]["text_status"] == "forbidden"

    def test_stable_id_fallback_when_act_id_missing(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_acts = [
            {"act_id": "", "municipality_code": "3550308", "date": "2026-02-01",
             "title": "Decreto X", "text": "Texto", "source_url": "https://qd/1"},
        ]
        pipeline.transform()
        assert pipeline.acts[0]["municipal_gazette_act_id"] != ""
        assert len(pipeline.acts[0]["municipal_gazette_act_id"]) == 24

    def test_transform_noop_on_empty_raw(self) -> None:
        pipeline = _make_pipeline()
        pipeline._raw_acts = []
        pipeline.transform()
        assert pipeline.acts == []
        assert pipeline.company_mentions == []
