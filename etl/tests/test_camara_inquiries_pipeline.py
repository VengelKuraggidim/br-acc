from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.camara_inquiries import (  # type: ignore[attr-defined]
    CamaraInquiriesPipeline,
    _stable_id,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None) -> CamaraInquiriesPipeline:
    return CamaraInquiriesPipeline(
        driver=MagicMock(), data_dir=data_dir or str(FIXTURES)
    )


def _write_fixture(dir_: Path, name: str, csv: str) -> None:
    (dir_ / name).write_text(csv, encoding="utf-8")


class TestCamaraInquiriesMetadata:
    def test_name(self) -> None:
        assert CamaraInquiriesPipeline.name == "camara_inquiries"

    def test_source_id(self) -> None:
        assert CamaraInquiriesPipeline.source_id == "camara_inquiries"


class TestStableId:
    def test_deterministic(self) -> None:
        assert _stable_id("a", "b") == _stable_id("a", "b")

    def test_default_length_is_24(self) -> None:
        assert len(_stable_id("a")) == 24

    def test_respects_custom_length(self) -> None:
        assert len(_stable_id("a", length=8)) == 8


class TestReadCsvOptional:
    def test_missing_file_returns_empty_frame(self, tmp_path: Path) -> None:
        pipeline = _make_pipeline()
        assert pipeline._read_csv_optional(tmp_path / "nope.csv").empty

    def test_empty_file_returns_empty_frame(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.csv"
        empty.write_text("", encoding="utf-8")
        pipeline = _make_pipeline()
        assert pipeline._read_csv_optional(empty).empty

    def test_valid_file_returns_populated_frame(self, tmp_path: Path) -> None:
        ok = tmp_path / "ok.csv"
        ok.write_text("a,b\n1,2\n", encoding="utf-8")
        pipeline = _make_pipeline()
        df = pipeline._read_csv_optional(ok)
        assert len(df) == 1
        assert list(df.columns) == ["a", "b"]


class TestCamaraInquiriesTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.inquiries) == 2
        assert len(pipeline.requirements) == 2
        assert len(pipeline.sessions) == 1

    def test_extracts_company_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        cnpjs = {m["cnpj"] for m in pipeline.requirement_company_mentions}
        assert "11.222.333/0001-81" in cnpjs
        assert "22.333.444/0001-90" in cnpjs

    def test_author_link_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.requirement_author_cpf_rels) == 1
        assert len(pipeline.requirement_author_name_rels) == 1

    def test_inquiry_without_name_is_skipped(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(
            data_dir,
            "inquiries.csv",
            "inquiry_id,name\nx,\ny,CPI REAL\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.inquiries) == 1
        assert pipeline.inquiries[0]["name"] == "CPI REAL"

    def test_inquiry_stable_id_fallback_when_id_missing(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(
            data_dir,
            "inquiries.csv",
            "inquiry_id,inquiry_code,name\n,ABC,CPI EXAMPLE\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.inquiries) == 1
        assert pipeline.inquiries[0]["inquiry_id"] != ""
        assert len(pipeline.inquiries[0]["inquiry_id"]) == 20  # default stable_id length

    def test_inquiry_kind_is_inferred_from_name_when_missing(
        self, tmp_path: Path
    ) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(
            data_dir,
            "inquiries.csv",
            "inquiry_id,name,kind\n"
            "a,CPMI DO EXEMPLO,\n"  # infers CPMI from name
            "b,CPI DO EXEMPLO,\n",  # infers CPI (default)
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        by_id = {inq["inquiry_id"]: inq for inq in pipeline.inquiries}
        assert by_id["a"]["kind"] == "CPMI"
        assert by_id["b"]["kind"] == "CPI"

    def test_requirement_without_inquiry_id_is_skipped(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(data_dir, "inquiries.csv", "inquiry_id,name\na,CPI\n")
        _write_fixture(
            data_dir,
            "requirements.csv",
            "requirement_id,inquiry_id,text\nreq-1,,orphan\nreq-2,a,linked\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        ids = {r["requirement_id"] for r in pipeline.requirements}
        assert ids == {"req-2"}

    def test_requirement_stable_id_fallback(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(data_dir, "inquiries.csv", "inquiry_id,name\na,CPI\n")
        _write_fixture(
            data_dir,
            "requirements.csv",
            "requirement_id,inquiry_id,type,text\n,a,REQ,pedido completo\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.requirements) == 1
        assert pipeline.requirements[0]["requirement_id"] != ""

    def test_requirement_text_cnpj_extraction_produces_mentions(
        self, tmp_path: Path
    ) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(data_dir, "inquiries.csv", "inquiry_id,name\na,CPI\n")
        _write_fixture(
            data_dir,
            "requirements.csv",
            "requirement_id,inquiry_id,text\n"
            "req-1,a,Menciona a empresa 11.222.333/0001-81 no corpo\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        methods = {m["method"] for m in pipeline.requirement_company_mentions}
        assert "text_cnpj_extract" in methods

    def test_session_without_inquiry_id_is_skipped(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "camara_inquiries"
        data_dir.mkdir()
        _write_fixture(data_dir, "inquiries.csv", "inquiry_id,name\na,CPI\n")
        _write_fixture(
            data_dir,
            "sessions.csv",
            "session_id,inquiry_id,date,topic\n"
            "sess-orphan,,2026-03-01,orphan\n"
            "sess-ok,a,2026-03-01,ok\n",
        )
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        ids = {s["session_id"] for s in pipeline.sessions}
        assert ids == {"sess-ok"}

    def test_transform_is_noop_when_no_inquiries(self, tmp_path: Path) -> None:
        # Empty data dir → `_raw_inquiries` stays empty → transform short-circuits.
        pipeline = _make_pipeline(data_dir=str(tmp_path))
        pipeline.extract()
        pipeline.transform()
        assert pipeline.inquiries == []
        assert pipeline.requirements == []
        assert pipeline.sessions == []


class TestCamaraInquiriesLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
