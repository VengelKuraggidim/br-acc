from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from bracc_etl.linking_hooks import (
    _split_statements,
    run_post_load_hooks,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestSplitStatements:
    def test_single_statement(self) -> None:
        assert _split_statements("MATCH (n) RETURN n;") == ["MATCH (n) RETURN n"]

    def test_multiple_statements(self) -> None:
        raw = "MATCH (a) RETURN a;\nMATCH (b) RETURN b;"
        assert _split_statements(raw) == [
            "MATCH (a) RETURN a",
            "MATCH (b) RETURN b",
        ]

    def test_strips_line_comments(self) -> None:
        raw = "// leading comment\nMATCH (n) RETURN n;"
        result = _split_statements(raw)
        assert result == ["MATCH (n) RETURN n"]

    def test_strips_inline_comment_line_but_keeps_statement(self) -> None:
        raw = "MATCH (n)\n// inner comment\nRETURN n;"
        assert _split_statements(raw) == ["MATCH (n)\nRETURN n"]

    def test_empty_input_returns_empty(self) -> None:
        assert _split_statements("") == []
        assert _split_statements("  \n  ") == []
        assert _split_statements(";;;") == []

    def test_skips_blank_segments(self) -> None:
        # Extra semicolons shouldn't produce empty strings.
        raw = "MATCH (a) RETURN a;;\nMATCH (b) RETURN b;"
        assert _split_statements(raw) == [
            "MATCH (a) RETURN a",
            "MATCH (b) RETURN b",
        ]

    def test_only_comments_returns_empty(self) -> None:
        assert _split_statements("// just a comment\n// another one") == []


class TestRunPostLoadHooks:
    def test_community_tier_skips_all(self, caplog: pytest.LogCaptureFixture) -> None:
        driver = MagicMock()
        with caplog.at_level(logging.INFO):
            run_post_load_hooks(
                driver=driver,
                source="cnpj",
                neo4j_database="neo4j",
                linking_tier="community",
            )
        driver.session.assert_not_called()
        assert any("skipped" in rec.message.lower() for rec in caplog.records)

    def test_unknown_source_no_op(self, caplog: pytest.LogCaptureFixture) -> None:
        driver = MagicMock()
        with caplog.at_level(logging.INFO):
            run_post_load_hooks(
                driver=driver,
                source="nonexistent_source",
                neo4j_database="neo4j",
                linking_tier="full",
            )
        driver.session.assert_not_called()
        assert any(
            "no post-load linking hook" in rec.message.lower()
            for rec in caplog.records
        )

    def test_missing_script_warns_and_continues(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Point repo_root at tmp (no scripts/ dir) so the hook sees
        # missing files and warns.
        import bracc_etl.linking_hooks as mod

        fake_file = tmp_path / "etl" / "src" / "bracc_etl" / "linking_hooks.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.write_text("# placeholder")
        monkeypatch.setattr(mod, "__file__", str(fake_file))

        driver = MagicMock()
        with caplog.at_level(logging.WARNING):
            run_post_load_hooks(
                driver=driver,
                source="cnpj",
                neo4j_database="neo4j",
                linking_tier="full",
            )

        assert any(
            "missing (skipped)" in rec.message.lower()
            for rec in caplog.records
        )
        driver.session.assert_not_called()

    def test_tier_defaults_to_full_when_invalid(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        # "garbage" → tier coerced to "full", so it tries (and skips on
        # missing scripts). No crash.
        driver = MagicMock()
        with caplog.at_level(logging.INFO):
            run_post_load_hooks(
                driver=driver,
                source="nonexistent_source",
                neo4j_database="neo4j",
                linking_tier="garbage",
            )
        # Because source is unknown, we still expect no session use.
        driver.session.assert_not_called()

    def test_cnpj_source_maps_to_two_scripts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import bracc_etl.linking_hooks as mod

        # Simulate repo_root = tmp_path. __file__ is at parents[3] level.
        fake_file = tmp_path / "etl" / "src" / "bracc_etl" / "linking_hooks.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.write_text("# placeholder")
        monkeypatch.setattr(mod, "__file__", str(fake_file))

        # Place both scripts at the expected path.
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "link_partners_probable.cypher").write_text(
            "MATCH (n) RETURN 1;",
            encoding="utf-8",
        )
        (scripts_dir / "link_persons.cypher").write_text(
            "MATCH (m) RETURN 2;",
            encoding="utf-8",
        )

        driver = MagicMock()
        session = MagicMock()
        driver.session.return_value.__enter__.return_value = session

        run_post_load_hooks(
            driver=driver,
            source="cnpj",
            neo4j_database="neo4j",
            linking_tier="full",
        )

        # Two scripts × one statement each = two session.run calls.
        assert session.run.call_count == 2

    @pytest.mark.parametrize(
        "source",
        ["tse", "transparencia", "camara", "senado", "senado_cpis", "tse_filiados"],
    )
    def test_person_sources_map_to_link_persons(
        self,
        source: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import bracc_etl.linking_hooks as mod

        fake_file = tmp_path / "etl" / "src" / "bracc_etl" / "linking_hooks.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.write_text("# placeholder")
        monkeypatch.setattr(mod, "__file__", str(fake_file))

        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "link_persons.cypher").write_text(
            "MATCH (p:Person) RETURN p;",
            encoding="utf-8",
        )

        driver = MagicMock()
        session = MagicMock()
        driver.session.return_value.__enter__.return_value = session

        run_post_load_hooks(
            driver=driver,
            source=source,
            neo4j_database="neo4j",
            linking_tier="full",
        )
        assert session.run.call_count == 1
