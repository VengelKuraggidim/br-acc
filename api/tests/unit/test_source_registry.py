from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from bracc.services.source_registry import (
    SourceRegistryEntry,
    _str_to_bool,
    get_registry_path,
    load_source_registry,
    source_registry_summary,
)

if TYPE_CHECKING:
    from pathlib import Path

CSV_HEADER = (
    "source_id,name,category,tier,status,implementation_state,load_state,"
    "frequency,in_universe_v1,primary_url,pipeline_id,owner_agent,access_mode,"
    "notes,public_access_mode,discovery_status,last_seen_url,"
    "cadence_expected,cadence_observed,quality_status\n"
)


def _write_registry(tmp_path: Path, rows: list[str]) -> Path:
    path = tmp_path / "registry.csv"
    path.write_text(CSV_HEADER + "\n".join(rows) + "\n", encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _clear_registry_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRACC_SOURCE_REGISTRY_PATH", raising=False)


class TestStrToBool:
    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "True", "yes", "Y", " true "])
    def test_truthy(self, value: str) -> None:
        assert _str_to_bool(value) is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "N", "maybe", " "])
    def test_falsy(self, value: str) -> None:
        assert _str_to_bool(value) is False


class TestGetRegistryPath:
    def test_env_override(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        custom = tmp_path / "alt.csv"
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(custom))
        assert get_registry_path() == custom

    def test_env_whitespace_falls_back_to_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", "   ")
        path = get_registry_path()
        assert path.name == "source_registry_br_v1.csv"
        assert path.parent.name == "docs"

    def test_default_points_to_repo_docs(self) -> None:
        path = get_registry_path()
        assert path.name == "source_registry_br_v1.csv"
        assert "docs" in path.parts


class TestLoadSourceRegistry:
    def test_missing_file_returns_empty_list(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setenv(
            "BRACC_SOURCE_REGISTRY_PATH", str(tmp_path / "does-not-exist.csv")
        )
        assert load_source_registry() == []

    def test_parses_entries_and_sorts_by_id(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        rows = [
            # Reversed alphabetical order to verify sort
            "zeta,Zeta source,cat,P1,loaded,implemented,loaded,monthly,true,https://z.example,zeta,AgentZ,file,note,,monitored,https://z.example,monthly,monthly,healthy",  # noqa: E501
            "alpha,Alpha source,cat,P0,loaded,implemented,loaded,monthly,true,https://a.example,alpha,AgentA,file,,,,,,,healthy",  # noqa: E501
        ]
        path = _write_registry(tmp_path, rows)
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(path))

        entries = load_source_registry()

        assert [e.id for e in entries] == ["alpha", "zeta"]
        assert entries[0].name == "Alpha source"
        assert entries[0].primary_url == "https://a.example"
        assert entries[0].in_universe_v1 is True

    def test_fallback_columns(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # public_access_mode, last_seen_url, cadence_expected, quality_status are
        # empty — loader should fall back to access_mode, primary_url,
        # frequency, and status respectively.
        rows = [
            "fallback,Fallback,cat,P1,loaded,implemented,loaded,monthly,true,"
            "https://f.example,fallback,AgentF,file,n,,,,,,",
        ]
        path = _write_registry(tmp_path, rows)
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(path))

        entry = load_source_registry()[0]

        assert entry.public_access_mode == "file"
        assert entry.last_seen_url == "https://f.example"
        assert entry.cadence_expected == "monthly"
        assert entry.quality_status == "loaded"
        assert entry.discovery_status == "discovered"

    def test_in_universe_v1_false_is_parsed(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        rows = [
            "out,Out of universe,cat,P2,not_built,not_implemented,not_loaded,"
            "monthly,false,https://o.example,,AgentO,api,,,,,,,not_built",
        ]
        path = _write_registry(tmp_path, rows)
        monkeypatch.setenv("BRACC_SOURCE_REGISTRY_PATH", str(path))

        entry = load_source_registry()[0]
        assert entry.in_universe_v1 is False

    def test_real_registry_loads_nonempty(self) -> None:
        # Sanity: the tracked CSV in docs/ must load with at least one entry.
        # Guards against schema changes that would break the dataclass mapping.
        # We don't assert exact counts here — that would duplicate test_health.
        assert "BRACC_SOURCE_REGISTRY_PATH" not in os.environ
        entries = load_source_registry()
        assert entries
        assert all(isinstance(e, SourceRegistryEntry) for e in entries)


class TestToPublicDict:
    def test_roundtrip_contains_all_public_fields(self) -> None:
        entry = SourceRegistryEntry(
            id="x",
            name="X",
            category="cat",
            tier="P1",
            status="loaded",
            implementation_state="implemented",
            load_state="loaded",
            frequency="monthly",
            in_universe_v1=True,
            primary_url="https://x.example",
            pipeline_id="x",
            owner_agent="AgentX",
            access_mode="api",
            public_access_mode="api",
            discovery_status="monitored",
            last_seen_url="https://x.example",
            cadence_expected="monthly",
            cadence_observed="monthly",
            quality_status="healthy",
            notes="ok",
        )
        public = entry.to_public_dict()
        assert public["id"] == "x"
        assert public["in_universe_v1"] is True
        # Every dataclass field should appear in the dict.
        expected_keys = {
            "id", "name", "category", "tier", "status", "implementation_state",
            "load_state", "frequency", "in_universe_v1", "primary_url",
            "pipeline_id", "owner_agent", "access_mode", "public_access_mode",
            "discovery_status", "last_seen_url", "cadence_expected",
            "cadence_observed", "quality_status", "notes",
        }
        assert set(public.keys()) == expected_keys


class TestSourceRegistrySummary:
    def _entry(self, **overrides: object) -> SourceRegistryEntry:
        defaults: dict[str, object] = {
            "id": "x",
            "name": "X",
            "category": "cat",
            "tier": "P1",
            "status": "loaded",
            "implementation_state": "implemented",
            "load_state": "loaded",
            "frequency": "monthly",
            "in_universe_v1": True,
            "primary_url": "",
            "pipeline_id": "",
            "owner_agent": "",
            "access_mode": "",
            "public_access_mode": "",
            "discovery_status": "monitored",
            "last_seen_url": "",
            "cadence_expected": "",
            "cadence_observed": "",
            "quality_status": "healthy",
            "notes": "",
        }
        defaults.update(overrides)
        return SourceRegistryEntry(**defaults)  # type: ignore[arg-type]

    def test_excludes_non_universe_entries(self) -> None:
        entries = [
            self._entry(id="a"),
            self._entry(id="b", in_universe_v1=False),
        ]
        summary = source_registry_summary(entries)
        assert summary["universe_v1_sources"] == 1

    def test_counts_buckets(self) -> None:
        entries = [
            self._entry(id="loaded1"),
            self._entry(id="loaded2"),
            self._entry(id="partial", status="partial", load_state="partial"),
            self._entry(id="stale", status="stale", load_state="partial"),
            self._entry(
                id="blocked",
                status="blocked_external",
                load_state="not_loaded",
            ),
            self._entry(
                id="failed",
                status="quality_fail",
                load_state="not_loaded",
            ),
            self._entry(
                id="scaffold",
                status="not_built",
                implementation_state="not_implemented",
                load_state="not_loaded",
                discovery_status="discovered_uningested",
            ),
        ]
        summary = source_registry_summary(entries)
        assert summary == {
            "universe_v1_sources": 7,
            "implemented_sources": 6,
            "loaded_sources": 2,
            "healthy_sources": 2,
            "stale_sources": 1,
            "blocked_external_sources": 1,
            "quality_fail_sources": 1,
            "discovered_uningested_sources": 1,
        }

    def test_implementation_state_not_implemented_counts_as_uningested(self) -> None:
        # Even when discovery_status is "monitored", not_implemented should
        # count as discovered_uningested.
        entries = [
            self._entry(
                id="impl-pending",
                status="partial",
                implementation_state="not_implemented",
                load_state="not_loaded",
                discovery_status="monitored",
            ),
        ]
        summary = source_registry_summary(entries)
        assert summary["discovered_uningested_sources"] == 1
