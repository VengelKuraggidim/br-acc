"""Tests for bracc_etl.archival — content-addressed snapshot storage."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from bracc_etl.archival import archive_fetch, restore_snapshot

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Point BRACC_ARCHIVAL_ROOT at a per-test tmp dir and return it."""
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    return root


class TestArchiveFetch:
    def test_writes_content_and_returns_relative_uri(
        self, archival_root: Path,
    ) -> None:
        uri = archive_fetch(
            url="https://example.gov.br/dataset/123",
            content=b"<html>hello</html>",
            content_type="text/html",
            run_id="folha_go_20260417123456",
            source_id="folha_go",
        )
        # URI shape: {source_id}/{YYYY-MM}/{hash12}.{ext}
        parts = uri.split("/")
        assert parts[0] == "folha_go"
        assert parts[1] == "2026-04"
        assert parts[2].endswith(".html")
        # Hash portion is 12 hex chars.
        hash_part = parts[2].removesuffix(".html")
        assert len(hash_part) == 12
        assert all(c in "0123456789abcdef" for c in hash_part)

        # File actually exists under the configured root.
        absolute = archival_root / uri
        assert absolute.exists()
        assert absolute.read_bytes() == b"<html>hello</html>"

    def test_idempotent_same_content_same_uri(
        self, archival_root: Path,
    ) -> None:
        kwargs: dict[str, object] = {
            "url": "https://example.gov.br/dataset/123",
            "content": b'{"key": "value"}',
            "content_type": "application/json",
            "run_id": "pncp_go_20260417120000",
            "source_id": "pncp_go",
        }
        uri1 = archive_fetch(**kwargs)  # type: ignore[arg-type]
        # Second call with same content: same URI, does NOT re-write.
        absolute = archival_root / uri1
        mtime_before = absolute.stat().st_mtime_ns
        uri2 = archive_fetch(**kwargs)  # type: ignore[arg-type]
        assert uri1 == uri2
        mtime_after = absolute.stat().st_mtime_ns
        assert mtime_after == mtime_before, "idempotent write must not touch existing file"

    def test_different_content_different_uri(
        self, archival_root: Path,
    ) -> None:
        uri_a = archive_fetch(
            url="https://x",
            content=b"payload A",
            content_type="text/plain",
            run_id="alego_20260417000000",
            source_id="alego",
        )
        uri_b = archive_fetch(
            url="https://x",
            content=b"payload B",
            content_type="text/plain",
            run_id="alego_20260417000000",
            source_id="alego",
        )
        assert uri_a != uri_b

    @pytest.mark.parametrize(
        ("content_type", "expected_ext"),
        [
            ("text/html", ".html"),
            ("application/json", ".json"),
            ("application/json; charset=utf-8", ".json"),
            ("application/pdf", ".pdf"),
            ("image/png", ".png"),
            ("application/xml", ".xml"),
            ("text/xml", ".xml"),
            ("text/csv", ".csv"),
            ("text/plain", ".txt"),
            ("application/octet-stream", ".bin"),
            ("", ".bin"),
            ("some/weird-type", ".bin"),
        ],
    )
    def test_content_type_extension_mapping(
        self,
        archival_root: Path,
        content_type: str,
        expected_ext: str,
    ) -> None:
        uri = archive_fetch(
            url="https://x",
            content=b"x" + content_type.encode(),
            content_type=content_type,
            run_id="tce_go_20260417000000",
            source_id="tce_go",
        )
        assert uri.endswith(expected_ext)

    def test_respects_custom_archival_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        custom_root = tmp_path / "nested" / "custom-root"
        monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(custom_root))
        uri = archive_fetch(
            url="https://x",
            content=b"custom",
            content_type="text/plain",
            run_id="ssp_go_20260417000000",
            source_id="ssp_go",
        )
        assert (custom_root / uri).exists()

    def test_default_root_is_relative_archival_dir(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # With no env override, the root defaults to ./archival/ relative
        # to the cwd. We cd into tmp_path so the test doesn't scatter
        # files in the real repo.
        monkeypatch.delenv("BRACC_ARCHIVAL_ROOT", raising=False)
        monkeypatch.chdir(tmp_path)
        uri = archive_fetch(
            url="https://x",
            content=b"default-root",
            content_type="text/plain",
            run_id="tcm_go_20260417000000",
            source_id="tcm_go",
        )
        assert (tmp_path / "archival" / uri).exists()

    def test_month_bucket_from_run_id(
        self, archival_root: Path,
    ) -> None:
        uri = archive_fetch(
            url="https://x",
            content=b"bucketed",
            content_type="text/plain",
            run_id="querido_diario_go_20251215093045",
            source_id="querido_diario_go",
        )
        assert uri.split("/")[1] == "2025-12"

    def test_unknown_run_id_shape_falls_back(
        self, archival_root: Path,
    ) -> None:
        # No timestamp suffix — should not crash, just use a sentinel bucket.
        uri = archive_fetch(
            url="https://x",
            content=b"adhoc",
            content_type="text/plain",
            run_id="adhoc-manual",
            source_id="tcmgo_sancoes",
        )
        assert uri.split("/")[1] == "unknown"
        assert (archival_root / uri).exists()

    @pytest.mark.parametrize("bad_arg", ["source_id", "run_id"])
    def test_rejects_empty_required_args(
        self, archival_root: Path, bad_arg: str,
    ) -> None:
        kwargs: dict[str, object] = {
            "url": "https://x",
            "content": b"c",
            "content_type": "text/plain",
            "run_id": "folha_go_20260417000000",
            "source_id": "folha_go",
        }
        kwargs[bad_arg] = ""
        with pytest.raises(ValueError, match=bad_arg):
            archive_fetch(**kwargs)  # type: ignore[arg-type]


class TestRestoreSnapshot:
    def test_round_trip(self, archival_root: Path) -> None:
        content = b"\x00\x01\x02binary\xff\xfe"
        uri = archive_fetch(
            url="https://x",
            content=content,
            content_type="application/octet-stream",
            run_id="state_portal_go_20260101000000",
            source_id="state_portal_go",
        )
        assert restore_snapshot(uri) == content

    def test_missing_uri_raises_file_not_found(
        self, archival_root: Path,
    ) -> None:
        with pytest.raises(FileNotFoundError, match="archival snapshot not found"):
            restore_snapshot("ghost/2026-04/deadbeefcafe.html")
