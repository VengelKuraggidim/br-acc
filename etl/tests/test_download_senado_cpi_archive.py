from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from download_senado_cpi_archive import (  # type: ignore[import-not-found]
    ArchiveSource,
    parse_archive_text,
)


def test_parse_archive_text_extracts_rows() -> None:
    source = ArchiveSource(
        url="https://example.test/archive.pdf",
        kind="CPI",
        house="senado",
        period_start="1946-01-01",
        period_end="1975-12-31",
    )
    raw = (
        "RSF 48/1975 | 17/09/1975 | Mobral | Senador Franco Montoro MDB/SP | "
        "SIM SIM SIM | 24/06/1976 | Comissao concluida | "
        "RQN 15/1976 | 03/03/1977 | Situacao da mulher | Senador Nelson Carneiro PMDB/RJ | "
        "SIM SIM SIM | 05/10/1977 | Comissao concluida"
    )

    inquiries, requirements, sessions = parse_archive_text(
        text=raw,
        source=source,
        run_id="test_run",
    )

    assert len(inquiries) == 2
    assert len(requirements) == 2
    assert len(sessions) == 2

    assert inquiries[0]["inquiry_code"] == "RSF 48/1975"
    assert requirements[0]["date"] == "1975-09-17"
    assert sessions[0]["date"] == "1976-06-24"
    assert requirements[0]["source_system"] == "senado_archive"


def test_parse_archive_text_without_end_date_creates_no_session() -> None:
    source = ArchiveSource(
        url="https://example.test/archive.pdf",
        kind="CPMI",
        house="congresso",
        period_start="1967-01-01",
        period_end="2016-12-31",
    )
    raw = (
        "RCN 4/1987 | 18/11/1987 | Acidente radioativo em Goiania | "
        "Deputado Joao Natal PMDB/GO | NAO NAO NAO | Decurso de prazo"
    )

    inquiries, requirements, sessions = parse_archive_text(
        text=raw,
        source=source,
        run_id="test_run",
    )

    assert len(inquiries) == 1
    assert len(requirements) == 1
    assert len(sessions) == 0
    assert inquiries[0]["kind"] == "CPMI"
