"""TCE-GO Qlik Sense panel scraper — Selenium-driven DOM extraction.

Two TCE-GO datasets live in Qlik Sense panels (sem REST equivalent):

- **Contas Irregulares** — index of 8 PDFs (one per even year 2010..2024)
  whose names follow ``Ano <YYYY> - Lista das servidores cujas contas foram
  julgadas irregulares.pdf`` (or 2014+ ``Relação de Responsáveis``). The
  panel is a flat 4-column table; each row has a ``Visualizar`` cell whose
  ``<a>`` ``title`` attribute carries the full PDF URL on
  ``portal.tce.go.gov.br``.
- **Fiscalizações em Andamento** — ~50 audit rows with structured columns
  (número, ano, tipo, status, descrição, relator), all rendered inline.

Why Selenium and not the WebSocket Engine API
=============================================

Recon in 2026-04-27 confirmed the engine WS endpoint exists at
``wss://paineis.tce.go.gov.br/app/<appid>`` but the openresty proxy in
front of it requires a ``qlik-csrf-token`` query parameter that is minted
by client-side JS during the bootstrap of the ``/single/`` embed (May
2024+ Qlik Sense CSRF behaviour). Reverse-engineering the bootstrap path
to obtain that token headless is fragile (the token-mint endpoint is not
exposed under a stable URL and the shape changes between Qlik patch
versions). A real browser already does the bootstrap, so the cost-balanced
path is to drive Firefox via Selenium and read the rendered DOM.

System requirements
-------------------

This module requires Firefox + ``geckodriver`` available on PATH (or via
the snap path resolved at runtime). ``selenium`` itself ships under the
optional ``qlik`` extra of the etl package; importing this module without
it raises a clear ImportError only when one of the ``fetch_*_via_selenium``
helpers is invoked. The pure parsers (``parse_irregulares_dom`` /
``parse_fiscalizacoes_dom``) work on captured JSON snapshots and have no
selenium dependency, so unit tests run offline against
``etl/tests/fixtures/tce_go/qlik_dom_*.json``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from selenium.webdriver.remote.webdriver import WebDriver

logger = logging.getLogger(__name__)

# Painéis públicos do portal (descobertos em recon 2026-04-27 via
# WebFetch das páginas /transparencia e /fiscalizacao-dos-controles-internos).
IRREGULARES_APP_ID = "67f0715a-2d34-4d94-9ff4-3d96777233ca"
IRREGULARES_SHEET_ID = "5caeae7c-be2d-4a6f-9180-19ba014cce9f"
FISCALIZACOES_APP_ID = "16a63cbf-32c8-4e12-b8f5-fe4d435d8f79"
FISCALIZACOES_SHEET_ID = "6f2407d5-8e7e-43f0-a0f4-f01009eca6e6"

_PANEL_URL_TMPL = (
    "https://paineis.tce.go.gov.br/single/"
    "?appid={app_id}&sheet={sheet_id}&lang=pt-BR"
)

# Qlik straight-table renders cada célula DUAS vezes: uma <td> com
# rowspan/colspan e dentro um <div class="qv-st-value"> espelhando o
# conteúdo. O scraper coleta as duas e a função abaixo dedupe pares
# consecutivos. Se o pattern mudar (Qlik atualização), refazer o
# fixture e ajustar.
def _dedupe_consecutive(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for cell in cells:
        if out and out[-1]["text"] == cell["text"] and out[-1].get("url") == cell.get("url"):
            continue
        out.append(cell)
    return out


def parse_irregulares_dom(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Convert captured DOM payload into rows for ``irregulares.csv``.

    Output columns match the aliases that ``TceGoPipeline._transform_irregular``
    accepts via ``row_pick``:

    - ``processo``   ← ``"TCE-GO/<ano>"`` (sintético, sem nº de processo no índice)
    - ``nome``       ← descrição da linha (e.g., "Lista das servidores...")
    - ``julgamento`` ← ano (DD/MM/YYYY com 31/12 do ano, pra ``parse_date``)
    - ``cnpj``       ← vazio (índice não traz CNPJ; PDF parsing seria fase 2)
    - ``motivo``     ← vazio
    - ``pdf_url``    ← coluna extra (preservada no CSV, ignorada pelo transform)

    Cada PDF lista internamente os servidores responsabilizados — extrair
    isso requer parser PDF dedicado e fica fora do scope deste loader.
    O índice já entrega 8 :TceGoIrregularAccount nodes (um por ano) com
    URL persistida pra investigação posterior.
    """
    rows: list[dict[str, str]] = []
    for raw_cells in payload.get("rows", []):
        cells = _dedupe_consecutive(raw_cells)
        # Esperado: 4 cols (ano, descrição, Visualizar, vazia trailing)
        if len(cells) < 3:
            logger.warning("[tce_go_qlik] irregulares row com %d cols; skip", len(cells))
            continue
        ano = cells[0]["text"].strip()
        descricao = cells[1]["text"].strip()
        url = cells[2].get("url") or ""
        if not ano or not descricao:
            continue
        rows.append({
            "processo": f"TCE-GO/{ano}",
            "nome": descricao,
            "julgamento": f"31/12/{ano}" if ano.isdigit() else "",
            "cnpj": "",
            "motivo": "",
            "pdf_url": url,
        })
    return rows


def parse_fiscalizacoes_dom(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Convert captured DOM payload into rows for ``fiscalizacoes.csv``.

    O sheet renderiza DOIS straight-tables com schemas diferentes — o
    Selenium scrape pega ambos e cada linha retorna com 7 ou 9 colunas
    após dedupe:

    - **summary** (7 cols): ``[numero, ano, tipo, status, descricao, relator, ""]``
    - **detail**  (9 cols): ``[numero, numero_pai, ano, tipo, jurisdicionado,
      descricao, objetivo, lace, ""]``

    Tabelas distintas → linhas distintas (mesmo ``numero`` aparece em
    ambas com ``descricao`` diferente, então o dedup downstream em
    ``_transform_audits`` pelo composto ``(numero, titulo, inicio)`` mantém
    ambas como :TceGoAudit nodes separados, o que é o comportamento desejado
    (dois pontos de vista do mesmo processo).

    Output columns match aliases que ``_transform_audits`` aceita
    (``numero``, ``descricao``, ``situacao``, ``inicio``, ``jurisdicionado``)
    + colunas extras preservadas (``ano``, ``tipo``, ``relator``, ``objetivo``,
    ``lace``) pra investigação posterior.
    """
    rows: list[dict[str, str]] = []
    for raw_cells in payload.get("rows", []):
        cells = _dedupe_consecutive(raw_cells)
        # Schema discriminado pelo número de colunas após dedup
        if len(cells) >= 8:
            row = _parse_fiscalizacao_detail(cells)
        elif len(cells) >= 6:
            row = _parse_fiscalizacao_summary(cells)
        else:
            logger.warning(
                "[tce_go_qlik] fiscalizacoes row com %d cols; skip", len(cells),
            )
            continue
        if row is None:
            continue
        rows.append(row)
    return rows


def _parse_fiscalizacao_summary(cells: list[dict[str, Any]]) -> dict[str, str] | None:
    numero = cells[0]["text"].strip()
    ano = cells[1]["text"].strip()
    tipo = cells[2]["text"].strip()
    status = cells[3]["text"].strip()
    descricao = cells[4]["text"].strip()
    relator = cells[5]["text"].strip()
    if not numero and not descricao:
        return None
    return {
        "numero": numero,
        "ano": ano,
        "tipo": tipo,
        "situacao": status,
        "descricao": descricao,
        "relator": relator,
        "inicio": f"01/01/{ano}" if ano.isdigit() else "",
        "jurisdicionado": "",
        "objetivo": "",
        "lace": "",
    }


def _parse_fiscalizacao_detail(cells: list[dict[str, Any]]) -> dict[str, str] | None:
    numero = cells[0]["text"].strip()
    # cells[1] é o numero_pai (referência ao processo principal); preservado
    # implicitamente em ``descricao`` via prefixo "Inspeção - Contrato..."
    ano = cells[2]["text"].strip()
    tipo = cells[3]["text"].strip()
    jurisdicionado = cells[4]["text"].strip()
    descricao = cells[5]["text"].strip()
    objetivo = cells[6]["text"].strip()
    lace = cells[7]["text"].strip()
    if not numero and not descricao:
        return None
    return {
        "numero": numero,
        "ano": ano,
        "tipo": tipo,
        "situacao": "",  # detail table não tem coluna status
        "descricao": descricao,
        "relator": "",
        "inicio": f"01/01/{ano}" if ano.isdigit() else "",
        "jurisdicionado": jurisdicionado,
        "objetivo": objetivo,
        "lace": lace,
    }


# ---------------------------------------------------------------------------
# Selenium-driven capture (lazy import — only triggers ImportError when a
# fetch_* helper is actually called without selenium installed).
# ---------------------------------------------------------------------------

# JS injetado no browser pra normalizar a tabela renderizada num shape JSON
# estável (mesmo schema que ``parse_*_dom`` consomem). ``qva-activate=
# "openUrl(cell.url)"`` é o handler Qlik pros links — o título do <a> é
# vinculado a ``{{cell.url}}`` então lemos via ``getAttribute('title')``.
_EXTRACT_DOM_JS = """
    const rows = document.querySelectorAll('.qv-st-data-row, .qv-st-row');
    const out = [];
    rows.forEach(r => {
        const cells = Array.from(r.querySelectorAll('.qv-st-data-cell, .qv-st-value')).map(c => {
            const a = c.querySelector('a[qva-activate]');
            const url = a?.getAttribute('title') || null;
            return { text: c.innerText.trim(), url };
        });
        out.push(cells);
    });
    return out;
"""

# Resolução do binário do Firefox — em distros que empacotam via snap
# (Ubuntu 22.04+), ``/usr/bin/firefox`` é um shell stub que reclama
# se chamado pelo Selenium, então caímos no path real do snap quando
# ele existe. Override via env ``BRACC_FIREFOX_BIN`` quando você tem
# Firefox instalado em outro lugar.
_FIREFOX_FALLBACKS = (
    "/snap/firefox/current/usr/lib/firefox/firefox",
    "/usr/lib/firefox/firefox",
    "/usr/bin/firefox-esr",
)


def _resolve_firefox_binary() -> str | None:
    env = os.environ.get("BRACC_FIREFOX_BIN")
    if env:
        return env
    for candidate in _FIREFOX_FALLBACKS:
        if Path(candidate).is_file():
            return candidate
    return None


def _open_driver(timeout: float = 60.0) -> WebDriver:
    try:
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options
    except ImportError as exc:
        raise ImportError(
            "tce_go_qlik requires the 'qlik' optional dependency: "
            "uv pip install -e 'etl[qlik]' (also needs Firefox + geckodriver "
            "installed at the system level)."
        ) from exc

    opts = Options()
    binary = _resolve_firefox_binary()
    if binary:
        opts.binary_location = binary
    opts.add_argument("--headless")
    opts.add_argument("--width=1600")
    opts.add_argument("--height=1200")
    driver = webdriver.Firefox(options=opts)
    driver.set_page_load_timeout(timeout)
    return driver


def fetch_panel_dom(
    app_id: str,
    sheet_id: str,
    *,
    wait_seconds: float = 45.0,
    settle_seconds: float = 3.0,
) -> dict[str, Any]:
    """Headless Firefox renders the Qlik single embed and dumps its DOM.

    Returns the same payload schema as the captured fixtures —
    ``{"captured_at", "appid", "sheet_id", "url", "rows"}`` — so callers
    can persist it as a snapshot or feed straight into ``parse_*_dom``.

    Args:
        app_id:    Qlik app UUID.
        sheet_id:  Qlik sheet UUID inside the app.
        wait_seconds: max time to wait for the first ``.qv-st-data-cell``
            element to appear in the DOM.
        settle_seconds: extra sleep after the first cell shows up so
            late-arriving rows from paginated hypercubes finish rendering.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC  # noqa: N812
    from selenium.webdriver.support.ui import WebDriverWait

    url = _PANEL_URL_TMPL.format(app_id=app_id, sheet_id=sheet_id)
    driver = _open_driver()
    try:
        logger.info("[tce_go_qlik] rendering %s", url)
        driver.get(url)
        WebDriverWait(driver, wait_seconds).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR, ".qv-st-data-cell, .qv-st-value",
            )),
        )
        time.sleep(settle_seconds)
        rows = driver.execute_script(_EXTRACT_DOM_JS)
    finally:
        driver.quit()

    return {
        "captured_at": datetime.now(UTC).isoformat(),
        "appid": app_id,
        "sheet_id": sheet_id,
        "url": url,
        "rows": rows,
    }


def fetch_irregulares_to_disk(output_dir: Path | str) -> Path:
    """Render the Contas Irregulares panel and write irregulares.csv.

    Persists ``qlik_dom_irregulares.json`` alongside the CSV pra debug e
    pra o archival layer ter o snapshot raw — segue o mesmo padrão que o
    pipeline já tem pros CSVs operator-fed.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = fetch_panel_dom(IRREGULARES_APP_ID, IRREGULARES_SHEET_ID)
    snap_path = output_dir / "qlik_dom_irregulares.json"
    snap_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                         encoding="utf-8")
    rows = parse_irregulares_dom(payload)
    csv_path = output_dir / "irregulares.csv"
    _write_csv(csv_path, rows,
               fieldnames=["processo", "nome", "julgamento", "cnpj",
                           "motivo", "pdf_url"])
    logger.info("[tce_go_qlik] wrote %s (%d rows) + %s",
                csv_path, len(rows), snap_path)
    return csv_path


def fetch_fiscalizacoes_to_disk(output_dir: Path | str) -> Path:
    """Render the Fiscalizações panel and write fiscalizacoes.csv."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = fetch_panel_dom(FISCALIZACOES_APP_ID, FISCALIZACOES_SHEET_ID)
    snap_path = output_dir / "qlik_dom_fiscalizacoes.json"
    snap_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                         encoding="utf-8")
    rows = parse_fiscalizacoes_dom(payload)
    csv_path = output_dir / "fiscalizacoes.csv"
    _write_csv(csv_path, rows,
               fieldnames=["numero", "ano", "tipo", "situacao", "descricao",
                           "relator", "inicio", "jurisdicionado",
                           "objetivo", "lace"])
    logger.info("[tce_go_qlik] wrote %s (%d rows) + %s",
                csv_path, len(rows), snap_path)
    return csv_path


def _write_csv(path: Path, rows: list[dict[str, str]],
               fieldnames: list[str]) -> None:
    import csv
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=";",
                                quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
