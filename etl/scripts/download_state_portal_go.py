#!/usr/bin/env python3
"""Download Goias state transparency portal CSVs via the CKAN API.

Populates data/state_portal_go/ with the latest monthly resources for:

- contratos
- fornecedores
- licitantes-sancionados-administrativamente

Usage::

    uv run python scripts/download_state_portal_go.py --output-dir ./data/state_portal_go
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
import httpx

logger = logging.getLogger(__name__)

CKAN_BASE = "https://dadosabertos.go.gov.br/api/3/action"

DATASETS = {
    "contratos": "contratos",
    "fornecedores": "fornecedores",
    "sancoes": "licitantes-sancionados-administrativamente",
}

TIMEOUT = 120


def _latest_csv_resource(client: httpx.Client, package_id: str) -> tuple[str, str] | None:
    try:
        resp = client.get(f"{CKAN_BASE}/package_show", params={"id": package_id})
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("package_show failed for %s: %s", package_id, exc)
        return None
    resources = resp.json().get("result", {}).get("resources", [])
    csv_resources = [
        r for r in resources
        if str(r.get("format", "")).lower() == "csv" and r.get("url")
    ]
    if not csv_resources:
        logger.warning("no CSV resources for %s", package_id)
        return None
    csv_resources.sort(key=lambda r: str(r.get("created", "")), reverse=True)
    top = csv_resources[0]
    return str(top["url"]), str(top.get("name", package_id))


def _download(client: httpx.Client, url: str, dest: Path) -> bool:
    try:
        with client.stream("GET", url, follow_redirects=True) as response:
            response.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=65536):
                    f.write(chunk)
        logger.info("wrote %s (%.1f KB)", dest, dest.stat().st_size / 1024)
    except httpx.HTTPError as exc:
        logger.error("download failed %s: %s", url, exc)
        return False
    else:
        return True


@click.command()
@click.option("--output-dir", default="./data/state_portal_go", help="Output directory")
@click.option(
    "--skip-existing/--no-skip-existing",
    default=True,
    help="Skip files that already exist on disk",
)
def main(output_dir: str, skip_existing: bool) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
        for slug, package_id in DATASETS.items():
            dest = out / f"{slug}_latest.csv"
            if skip_existing and dest.exists() and dest.stat().st_size > 0:
                logger.info("skipping existing %s", dest)
                continue
            resolved = _latest_csv_resource(client, package_id)
            if not resolved:
                continue
            url, name = resolved
            logger.info("fetching %s (%s) -> %s", package_id, name, url)
            _download(client, url, dest)


if __name__ == "__main__":
    main()
