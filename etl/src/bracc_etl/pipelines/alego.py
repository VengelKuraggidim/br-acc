"""ETL pipeline for ALEGO (Assembleia Legislativa de Goias).

ALEGO's transparency portal exposes an undocumented-but-public JSON API
under https://transparencia.al.go.leg.br/api/transparencia/ (discovered by
inspecting the Angular SPA bundle). The ``fetch_to_disk`` helper in this
module downloads the three feeds the pipeline needs and writes them as
the CSV filenames the offline ``extract`` path expects:

- ``deputados.csv``           -> StateLegislator nodes (UF=GO)
- ``cota_parlamentar.csv``    -> LegislativeExpense nodes + GASTOU_COTA_GO rels
- ``proposicoes.csv``         -> LegislativeProposition nodes

The pipeline's ``extract`` still reads CSVs from ``data/alego/`` so existing
fixture-based tests keep working; the new CLI wrapper at
``scripts/download_alego.py`` is the canonical way to refresh the data.

Upstream note: ``alegodigital.al.go.leg.br`` is the SPL (Sistema de Produção
Legislativa) site and has ``Disallow: /`` in its robots.txt, so we deliberately
do not scrape it. ``transparencia.al.go.leg.br`` only disallows specific
``quadro-de-remuneracao`` paths, which we also avoid.

Data source: https://transparencia.al.go.leg.br/
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    mask_cpf,
    normalize_name,
    parse_brl_flexible,
    parse_date,
    row_pick,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Live JSON API (transparencia.al.go.leg.br)
# ---------------------------------------------------------------------------

_API_BASE = "https://transparencia.al.go.leg.br"
_TIMEOUT = 30
# Be polite: the portal is a public transparency site but we still want to
# keep load low. ~1 req/s is well within anything a browser would issue.
_RATE_LIMIT_SECONDS = 1.0

# Minimum set of endpoints we know are public and stable (discovered by
# reading the Angular SPA bundle's embedded Rails-style route table and
# confirming 200 responses live). Kept as a module-level constant so tests
# and the CLI wrapper can reference them without re-reading the code.
_ENDPOINTS: dict[str, str] = {
    "deputados_periodos": "/api/transparencia/verbas_indenizatorias/periodos",
    "deputados_listing": "/api/transparencia/verbas_indenizatorias/deputados",
    "deputado_exibir": "/api/transparencia/verbas_indenizatorias/exibir",
    "processos_recentes": "/api/transparencia/processos/recentes",
    "proposicoes_mais_votadas": (
        "/api/transparencia/processos/proposicoes-mais-votadas"
    ),
}

# Fallback content-type when the ALEGO API omite o header (raro). A API
# devolve sempre ``application/json`` na prática, mas archival é
# content-addressed, então o único efeito seria a extensão do arquivo.
_ALEGO_JSON_CONTENT_TYPE = "application/json"
# Coluna privada nos DataFrames que carrega a URI do snapshot archival
# por-linha (prefixo ``__`` não colide com campos reais da API). A
# ``transform`` lê essa coluna pra popular ``source_snapshot_uri`` em
# cada ``attach_provenance`` e a filtra de volta antes de chegar ao
# Neo4jBatchLoader, porque archival é opt-in e não faz parte dos schemas
# de StateLegislator/LegislativeExpense/LegislativeProposition.
_SNAPSHOT_COLUMN = "__snapshot_uri"


def _http_get_json(
    path: str,
    params: dict[str, Any] | None = None,
    *,
    client: httpx.Client | None = None,
) -> Any:
    """Fetch a JSON endpoint from the ALEGO transparency API.

    Returns the decoded payload or ``None`` on failure. A caller can pass its
    own ``httpx.Client`` to reuse a single connection pool across many
    requests (important when iterating over all deputies x months).
    """
    payload, _content, _ctype = _http_get_json_raw(path, params, client=client)
    return payload


def _http_get_json_raw(
    path: str,
    params: dict[str, Any] | None = None,
    *,
    client: httpx.Client | None = None,
) -> tuple[Any, bytes | None, str | None]:
    """Fetch a JSON endpoint, returning ``(payload, content_bytes, content_type)``.

    Versão archival-aware de :func:`_http_get_json`. Quando a chamada falha
    (HTTP ou JSON inválido), ``payload`` é ``None`` e ``content`` também —
    nada pra arquivar nesses casos. Em sucesso, ``content`` carrega os bytes
    crus exatamente como o servidor devolveu pra :func:`archive_fetch`
    preservar sem reprocessamento.
    """
    url = f"{_API_BASE}{path}"
    try:
        if client is not None:
            resp = client.get(url, params=params or None)
        else:
            with httpx.Client(timeout=_TIMEOUT) as one_shot:
                resp = one_shot.get(url, params=params or None)
        resp.raise_for_status()
        content = resp.content
        content_type = resp.headers.get("content-type", _ALEGO_JSON_CONTENT_TYPE)
        return resp.json(), content, content_type
    except (httpx.HTTPError, json.JSONDecodeError) as exc:
        logger.warning(
            "[alego] API request failed (%s params=%s): %s", path, params, exc
        )
        return None, None, None


def _iter_periodos(periods: Any) -> list[tuple[int, int]]:
    """Normalise the ``verbas_indenizatorias/periodos`` payload into (ano, mes)
    tuples. The upstream shape is
    ``[{"ano": 2025, "meses": [4, 3, ...]}, ...]``."""
    out: list[tuple[int, int]] = []
    if not isinstance(periods, list):
        return out
    for entry in periods:
        if not isinstance(entry, dict):
            continue
        ano = entry.get("ano")
        meses = entry.get("meses") or []
        if not isinstance(ano, int) or not isinstance(meses, list):
            continue
        for mes in meses:
            if isinstance(mes, int) and 1 <= mes <= 12:
                out.append((ano, mes))
    # Newest first so ``limit`` gives the most recent data.
    out.sort(reverse=True)
    return out


def _write_csv(path: Path, header: list[str], rows: list[dict[str, Any]]) -> None:
    """Write ``rows`` to ``path`` as ``;``-delimited CSV matching the existing
    ALEGO fixture format and what ``AlegoPipeline.extract`` expects."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=header, delimiter=";", extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in header})


def _fetch_deputados_listing(
    client: httpx.Client, periodos: list[tuple[int, int]]
) -> list[dict[str, Any]]:
    """Return the distinct deputy roster. The API requires ``ano`` and ``mes``;
    we walk newest-first until we get a non-empty list."""
    for ano, mes in periodos:
        payload = _http_get_json(
            _ENDPOINTS["deputados_listing"],
            params={"ano": ano, "mes": mes},
            client=client,
        )
        if isinstance(payload, list) and payload:
            logger.info(
                "[alego] deputy roster taken from %d-%02d (n=%d)",
                ano, mes, len(payload),
            )
            return [p for p in payload if isinstance(p, dict)]
    logger.warning("[alego] no deputy roster found across any period")
    return []


def _fetch_deputado_exibir(
    client: httpx.Client, deputado_id: int, ano: int, mes: int
) -> dict[str, Any] | None:
    payload = _http_get_json(
        _ENDPOINTS["deputado_exibir"],
        params={"deputado_id": deputado_id, "ano": ano, "mes": mes},
        client=client,
    )
    return payload if isinstance(payload, dict) else None


def _flatten_cota_lancamentos(
    deputado_name: str, ano: int, mes: int, exibir: dict[str, Any]
) -> list[dict[str, Any]]:
    """Convert an ``/exibir`` payload's ``grupos[].subgrupos[].lancamentos[]``
    tree into flat rows matching the ``cota_parlamentar.csv`` schema."""
    out: list[dict[str, Any]] = []
    grupos = exibir.get("grupos") or []
    if not isinstance(grupos, list):
        return out
    for grupo in grupos:
        if not isinstance(grupo, dict):
            continue
        tipo_grupo = str(grupo.get("descricao") or "").strip()
        subgrupos = grupo.get("subgrupos") or []
        if not isinstance(subgrupos, list):
            continue
        for sub in subgrupos:
            if not isinstance(sub, dict):
                continue
            tipo_sub = str(sub.get("descricao") or "").strip()
            tipo_despesa = (
                f"{tipo_grupo} / {tipo_sub}" if tipo_sub else tipo_grupo
            )
            for lanc in sub.get("lancamentos") or []:
                if not isinstance(lanc, dict):
                    continue
                forn = lanc.get("fornecedor") or {}
                if not isinstance(forn, dict):
                    continue
                data_raw = str(forn.get("data") or "").strip()
                # Keep only the YYYY-MM-DD part; parse_date downstream is
                # forgiving but the fixture style used dd/mm/yyyy too.
                data_iso = data_raw[:10] if data_raw else f"{ano:04d}-{mes:02d}-01"
                out.append({
                    "deputado": deputado_name,
                    "fornecedor": str(forn.get("nome") or "").strip(),
                    "cnpj_fornecedor": str(forn.get("cnpj_cpf") or "").strip(),
                    "tipo_despesa": tipo_despesa,
                    "valor": str(forn.get("valor_indenizado")
                                 or forn.get("valor_apresentado") or ""),
                    "data": data_iso,
                    "numero_documento": str(forn.get("numero") or "").strip(),
                    "ano": ano,
                    "mes": mes,
                })
    return out


def _fetch_proposicoes(client: httpx.Client) -> list[dict[str, Any]]:
    """Combine ``processos/recentes`` + ``proposicoes-mais-votadas`` into a
    single deduplicated list of ``proposicoes.csv`` rows.

    Both endpoints return the same item shape (``autores``, ``assunto``,
    ``numero``, ``data_autuacao``, ``ementa``, ...). We keep the first
    occurrence of each ``numero``.
    """
    seen: dict[str, dict[str, Any]] = {}

    recentes = _http_get_json(_ENDPOINTS["processos_recentes"], client=client)
    # ``recentes`` is a list-of-lists (categorized); flatten one level.
    if isinstance(recentes, list):
        for group in recentes:
            if isinstance(group, list):
                for item in group:
                    if isinstance(item, dict) and item.get("numero"):
                        seen.setdefault(str(item["numero"]), item)
            elif isinstance(group, dict) and group.get("numero"):
                seen.setdefault(str(group["numero"]), group)

    mais_votadas = _http_get_json(
        _ENDPOINTS["proposicoes_mais_votadas"], client=client
    )
    if isinstance(mais_votadas, dict):
        for item in mais_votadas.get("processos") or []:
            if isinstance(item, dict) and item.get("numero"):
                seen.setdefault(str(item["numero"]), item)

    rows: list[dict[str, Any]] = []
    for item in seen.values():
        autores = item.get("autores") or []
        autor = (
            "; ".join(a for a in autores if isinstance(a, str))
            if isinstance(autores, list)
            else str(autores or "")
        )
        rows.append({
            "numero": str(item.get("numero") or "").strip(),
            "titulo": str(item.get("assunto") or "").strip(),
            "ementa": str(item.get("ementa") or "").strip(),
            "autor": autor,
            "data": str(item.get("data_autuacao") or "").strip(),
            "situacao": str(item.get("situacao") or "").strip(),
            "a_favor": item.get("a_favor"),
            "contra": item.get("contra"),
        })
    return rows


def fetch_to_disk(
    output_dir: Path | str,
    limit: int | None = None,
    *,
    max_expense_months: int | None = 3,
) -> list[Path]:
    """Download ALEGO transparency feeds into ``output_dir`` as CSV files.

    Generates three files (same names ``AlegoPipeline.extract`` reads):

    - ``deputados.csv``           — current/most-recent deputy roster with
                                    party sigla pulled from /exibir.
    - ``cota_parlamentar.csv``    — flattened ``verbas indenizatorias``
                                    lancamentos for the most recent
                                    ``max_expense_months`` month(s) across
                                    every deputy.
    - ``proposicoes.csv``         — union of ``processos/recentes`` and
                                    ``proposicoes-mais-votadas``.

    ``limit`` caps the number of deputies pulled (useful for smoke tests).
    ``max_expense_months`` caps the months iterated for cota parlamentar
    (default 3 ≈ one quarter); pass ``None`` to walk every available period
    but be aware the API has ~180 (ano, mes) pairs from 2011 to 2026.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    with httpx.Client(
        timeout=_TIMEOUT,
        headers={"Accept": "application/json", "User-Agent": "br-acc-etl/1.0"},
    ) as client:
        # --- 1. periodos ---------------------------------------------------
        raw_periods = _http_get_json(
            _ENDPOINTS["deputados_periodos"], client=client
        )
        periodos = _iter_periodos(raw_periods)
        if not periodos:
            logger.warning(
                "[alego] /verbas_indenizatorias/periodos returned nothing; "
                "aborting fetch."
            )
            return written
        time.sleep(_RATE_LIMIT_SECONDS)

        # --- 2. deputados listing -----------------------------------------
        raw_deputies = _fetch_deputados_listing(client, periodos)
        if limit is not None:
            raw_deputies = raw_deputies[:limit]
        time.sleep(_RATE_LIMIT_SECONDS)

        # --- 3. expenses + party enrichment -------------------------------
        cota_rows: list[dict[str, Any]] = []
        deputy_rows: list[dict[str, Any]] = []
        target_periods = (
            periodos[:max_expense_months] if max_expense_months else periodos
        )

        for dep in raw_deputies:
            dep_id = dep.get("id")
            dep_name = str(dep.get("nome") or "").strip()
            if not isinstance(dep_id, int) or not dep_name:
                continue

            party = ""
            # hit the first period that has data; also gives us party sigla
            for ano, mes in target_periods:
                exibir = _fetch_deputado_exibir(client, dep_id, ano, mes)
                time.sleep(_RATE_LIMIT_SECONDS)
                if not exibir:
                    continue
                if not party:
                    dep_block = exibir.get("deputado") or {}
                    if isinstance(dep_block, dict):
                        party = str(dep_block.get("partido") or "").strip()
                cota_rows.extend(
                    _flatten_cota_lancamentos(dep_name, ano, mes, exibir)
                )

            deputy_rows.append({
                "nome": dep_name,
                "cpf": "",  # Upstream does not publish CPF.
                "partido": party,
                "legislatura": "",
                "deputado_id_alego": dep_id,
            })

        # --- 4. proposicoes -----------------------------------------------
        prop_rows = _fetch_proposicoes(client)
        time.sleep(_RATE_LIMIT_SECONDS)

    # --- 5. write CSVs ----------------------------------------------------
    dep_path = output_dir / "deputados.csv"
    _write_csv(
        dep_path,
        ["nome", "cpf", "partido", "legislatura", "deputado_id_alego"],
        deputy_rows,
    )
    written.append(dep_path)
    logger.info("[alego] wrote %s (%d deputies)", dep_path, len(deputy_rows))

    cota_path = output_dir / "cota_parlamentar.csv"
    _write_csv(
        cota_path,
        [
            "deputado", "fornecedor", "cnpj_fornecedor", "tipo_despesa",
            "valor", "data", "numero_documento", "ano", "mes",
        ],
        cota_rows,
    )
    written.append(cota_path)
    logger.info(
        "[alego] wrote %s (%d lancamentos)", cota_path, len(cota_rows)
    )

    prop_path = output_dir / "proposicoes.csv"
    _write_csv(
        prop_path,
        ["numero", "titulo", "ementa", "autor", "data", "situacao",
         "a_favor", "contra"],
        prop_rows,
    )
    written.append(prop_path)
    logger.info(
        "[alego] wrote %s (%d proposicoes)", prop_path, len(prop_rows)
    )

    return written


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _snapshot_from_row(row: pd.Series) -> str | None:
    """Leitura defensiva do URI de snapshot presente numa linha do DataFrame.

    Retorna ``None`` quando a coluna está ausente, vazia, ou não-string — o
    caller decide o que fazer a partir daí (normalmente: não passar
    ``snapshot_uri`` pro ``attach_provenance``).
    """
    raw = row.get(_SNAPSHOT_COLUMN)
    if isinstance(raw, str) and raw:
        return raw
    return None


class AlegoPipeline(Pipeline):
    """Scaffold pipeline for ALEGO transparency data."""

    name = "alego"
    source_id = "alego"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        # Quantos meses-período puxar do endpoint /verbas_indenizatorias/exibir
        # no fallback online. Default 3 cobre o trimestre corrente (uso normal
        # pra não bombardear a API), 12 cobre a legislatura anual, None baixa
        # tudo (~180 períodos 2011→hoje). Consumido via kwargs.pop pra o
        # runner poder injetar sem quebrar outros pipelines.
        self._max_expense_months: int | None = kwargs.pop("max_expense_months", 3)
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_deputados: pd.DataFrame = pd.DataFrame()
        self._raw_cota: pd.DataFrame = pd.DataFrame()
        self._raw_propositions: pd.DataFrame = pd.DataFrame()

        self.legislators: list[dict[str, Any]] = []
        self.expenses: list[dict[str, Any]] = []
        self.propositions: list[dict[str, Any]] = []
        self.expense_rels: list[dict[str, Any]] = []

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        for sep in (";", ","):
            try:
                df = pd.read_csv(
                    path, sep=sep, dtype=str, keep_default_na=False,
                    encoding="utf-8", engine="python", on_bad_lines="skip",
                )
                if len(df.columns) > 1:
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        try:
            return pd.read_csv(
                path, sep=";", dtype=str, keep_default_na=False,
                encoding="latin-1", engine="python", on_bad_lines="skip",
            )
        except (OSError, pd.errors.ParserError) as exc:
            logger.warning("[alego] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "alego"
        have_local = False
        if src_dir.exists():
            self._raw_deputados = self._read_csv_optional(src_dir / "deputados.csv")
            self._raw_cota = self._read_csv_optional(src_dir / "cota_parlamentar.csv")
            self._raw_propositions = self._read_csv_optional(
                src_dir / "proposicoes.csv"
            )
            have_local = not (
                self._raw_deputados.empty
                and self._raw_cota.empty
                and self._raw_propositions.empty
            )
        else:
            logger.warning(
                "[alego] expected directory %s missing; will try ALEGO API.",
                src_dir,
            )

        # Online fallback: se não achou CSVs locais, baixa direto da API de
        # transparência gravando snapshots content-addressed pra cada
        # endpoint consultado. Esse é o único caminho em que ``extract``
        # fala com a rede — por isso também é o único que popula
        # ``source_snapshot_uri`` (offline/fixture path preserva o
        # contrato opt-in).
        if not have_local:
            self._fetch_from_api(max_expense_months=self._max_expense_months)

        if self.limit:
            self._raw_deputados = self._raw_deputados.head(self.limit)
            self._raw_cota = self._raw_cota.head(self.limit)
            self._raw_propositions = self._raw_propositions.head(self.limit)

        self.rows_in = (
            len(self._raw_deputados)
            + len(self._raw_cota)
            + len(self._raw_propositions)
        )

    def _fetch_from_api(
        self, *, max_expense_months: int | None = 3
    ) -> None:
        """Online fallback: popula os DataFrames via transparência ALEGO.

        Cada fetch HTTP é espelhado em :func:`archive_fetch` antes de
        virar DataFrame, e a URI resultante vai na coluna
        :data:`_SNAPSHOT_COLUMN` de cada linha derivada. Três "famílias"
        de snapshot convivem:

        * listagem de deputados (``/verbas_indenizatorias/deputados``) —
          alimenta ``_raw_deputados``;
        * ``/verbas_indenizatorias/exibir`` por (deputado, ano, mes) —
          alimenta ``_raw_cota`` (um snapshot por chamada ``exibir``,
          já que é essa a granularidade da fonte; a URI é replicada nos
          ``lancamentos`` que saem dela);
        * ``/processos/recentes`` + ``/processos/proposicoes-mais-votadas``
          — alimenta ``_raw_propositions`` (item-level: URI do endpoint
          que viu o ``numero`` pela primeira vez).
        """
        with httpx.Client(
            timeout=_TIMEOUT,
            headers={
                "Accept": "application/json",
                "User-Agent": "br-acc-etl/1.0",
            },
        ) as client:
            periodos_payload, periodos_content, periodos_ctype = (
                _http_get_json_raw(
                    _ENDPOINTS["deputados_periodos"], client=client,
                )
            )
            if periodos_content is not None and periodos_ctype is not None:
                archive_fetch(
                    url=f"{_API_BASE}{_ENDPOINTS['deputados_periodos']}",
                    content=periodos_content,
                    content_type=periodos_ctype,
                    run_id=self.run_id,
                    source_id=self.source_id,
                )
            periodos = _iter_periodos(periodos_payload)
            if not periodos:
                logger.warning(
                    "[alego] /verbas_indenizatorias/periodos vazio; "
                    "abortando fallback online."
                )
                return

            # ---- deputados listing (newest period with data) -------------
            deputy_rows: list[dict[str, Any]] = []
            deputy_uris: list[str | None] = []
            listing_uri: str | None = None
            for ano, mes in periodos:
                payload, content, ctype = _http_get_json_raw(
                    _ENDPOINTS["deputados_listing"],
                    params={"ano": ano, "mes": mes},
                    client=client,
                )
                if (
                    isinstance(payload, list)
                    and payload
                    and content is not None
                    and ctype is not None
                ):
                    listing_uri = archive_fetch(
                        url=f"{_API_BASE}{_ENDPOINTS['deputados_listing']}",
                        content=content,
                        content_type=ctype,
                        run_id=self.run_id,
                        source_id=self.source_id,
                    )
                    raw_deputies = [p for p in payload if isinstance(p, dict)]
                    break
            else:
                raw_deputies = []

            if self.limit is not None:
                raw_deputies = raw_deputies[: self.limit]

            # ---- expenses + party enrichment ------------------------------
            cota_rows: list[dict[str, Any]] = []
            cota_uris: list[str | None] = []
            target_periods = (
                periodos[:max_expense_months] if max_expense_months else periodos
            )

            for dep in raw_deputies:
                dep_id = dep.get("id")
                dep_name = str(dep.get("nome") or "").strip()
                if not isinstance(dep_id, int) or not dep_name:
                    continue

                party = ""
                for ano, mes in target_periods:
                    payload, content, ctype = _http_get_json_raw(
                        _ENDPOINTS["deputado_exibir"],
                        params={
                            "deputado_id": dep_id,
                            "ano": ano,
                            "mes": mes,
                        },
                        client=client,
                    )
                    time.sleep(_RATE_LIMIT_SECONDS)
                    if not isinstance(payload, dict):
                        continue
                    exibir_uri: str | None = None
                    if content is not None and ctype is not None:
                        exibir_uri = archive_fetch(
                            url=(
                                f"{_API_BASE}{_ENDPOINTS['deputado_exibir']}"
                            ),
                            content=content,
                            content_type=ctype,
                            run_id=self.run_id,
                            source_id=self.source_id,
                        )
                    if not party:
                        dep_block = payload.get("deputado") or {}
                        if isinstance(dep_block, dict):
                            party = str(
                                dep_block.get("partido") or "",
                            ).strip()
                    lancamentos = _flatten_cota_lancamentos(
                        dep_name, ano, mes, payload,
                    )
                    cota_rows.extend(lancamentos)
                    cota_uris.extend([exibir_uri] * len(lancamentos))

                deputy_rows.append({
                    "nome": dep_name,
                    "cpf": "",
                    "partido": party,
                    "legislatura": "",
                    "deputado_id_alego": dep_id,
                })
                deputy_uris.append(listing_uri)

            # ---- proposicoes ----------------------------------------------
            prop_rows, prop_uris = self._fetch_proposicoes_with_archival(client)

        self._raw_deputados = pd.DataFrame(deputy_rows, dtype=str)
        if not self._raw_deputados.empty:
            self._raw_deputados[_SNAPSHOT_COLUMN] = pd.array(
                deputy_uris, dtype="object",
            )
        self._raw_cota = pd.DataFrame(cota_rows, dtype=str)
        if not self._raw_cota.empty:
            self._raw_cota[_SNAPSHOT_COLUMN] = pd.array(
                cota_uris, dtype="object",
            )
        self._raw_propositions = pd.DataFrame(prop_rows, dtype=str)
        if not self._raw_propositions.empty:
            self._raw_propositions[_SNAPSHOT_COLUMN] = pd.array(
                prop_uris, dtype="object",
            )

    def _fetch_proposicoes_with_archival(
        self, client: httpx.Client,
    ) -> tuple[list[dict[str, Any]], list[str | None]]:
        """Fetch ``processos/recentes`` + ``proposicoes-mais-votadas`` com
        archival, tracking por-``numero`` a URI do payload que o viu primeiro.
        """
        seen: dict[str, dict[str, Any]] = {}
        seen_uri: dict[str, str | None] = {}

        recentes, rec_content, rec_ctype = _http_get_json_raw(
            _ENDPOINTS["processos_recentes"], client=client,
        )
        recentes_uri: str | None = None
        if rec_content is not None and rec_ctype is not None:
            recentes_uri = archive_fetch(
                url=f"{_API_BASE}{_ENDPOINTS['processos_recentes']}",
                content=rec_content,
                content_type=rec_ctype,
                run_id=self.run_id,
                source_id=self.source_id,
            )
        if isinstance(recentes, list):
            for group in recentes:
                if isinstance(group, list):
                    for item in group:
                        if isinstance(item, dict) and item.get("numero"):
                            key = str(item["numero"])
                            if key not in seen:
                                seen[key] = item
                                seen_uri[key] = recentes_uri
                elif isinstance(group, dict) and group.get("numero"):
                    key = str(group["numero"])
                    if key not in seen:
                        seen[key] = group
                        seen_uri[key] = recentes_uri

        mais_votadas, mv_content, mv_ctype = _http_get_json_raw(
            _ENDPOINTS["proposicoes_mais_votadas"], client=client,
        )
        mais_votadas_uri: str | None = None
        if mv_content is not None and mv_ctype is not None:
            mais_votadas_uri = archive_fetch(
                url=f"{_API_BASE}{_ENDPOINTS['proposicoes_mais_votadas']}",
                content=mv_content,
                content_type=mv_ctype,
                run_id=self.run_id,
                source_id=self.source_id,
            )
        if isinstance(mais_votadas, dict):
            for item in mais_votadas.get("processos") or []:
                if isinstance(item, dict) and item.get("numero"):
                    key = str(item["numero"])
                    if key not in seen:
                        seen[key] = item
                        seen_uri[key] = mais_votadas_uri

        rows: list[dict[str, Any]] = []
        uris: list[str | None] = []
        for key, item in seen.items():
            autores = item.get("autores") or []
            autor = (
                "; ".join(a for a in autores if isinstance(a, str))
                if isinstance(autores, list)
                else str(autores or "")
            )
            rows.append({
                "numero": str(item.get("numero") or "").strip(),
                "titulo": str(item.get("assunto") or "").strip(),
                "ementa": str(item.get("ementa") or "").strip(),
                "autor": autor,
                "data": str(item.get("data_autuacao") or "").strip(),
                "situacao": str(item.get("situacao") or "").strip(),
                "a_favor": item.get("a_favor"),
                "contra": item.get("contra"),
            })
            uris.append(seen_uri.get(key))
        return rows, uris

    def transform(self) -> None:
        # Snapshot URI por-linha só aparece quando ``extract`` caiu no
        # fallback online (``_fetch_from_api``). Offline/fixture path →
        # coluna ausente → ``snapshot_uri`` fica ``None`` e ``attach_provenance``
        # não injeta o campo (compat com contrato opt-in).
        has_dep_snapshot = _SNAPSHOT_COLUMN in self._raw_deputados.columns
        has_cota_snapshot = _SNAPSHOT_COLUMN in self._raw_cota.columns
        has_prop_snapshot = _SNAPSHOT_COLUMN in self._raw_propositions.columns

        for _, row in self._raw_deputados.iterrows():
            name = normalize_name(
                row_pick(row, "nome", "deputado", "nome_parlamentar"),
            )
            cpf_raw = row_pick(row, "cpf", "documento")
            party = row_pick(row, "partido", "sigla_partido")
            legislature = row_pick(row, "legislatura", "mandato")
            if not name:
                continue
            snapshot_uri = _snapshot_from_row(row) if has_dep_snapshot else None
            cpf_digits = strip_document(cpf_raw)
            legislator_id = _hash_id(
                name, cpf_digits[-4:] if cpf_digits else "", legislature,
            )
            legislator_record_id = f"{name}|{party}|{legislature}"
            self.legislators.append(self.attach_provenance(
                {
                    "legislator_id": legislator_id,
                    "name": name,
                    "cpf": mask_cpf(cpf_raw) if cpf_digits else "",
                    "party": party,
                    "legislature": legislature,
                    "uf": "GO",
                    "source": "alego",
                },
                record_id=legislator_record_id,
                snapshot_uri=snapshot_uri,
            ))

        for _, row in self._raw_cota.iterrows():
            legislator_name = normalize_name(
                row_pick(row, "deputado", "nome", "nome_parlamentar"),
            )
            fornecedor = normalize_name(
                row_pick(row, "fornecedor", "razao_social"),
            )
            cnpj_raw = row_pick(row, "cnpj_fornecedor", "cnpj")
            cnpj_digits = strip_document(cnpj_raw)
            amount = parse_brl_flexible(
                row_pick(row, "valor", "valor_liquido", "valor_total"),
                default=None,
            )
            data = row_pick(row, "data", "data_emissao", "dt_documento")
            tipo = row_pick(row, "tipo_despesa", "natureza", "descricao")
            if not legislator_name and not fornecedor:
                continue
            snapshot_uri = _snapshot_from_row(row) if has_cota_snapshot else None
            expense_id = _hash_id(
                legislator_name, cnpj_digits, tipo, data, str(amount or ""),
            )
            expense_record_id = (
                f"{legislator_name}|{data}|{fornecedor}|{amount}"
            )
            self.expenses.append(self.attach_provenance(
                {
                    "expense_id": expense_id,
                    "legislator": legislator_name,
                    "supplier": fornecedor,
                    "cnpj_supplier": (
                        format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""
                    ),
                    "tipo": tipo,
                    "amount": amount,
                    "date": parse_date(data) if data else "",
                    "uf": "GO",
                    "source": "alego",
                },
                record_id=expense_record_id,
                snapshot_uri=snapshot_uri,
            ))
            if legislator_name:
                legislator_id = _hash_id(legislator_name, "", "")
                self.expense_rels.append(self.attach_provenance(
                    {
                        "source_key": legislator_id,
                        "target_key": expense_id,
                    },
                    record_id=expense_record_id,
                    snapshot_uri=snapshot_uri,
                ))

        for _, row in self._raw_propositions.iterrows():
            numero = row_pick(row, "numero", "nr_proposicao", "identificacao")
            titulo = normalize_name(row_pick(row, "titulo", "ementa", "assunto"))
            autor = normalize_name(row_pick(row, "autor", "proponente"))
            data = row_pick(row, "data", "data_apresentacao")
            if not numero and not titulo:
                continue
            snapshot_uri = _snapshot_from_row(row) if has_prop_snapshot else None
            prop_id = _hash_id(numero, titulo, data)
            # Prefer the ``numero`` field (natural key of the legislative
            # process at ALEGO) when available; fall back to the composite
            # that generated the stable ID so rows without a numbered
            # proposition are still uniquely traceable.
            proposition_record_id = (
                str(numero) if numero else f"{titulo}|{data}"
            )
            self.propositions.append(self.attach_provenance(
                {
                    "proposition_id": prop_id,
                    "numero": numero,
                    "titulo": titulo,
                    "autor": autor,
                    "date": parse_date(data) if data else "",
                    "uf": "GO",
                    "source": "alego",
                },
                record_id=proposition_record_id,
                snapshot_uri=snapshot_uri,
            ))

        self.legislators = deduplicate_rows(self.legislators, ["legislator_id"])
        self.expenses = deduplicate_rows(self.expenses, ["expense_id"])
        self.propositions = deduplicate_rows(self.propositions, ["proposition_id"])
        self.expense_rels = deduplicate_rows(
            self.expense_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.legislators) + len(self.expenses) + len(self.propositions)
        )

    def load(self) -> None:
        if not (self.legislators or self.expenses or self.propositions):
            logger.warning("[alego] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        if self.legislators:
            loader.load_nodes(
                "StateLegislator", self.legislators, key_field="legislator_id",
            )
        if self.expenses:
            loader.load_nodes(
                "LegislativeExpense", self.expenses, key_field="expense_id",
            )
        if self.propositions:
            loader.load_nodes(
                "LegislativeProposition",
                self.propositions,
                key_field="proposition_id",
            )
        if self.expense_rels:
            loader.load_relationships(
                rel_type="GASTOU_COTA_GO",
                rows=self.expense_rels,
                source_label="StateLegislator",
                source_key="legislator_id",
                target_label="LegislativeExpense",
                target_key="expense_id",
            )
