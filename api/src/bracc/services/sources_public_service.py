"""Hidrata o registry técnico com copy pedagógico pt-BR para exibição na PWA.

O registry (`docs/source_registry_br_v1.csv`) tem campos operacionais
(tier, load_state, frequency). A PWA pública precisa de linguagem leiga:
sigla expandida, explicação simples, o que baixamos e por que importa.

O copy vive em `docs/sources_public_copy.json` (editável manualmente —
separado do CSV técnico pra não misturar conteúdo editorial com contrato
de pipelines).
"""

from __future__ import annotations

import json
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc.services.neo4j_service import execute_query
from bracc.services.source_registry import SourceRegistryEntry, load_source_registry

if TYPE_CHECKING:
    from neo4j import AsyncSession

# Cache process-wide do live status (5min TTL). Evita bater no Neo4j a
# cada abertura da aba Fontes.
_LIVE_STATUS_CACHE: dict[str, Any] | None = None
_LIVE_STATUS_CACHE_TIME: float = 0.0
_LIVE_STATUS_TTL_SECONDS = 300


def _default_copy_path() -> Path:
    # .../api/src/bracc/services/sources_public_service.py -> repo root é parents[4]
    return Path(__file__).resolve().parents[4] / "docs" / "sources_public_copy.json"


def get_copy_path() -> Path:
    """Retorna caminho do JSON de copy (env override em `BRACC_SOURCES_COPY_PATH`).

    Env override é pra admin em ambiente controlado — não aceitar input de
    usuário aqui (risco path traversal).
    """
    configured = os.getenv("BRACC_SOURCES_COPY_PATH", "").strip()
    return Path(configured) if configured else _default_copy_path()


@lru_cache(maxsize=1)
def load_public_copy() -> dict[str, Any]:
    """Carrega copy editorial. Cache process-wide — copy é estático em runtime."""
    path = get_copy_path()
    if not path.exists():
        return {"_meta": {}, "category_labels": {}, "sources": {}}
    with path.open(encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)
    return data


def _exclude_source(entry: SourceRegistryEntry) -> bool:
    """Fontes que não devem aparecer na aba pública 'Fontes'.

    Só exclui pipelines de **derivação interna** (não são fontes externas,
    são processamento local que deriva de outros nodes já no grafo).

    TCEs e portais de outros estados ENTRAM na lista — o grafo ingere dados
    nacionais de propósito pra descobrir conexões cross-estado de políticos
    goianos (ver `project_go_scope_policy` na memória). A aba Fontes mostra
    todas as fontes do registry, não só as GO-específicas.
    """
    return entry.id in {"entity_resolution_politicos_go", "propagacao_fotos_person"}


def _derive_live_badge(statuses: list[str], total_rows: int) -> str:
    """Deriva badge a partir dos statuses observados e rows carregadas.

    - com_dados: pipeline terminou (status=loaded) e carregou >0 linhas.
    - parcial: pipeline rodou mas sem linhas efetivas, ou está em execução.
    - falhou: a run mais recente reportou falha de qualidade.
    - sem_dados: nenhuma run registrada (fonte catalogada, não rodou).
    """
    if not statuses:
        return "sem_dados"
    if "loaded" in statuses and total_rows > 0:
        return "com_dados"
    if "quality_fail" in statuses and "loaded" not in statuses:
        return "falhou"
    return "parcial"


async def load_live_source_status(session: AsyncSession) -> dict[str, dict[str, Any]]:
    """Consulta Neo4j por IngestionRun agregado e retorna por source_id.

    ``source_id`` usado no grafo é o slug canônico do registry (ver
    ``docs/source_registry_br_v1.csv``). Cache 5min.
    """
    global _LIVE_STATUS_CACHE, _LIVE_STATUS_CACHE_TIME  # noqa: PLW0603
    if (
        _LIVE_STATUS_CACHE is not None
        and (time.monotonic() - _LIVE_STATUS_CACHE_TIME) < _LIVE_STATUS_TTL_SECONDS
    ):
        return _LIVE_STATUS_CACHE

    result: dict[str, dict[str, Any]] = {}
    try:
        records = await execute_query(session, "live_source_status", {})
    except Exception:  # noqa: BLE001
        # Neo4j indisponível: devolve dict vazio (PWA cai pro fallback de
        # registry estático sem badge live).
        return {}

    for record in records:
        sid = record["source_id"]
        statuses = list(record["statuses"] or [])
        total_rows = int(record["total_rows"] or 0)
        result[sid] = {
            "runs": int(record["runs"] or 0),
            "last_run_at": record["last_run_at"],
            "rows_loaded": total_rows,
            "statuses": statuses,
            "badge": _derive_live_badge(statuses, total_rows),
        }

    _LIVE_STATUS_CACHE = result
    _LIVE_STATUS_CACHE_TIME = time.monotonic()
    return result


def clear_live_status_cache() -> None:
    """Utilitário pra tests e scripts invalidarem o cache process-wide."""
    global _LIVE_STATUS_CACHE, _LIVE_STATUS_CACHE_TIME  # noqa: PLW0603
    _LIVE_STATUS_CACHE = None
    _LIVE_STATUS_CACHE_TIME = 0.0


def _merge_entry(
    entry: SourceRegistryEntry,
    copy: dict[str, Any],
    labels: dict[str, str],
    live: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    source_copy = copy.get(entry.id, {})
    category = entry.category or ""
    live_entry = live.get(entry.id)
    return {
        "id": entry.id,
        "name": entry.name,
        "category": category,
        "category_label": labels.get(category, category.replace("_", " ").title() or "Outros"),
        "primary_url": entry.primary_url,
        "frequency": entry.frequency,
        "declared_load_state": entry.load_state,
        "sigla_full": source_copy.get("sigla_full", entry.name),
        "o_que_e": source_copy.get("o_que_e"),
        "o_que_pegamos": source_copy.get("o_que_pegamos"),
        "por_que_importa": source_copy.get("por_que_importa"),
        "arquivos_exemplo": source_copy.get("arquivos_exemplo", []),
        "copy_disponivel": entry.id in copy,
        "live": live_entry or {
            "runs": 0,
            "last_run_at": None,
            "rows_loaded": 0,
            "statuses": [],
            "badge": "sem_dados",
        },
    }


def build_public_sources(
    live: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Retorna lista ordenada de fontes hidratadas (registry + copy + live).

    Filtra pipelines de enriquecimento interno. Ordena por category_label,
    depois por name. Se ``live`` for ``None``, retorna sem info live
    (badge="sem_dados" pra todas) — útil pra render puramente estático.
    """
    data = load_public_copy()
    copy = data.get("sources", {})
    labels = data.get("category_labels", {})
    live_status = live or {}

    entries = [
        e for e in load_source_registry() if e.in_universe_v1 and not _exclude_source(e)
    ]
    merged = [_merge_entry(e, copy, labels, live_status) for e in entries]
    merged.sort(key=lambda x: (x["category_label"], x["name"].lower()))
    return merged


def build_public_sources_grouped(
    live: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Mesma lista, agrupada por category_label pra consumo direto da PWA."""
    flat = build_public_sources(live)
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in flat:
        groups.setdefault(item["category_label"], []).append(item)

    return [
        {"category_label": label, "category": items[0]["category"], "sources": items}
        for label, items in sorted(groups.items(), key=lambda kv: kv[0].lower())
    ]
