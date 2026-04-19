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
from functools import lru_cache
from pathlib import Path
from typing import Any

from bracc.services.source_registry import SourceRegistryEntry, load_source_registry


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

    - TCEs de outros estados (escopo Fiscal Cidadão é GO; só tce_go entra).
    - Portais de transparência de outros estados (só state_portal_go entra).
    - Pipelines de enriquecimento interno (derivações, não fontes externas).
    """
    sid = entry.id
    if sid.startswith("tce_") and sid != "tce_go":
        return True
    if sid.startswith("state_portal_") and sid != "state_portal_go":
        return True
    return sid in {"entity_resolution_politicos_go", "propagacao_fotos_person"}


def _merge_entry(
    entry: SourceRegistryEntry,
    copy: dict[str, Any],
    labels: dict[str, str],
) -> dict[str, Any]:
    source_copy = copy.get(entry.id, {})
    category = entry.category or ""
    return {
        "id": entry.id,
        "name": entry.name,
        "category": category,
        "category_label": labels.get(category, category.replace("_", " ").title() or "Outros"),
        "primary_url": entry.primary_url,
        "frequency": entry.frequency,
        "sigla_full": source_copy.get("sigla_full", entry.name),
        "o_que_e": source_copy.get("o_que_e"),
        "o_que_pegamos": source_copy.get("o_que_pegamos"),
        "por_que_importa": source_copy.get("por_que_importa"),
        "arquivos_exemplo": source_copy.get("arquivos_exemplo", []),
        "copy_disponivel": entry.id in copy,
    }


def build_public_sources() -> list[dict[str, Any]]:
    """Retorna lista ordenada de fontes hidratadas (registry + copy).

    Filtra fora TCEs/portais fora-de-escopo e pipelines de enriquecimento
    interno. Ordena por category_label, depois por name.
    """
    data = load_public_copy()
    copy = data.get("sources", {})
    labels = data.get("category_labels", {})

    entries = [
        e for e in load_source_registry() if e.in_universe_v1 and not _exclude_source(e)
    ]
    merged = [_merge_entry(e, copy, labels) for e in entries]
    merged.sort(key=lambda x: (x["category_label"], x["name"].lower()))
    return merged


def build_public_sources_grouped() -> list[dict[str, Any]]:
    """Mesma lista, agrupada por category_label pra consumo direto da PWA."""
    flat = build_public_sources()
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in flat:
        groups.setdefault(item["category_label"], []).append(item)

    return [
        {"category_label": label, "category": items[0]["category"], "sources": items}
        for label, items in sorted(groups.items(), key=lambda kv: kv[0].lower())
    ]
