"""Propaga ``foto_url`` de labels de cargo pro ``:Person`` homônimo.

Contexto arquitetural
=====================

Os pipelines de foto (``camara_politicos_go``, ``alego_deputados_foto``,
``senado_senadores_foto``, ``wikidata_politicos_foto``) escrevem em
labels específicos de cargo — ``:FederalLegislator``,
``:StateLegislator``, ``:Senator``. Porém, a busca da PWA usa o
fulltext index ``entity_search`` que só cobre ``:Person`` (e outros
labels de entidade genéricos). Resultado observável em 2026-04-18:
20+ políticos GO com foto oficial no grafo, mas foto invisível nos
cards de pesquisa — o usuário só via a foto ao clicar no perfil.

Este pipeline costura os dois grafos paralelos: para cada nó com
``foto_url`` em label de cargo, acha o ``:Person`` homônimo (match
determinístico por ``name`` já normalizado upstream) e copia
``foto_url`` + bloco ``foto_*`` de proveniência. Idempotente (não
sobrescreve ``:Person`` que já tem foto carimbada) e seguro
(skippa quando >1 ``:Person`` casa com o mesmo nome — política
do projeto de "stop on ambiguidade"; ver CLAUDE.md §3).

Cadência
========

Rodar depois dos 4 pipelines de foto — ordem canônica em
``scripts/refresh_photos.py``. Não cria nós, não faz HTTP: é um
costureiro puro de grafo. Se os pipelines upstream não rodaram,
sai no-op sem erro.

Proveniência preservada
=======================

O ``:Person`` recebe o bloco ``foto_*`` completo da fonte original
(``foto_source_id``, ``foto_source_url``, ``foto_snapshot_uri``,
``foto_content_type``, ``foto_ingested_at``) **mais** ``foto_run_id``
atualizado pra este pipeline. A origem do dado de foto fica
rastreável — o usuário vê "foto via ``alego_deputados_foto``" no chip
de fonte do PWA, mesmo que o ``:Person`` tenha vindo do pipeline
``tse_prestacao_contas``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_SOURCE_ID = "propagacao_fotos_person"

# Labels de cargo que os pipelines de foto escrevem. Mantido hardcoded
# (e não derivado do grafo) porque a lista é pequena, estável, e o
# universo é fechado — adicionar um label novo aqui é decisão consciente.
_SOURCE_LABELS: tuple[str, ...] = (
    "FederalLegislator",
    "StateLegislator",
    "Senator",
)

# Propaga só em :Person GO (ou sem uf). Política do projeto é escopo
# Goias (CLAUDE.md §1). Defensivo: se um :FederalLegislator GO tem
# homônimo em SP por acaso, não propagamos pra ele.
_PROPAGATION_QUERY = """
UNWIND $source_labels AS source_label
MATCH (src)
WHERE source_label IN labels(src)
  AND coalesce(src.foto_url, '') <> ''
  AND coalesce(src.name, '') <> ''
OPTIONAL MATCH (p:Person {name: src.name})
WHERE coalesce(p.foto_url, '') = ''
  AND coalesce(p.uf, 'GO') = 'GO'
WITH src, source_label, collect(p) AS candidates
WHERE size(candidates) = 1
UNWIND candidates AS p
SET p.foto_url = src.foto_url,
    p.foto_snapshot_uri = coalesce(src.foto_snapshot_uri, p.foto_snapshot_uri),
    p.foto_content_type = coalesce(src.foto_content_type, p.foto_content_type),
    p.foto_source_id = coalesce(src.foto_source_id, src.source_id, $fallback_source_id),
    p.foto_source_url = coalesce(src.foto_source_url, src.source_url, src.foto_url),
    p.foto_run_id = $run_id,
    p.foto_ingested_at = $ingested_at,
    p.foto_propagated_from = source_label
RETURN source_label AS label, count(DISTINCT p) AS propagated
"""


class PropagacaoFotosPersonPipeline(Pipeline):
    """Propaga ``foto_url`` cross-label pro ``:Person`` homônimo.

    Pipeline graph-internal: não faz fetch externo nem archival.
    O ``attach_provenance`` não é chamado — a proveniência copiada
    pro ``:Person`` é a da **fonte original** da foto (preservando o
    chip de fonte exibido na PWA), e este pipeline só carimba o
    ``foto_run_id`` pra ficar rastreável na tabela de
    ``:IngestionRun``.
    """

    name = _SOURCE_ID
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        self._stats: dict[str, int] = {label: 0 for label in _SOURCE_LABELS}

    def extract(self) -> None:
        """No-op: pipeline lê do próprio grafo."""
        logger.info(
            "[%s] pipeline graph-internal — sem fetch externo", self.name
        )

    def transform(self) -> None:
        """No-op: a lógica de match e carimbo roda atomicamente no ``load``."""

    def load(self) -> None:
        ingested_at = datetime.now(tz=UTC).isoformat()
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(
                    _PROPAGATION_QUERY,
                    {
                        "source_labels": list(_SOURCE_LABELS),
                        "run_id": self.run_id,
                        "ingested_at": ingested_at,
                        "fallback_source_id": self.source_id,
                    },
                )
                for record in result:
                    label = str(record.get("label") or "")
                    propagated = int(record.get("propagated") or 0)
                    if label in self._stats:
                        self._stats[label] = propagated
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning(
                "[%s] propagation query failed: %s", self.name, exc
            )
            return

        total = sum(self._stats.values())
        self.rows_loaded = total
        logger.info(
            "[%s] propagado foto_url para %d :Person (%s)",
            self.name,
            total,
            ", ".join(f"{k}={v}" for k, v in self._stats.items()),
        )
