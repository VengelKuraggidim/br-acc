"""Custo dos cargos eletivos municipais GO — prefeito e vereador de Goiânia.

Estende a cobertura de `custo_mandato_br` (federal + estadual) pra esfera
municipal. MVP cobre só Goiânia — os outros 245 municípios goianos ficam
fora do escopo inicial porque cada lei orgânica é publicada em diário
oficial municipal sem API consolidada.

Cobertura MVP:

* ``prefeito_goiania`` — subsídio fixado pela Lei Orgânica do Município
  de Goiânia (sem API pública machine-readable). Valor marcado
  ``None`` com observação + link pro Diário Oficial; o teto CF é o
  subsídio do governador de GO (CF Art. 37 XI / CF Art. 29 V), que já
  está marcado ``None`` em ``custo_mandato_br`` — coerência de padrão.
* ``vereador_goiania`` — subsídio capped a 75% do subsídio do deputado
  estadual (CF Art. 29 VI; Goiânia tem >500.000 habitantes). Valor
  calculado: 75% × R$ 34.774,64 = R$ 26.080,98. CMG pode fixar abaixo
  do teto por Resolução; esse é o **teto legal**, não o valor efetivo
  pago. Se a CMG fixar mais baixo, atualizar a constante.

Fora do escopo do MVP:

* Outros municípios GO (Aparecida de Goiânia, Anápolis, etc.) — mesmo
  padrão aplica, mas cada lei orgânica precisa de pesquisa. Candidato a
  fallback via ``basedosdados.org`` se a tabela
  ``municipio_subsidio_vereador`` materializar-se.
* Verba de gabinete, diárias, outros componentes — dependem de
  Resolução da CMG publicada em Diário Oficial Municipal sem formato
  consolidado. Componente fica marcado ``None`` com observação.

Schema no grafo (mesmo de ``custo_mandato_br``):

* ``(:CustoMandato {cargo, esfera, n_titulares, custo_anual_total, ...})``
  — chave: ``cargo`` (``prefeito_goiania`` / ``vereador_goiania``).
* ``(:CustoComponente {componente_id, cargo, rotulo, valor_mensal, ...})``
  — chave: ``componente_id``.
* Rel ``(:CustoMandato)-[:TEM_COMPONENTE]->(:CustoComponente)``.

``cargo`` virou chave composta-por-convenção (``<cargo>_<municipio>``)
em vez de adicionar um campo ``municipio`` separado. Evita migration no
modelo e na query existente; custo: o cargo fica com o nome do
município concatenado, que o PWA pode resolver com split simples.

Idempotência: ``componente_id`` e ``cargo`` são chaves estáveis; rerun
com mesmas constantes gera o mesmo grafo (MERGE no loader).

Cadência recomendada (registry): ``yearly`` — subsídio muda por lei
municipal / Resolução da CMG (raro). Forçar re-run quando sair a
atualização.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_SOURCE_ID = "custo_mandato_municipal_go"
_REGISTRY_URL = (
    "https://www.goiania.go.gov.br/casa-civil/diario-oficial/"
)
_HTTP_TIMEOUT = 30.0
_DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
    "User-Agent": "Fiscal-Cidadao/1.0 (custo_mandato_municipal_go pipeline)",
}

# Salário mínimo nacional 2026 (Decreto nº 12.342/2025). Manter alinhado
# com o valor em ``custo_mandato_br`` — se divergir, a equivalência em
# trabalhadores fica inconsistente entre esferas.
_SALARIO_MINIMO_2026 = 1518.00
_SALARIO_MINIMO_FONTE = (
    "https://www.planalto.gov.br/ccivil_03/_ato2023-2026/2025/decreto/d12342.htm"
)

# Subsídio do deputado estadual GO (75% do dep federal por CF Art. 27 §2°).
# Base pro cálculo do cap constitucional do vereador de Goiânia (CF Art.
# 29 VI — municípios > 500k hab: até 75% do subsídio do dep estadual).
# Manter alinhado com _COMPONENTS["dep_estadual_go"] em ``custo_mandato_br``.
_SUBSIDIO_DEP_ESTADUAL_GO = 34774.64
_VEREADOR_GOIANIA_CAP = _SUBSIDIO_DEP_ESTADUAL_GO * 0.75  # 26080.98


def _comp(
    cargo: str,
    componente_id: str,
    rotulo: str,
    *,
    valor_mensal: float | None,
    valor_observacao: str = "",
    fonte_legal: str,
    fonte_url: str,
    incluir_no_total: bool = True,
) -> dict[str, Any]:
    """Constrói dict bruto de um componente. Mesmo contrato de ``custo_mandato_br``."""
    return {
        "componente_id": f"{cargo}:{componente_id}",
        "cargo": cargo,
        "rotulo": rotulo,
        "valor_mensal": valor_mensal,
        "valor_observacao": valor_observacao,
        "fonte_legal": fonte_legal,
        "fonte_url": fonte_url,
        "incluir_no_total": incluir_no_total,
    }


_COMPONENTS: dict[str, list[dict[str, Any]]] = {
    "prefeito_goiania": [
        _comp(
            "prefeito_goiania", "subsidio", "Subsídio mensal",
            valor_mensal=None,
            valor_observacao=(
                "fixado por Lei Municipal; teto constitucional é o "
                "subsídio do governador (CF Art. 37 XI / Art. 29 V). "
                "Consulte Diário Oficial Municipal / Lei Orgânica de Goiânia."
            ),
            fonte_legal="CF Art. 29 V; Lei Orgânica Municipal de Goiânia",
            fonte_url="https://www.goiania.go.gov.br/casa-civil/diario-oficial/",
        ),
        _comp(
            "prefeito_goiania", "verba_gabinete",
            "Verba de gabinete / cargos comissionados",
            valor_mensal=None,
            valor_observacao=(
                "fixada por decreto municipal; não divulgada em "
                "formato consolidado — consulte Portal da Transparência "
                "de Goiânia."
            ),
            fonte_legal="Decreto Municipal / Lei Orgânica de Goiânia",
            fonte_url="https://transparencia.goiania.go.gov.br/",
        ),
    ],
    "vereador_goiania": [
        _comp(
            "vereador_goiania", "subsidio", "Subsídio mensal (teto legal)",
            valor_mensal=_VEREADOR_GOIANIA_CAP,
            valor_observacao=(
                "teto constitucional: 75% do subsídio do deputado "
                "estadual de GO (CF Art. 29 VI; municípios >500 mil hab). "
                "Valor efetivo pago pela CMG pode ser menor — verificar "
                "Resolução da Câmara Municipal em vigor."
            ),
            fonte_legal="Constituição Federal Art. 29 VI",
            fonte_url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        ),
        _comp(
            "vereador_goiania", "verba_gabinete",
            "Verba de gabinete (assessores CMG)",
            valor_mensal=None,
            valor_observacao=(
                "fixada por Resolução da Câmara Municipal de Goiânia; "
                "não divulgada em formato consolidado — consulte portal "
                "da CMG."
            ),
            fonte_legal="Resolução da Câmara Municipal de Goiânia",
            fonte_url="https://www.goiania.go.leg.br/",
        ),
    ],
}


# Metadados por cargo. ``n_titulares``: 1 prefeito + 35 vereadores
# (legislatura atual de Goiânia, 2025-2028). O número pode variar por
# legislatura — atualizar quando mudar o número de cadeiras.
_CARGO_META: dict[str, dict[str, Any]] = {
    "prefeito_goiania": {
        "esfera": "municipal",
        "rotulo_humano": "Prefeito(a) de Goiânia",
        "n_titulares": 1,
        "uf": "GO",
        "municipio": "goiania",
    },
    "vereador_goiania": {
        "esfera": "municipal",
        "rotulo_humano": "Vereador(a) de Goiânia",
        "n_titulares": 35,
        "uf": "GO",
        "municipio": "goiania",
    },
}


def _agg_total_mensal(componentes: list[dict[str, Any]]) -> float:
    """Soma componentes com ``incluir_no_total=True`` e ``valor_mensal != None``."""
    return sum(
        float(c["valor_mensal"])
        for c in componentes
        if c.get("incluir_no_total", True) and c.get("valor_mensal") is not None
    )


class CustoMandatoMunicipalGoPipeline(Pipeline):
    """Pipeline pedagógico — custo mensal/anual de prefeito + vereador GYN.

    Sem consulta a banco externo: valores são constantes derivadas da
    Constituição Federal (cap do vereador) e da Lei Orgânica (prefeito,
    marcado como ``None`` quando fonte é ilegível por máquina). O
    ``extract`` faz ``archive_fetch`` das páginas legais pra preservar
    snapshot do dia da run.

    Cadência recomendada: ``yearly``. Re-run quando a Resolução da CMG
    for atualizada ou quando o subsídio do dep estadual GO mudar (o cap
    do vereador é derivado dele).
    """

    name = "custo_mandato_municipal_go"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        http_client_factory = kwargs.pop(
            "http_client_factory",
            lambda: httpx.Client(
                timeout=_HTTP_TIMEOUT, follow_redirects=True,
            ),
        )
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        self._http_client_factory = http_client_factory
        self.cargos: list[dict[str, Any]] = []
        self.componentes: list[dict[str, Any]] = []
        self.relacionamentos: list[dict[str, Any]] = []
        self._snapshot_by_url: dict[str, str | None] = {}

    # ------------------------------------------------------------------
    # extract — archival das fontes legais (idempotente, falha graciosa)
    # ------------------------------------------------------------------

    def extract(self) -> None:
        urls_unicas: set[str] = set()
        urls_unicas.add(_REGISTRY_URL)
        for componentes in _COMPONENTS.values():
            for comp in componentes:
                if comp.get("fonte_url"):
                    urls_unicas.add(comp["fonte_url"])

        with self._http_client_factory() as client:
            for url in sorted(urls_unicas):
                try:
                    resp = client.get(url, headers=_DEFAULT_HEADERS)
                    resp.raise_for_status()
                except httpx.HTTPError as exc:
                    logger.warning(
                        "[custo_mandato_municipal_go] fetch %s falhou: %s — "
                        "componente entra sem snapshot",
                        url, exc,
                    )
                    self._snapshot_by_url[url] = None
                    continue
                content_type = resp.headers.get("content-type", "text/html")
                try:
                    snapshot_uri = archive_fetch(
                        url=str(resp.request.url),
                        content=resp.content,
                        content_type=content_type,
                        run_id=self.run_id,
                        source_id=_SOURCE_ID,
                    )
                except OSError as exc:
                    logger.warning(
                        "[custo_mandato_municipal_go] archival %s falhou: %s",
                        url, exc,
                    )
                    snapshot_uri = None
                self._snapshot_by_url[url] = snapshot_uri

        self.rows_in = sum(len(v) for v in _COMPONENTS.values()) + len(_COMPONENTS)
        logger.info(
            "[custo_mandato_municipal_go] arquivadas %d URLs distintas "
            "(%d com snapshot)",
            len(urls_unicas),
            sum(1 for v in self._snapshot_by_url.values() if v),
        )

    # ------------------------------------------------------------------
    # transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        for cargo, componentes_brutos in _COMPONENTS.items():
            meta = _CARGO_META[cargo]
            total_mensal = _agg_total_mensal(componentes_brutos)
            n_titulares = meta.get("n_titulares") or 0
            custo_anual_total = (
                total_mensal * 12 * n_titulares if n_titulares else 0.0
            )
            equivalente_trabalhadores = (
                int(total_mensal // _SALARIO_MINIMO_2026)
                if total_mensal > 0
                else 0
            )

            cargo_node = self.attach_provenance(
                {
                    "cargo": cargo,
                    "esfera": meta["esfera"],
                    "rotulo_humano": meta["rotulo_humano"],
                    "uf": meta.get("uf"),
                    "municipio": meta.get("municipio"),
                    "n_titulares": n_titulares,
                    "custo_mensal_individual": total_mensal,
                    "custo_anual_total": custo_anual_total,
                    "equivalente_trabalhadores_min": equivalente_trabalhadores,
                    "salario_minimo_referencia": _SALARIO_MINIMO_2026,
                    "salario_minimo_fonte": _SALARIO_MINIMO_FONTE,
                },
                record_id=cargo,
                record_url=_REGISTRY_URL,
                snapshot_uri=self._snapshot_by_url.get(_REGISTRY_URL),
            )
            self.cargos.append(cargo_node)

            for ordem, comp in enumerate(componentes_brutos):
                comp_url = str(comp.get("fonte_url") or _REGISTRY_URL)
                comp_node = self.attach_provenance(
                    {
                        "componente_id": comp["componente_id"],
                        "cargo": cargo,
                        "rotulo": comp["rotulo"],
                        "valor_mensal": comp["valor_mensal"],
                        "valor_observacao": comp["valor_observacao"],
                        "fonte_legal": comp["fonte_legal"],
                        "fonte_url": comp_url,
                        "incluir_no_total": bool(comp.get("incluir_no_total", True)),
                        "ordem": ordem,
                    },
                    record_id=comp["componente_id"],
                    record_url=comp_url,
                    snapshot_uri=self._snapshot_by_url.get(comp_url),
                )
                self.componentes.append(comp_node)

                rel_row = self.attach_provenance(
                    {
                        "source_key": cargo,
                        "target_key": comp["componente_id"],
                    },
                    record_id=comp["componente_id"],
                    record_url=comp_url,
                    snapshot_uri=self._snapshot_by_url.get(comp_url),
                )
                self.relacionamentos.append(rel_row)

        self.rows_loaded = (
            len(self.cargos) + len(self.componentes) + len(self.relacionamentos)
        )
        logger.info(
            "[custo_mandato_municipal_go] transformados %d cargos, "
            "%d componentes, %d rels",
            len(self.cargos), len(self.componentes), len(self.relacionamentos),
        )

    # ------------------------------------------------------------------
    # load
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.cargos:
            logger.warning("[custo_mandato_municipal_go] nada a carregar")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes("CustoMandato", self.cargos, key_field="cargo")
        loader.load_nodes(
            "CustoComponente", self.componentes, key_field="componente_id",
        )
        loader.load_relationships(
            rel_type="TEM_COMPONENTE",
            rows=self.relacionamentos,
            source_label="CustoMandato",
            source_key="cargo",
            target_label="CustoComponente",
            target_key="componente_id",
        )
