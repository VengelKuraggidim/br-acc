"""Custo dos cargos eletivos (federal + estadual GO) — pipeline pedagógico.

Substitui o card hardcoded ``Quanto custa um deputado federal?`` que vivia
em ``pwa/index.html`` (linhas 1641-1688 antes desta refatoração) e estende
o conceito pra senador, deputado estadual GO e governador GO. O card era
estático: copy + valores fixos sem endpoint, sem proveniência clicável e
sem snapshot — viola o contrato de proveniência (``docs/provenance.md``).

Este pipeline materializa cada cargo como ``(:CustoMandato {cargo, ...})``
com componentes filhos ``(:CustoComponente {...})`` ligados por
``[:TEM_COMPONENTE]``. Cada componente carrega seu próprio
``ProvenanceBlock`` (a fonte legal de um componente — ex.: Ato da Mesa
243/2024 pra verba de gabinete — pode diferir da fonte do subsídio
consolidado), e o ``CustoMandato`` agregado carrega proveniência da fonte
guarda-chuva (registry).

Cobertura MVP (4 cargos):

* ``dep_federal`` — todos os componentes hoje exibidos no card (subsídio,
  CEAP, gabinete, auxílio-moradia) + saúde/encargos marcados "não
  divulgado" pra explicitar a opacidade da Câmara. Valores reaproveitam
  os do card existente, todos verificáveis nos Atos/Decretos citados.
* ``senador`` — subsídio idêntico ao DF (CF Art. 39 §4°). Componentes
  CEAPS/gabinete dependem de Resolução do Senado e variam por estado;
  ficam marcados "consulte portal de transparência" com link clicável.
* ``dep_estadual_go`` — subsídio capped em 75% do federal (CF Art. 27
  §2°). Verba indenizatória / gabinete vêm de Resolução ALEGO; deferidos.
* ``governador_go`` — subsídio capped no teto Min STF (CF Art. 37 XI).

Cobertura **fora** do MVP (débito): prefeito, vereador. Cada município
tem lei orgânica própria, sem API consolidada. Ver
``todo-list-prompts/high_priority/debitos/custo-mandato-municipal.md``.

Schema no grafo:

* ``(:CustoMandato {cargo, esfera, n_titulares, custo_anual_total, ...})``
  — chave: ``cargo`` (ex.: ``dep_federal``, ``senador``).
* ``(:CustoComponente {componente_id, cargo, rotulo, valor_mensal,
  fonte_legal, ...})`` — chave: ``componente_id`` (ex.:
  ``dep_federal:subsidio``).
* Rel ``(:CustoMandato)-[:TEM_COMPONENTE]->(:CustoComponente)`` —
  carrega ``ordem`` pra preservar exibição estável no PWA.

Idempotência: ``componente_id`` e ``cargo`` são chaves estáveis; rerun
com mesma constante gera o mesmo grafo (Neo4jBatchLoader faz MERGE).
Quando os valores mudam (reajuste de subsídio, novo Ato da Mesa),
atualiza ``_COMPONENTS`` e roda de novo — o MERGE sobrescreve as props.

Archival: cada URL legal é fetch-ada via ``archive_fetch`` em ``extract``;
falha de fetch (404, timeout) **não** quebra o pipeline — o componente
ainda entra no grafo sem ``source_snapshot_uri`` (gracioso, igual a
``camara_politicos_go`` faz com fotos). Re-run posterior captura o
snapshot quando a fonte voltar.

Cadência recomendada (registry): ``yearly`` — valores mudam só com novo
Decreto Legislativo / Ato / Lei. Operador pode forçar re-run após
publicação de reajuste sem esperar o ciclo.
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

_SOURCE_ID = "custo_mandato_br"
_REGISTRY_URL = (
    "https://www2.camara.leg.br/transparencia/recursos-humanos/remuneracao"
)
_HTTP_TIMEOUT = 30.0
_DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
    "User-Agent": "Fiscal-Cidadao/1.0 (custo_mandato_br pipeline)",
}

# Salário mínimo nacional 2026 (Decreto nº 12.342/2025) — usado pro cálculo
# "equivalente em trabalhadores". Atualizar quando publicar Decreto novo.
_SALARIO_MINIMO_2026 = 1518.00
_SALARIO_MINIMO_FONTE = (
    "https://www.planalto.gov.br/ccivil_03/_ato2023-2026/2025/decreto/d12342.htm"
)


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
    """Constrói o dict bruto de um componente (proveniência carimbada depois).

    ``valor_mensal=None`` significa "valor não divulgado pela fonte" — o
    componente entra no grafo pra explicitar a opacidade (ex.: saúde dos
    deputados federais), não soma no total e o PWA renderiza "não
    divulgado". ``incluir_no_total=False`` permite excluir do somatório
    valores que existem mas representam **teto** e não custo certo (ex.:
    CEAP "até R$ 57,3 mil" — usa-se o valor médio efetivo no total).
    """
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


# Componentes por cargo. Cada componente referencia uma fonte legal
# clicável; a URL é archive-fetchada em ``extract`` (uma vez por URL
# distinta) e o snapshot vira ``source_snapshot_uri`` no
# ProvenanceBlock do componente.
#
# Valores em BRL. Quando o valor é "até X" (CEAP varia por estado),
# usamos o teto e marcamos ``incluir_no_total=False`` pra não inflar o
# total mensal — o total reflete apenas custos certos por titular.
_COMPONENTS: dict[str, list[dict[str, Any]]] = {
    "dep_federal": [
        _comp(
            "dep_federal", "subsidio", "Subsídio mensal",
            valor_mensal=46366.19,
            fonte_legal="Decreto Legislativo nº 277/2024",
            fonte_url="https://www.congressonacional.leg.br/materias/materias-legislativas/-/materia/166003",
        ),
        _comp(
            "dep_federal", "ceap", "Cota parlamentar (CEAP)",
            valor_mensal=57300.00,
            valor_observacao="até R$ 57,3 mil — varia por estado",
            fonte_legal="Ato da Mesa nº 43/2009",
            fonte_url="https://www2.camara.leg.br/transparencia/cota-para-exercicio-da-atividade-parlamentar",
            incluir_no_total=False,
        ),
        _comp(
            "dep_federal", "gabinete", "Verba de gabinete (até 25 assessores)",
            valor_mensal=165844.80,
            fonte_legal="Ato da Mesa nº 243/2024",
            fonte_url="https://www2.camara.leg.br/transparencia/recursos-humanos/remuneracao",
        ),
        _comp(
            "dep_federal", "auxilio_moradia", "Auxílio-moradia",
            valor_mensal=4253.00,
            valor_observacao="opcional, pago a quem não usa imóvel funcional",
            fonte_legal="Resolução da Câmara dos Deputados",
            fonte_url="https://www2.camara.leg.br/transparencia/recursos-humanos/remuneracao",
        ),
        _comp(
            "dep_federal", "saude_encargos",
            "Saúde (Demed) + encargos trabalhistas dos assessores",
            valor_mensal=None,
            valor_observacao="não divulgado em formato consolidado pela Câmara",
            fonte_legal="—",
            fonte_url="https://www2.camara.leg.br/transparencia/recursos-humanos/remuneracao",
        ),
    ],
    "senador": [
        _comp(
            "senador", "subsidio", "Subsídio mensal",
            valor_mensal=46366.19,
            valor_observacao="igual ao deputado federal por força da CF Art. 39 §4°",
            fonte_legal="Constituição Federal Art. 39 §4°; Decreto Legislativo nº 277/2024",
            fonte_url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        ),
        _comp(
            "senador", "ceaps", "CEAPS (cota para o exercício parlamentar)",
            valor_mensal=None,
            valor_observacao="varia por estado — consulte portal de transparência do Senado",
            fonte_legal="Resolução do Senado Federal",
            fonte_url="https://www12.senado.leg.br/transparencia/sen",
        ),
        _comp(
            "senador", "gabinete", "Verba de gabinete (assessores)",
            valor_mensal=None,
            valor_observacao="valor por gabinete — consulte transparência do Senado",
            fonte_legal="Resolução do Senado Federal",
            fonte_url="https://www12.senado.leg.br/transparencia/sen",
        ),
    ],
    "dep_estadual_go": [
        _comp(
            "dep_estadual_go", "subsidio", "Subsídio mensal",
            valor_mensal=34774.64,
            valor_observacao="teto constitucional: 75% do subsídio do deputado federal",
            fonte_legal="Constituição Federal Art. 27 §2°",
            fonte_url="https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm",
        ),
        _comp(
            "dep_estadual_go", "verba_indenizatoria", "Verba indenizatória",
            valor_mensal=None,
            valor_observacao="consulte portal de transparência da ALEGO",
            fonte_legal="Resolução ALEGO",
            fonte_url="https://transparencia.al.go.leg.br/",
        ),
        _comp(
            "dep_estadual_go", "gabinete", "Verba de gabinete (assessores)",
            valor_mensal=None,
            valor_observacao="consulte portal de transparência da ALEGO",
            fonte_legal="Resolução ALEGO",
            fonte_url="https://transparencia.al.go.leg.br/",
        ),
    ],
    "governador_go": [
        _comp(
            "governador_go", "subsidio", "Subsídio mensal",
            valor_mensal=None,
            valor_observacao=(
                "teto constitucional: subsídio do Ministro do STF "
                "(CF Art. 37 XI). Valor exato fixado por Lei estadual GO; "
                "consulte Casa Civil/DOE-GO."
            ),
            fonte_legal="Constituição Federal Art. 37 XI; Lei estadual GO",
            fonte_url="https://www.casacivil.go.gov.br/",
        ),
    ],
}


# Metadados por cargo (esfera, número de titulares pra calcular custo
# anual nacional/estadual). ``n_titulares=None`` quando o cargo é unitário
# (governador) ou variável (CEAPS por estado).
_CARGO_META: dict[str, dict[str, Any]] = {
    "dep_federal": {
        "esfera": "federal",
        "rotulo_humano": "Deputado(a) federal",
        "n_titulares": 513,
        "uf": None,
    },
    "senador": {
        "esfera": "federal",
        "rotulo_humano": "Senador(a)",
        "n_titulares": 81,
        "uf": None,
    },
    "dep_estadual_go": {
        "esfera": "estadual",
        "rotulo_humano": "Deputado(a) estadual de Goiás",
        "n_titulares": 41,
        "uf": "GO",
    },
    "governador_go": {
        "esfera": "estadual",
        "rotulo_humano": "Governador(a) de Goiás",
        "n_titulares": 1,
        "uf": "GO",
    },
}


def _agg_total_mensal(componentes: list[dict[str, Any]]) -> float:
    """Soma componentes com ``incluir_no_total=True`` e ``valor_mensal != None``."""
    return sum(
        float(c["valor_mensal"])
        for c in componentes
        if c.get("incluir_no_total", True) and c.get("valor_mensal") is not None
    )


class CustoMandatoBrPipeline(Pipeline):
    """Pipeline pedagógico — materializa custo mensal/anual por cargo eletivo.

    Não consulta banco de dados externo: os valores são constantes legais
    referenciadas a Decretos/Atos/Leis públicos. O ``extract`` se limita a
    fazer ``archive_fetch`` da página/PDF de cada fonte legal pra preservar
    snapshot — a "ingestão" verdadeira é a constante hardcoded validada
    contra a Lei.

    Cadência recomendada: ``yearly`` (registry). Forçar re-run quando sair
    novo Decreto Legislativo / Ato da Mesa / Lei estadual de reajuste.
    """

    name = "custo_mandato_br"
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
        # Mapa URL -> snapshot_uri preenchido em extract; transform consulta.
        self._snapshot_by_url: dict[str, str | None] = {}

    # ------------------------------------------------------------------
    # extract — archival das fontes legais (idempotente, falha graciosa)
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Para cada URL de fonte legal distinta, faz GET + archive_fetch.

        Falha de fetch (timeout, 404, DNS) é logada e o snapshot fica
        ``None`` — o componente ainda entra no grafo com proveniência
        textual (``fonte_legal`` + ``fonte_url``), só sem cópia preservada.
        Re-run captura quando a fonte voltar.
        """
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
                        "[custo_mandato_br] fetch %s falhou: %s — "
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
                        "[custo_mandato_br] archival %s falhou: %s",
                        url, exc,
                    )
                    snapshot_uri = None
                self._snapshot_by_url[url] = snapshot_uri

        self.rows_in = sum(len(v) for v in _COMPONENTS.values()) + len(_COMPONENTS)
        logger.info(
            "[custo_mandato_br] arquivadas %d URLs distintas (%d com snapshot)",
            len(urls_unicas),
            sum(1 for v in self._snapshot_by_url.values() if v),
        )

    # ------------------------------------------------------------------
    # transform — produz nodes/rels com proveniência carimbada
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

                # ``ordem`` vive no nó CustoComponente — não duplica na rel.
                # Loader auto-propaga PROVENANCE_FIELDS pra rel; não precisa
                # passar ``properties=`` aqui.
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
            "[custo_mandato_br] transformados %d cargos, %d componentes, %d rels",
            len(self.cargos), len(self.componentes), len(self.relacionamentos),
        )

    # ------------------------------------------------------------------
    # load — grava no grafo via Neo4jBatchLoader (MERGE idempotente)
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.cargos:
            logger.warning("[custo_mandato_br] nada a carregar")
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
