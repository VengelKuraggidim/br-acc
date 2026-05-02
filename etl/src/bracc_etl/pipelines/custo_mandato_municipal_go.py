"""Custo dos cargos eletivos municipais GO — prefeito + vereador.

Estende a cobertura de ``custo_mandato_br`` (federal + estadual) pra esfera
municipal. A primeira leva (MVP, abr/2026) cobriu só Goiânia. Esta segunda
fase adiciona os 9 demais municípios goianos com mais de 100 mil habitantes
(Censo IBGE 2022) — Aparecida de Goiânia, Anápolis, Rio Verde, Águas
Lindas de Goiás, Luziânia, Valparaíso de Goiás, Trindade, Formosa e
Senador Canedo.

Por que parar em 100k? CF Art. 29 VI fixa o **teto** do subsídio do
vereador como % do subsídio do dep estadual, com 6 faixas por população.
Cidades >100k caem nas 3 faixas mais altas (50%/60%/75%) e concentram a
maior parte da população do estado e da relevância política. Os outros
236 municípios goianos ficam como débito (ver
``todo-list-prompts/medium_priority/debitos/custo-mandato-municipal-expansao.md``).

Cobertura:

* ``prefeito_<municipio>`` — subsídio fixado por Lei Orgânica Municipal,
  publicada em Diário Oficial Municipal sem formato consolidado. Valor
  marcado ``None`` em todas as cidades + observação textual; o teto CF é
  o subsídio do governador (CF Art. 37 XI / Art. 29 V). Mesmo padrão do
  ``governador_go`` em ``custo_mandato_br``.
* ``vereador_<municipio>`` — subsídio capped por CF Art. 29 VI:

  - até 10.000 hab: 20% do dep estadual
  - 10.001 a 50.000 hab: 30%
  - 50.001 a 100.000 hab: 40%
  - 100.001 a 300.000 hab: 50%
  - 300.001 a 500.000 hab: 60%
  - >500.000 hab: 75%

  O valor materializado é o **teto legal**, não o efetivo pago — Câmaras
  Municipais podem fixar abaixo do teto via Resolução. ``valor_observacao``
  explicita isso ("verificar Resolução da Câmara Municipal").

Componentes ``verba_gabinete`` ficam ``None`` em todas as cidades (sem
formato consolidado por município).

Fora do escopo desta fase:

* Os 236 municípios goianos com menos de 100 mil habitantes — mesmo padrão
  aplica, mas inflar o backend com ~470 cargos sem demanda do PWA é
  ruído. Quando precisar (ex.: PWA mostrar "custo do vereador da minha
  cidade"), a tabela ``_GO_MUNICIPIOS`` aceita extensão direta — formula
  CF Art. 29 VI cuida do cálculo.
* Verba indenizatória, diárias, encargos — dependem de Resolução da
  Câmara Municipal local sem formato consolidado.
* Lei Orgânica de Goiânia / outras cidades em formato máquina-legível —
  candidato futuro: pipeline ``querido_diario_go`` parsing PDFs com
  regex/LLM (alto ROI por município, baixo no agregado).

Schema no grafo (mesmo de ``custo_mandato_br``):

* ``(:CustoMandato {cargo, esfera, n_titulares, custo_anual_total, ...})``
  — chave: ``cargo`` (ex.: ``vereador_anapolis``).
* ``(:CustoComponente {componente_id, cargo, rotulo, valor_mensal, ...})``
  — chave: ``componente_id``.
* Rel ``(:CustoMandato)-[:TEM_COMPONENTE]->(:CustoComponente)``.

Idempotência: ``componente_id`` e ``cargo`` são chaves estáveis; rerun
com mesmas constantes gera o mesmo grafo (MERGE no loader).

Cadência recomendada (registry): ``yearly`` — subsídio do dep estadual
GO muda raramente (decreto legislativo); reajustes municipais são raros.
Forçar re-run quando a base estadual mudar.
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
# Base pro cálculo do cap constitucional do vereador (CF Art. 29 VI).
# Manter alinhado com _COMPONENTS["dep_estadual_go"] em ``custo_mandato_br``.
_SUBSIDIO_DEP_ESTADUAL_GO = 34774.64

_PLANALTO_CF_URL = (
    "https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm"
)
_LEGISLA_GO_URL = "https://legisla.casacivil.go.gov.br/pesquisa_legislacao"


# CF Art. 29 VI — teto do subsídio do vereador como % do dep estadual.
# Cada par é (limite_superior_inclusivo_populacao, percentual).
_VEREADOR_PCT_TIERS: tuple[tuple[int, float], ...] = (
    (10_000, 0.20),
    (50_000, 0.30),
    (100_000, 0.40),
    (300_000, 0.50),
    (500_000, 0.60),
)
_VEREADOR_PCT_GT_500K = 0.75

# CF Art. 29 IV (após EC 58/2009) — número mínimo de vereadores por faixa
# populacional. Câmaras podem ter mais (até o limite da próxima faixa - 1).
_VEREADOR_MIN_SEATS_TIERS: tuple[tuple[int, int], ...] = (
    (15_000, 9),
    (30_000, 11),
    (50_000, 13),
    (80_000, 15),
    (120_000, 17),
    (160_000, 19),
    (300_000, 21),
    (450_000, 23),
    (600_000, 25),
    (750_000, 27),
    (900_000, 29),
    (1_050_000, 31),
    (1_200_000, 33),
    (1_350_000, 35),
    (1_500_000, 37),
    (1_800_000, 39),
    (2_400_000, 41),
)


def _vereador_pct_tier(populacao: int) -> float:
    """Retorna o teto CF Art. 29 VI (% do subsídio do dep estadual)."""
    for limite, pct in _VEREADOR_PCT_TIERS:
        if populacao <= limite:
            return pct
    return _VEREADOR_PCT_GT_500K


def _vereador_min_seats(populacao: int) -> int:
    """Retorna o mínimo de vereadores por CF Art. 29 IV (EC 58/2009)."""
    for limite, n in _VEREADOR_MIN_SEATS_TIERS:
        if populacao <= limite:
            return n
    return _VEREADOR_MIN_SEATS_TIERS[-1][1]


def _tier_descritor(populacao: int) -> str:
    """Descrição humana da faixa populacional usada no observação."""
    if populacao <= 10_000:
        return "municípios até 10 mil hab"
    if populacao <= 50_000:
        return "municípios de 10 a 50 mil hab"
    if populacao <= 100_000:
        return "municípios de 50 a 100 mil hab"
    if populacao <= 300_000:
        return "municípios de 100 a 300 mil hab"
    if populacao <= 500_000:
        return "municípios de 300 a 500 mil hab"
    return "municípios acima de 500 mil hab"


# Tabela dos 10 maiores municípios GO por população (Censo IBGE 2022).
# ``n_vereadores`` quando conhecido (legislatura atual); ``None`` cai pro
# mínimo CF Art. 29 IV via ``_vereador_min_seats``.
_GO_MUNICIPIOS: tuple[dict[str, Any], ...] = (
    {
        "slug": "goiania",
        "rotulo": "Goiânia",
        "populacao": 1_437_237,
        "n_vereadores": 35,
    },
    {
        "slug": "aparecida_de_goiania",
        "rotulo": "Aparecida de Goiânia",
        "populacao": 591_418,
        "n_vereadores": None,
    },
    {
        "slug": "anapolis",
        "rotulo": "Anápolis",
        "populacao": 391_772,
        "n_vereadores": None,
    },
    {
        "slug": "rio_verde",
        "rotulo": "Rio Verde",
        "populacao": 245_580,
        "n_vereadores": None,
    },
    {
        "slug": "aguas_lindas_de_goias",
        "rotulo": "Águas Lindas de Goiás",
        "populacao": 218_429,
        "n_vereadores": None,
    },
    {
        "slug": "luziania",
        "rotulo": "Luziânia",
        "populacao": 211_240,
        "n_vereadores": None,
    },
    {
        "slug": "valparaiso_de_goias",
        "rotulo": "Valparaíso de Goiás",
        "populacao": 170_661,
        "n_vereadores": None,
    },
    {
        "slug": "trindade",
        "rotulo": "Trindade",
        "populacao": 134_645,
        "n_vereadores": None,
    },
    {
        "slug": "formosa",
        "rotulo": "Formosa",
        "populacao": 123_528,
        "n_vereadores": None,
    },
    {
        "slug": "senador_canedo",
        "rotulo": "Senador Canedo",
        "populacao": 115_103,
        "n_vereadores": None,
    },
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


def _goiania_components() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Componentes específicos de Goiânia — preserva URLs do MVP (DOM-GYN, CMG).

    Goiânia foi entregue antes da generalização e tem URLs de portal
    municipal real (Diário Oficial Municipal, transparência da prefeitura,
    portal da CMG). Para os outros 9 municípios, cai-se no padrão
    genérico via ``_municipio_padrao_components`` (CF + Casa Civil GO).
    """
    cap_vereador = _SUBSIDIO_DEP_ESTADUAL_GO * _vereador_pct_tier(1_437_237)
    prefeito = [
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
    ]
    vereador = [
        _comp(
            "vereador_goiania", "subsidio", "Subsídio mensal (teto legal)",
            valor_mensal=cap_vereador,
            valor_observacao=(
                "teto constitucional: 75% do subsídio do deputado "
                "estadual de GO (CF Art. 29 VI; municípios >500 mil hab). "
                "Valor efetivo pago pela CMG pode ser menor — verificar "
                "Resolução da Câmara Municipal em vigor."
            ),
            fonte_legal="Constituição Federal Art. 29 VI",
            fonte_url=_PLANALTO_CF_URL,
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
    ]
    return prefeito, vereador


def _municipio_padrao_components(
    municipio: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Componentes genéricos pra cidades sem URLs municipais específicas.

    Fonte legal default: CF Art. 29 (V/VI) + Casa Civil GO (legisla
    estadual hospeda registry de leis orgânicas municipais). Quando uma
    cidade ganhar URL específica de transparência/câmara, vale promovê-la
    pra função própria como ``_goiania_components`` (não tem ganho de
    refatoração — cada cidade tem 4 componentes textualmente distintos).
    """
    slug = str(municipio["slug"])
    rotulo = str(municipio["rotulo"])
    populacao = int(municipio["populacao"])
    pct = _vereador_pct_tier(populacao)
    cap_vereador = _SUBSIDIO_DEP_ESTADUAL_GO * pct
    pct_label = f"{int(round(pct * 100))}%"
    tier_desc = _tier_descritor(populacao)

    prefeito_cargo = f"prefeito_{slug}"
    vereador_cargo = f"vereador_{slug}"

    prefeito = [
        _comp(
            prefeito_cargo, "subsidio", "Subsídio mensal",
            valor_mensal=None,
            valor_observacao=(
                f"fixado pela Lei Orgânica de {rotulo}; teto "
                f"constitucional é o subsídio do governador "
                f"(CF Art. 37 XI / Art. 29 V). Consulte Diário Oficial "
                f"Municipal."
            ),
            fonte_legal=f"CF Art. 29 V; Lei Orgânica de {rotulo}",
            fonte_url=_LEGISLA_GO_URL,
        ),
        _comp(
            prefeito_cargo, "verba_gabinete",
            "Verba de gabinete / cargos comissionados",
            valor_mensal=None,
            valor_observacao=(
                "fixada por decreto municipal; consulte portal de "
                "transparência da prefeitura."
            ),
            fonte_legal="Decreto Municipal",
            fonte_url=_LEGISLA_GO_URL,
        ),
    ]
    vereador = [
        _comp(
            vereador_cargo, "subsidio", "Subsídio mensal (teto legal)",
            valor_mensal=cap_vereador,
            valor_observacao=(
                f"teto constitucional: {pct_label} do subsídio do deputado "
                f"estadual de GO (CF Art. 29 VI; {tier_desc}). Valor "
                f"efetivo pago pela Câmara Municipal pode ser menor — "
                f"verificar Resolução em vigor."
            ),
            fonte_legal="Constituição Federal Art. 29 VI",
            fonte_url=_PLANALTO_CF_URL,
        ),
        _comp(
            vereador_cargo, "verba_gabinete",
            "Verba de gabinete (assessores)",
            valor_mensal=None,
            valor_observacao=(
                "fixada por Resolução da Câmara Municipal; consulte "
                "portal de transparência da câmara."
            ),
            fonte_legal="Resolução da Câmara Municipal",
            fonte_url=_PLANALTO_CF_URL,
        ),
    ]
    return prefeito, vereador


def _build_components_and_meta() -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, dict[str, Any]],
]:
    """Materializa _COMPONENTS e _CARGO_META a partir de _GO_MUNICIPIOS.

    Goiânia usa branch dedicado (URLs específicas); os demais municípios
    seguem o padrão genérico CF + Casa Civil GO.
    """
    components: dict[str, list[dict[str, Any]]] = {}
    meta: dict[str, dict[str, Any]] = {}
    for m in _GO_MUNICIPIOS:
        slug = str(m["slug"])
        rotulo = str(m["rotulo"])
        populacao = int(m["populacao"])
        n_vereadores = m.get("n_vereadores") or _vereador_min_seats(populacao)

        if slug == "goiania":
            prefeito_comps, vereador_comps = _goiania_components()
        else:
            prefeito_comps, vereador_comps = _municipio_padrao_components(m)

        prefeito_cargo = f"prefeito_{slug}"
        vereador_cargo = f"vereador_{slug}"
        components[prefeito_cargo] = prefeito_comps
        components[vereador_cargo] = vereador_comps

        meta[prefeito_cargo] = {
            "esfera": "municipal",
            "rotulo_humano": f"Prefeito(a) de {rotulo}",
            "n_titulares": 1,
            "uf": "GO",
            "municipio": slug,
        }
        meta[vereador_cargo] = {
            "esfera": "municipal",
            "rotulo_humano": f"Vereador(a) de {rotulo}",
            "n_titulares": int(n_vereadores),
            "uf": "GO",
            "municipio": slug,
        }
    return components, meta


_COMPONENTS, _CARGO_META = _build_components_and_meta()

# Constante derivada — preservada pra compat com testes que importam
# ``_VEREADOR_GOIANIA_CAP`` direto. É o mesmo valor materializado em
# ``_COMPONENTS["vereador_goiania"][0]["valor_mensal"]``.
_VEREADOR_GOIANIA_CAP: float = float(
    next(
        c["valor_mensal"]
        for c in _COMPONENTS["vereador_goiania"]
        if c["componente_id"] == "vereador_goiania:subsidio"
    )
)


def _agg_total_mensal(componentes: list[dict[str, Any]]) -> float:
    """Soma componentes com ``incluir_no_total=True`` e ``valor_mensal != None``."""
    return sum(
        float(c["valor_mensal"])
        for c in componentes
        if c.get("incluir_no_total", True) and c.get("valor_mensal") is not None
    )


class CustoMandatoMunicipalGoPipeline(Pipeline):
    """Pipeline pedagógico — custo mensal/anual de prefeito + vereador GO.

    Sem consulta a banco externo: valores são constantes derivadas da
    Constituição Federal (cap do vereador por CF Art. 29 VI) e da Lei
    Orgânica (prefeito, marcado como ``None`` quando fonte é ilegível
    por máquina). O ``extract`` faz ``archive_fetch`` das páginas legais
    pra preservar snapshot do dia da run.

    Cadência recomendada: ``yearly``. Re-run quando o subsídio do dep
    estadual GO mudar (cap do vereador é derivado dele) ou quando uma
    Resolução municipal nova for capturada.
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
