"""Modelos Pydantic do endpoint ``GET /custo-mandato/{cargo}``.

Materializa o conteúdo do nó ``:CustoMandato`` (gerado pelo pipeline
``custo_mandato_br``) + lista de ``:CustoComponente`` ligados, com
ProvenanceBlock por componente. Substitui o card hardcoded
``Quanto custa um deputado federal?`` que vivia no PWA com copy estático
sem proveniência clicável.

Cargos suportados (MVP): ``dep_federal``, ``senador``, ``dep_estadual_go``,
``governador_go``. Prefeito e vereador ficam como débito (lei orgânica
municipal sem API consolidada — ver
``todo-list-prompts/high_priority/debitos/custo-mandato-municipal.md``).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from bracc.models.entity import ProvenanceBlock  # noqa: TC001 (pydantic runtime annotation)

_CUSTO_MODEL_CONFIG = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
)


class CustoComponente(BaseModel):
    """Linha individual da composição de custo (subsídio, gabinete, etc.).

    ``valor_mensal=None`` significa "fonte não divulga em formato
    consolidado" (ex.: saúde dos deputados federais; subsídio do
    governador GO sem Lei estadual ainda capturada). O PWA renderiza
    "não divulgado" + link clicável pra fonte (ProvenanceBlock).

    ``incluir_no_total=False`` marca componentes que existem mas
    representam **teto** ou referência (ex.: CEAP "até R$ 57,3 mil"
    varia por estado), pra não inflarem ``custo_mensal_individual``.
    """

    model_config = _CUSTO_MODEL_CONFIG

    componente_id: str
    rotulo: str
    valor_mensal: float | None = None
    valor_mensal_fmt: str | None = None
    valor_observacao: str = ""
    fonte_legal: str
    fonte_url: str
    incluir_no_total: bool = True
    ordem: int = 0
    provenance: ProvenanceBlock | None = None


class CustoMandato(BaseModel):
    """Custo agregado do cargo eletivo + composição detalhada.

    ``custo_mensal_individual`` soma só componentes com
    ``incluir_no_total=True`` e ``valor_mensal != None`` — reflete custo
    certo por titular. ``custo_anual_total`` multiplica por 12 e por
    ``n_titulares`` (ex.: 513 deputados federais).

    ``equivalente_trabalhadores_min`` divide ``custo_mensal_individual``
    pelo salário mínimo nacional pra contextualizar
    "para sustentar 1 titular precisa-se de N salários mínimos".
    """

    model_config = _CUSTO_MODEL_CONFIG

    cargo: str
    rotulo_humano: str
    esfera: str
    uf: str | None = None
    municipio: str | None = None
    n_titulares: int = 0
    custo_mensal_individual: float = 0.0
    custo_mensal_individual_fmt: str = "R$ 0,00"
    custo_anual_total: float = 0.0
    custo_anual_total_fmt: str = "R$ 0,00"
    equivalente_trabalhadores_min: int = 0
    salario_minimo_referencia: float = 0.0
    salario_minimo_fonte: str | None = None
    componentes: list[CustoComponente]
    provenance: ProvenanceBlock | None = None
