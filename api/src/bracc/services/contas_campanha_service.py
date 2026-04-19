"""Cross-check TSE: receitas declaradas vs despesas pagas declaradas.

Fase 1 do roadmap "cross-check de perspectivas TSE" (ver
``todo-list-prompts/high_priority/debitos/cross-check-perspectivas-tse.md``).

Compara duas declarações do próprio candidato (lado A: receitas; lado B:
despesas) obtidas do mesmo ``:Person`` no grafo, populadas pelo pipeline
:mod:`bracc_etl.pipelines.tse_prestacao_contas_go`:

* ``total_tse_{ano}``           — soma bruta de receitas declaradas.
* ``total_despesas_tse_{ano}``  — soma bruta de despesas pagas declaradas.

Divergência grande pode indicar inconsistência na prestação de contas,
mas pode ter causas legítimas (empréstimo declarado, doação posterior,
gastos próprios, sobras devolvidas ao TSE). Surfaceamos a divergência
sem rotular causa — link pra prestação oficial deixa o cidadão validar.

Função pura — zero network, zero grafo.

Status:
    - "ok"         se divergência < 5%
    - "atencao"    se 5% <= divergência < 20%
    - "divergente" se divergência >= 20%
"""

from __future__ import annotations

from typing import Any

from bracc.models.perfil import ComparacaoContas
from bracc.services.formatacao_service import fmt_brl


def gerar_comparacao_contas(
    props: dict[str, Any],
    ano: int = 2022,
) -> ComparacaoContas | None:
    """Retorna ``ComparacaoContas`` ou ``None`` se faltar receita/despesa.

    Ambas as props (``total_tse_{ano}`` e ``total_despesas_tse_{ano}``)
    precisam estar presentes e > 0 pra que a comparação tenha sentido —
    senão o card é omitido (degradação silenciosa, igual
    ``validacao_tse_service``).
    """
    receitas_raw = props.get(f"total_tse_{ano}")
    despesas_raw = props.get(f"total_despesas_tse_{ano}")
    if not receitas_raw or not despesas_raw:
        return None

    try:
        receitas = float(receitas_raw)
        despesas = float(despesas_raw)
    except (TypeError, ValueError):
        return None

    if receitas <= 0 or despesas <= 0:
        return None

    div = receitas - despesas  # sinal: positivo → sobra; negativo → estouro
    maior = max(receitas, despesas)
    pct = (abs(div) / maior * 100) if maior > 0 else 0.0

    if pct < 5:
        status: str = "ok"
    elif pct < 20:
        status = "atencao"
    else:
        status = "divergente"

    direcao: str = "despesas_excedem" if despesas > receitas else "receitas_excedem"

    return ComparacaoContas(
        ano_eleicao=ano,
        total_receitas=receitas,
        total_receitas_fmt=fmt_brl(receitas),
        total_despesas=despesas,
        total_despesas_fmt=fmt_brl(despesas),
        divergencia_valor=div,
        divergencia_valor_fmt=fmt_brl(abs(div)),
        divergencia_pct=round(pct, 1),
        direcao=direcao,
        status=status,
    )
