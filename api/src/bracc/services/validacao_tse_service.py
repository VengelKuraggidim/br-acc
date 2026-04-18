"""Cross-check TSE: total declarado vs total ingerido no BRACC.

Portado do Flask (`backend/app.py` linhas 898-933) como parte da fase 04.A.
Função pura — zero network, zero BRACC grafo.

Entrada: `props` do nó (properties do político no grafo) + `total_doacoes`
(soma do que BRACC agregou). Saída: `ValidacaoTSE | None` (None se o
político não tem dados TSE ingeridos — campo `total_tse_2022` ausente).

Status:
    - "ok"         se divergência < 5%
    - "atencao"    se 5% <= divergência < 20%
    - "divergente" se divergência >= 20%
"""

from __future__ import annotations

from typing import Any

from bracc.models.perfil import ValidacaoTSE
from bracc.services.formatacao_service import fmt_brl

_BREAKDOWN_LABELS = [
    ("Partido político (fundo partidário + FEFC)", "tse_2022_partido"),
    ("Pessoas físicas", "tse_2022_pessoa_fisica"),
    ("Recursos próprios (autofinanciamento)", "tse_2022_proprios"),
    ("Financiamento coletivo (vaquinha)", "tse_2022_fin_coletivo"),
]


def gerar_validacao_tse(
    props: dict[str, Any],
    total_doacoes: float,
) -> ValidacaoTSE | None:
    """Retorna ValidacaoTSE ou None se o político não tem total_tse_2022."""
    total_tse = props.get("total_tse_2022")
    if not total_tse:
        return None

    declarado = float(total_tse)
    ingerido = total_doacoes
    div = declarado - ingerido
    pct = (abs(div) / declarado * 100) if declarado > 0 else 0.0

    if pct < 5:
        status = "ok"
    elif pct < 20:
        status = "atencao"
    else:
        status = "divergente"

    breakdown: list[dict[str, str]] = []
    for label, key in _BREAKDOWN_LABELS:
        v = props.get(key)
        if v and float(v) > 0:
            breakdown.append({"origem": label, "valor_fmt": fmt_brl(float(v))})

    return ValidacaoTSE(
        ano_eleicao=2022,
        total_declarado_tse=declarado,
        total_declarado_tse_fmt=fmt_brl(declarado),
        total_ingerido=ingerido,
        total_ingerido_fmt=fmt_brl(ingerido),
        divergencia_valor=div,
        divergencia_valor_fmt=fmt_brl(abs(div)),
        divergencia_pct=round(pct, 1),
        breakdown_tse=breakdown,
        status=status,
    )
