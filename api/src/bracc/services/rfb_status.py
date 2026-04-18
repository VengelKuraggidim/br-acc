"""Situações cadastrais da Receita Federal — constantes canônicas.

Consolidado em 2026-04-18 pra remover drift entre `conexoes_service` e
`alertas_service` que definiam o mesmo conjunto de códigos de forma
independente. Quando a Receita introduzir uma situação nova, altera só
aqui.

Fonte: Receita Federal — cadastro CNPJ. Campo `situacao_cadastral` tem
5 valores canônicos (mais ``None`` se ainda não foi verificado pelo
pipeline `brasilapi_cnpj_status` ou pelo bulk `cnpj`).
"""

from __future__ import annotations

# Todas as situações RFB conhecidas. Ordem preservada do PDF da RFB.
SITUACOES_CADASTRAIS: tuple[str, ...] = (
    "ATIVA",
    "BAIXADA",
    "SUSPENSA",
    "INAPTA",
    "NULA",
)

# Situações não-operacionais — empresa não devia estar ativa no mercado.
# Doador ou sócio com situação aqui levanta alerta grave no
# `alertas_service` (sinal forte de laranja, caixa 2 ou fraude eleitoral).
SITUACOES_GRAVES: frozenset[str] = frozenset({"BAIXADA", "SUSPENSA", "INAPTA"})

# Labels leigos (pt-BR) pra exibir no UI. Mantém string bruta em
# ``situacao`` (buscas/filtros) e a versão formatada em ``situacao_fmt``.
LABEL_LEIGA: dict[str, str] = {
    "ATIVA": "Ativa",
    "BAIXADA": "Baixada",
    "SUSPENSA": "Suspensa",
    "INAPTA": "Inapta",
    "NULA": "Nula",
}
