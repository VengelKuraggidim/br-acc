"""Análises comparativas determinísticas (CEAP vs cidadão comum, resumo leigo).

Portado do Flask (`backend/analise.py` linhas 308-521, 691-709) como parte
da fase 04.A. Funções puras — zero network. Constantes `FAIXA_NORMAL`,
`FAIXA_ELEVADO`, `REFERENCIA_CIDADA_MENSAL` preservadas como estavam no
Flask (refactor de conteúdo é fora de escopo).

FONTES (consultadas em abril/2026):
    - Renda per capita mensal Brasil 2025: R$ 2.316 (IBGE/PNAD Contínua)
    - Cesta basica DIEESE Jan/2026: R$ 553 (Aracaju) a R$ 854 (São Paulo)
    - Gasolina media 2025: ~R$ 6,20/L (ANP)
    - Passagem aerea domestica media 2025: R$ 642 (ANAC)
    - Telefonia: telecom = 2,6% do orçamento familiar (Teleco/POF)
    - Transporte por app: custo médio diário R$ 26,77 (IPCA/CNN Brasil 2025)
    - Locação mensal veículos: a partir de R$ 2.200 (Localiza/Movida 2025)
    - Diária hotel média Brasil 2025: ~R$ 430 (FBHA)
    - POF 2017-2018 (IBGE): despesa média familiar R$ 4.649/mês
    - Consultoria básica: R$ 70-200/hora (Roberto Dias Duarte 2024-2025)
"""

from __future__ import annotations

from typing import Any

from bracc.services.formatacao_service import fmt_brl
from bracc.services.traducao_service import (
    _sem_acento,
    traduzir_cargo,
    traduzir_despesa,
)

# --- Referência cidadã mensal (R$/mês) -------------------------------------

REFERENCIA_CIDADA_MENSAL: dict[str, float] = {
    # Transporte
    "combustiveis e lubrificantes": 415,
    "combustivel": 415,
    "passagem aerea": 54,
    "emissao bilhete aereo": 54,
    "passagens aereas": 54,
    "servico de taxi": 120,
    "locacao de veiculos": 0,
    "locacao ou fretamento de veiculos": 0,
    "fretamento de veiculos": 0,
    "locacao ou fretamento de aeronaves": 0,
    # Comunicação
    "telefonia": 55,
    "servico postal": 15,
    "assinatura de publicacoes": 45,
    # Escritório e trabalho
    "manutencao de escritorio": 0,
    "consultorias": 0,
    "participacao em curso": 50,
    # Alimentação e hospedagem
    "alimentacao": 780,
    "fornecimento de alimentacao": 780,
    "hospedagem": 90,
    # Divulgação (exclusiva política/empresarial)
    "divulgacao da atividade": 0,
    "divulgacao": 0,
    # Segurança
    "servicos de seguranca": 0,
}

# Faixas de classificação de gasto político vs cidadão.
FAIXA_NORMAL = 3     # Até 3x = aceitável (verde)
FAIXA_ELEVADO = 8    # Entre 3x e 8x = elevado (amarelo); acima = abusivo (vermelho)

# Fallback quando categoria não está no dict: ~8% da renda per capita 2025.
_FALLBACK_REF = 185


def analisar_despesas_vs_cidadao(
    despesas: list[dict[str, Any]],
    num_meses: int = 24,
) -> dict[str, Any]:
    """Compara gastos CEAP do político com referência do cidadão comum.

    Retorna:
        {
            "comparacoes": list[dict],  # 1 item por categoria
            "alertas": list[dict[str, str]],  # elevado ou abusivo
            "resumo": str,  # linguagem leiga
        }
    """
    if not despesas:
        return {"comparacoes": [], "alertas": [], "resumo": ""}

    por_tipo: dict[str, float] = {}
    for d in despesas:
        tipo = d.get("tipoDespesa", "Outros")
        valor = d.get("valorLiquido", 0) or 0
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor

    comparacoes: list[dict[str, Any]] = []
    alertas: list[dict[str, str]] = []

    for tipo_original, total in sorted(por_tipo.items(), key=lambda x: -x[1]):
        tipo_lower = _sem_acento(tipo_original.lower().strip())

        ref_mensal: float | None = None
        for chave, valor_ref in REFERENCIA_CIDADA_MENSAL.items():
            if chave in tipo_lower:
                ref_mensal = valor_ref
                break
        if ref_mensal is None:
            ref_mensal = _FALLBACK_REF

        media_mensal_politico = total / num_meses if num_meses > 0 else total

        if ref_mensal == 0:
            # Categoria que cidadão NÃO tem (segurança, fretamento, etc)
            if media_mensal_politico > 0:
                classificacao = "abusivo"
                razao: float | None = None
                razao_texto = "Cidadao nao tem esse gasto"
            else:
                continue
        else:
            razao_calc = media_mensal_politico / ref_mensal
            if razao_calc <= FAIXA_NORMAL:
                classificacao = "normal"
            elif razao_calc <= FAIXA_ELEVADO:
                classificacao = "elevado"
            else:
                classificacao = "abusivo"
            razao = round(razao_calc, 1)
            razao_texto = f"{razao_calc:.1f}x"

        tipo_traduzido = traduzir_despesa(tipo_original)

        comparacao: dict[str, Any] = {
            "categoria": tipo_traduzido,
            "categoria_original": tipo_original,
            "total_politico": total,
            "total_politico_fmt": fmt_brl(total),
            "media_mensal_politico": round(media_mensal_politico, 2),
            "media_mensal_politico_fmt": fmt_brl(media_mensal_politico),
            "referencia_cidadao": ref_mensal,
            "referencia_cidadao_fmt": fmt_brl(ref_mensal),
            "razao": razao,
            "razao_texto": razao_texto,
            "classificacao": classificacao,
        }
        comparacoes.append(comparacao)

        if classificacao == "abusivo":
            if ref_mensal > 0:
                alertas.append({
                    "tipo": "grave",
                    "icone": "cidadao",
                    "texto": (
                        f"{tipo_traduzido}: gasta {razao_texto} mais que um "
                        f"cidadao comum ({fmt_brl(media_mensal_politico)}/mes "
                        f"vs {fmt_brl(ref_mensal)}/mes de uma pessoa normal)"
                    ),
                })
            else:
                alertas.append({
                    "tipo": "grave",
                    "icone": "cidadao",
                    "texto": (
                        f"{tipo_traduzido}: {fmt_brl(media_mensal_politico)}/mes "
                        "- gasto que cidadao comum nao tem"
                    ),
                })
        elif classificacao == "elevado":
            alertas.append({
                "tipo": "atencao",
                "icone": "cidadao",
                "texto": (
                    f"{tipo_traduzido}: gasta {razao_texto} mais que um "
                    f"cidadao comum ({fmt_brl(media_mensal_politico)}/mes vs "
                    f"{fmt_brl(ref_mensal)}/mes de referencia)"
                ),
            })

    n_abusivo = sum(1 for c in comparacoes if c["classificacao"] == "abusivo")
    n_elevado = sum(1 for c in comparacoes if c["classificacao"] == "elevado")
    n_normal = sum(1 for c in comparacoes if c["classificacao"] == "normal")

    if n_abusivo > 0:
        resumo = (
            f"{n_abusivo} categoria(s) com gasto ABUSIVO comparado ao cidadao "
            f"comum{f', {n_elevado} elevada(s)' if n_elevado else ''}"
            f" e {n_normal} dentro do aceitavel."
        )
    elif n_elevado > 0:
        resumo = (
            f"{n_elevado} categoria(s) com gasto elevado comparado ao cidadao "
            f"comum e {n_normal} dentro do aceitavel."
        )
    else:
        resumo = (
            "Todos os gastos estao dentro de faixas aceitaveis comparados ao "
            "cidadao comum."
        )

    return {"comparacoes": comparacoes, "alertas": alertas, "resumo": resumo}


def gerar_resumo_politico(
    nome: str,
    cargo: str | None,
    patrimonio: float | None,
    num_emendas: int,
    total_emendas: float,
    num_conexoes: int,
) -> str:
    """Monta resumo em linguagem simples sobre o político."""
    partes: list[str] = []
    cargo_txt = traduzir_cargo(cargo) if cargo else "politico(a)"
    partes.append(f"{nome.title()} e {cargo_txt}.")

    if patrimonio:
        partes.append(f"Patrimonio declarado de {fmt_brl(patrimonio)}.")

    if num_emendas > 0:
        partes.append(
            f"Autor(a) de {num_emendas} emenda(s) parlamentar(es) "
            f"totalizando {fmt_brl(total_emendas)}."
        )

    if num_conexoes > 0:
        partes.append(
            f"Foram identificadas {num_conexoes} conexao(oes) registrada(s) com "
            "empresas, pessoas e contratos publicos."
        )

    return " ".join(partes)
