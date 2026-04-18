"""Geração de alertas determinísticos (patrimônio, emendas, conexões, CEAP).

Portado do Flask (`backend/analise.py::analisar_*` linhas 139-674 e
`backend/app.py::gerar_alertas_completos` linhas 279-314) como parte da
fase 04.A da consolidação FastAPI. Funções puras — zero network, zero
BRACC grafo (isso é 04.B).

Cada alerta é um `dict[str, str]` com chaves `tipo`, `icone`, `texto`.
`tipo ∈ {"grave", "atencao", "info", "ok"}` — ordenação por severidade
fica a cargo do chamador.

Análises que dependem de API externa (Câmara CEAP live) — `analisar_despesas_vs_media`
recebe valores já computados; a busca em si é 04.D.
"""

from __future__ import annotations

from typing import Any

from bracc.services.formatacao_service import fmt_brl, nomear_mes
from bracc.services.traducao_service import (
    _sem_acento,
    traduzir_cargo,
    traduzir_despesa,
)

# --- Constantes de análise ---------------------------------------------------

# Limites de patrimônio compatíveis por cargo (espelho do Flask).
PATRIMONIO_LIMITES_POR_CARGO: dict[str, float] = {
    "vereador": 2_000_000,
    "deputado estadual": 5_000_000,
    "deputado federal": 10_000_000,
    "senador": 15_000_000,
    "prefeito": 5_000_000,
    "governador": 20_000_000,
}

PATRIMONIO_ABSURDO = 50_000_000  # Acima: alerta info genérico


# Cotas CEAP mensais por UF (R$). Fonte: Câmara dos Deputados, atualizado 2026.
COTA_CEAP_MENSAL: dict[str, int] = {
    "AC": 57_360, "AL": 53_164, "AM": 56_151, "AP": 55_929,
    "BA": 50_965, "CE": 54_879, "DF": 41_613, "ES": 49_160,
    "GO": 46_980, "MA": 54_538, "MG": 47_646, "MS": 52_708,
    "MT": 51_440, "PA": 54_624, "PB": 54_402, "PE": 53_998,
    "PI": 53_196, "PR": 51_952, "RJ": 47_267, "RN": 55_198,
    "RO": 56_268, "RR": 58_475, "RS": 53_087, "SC": 51_951,
    "SE": 52_249, "SP": 48_727, "TO": 51_526,
}


# --- Análises individuais ---------------------------------------------------


def analisar_patrimonio(
    patrimonio: float | None,
    cargo: str | None,
) -> dict[str, str] | None:
    """Alerta se patrimônio declarado é incompatível com cargo (ou absurdo)."""
    if not patrimonio:
        return None

    if cargo:
        cargo_lower = cargo.lower()
        for chave, limite in PATRIMONIO_LIMITES_POR_CARGO.items():
            if chave in cargo_lower and patrimonio > limite:
                return {
                    "tipo": "atencao",
                    "icone": "patrimonio",
                    "texto": (
                        f"Patrimonio declarado ({fmt_brl(patrimonio)}) acima "
                        f"da media para o cargo de {traduzir_cargo(cargo)}"
                    ),
                }

    if patrimonio > PATRIMONIO_ABSURDO:
        return {
            "tipo": "info",
            "icone": "patrimonio",
            "texto": f"Patrimonio declarado muito alto: {fmt_brl(patrimonio)}",
        }
    return None


def analisar_emendas(emendas: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Detecta: concentração em município, emendas relator, não pagas, parciais."""
    alertas: list[dict[str, str]] = []
    if not emendas:
        return alertas

    total = sum(
        e.get("value_paid", 0) or e.get("value_committed", 0) or 0
        for e in emendas
    )

    # Concentração em um município (ignora "Múltiplo" — marcador SIOP).
    municipios: dict[str, float] = {}
    for e in emendas:
        mun_raw = e.get("municipality") or ""
        mun = mun_raw.strip() if isinstance(mun_raw, str) else ""
        if not mun or _sem_acento(mun).lower() == "multiplo":
            continue
        val = e.get("value_paid", 0) or e.get("value_committed", 0) or 0
        municipios[mun] = municipios.get(mun, 0) + val

    if municipios and total > 0:
        maior_mun = max(municipios, key=lambda k: municipios[k])
        pct = municipios[maior_mun] / total * 100
        if pct > 60 and total > 1_000_000:
            alertas.append({
                "tipo": "atencao",
                "icone": "emenda",
                "texto": (
                    f"{pct:.0f}% das emendas concentradas em "
                    f"{maior_mun.title()} ({fmt_brl(municipios[maior_mun])})"
                ),
            })

    # Emendas tipo relator (orçamento secreto).
    relator = [e for e in emendas if "relator" in (e.get("type", "") or "").lower()]
    if relator:
        total_relator = sum(e.get("value_paid", 0) or 0 for e in relator)
        alertas.append({
            "tipo": "grave",
            "icone": "sancao",
            "texto": (
                f"{len(relator)} emenda(s) de relator (orcamento secreto) "
                f"no valor de {fmt_brl(total_relator)}"
            ),
        })

    # Empenhadas mas não pagas.
    nao_pagas = [
        e for e in emendas
        if (e.get("value_committed", 0) or 0) > 0
        and (e.get("value_paid", 0) or 0) <= 0
    ]
    if nao_pagas:
        total_nao_pago = sum(e.get("value_committed", 0) or 0 for e in nao_pagas)
        munic_nao_pago: dict[str, float] = {}
        for e in nao_pagas:
            mun_raw = e.get("municipality") or ""
            mun = mun_raw.strip() if isinstance(mun_raw, str) else ""
            if not mun or _sem_acento(mun).lower() == "multiplo":
                continue
            munic_nao_pago[mun] = (
                munic_nao_pago.get(mun, 0) + (e.get("value_committed", 0) or 0)
            )
        local_txt = ""
        if munic_nao_pago:
            top_mun = max(munic_nao_pago, key=lambda k: munic_nao_pago[k])
            if (
                munic_nao_pago[top_mun] / total_nao_pago >= 0.5
                and len(munic_nao_pago) > 0
            ):
                local_txt = (
                    f" (principal destino: {top_mun.title()} com "
                    f"{fmt_brl(munic_nao_pago[top_mun])})"
                )
        alertas.append({
            "tipo": "grave",
            "icone": "emenda",
            "texto": (
                f"{len(nao_pagas)} emenda(s) empenhada(s) mas nao paga(s): "
                f"{fmt_brl(total_nao_pago)} prometidos que nao chegaram ao "
                f"destino{local_txt}"
            ),
        })

    # Pagas parcialmente (< 99% do empenhado).
    parciais = [
        e for e in emendas
        if (e.get("value_committed", 0) or 0) > 0
        and 0 < (e.get("value_paid", 0) or 0) < (e.get("value_committed", 0) or 0) * 0.99
    ]
    if parciais:
        total_emp_parcial = sum(e.get("value_committed", 0) or 0 for e in parciais)
        total_pago_parcial = sum(e.get("value_paid", 0) or 0 for e in parciais)
        falta = total_emp_parcial - total_pago_parcial
        alertas.append({
            "tipo": "atencao",
            "icone": "emenda",
            "texto": (
                f"{len(parciais)} emenda(s) paga(s) parcialmente: "
                f"{fmt_brl(total_pago_parcial)} de {fmt_brl(total_emp_parcial)} "
                f"empenhados (faltam {fmt_brl(falta)})"
            ),
        })

    return alertas


def analisar_conexoes(
    conexoes: list[dict[str, Any]],
    entidades: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    """Detecta: familiares com empresa, sanções, muitas empresas."""
    alertas: list[dict[str, str]] = []

    familiares_com_empresa: list[str] = []
    for c in conexoes:
        rel = c.get("relationship_type", "")
        if rel in ("CONJUGE_DE", "PARENTE_DE"):
            target = entidades.get(c["target_id"], {})
            if target.get("type") == "person":
                nome = target.get("properties", {}).get("name", "")
                if nome:
                    familiares_com_empresa.append(nome)

    if familiares_com_empresa:
        alertas.append({
            "tipo": "atencao",
            "icone": "familia",
            "texto": (
                "Familiar(es) com vinculos empresariais: "
                + ", ".join(f.title() for f in familiares_com_empresa[:3])
            ),
        })

    for c in conexoes:
        target = entidades.get(c["target_id"], {})
        if target.get("type") == "sanction":
            alertas.append({
                "tipo": "grave",
                "icone": "sancao",
                "texto": "Conexao com entidade sancionada pelo governo",
            })
            break

    empresas = [
        c for c in conexoes
        if entidades.get(c["target_id"], {}).get("type") == "company"
    ]
    if len(empresas) > 5:
        alertas.append({
            "tipo": "atencao",
            "icone": "empresa",
            "texto": f"{len(empresas)} empresas conectadas a este politico",
        })

    return alertas


def analisar_despesas_gabinete(
    despesas: list[dict[str, Any]],
    uf: str | None = None,
    num_meses: int = 24,
) -> list[dict[str, str]]:
    """CEAP: gasto >80% da cota ou categoria dominante >40%."""
    alertas: list[dict[str, str]] = []
    if not despesas:
        return alertas

    total = sum(d.get("valorLiquido", 0) or 0 for d in despesas)
    if total <= 0:
        return alertas

    if uf and uf.upper() in COTA_CEAP_MENSAL:
        cota_mensal = COTA_CEAP_MENSAL[uf.upper()]
        cota_periodo = cota_mensal * num_meses
        pct_cota = total / cota_periodo * 100
        if pct_cota > 80:
            alertas.append({
                "tipo": "atencao",
                "icone": "despesa",
                "texto": (
                    f"Gastou {pct_cota:.0f}% da cota parlamentar "
                    f"({fmt_brl(total)} de {fmt_brl(cota_periodo)} disponiveis)"
                ),
            })

    por_tipo: dict[str, float] = {}
    for d in despesas:
        tipo = d.get("tipoDespesa", "Outros")
        valor = d.get("valorLiquido", 0) or 0
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor

    if por_tipo:
        maior_tipo = max(por_tipo, key=lambda k: por_tipo[k])
        pct_maior = por_tipo[maior_tipo] / total * 100
        if pct_maior > 40:
            alertas.append({
                "tipo": "atencao",
                "icone": "despesa",
                "texto": (
                    f"{pct_maior:.0f}% dos gastos de gabinete concentrados em "
                    f"'{traduzir_despesa(maior_tipo)}' "
                    f"({fmt_brl(por_tipo[maior_tipo])})"
                ),
            })

    return alertas


def analisar_despesas_vs_media(
    total_deputado: float,
    media_estado: float,
    uf: str | None = None,
) -> dict[str, str] | None:
    """Alerta se deputado gasta >1.5x média do estado."""
    if media_estado <= 0 or total_deputado <= 0:
        return None

    razao = total_deputado / media_estado
    if razao > 1.5:
        local = f" de {uf}" if uf else ""
        return {
            "tipo": "atencao",
            "icone": "comparacao",
            "texto": (
                f"Gasta {razao:.1f}x mais que a media dos deputados{local} "
                f"({fmt_brl(total_deputado)} vs media de {fmt_brl(media_estado)})"
            ),
        }
    return None


def analisar_picos_mensais(despesas: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Detecta meses com gasto >2.5x a média mensal do deputado."""
    alertas: list[dict[str, str]] = []
    if not despesas:
        return alertas

    por_mes: dict[str, float] = {}
    for d in despesas:
        ano = d.get("ano")
        mes = d.get("mes")
        valor = d.get("valorLiquido", 0) or 0
        if ano and mes is not None:
            chave = f"{ano}-{mes:02d}" if isinstance(mes, int) else f"{ano}-{mes}"
            por_mes[chave] = por_mes.get(chave, 0) + valor

    if len(por_mes) < 3:
        return alertas

    valores = list(por_mes.values())
    media = sum(valores) / len(valores)
    if media <= 0:
        return alertas

    picos: list[tuple[str, float]] = []
    for mes_key, valor in por_mes.items():
        if valor > media * 2.5 and valor > 20_000:
            picos.append((mes_key, valor))

    if picos:
        picos.sort(key=lambda x: -x[1])
        pico_top = picos[0]
        partes = pico_top[0].split("-")
        mes_nome = nomear_mes(int(partes[1])) if len(partes) == 2 else pico_top[0]
        ano = partes[0] if len(partes) == 2 else ""
        alertas.append({
            "tipo": "atencao",
            "icone": "pico",
            "texto": (
                f"Pico de gasto em {mes_nome}/{ano}: {fmt_brl(pico_top[1])} "
                f"({pico_top[1] / media:.1f}x a media mensal de {fmt_brl(media)})"
            ),
        })
        if len(picos) > 1:
            alertas.append({
                "tipo": "info",
                "icone": "pico",
                "texto": (
                    f"Outros {len(picos) - 1} mes(es) com gasto "
                    "acima de 2.5x a media"
                ),
            })

    return alertas


# --- Orquestração -----------------------------------------------------------


def gerar_alertas_completos(
    entidade: dict[str, Any],
    conexoes_raw: list[dict[str, Any]],
    entidades_conectadas: dict[str, dict[str, Any]],
    emendas_raw: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Orquestra análises de patrimônio + emendas + conexões.

    Se nenhum alerta for gerado, retorna 1 alerta `info` padrão avisando
    que a avaliação está indisponível.
    """
    alertas: list[dict[str, str]] = []
    props = entidade.get("properties", {})

    alerta_pat = analisar_patrimonio(
        props.get("patrimonio_declarado"),
        props.get("role") or props.get("cargo"),
    )
    if alerta_pat:
        alertas.append(alerta_pat)

    alertas.extend(analisar_emendas(emendas_raw))
    alertas.extend(analisar_conexoes(conexoes_raw, entidades_conectadas))

    if not alertas:
        alertas.append({
            "tipo": "info",
            "icone": "info",
            "texto": (
                "Avaliação indisponível no momento. "
                "Não foi possível obter dados suficientes para analisar esta "
                "entidade. A ausência de alertas não significa que não existam "
                "irregularidades."
            ),
        })

    return alertas
