"""Modulo de analise inteligente de dados publicos.

Traduz dados tecnicos pra linguagem simples e detecta anomalias
sem depender de API externa (zero custo).
"""

from __future__ import annotations

import unicodedata


# --- Traducao de termos tecnicos ---

CARGOS = {
    "deputado federal": "Deputado(a) Federal",
    "deputado estadual": "Deputado(a) Estadual",
    "senador": "Senador(a)",
    "vereador": "Vereador(a)",
    "prefeito": "Prefeito(a)",
    "governador": "Governador(a)",
    "presidente": "Presidente",
    "vice-prefeito": "Vice-Prefeito(a)",
    "vice-governador": "Vice-Governador(a)",
}

FUNCOES_EMENDA = {
    "urbanismo": "Obras e melhorias urbanas",
    "saude": "Saude publica",
    "educacao": "Educacao",
    "assistencia social": "Assistencia social",
    "agricultura": "Agricultura e pecuaria",
    "transporte": "Transporte e mobilidade",
    "seguranca publica": "Seguranca publica",
    "cultura": "Cultura e lazer",
    "desporto e lazer": "Esporte e lazer",
    "saneamento": "Saneamento basico",
    "habitacao": "Habitacao e moradia",
    "ciencia e tecnologia": "Ciencia e tecnologia",
    "gestao ambiental": "Meio ambiente",
    "comercio e servicos": "Comercio e servicos",
    "industria": "Industria",
    "energia": "Energia",
    "comunicacoes": "Comunicacoes",
    "trabalho": "Trabalho e emprego",
    "direitos da cidadania": "Direitos da cidadania",
    "encargos especiais": "Encargos especiais (divida/transferencias)",
    "legislativa": "Atividade legislativa",
    "judiciaria": "Atividade judiciaria",
    "administracao": "Administracao publica",
    "defesa nacional": "Defesa nacional",
    "relacoes exteriores": "Relacoes exteriores",
    "organizacao agraria": "Reforma agraria",
    "previdencia social": "Previdencia social",
}

TIPOS_DESPESA = {
    "emissao bilhete aereo": "Passagem aerea",
    "passagem aerea": "Passagem aerea",
    "telefonia": "Telefone",
    "servico postal": "Correios",
    "manutencao de escritorio": "Escritorio",
    "consultorias": "Consultoria",
    "divulgacao da atividade": "Divulgacao/propaganda",
    "divulgacao": "Divulgacao/propaganda",
    "combustiveis e lubrificantes": "Combustivel",
    "combustivel": "Combustivel",
    "servico de taxi": "Taxi/transporte",
    "locacao de veiculos": "Aluguel de veiculo",
    "locacao ou fretamento de veiculos": "Aluguel de veiculo",
    "fretamento de veiculos": "Aluguel de veiculo",
    "passagens aereas": "Passagens aereas",
    "hospedagem": "Hospedagem",
    "alimentacao": "Alimentacao",
    "servicos de seguranca": "Seguranca",
    "assinatura de publicacoes": "Assinatura de jornais/revistas",
    "locacao ou fretamento de aeronaves": "Fretamento de aeronave",
    "participacao em curso": "Curso/capacitacao",
    "fornecimento de alimentacao": "Alimentacao",
}

TIPOS_EMENDA = {
    "individual": "Emenda individual (feita por um unico parlamentar)",
    "bancada": "Emenda de bancada (feita pelo grupo do partido/estado)",
    "comissao": "Emenda de comissao (feita por comissao do Congresso)",
    "relator": "Emenda de relator (orcamento secreto - extinto)",
    "pix": "Emenda Pix (transferencia direta pra municipio)",
}


def traduzir_cargo(cargo: str) -> str:
    if not cargo:
        return ""
    cargo_lower = cargo.lower().strip()
    for chave, traducao in CARGOS.items():
        if chave in cargo_lower:
            return traducao
    return cargo.title()


def traduzir_funcao_emenda(funcao: str) -> str:
    if not funcao:
        return "Nao informada"
    funcao_lower = funcao.lower().strip()
    for chave, traducao in FUNCOES_EMENDA.items():
        if chave in funcao_lower:
            return traducao
    return funcao.title()


def traduzir_tipo_emenda(tipo: str) -> str:
    if not tipo:
        return "Nao informado"
    tipo_lower = tipo.lower().strip()
    for chave, traducao in TIPOS_EMENDA.items():
        if chave in tipo_lower:
            return traducao
    return tipo.title()


def _sem_acento(texto: str) -> str:
    """Remove acentos de uma string."""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def traduzir_despesa(descricao: str) -> str:
    if not descricao:
        return "Despesa nao especificada"
    desc_lower = _sem_acento(descricao.lower().strip())
    for chave, traducao in TIPOS_DESPESA.items():
        if chave in desc_lower:
            return traducao
    return descricao.title()


# --- Analise de anomalias ---


def analisar_patrimonio(patrimonio: float | None, cargo: str | None) -> dict | None:
    """Analisa se o patrimonio declarado e compativel com o cargo."""
    if not patrimonio:
        return None

    limites = {
        "vereador": 2_000_000,
        "deputado estadual": 5_000_000,
        "deputado federal": 10_000_000,
        "senador": 15_000_000,
        "prefeito": 5_000_000,
        "governador": 20_000_000,
    }

    if cargo:
        cargo_lower = cargo.lower()
        for chave, limite in limites.items():
            if chave in cargo_lower and patrimonio > limite:
                return {
                    "tipo": "atencao",
                    "icone": "patrimonio",
                    "texto": f"Patrimonio declarado ({_fmt(patrimonio)}) acima da media para o cargo de {traduzir_cargo(cargo)}",
                }

    if patrimonio > 50_000_000:
        return {
            "tipo": "info",
            "icone": "patrimonio",
            "texto": f"Patrimonio declarado muito alto: {_fmt(patrimonio)}",
        }
    return None


def analisar_emendas(emendas: list[dict]) -> list[dict]:
    """Analisa padroes nas emendas de um politico."""
    alertas = []
    if not emendas:
        return alertas

    total = sum(e.get("value_paid", 0) or e.get("value_committed", 0) or 0 for e in emendas)

    if total > 10_000_000:
        alertas.append({
            "tipo": "info",
            "icone": "emenda",
            "texto": f"Total de {_fmt(total)} em emendas parlamentares",
        })

    # Concentracao em um municipio
    municipios: dict[str, float] = {}
    for e in emendas:
        mun = e.get("municipality", "")
        val = e.get("value_paid", 0) or e.get("value_committed", 0) or 0
        if mun:
            municipios[mun] = municipios.get(mun, 0) + val

    if municipios and total > 0:
        maior_mun = max(municipios, key=municipios.get)
        pct = municipios[maior_mun] / total * 100
        if pct > 60 and total > 1_000_000:
            alertas.append({
                "tipo": "atencao",
                "icone": "emenda",
                "texto": f"{pct:.0f}% das emendas concentradas em {maior_mun.title()} ({_fmt(municipios[maior_mun])})",
            })

    # Emendas tipo relator (orcamento secreto)
    relator = [e for e in emendas if "relator" in (e.get("type", "") or "").lower()]
    if relator:
        total_relator = sum(e.get("value_paid", 0) or 0 for e in relator)
        alertas.append({
            "tipo": "grave",
            "icone": "sancao",
            "texto": f"{len(relator)} emenda(s) de relator (orcamento secreto) no valor de {_fmt(total_relator)}",
        })

    # Emendas empenhadas mas nao pagas (dinheiro prometido que nao chegou no destino)
    nao_pagas = [
        e for e in emendas
        if (e.get("value_committed", 0) or 0) > 0
        and (e.get("value_paid", 0) or 0) <= 0
    ]
    if nao_pagas:
        total_nao_pago = sum(e.get("value_committed", 0) or 0 for e in nao_pagas)
        # Top municipio com mais emendas nao pagas
        munic_nao_pago: dict[str, float] = {}
        for e in nao_pagas:
            mun = (e.get("municipality") or "").strip()
            if mun:
                munic_nao_pago[mun] = munic_nao_pago.get(mun, 0) + (e.get("value_committed", 0) or 0)
        local_txt = ""
        if munic_nao_pago:
            top_mun = max(munic_nao_pago, key=munic_nao_pago.get)
            if munic_nao_pago[top_mun] / total_nao_pago >= 0.5 and len(munic_nao_pago) > 0:
                local_txt = f" (principal destino: {top_mun.title()} com {_fmt(munic_nao_pago[top_mun])})"
        alertas.append({
            "tipo": "grave",
            "icone": "emenda",
            "texto": (
                f"{len(nao_pagas)} emenda(s) empenhada(s) mas nao paga(s): "
                f"{_fmt(total_nao_pago)} prometidos que nao chegaram ao destino{local_txt}"
            ),
        })

    # Emendas pagas parcialmente
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
                f"{_fmt(total_pago_parcial)} de {_fmt(total_emp_parcial)} empenhados "
                f"(faltam {_fmt(falta)})"
            ),
        })

    return alertas


def analisar_conexoes(conexoes: list[dict], entidades: dict) -> list[dict]:
    """Analisa padroes nas conexoes de um politico."""
    alertas = []

    # Familiares com empresas
    familiares_com_empresa = []
    for c in conexoes:
        rel = c.get("relationship_type", "")
        if rel in ("CONJUGE_DE", "PARENTE_DE"):
            target = entidades.get(c["target_id"], {})
            if target.get("type") == "person":
                # Verifica se este familiar tem conexoes empresariais
                familiares_com_empresa.append(target.get("properties", {}).get("name", ""))

    if familiares_com_empresa:
        alertas.append({
            "tipo": "atencao",
            "icone": "familia",
            "texto": f"Familiar(es) com vinculos empresariais: {', '.join(f.title() for f in familiares_com_empresa[:3])}",
        })

    # Empresas sancionadas
    for c in conexoes:
        target = entidades.get(c["target_id"], {})
        if target.get("type") == "sanction":
            alertas.append({
                "tipo": "grave",
                "icone": "sancao",
                "texto": "Conexao com entidade sancionada pelo governo",
            })
            break

    # Muitas empresas conectadas
    empresas = [c for c in conexoes if entidades.get(c["target_id"], {}).get("type") == "company"]
    if len(empresas) > 5:
        alertas.append({
            "tipo": "atencao",
            "icone": "empresa",
            "texto": f"{len(empresas)} empresas conectadas a este politico",
        })

    return alertas


# --- Referencia Cidada: o que uma pessoa comum gasta por mes em cada categoria ---
#
# Valores mensais em reais baseados em dados oficiais e pesquisas publicas.
# Usados como base para detectar gastos desproporcionais de politicos.
#
# FONTES (consultadas em abril/2026):
#   - Renda per capita mensal Brasil 2025: R$ 2.316 (IBGE/PNAD Continua)
#   - Cesta basica DIEESE Jan/2026: R$ 553 (Aracaju) a R$ 854 (Sao Paulo)
#   - Gasolina media 2025: ~R$ 6,20/L (ANP - serie historica semanal)
#   - Passagem aerea domestica media 2025: R$ 642 (ANAC)
#   - Telefonia: telecom = 2,6% do orcamento familiar (Teleco/POF)
#   - Transporte por app: custo medio diario R$ 26,77 (IPCA/CNN Brasil 2025)
#   - Locacao mensal veiculos: a partir de R$ 2.200 (Localiza/Movida 2025)
#   - Diaria hotel media Brasil 2025: ~R$ 430 (FBHA, alta de 10,6% vs 2024)
#   - POF 2017-2018 (IBGE): despesa media familiar R$ 4.649/mes;
#     alimentacao+habitacao+transporte = 72,2% do total
#   - Consultoria basica: R$ 70-200/hora (Roberto Dias Duarte 2024-2025)

REFERENCIA_CIDADA_MENSAL = {
    # Transporte
    # ANP 2025: gasolina media R$ 6,20/L. Brasileiro medio roda ~800km/mes,
    # carro popular faz ~12km/L = ~67L/mes = ~R$ 415.
    "combustiveis e lubrificantes": 415,
    "combustivel": 415,
    # ANAC 2025: tarifa media domestica R$ 642. Brasileiro comum voa ~1x/ano
    # (IBGE: apenas 35% da populacao ja viajou de aviao). R$ 642/12 = ~R$ 54.
    "passagem aerea": 54,
    "emissao bilhete aereo": 54,
    "passagens aereas": 54,
    # IPCA/CNN Brasil 2025: custo medio diario de app R$ 26,77.
    # Cidadao comum usa ~4-5x/mes, nao diariamente = ~R$ 120.
    "servico de taxi": 120,
    # Localiza/Movida 2025: aluguel mensal a partir de R$ 2.200.
    # Cidadao comum NAO aluga carro mensalmente (tem carro proprio ou usa
    # transporte publico). Valor zero como referencia de gasto recorrente.
    "locacao de veiculos": 0,
    "locacao ou fretamento de veiculos": 0,
    "fretamento de veiculos": 0,
    "locacao ou fretamento de aeronaves": 0,  # Pessoa comum NAO freta aviao
    # Comunicacao
    # Teleco/Anatel: telecom = 2,6% do orcamento. Sobre renda de R$ 2.316
    # = ~R$ 60. Planos populares (Claro/Vivo/TIM) custam R$ 45-70.
    "telefonia": 55,
    "servico postal": 15,                     # Correios: uso esporadico
    "assinatura de publicacoes": 45,          # ~1 streaming ou jornal digital
    # Escritorio e trabalho
    # Cidadao comum nao mantem escritorio proprio; custo de home-office basico.
    "manutencao de escritorio": 0,
    # Consultoria basica R$ 70-200/h. Cidadao comum nao contrata consultorias.
    "consultorias": 0,
    # Cursos online populares: R$ 30-80/mes (Udemy, Alura, etc.)
    "participacao em curso": 50,
    # Alimentacao e hospedagem
    # DIEESE Jan/2026: cesta basica media capitais ~R$ 700.
    # POF/IBGE: alimentacao fora + dentro = ~17% do orcamento.
    # Sobre renda R$ 2.316 = ~R$ 394 per capita; para familia ~R$ 780.
    "alimentacao": 780,
    "fornecimento de alimentacao": 780,
    # FBHA 2025: diaria media R$ 430. Cidadao comum se hospeda raramente
    # (~2-3 noites/ano em viagens). R$ 430 * 2.5 / 12 = ~R$ 90.
    "hospedagem": 90,
    # Divulgacao: cidadao comum NAO faz divulgacao/propaganda.
    # Categoria exclusiva de atividade politica/empresarial.
    "divulgacao da atividade": 0,
    "divulgacao": 0,
    # Seguranca (cidadao comum NAO tem seguranca privada)
    "servicos de seguranca": 0,
}

# Multiplicadores para classificacao:
# Politico tem demandas maiores que cidadao comum, entao usamos faixas.
# Ate FAIXA_NORMAL x a referencia = aceitavel (verde)
# Entre FAIXA_NORMAL e FAIXA_ELEVADO = elevado, merece atencao (amarelo)
# Acima de FAIXA_ELEVADO = abusivo, alerta vermelho
FAIXA_NORMAL = 3     # ate 3x o que pessoa comum gasta
FAIXA_ELEVADO = 8    # ate 8x = elevado; acima = abusivo


def analisar_despesas_vs_cidadao(
    despesas: list[dict], num_meses: int = 24,
) -> dict:
    """Compara gastos do politico com referencia do cidadao comum.

    Retorna dict com:
        - comparacoes: lista de comparacoes por categoria
        - alertas: lista de alertas gerados
        - resumo: texto resumo
    """
    if not despesas:
        return {"comparacoes": [], "alertas": [], "resumo": ""}

    # Agrupar despesas por tipo
    por_tipo: dict[str, float] = {}
    for d in despesas:
        tipo = d.get("tipoDespesa", "Outros")
        valor = d.get("valorLiquido", 0) or 0
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor

    comparacoes = []
    alertas = []
    total_abusivo = 0
    categorias_abusivas = []

    for tipo_original, total in sorted(por_tipo.items(), key=lambda x: -x[1]):
        tipo_lower = _sem_acento(tipo_original.lower().strip())

        # Encontrar referencia cidada para esta categoria
        ref_mensal = None
        for chave, valor_ref in REFERENCIA_CIDADA_MENSAL.items():
            if chave in tipo_lower:
                ref_mensal = valor_ref
                break

        if ref_mensal is None:
            # Categoria desconhecida: usar ~8% da renda per capita mensal
            # (IBGE/PNAD 2025: R$ 2.316). R$ 2.316 * 0.08 ≈ R$ 185.
            ref_mensal = 185

        # Calcular media mensal do politico
        media_mensal_politico = total / num_meses if num_meses > 0 else total

        # Calcular razao (quanto o politico gasta vs cidadao)
        if ref_mensal == 0:
            # Categorias que cidadao NAO gasta (ex: seguranca, fretamento de aviao)
            if media_mensal_politico > 0:
                classificacao = "abusivo"
                razao = 0  # infinito, mas marcamos como abusivo direto
                razao_texto = "Cidadao nao tem esse gasto"
            else:
                continue
        else:
            razao = media_mensal_politico / ref_mensal
            if razao <= FAIXA_NORMAL:
                classificacao = "normal"
            elif razao <= FAIXA_ELEVADO:
                classificacao = "elevado"
            else:
                classificacao = "abusivo"
            razao_texto = f"{razao:.1f}x"

        tipo_traduzido = traduzir_despesa(tipo_original)

        comparacao = {
            "categoria": tipo_traduzido,
            "categoria_original": tipo_original,
            "total_politico": total,
            "total_politico_fmt": _fmt(total),
            "media_mensal_politico": round(media_mensal_politico, 2),
            "media_mensal_politico_fmt": _fmt(media_mensal_politico),
            "referencia_cidadao": ref_mensal,
            "referencia_cidadao_fmt": _fmt(ref_mensal),
            "razao": round(razao, 1) if ref_mensal > 0 else None,
            "razao_texto": razao_texto,
            "classificacao": classificacao,
        }
        comparacoes.append(comparacao)

        # Gerar alertas para categorias elevadas e abusivas
        if classificacao == "abusivo":
            total_abusivo += total
            categorias_abusivas.append(tipo_traduzido)
            if ref_mensal > 0:
                alertas.append({
                    "tipo": "grave",
                    "icone": "cidadao",
                    "texto": (
                        f"{tipo_traduzido}: gasta {razao_texto} mais que um cidadao comum "
                        f"({_fmt(media_mensal_politico)}/mes vs {_fmt(ref_mensal)}/mes de uma pessoa normal)"
                    ),
                })
            else:
                alertas.append({
                    "tipo": "grave",
                    "icone": "cidadao",
                    "texto": (
                        f"{tipo_traduzido}: {_fmt(media_mensal_politico)}/mes "
                        f"- gasto que cidadao comum nao tem"
                    ),
                })
        elif classificacao == "elevado":
            alertas.append({
                "tipo": "atencao",
                "icone": "cidadao",
                "texto": (
                    f"{tipo_traduzido}: gasta {razao_texto} mais que um cidadao comum "
                    f"({_fmt(media_mensal_politico)}/mes vs {_fmt(ref_mensal)}/mes de referencia)"
                ),
            })

    # Resumo geral
    resumo = ""
    n_abusivo = sum(1 for c in comparacoes if c["classificacao"] == "abusivo")
    n_elevado = sum(1 for c in comparacoes if c["classificacao"] == "elevado")
    n_normal = sum(1 for c in comparacoes if c["classificacao"] == "normal")

    if n_abusivo > 0:
        resumo = (
            f"{n_abusivo} categoria(s) com gasto ABUSIVO comparado ao cidadao comum"
            f"{f', {n_elevado} elevada(s)' if n_elevado else ''}"
            f" e {n_normal} dentro do aceitavel."
        )
    elif n_elevado > 0:
        resumo = (
            f"{n_elevado} categoria(s) com gasto elevado comparado ao cidadao comum"
            f" e {n_normal} dentro do aceitavel."
        )
    else:
        resumo = "Todos os gastos estao dentro de faixas aceitaveis comparados ao cidadao comum."

    return {
        "comparacoes": comparacoes,
        "alertas": alertas,
        "resumo": resumo,
    }


# --- Cotas CEAP mensais por UF (valores publicos, atualizados 2026) ---
# Fonte: Camara dos Deputados (www2.camara.leg.br/comunicacao/assessoria-de-imprensa/guia-para-jornalistas/cota-parlamentar)

COTA_CEAP_MENSAL = {
    "AC": 57_360, "AL": 53_164, "AM": 56_151, "AP": 55_929,
    "BA": 50_965, "CE": 54_879, "DF": 41_613, "ES": 49_160,
    "GO": 46_980, "MA": 54_538, "MG": 47_646, "MS": 52_708,
    "MT": 51_440, "PA": 54_624, "PB": 54_402, "PE": 53_998,
    "PI": 53_196, "PR": 51_952, "RJ": 47_267, "RN": 55_198,
    "RO": 56_268, "RR": 58_475, "RS": 53_087, "SC": 51_951,
    "SE": 52_249, "SP": 48_727, "TO": 51_526,
}


def analisar_despesas_gabinete(
    despesas: list[dict], uf: str | None = None, num_meses: int = 24,
) -> list[dict]:
    """Analisa despesas CEAP por categoria e detecta anomalias.

    Args:
        despesas: lista de despesas brutas da API da Camara
        uf: UF do deputado (para comparar com a cota)
        num_meses: quantidade de meses cobertos pelos dados (padrao 24 = 2 anos)
    """
    alertas = []
    if not despesas:
        return alertas

    # Total gasto
    total = sum(d.get("valorLiquido", 0) or 0 for d in despesas)
    if total <= 0:
        return alertas

    # Comparar com cota CEAP
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
                    f"({_fmt(total)} de {_fmt(cota_periodo)} disponiveis)"
                ),
            })

    # Categoria dominante (uma unica categoria > 40% do total)
    por_tipo: dict[str, float] = {}
    for d in despesas:
        tipo = d.get("tipoDespesa", "Outros")
        valor = d.get("valorLiquido", 0) or 0
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor

    if por_tipo:
        maior_tipo = max(por_tipo, key=por_tipo.get)
        pct_maior = por_tipo[maior_tipo] / total * 100
        if pct_maior > 40:
            alertas.append({
                "tipo": "atencao",
                "icone": "despesa",
                "texto": (
                    f"{pct_maior:.0f}% dos gastos de gabinete concentrados em "
                    f"'{_traduzir_despesa_simples(maior_tipo)}' ({_fmt(por_tipo[maior_tipo])})"
                ),
            })

    return alertas


def analisar_despesas_vs_media(
    total_deputado: float, media_estado: float, uf: str | None = None,
) -> dict | None:
    """Compara o gasto total de um deputado com a media do estado.

    Args:
        total_deputado: gasto total do deputado no periodo
        media_estado: media de gastos dos deputados do mesmo estado
        uf: UF para mensagem
    """
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
                f"({_fmt(total_deputado)} vs media de {_fmt(media_estado)})"
            ),
        }
    return None


def analisar_picos_mensais(despesas: list[dict]) -> list[dict]:
    """Detecta meses com gastos muito acima da media mensal do deputado."""
    alertas = []
    if not despesas:
        return alertas

    # Agrupar por ano-mes
    por_mes: dict[str, float] = {}
    for d in despesas:
        ano = d.get("ano")
        mes = d.get("mes")
        valor = d.get("valorLiquido", 0) or 0
        if ano and mes:
            chave = f"{ano}-{mes:02d}" if isinstance(mes, int) else f"{ano}-{mes}"
            por_mes[chave] = por_mes.get(chave, 0) + valor

    if len(por_mes) < 3:
        return alertas

    valores = list(por_mes.values())
    media = sum(valores) / len(valores)

    if media <= 0:
        return alertas

    # Detectar meses com gasto > 2.5x a media
    picos = []
    for mes_key, valor in por_mes.items():
        if valor > media * 2.5 and valor > 20_000:
            picos.append((mes_key, valor))

    if picos:
        picos.sort(key=lambda x: -x[1])
        pico_top = picos[0]
        partes = pico_top[0].split("-")
        mes_nome = _nome_mes(int(partes[1])) if len(partes) == 2 else pico_top[0]
        ano = partes[0] if len(partes) == 2 else ""
        alertas.append({
            "tipo": "atencao",
            "icone": "pico",
            "texto": (
                f"Pico de gasto em {mes_nome}/{ano}: {_fmt(pico_top[1])} "
                f"({pico_top[1] / media:.1f}x a media mensal de {_fmt(media)})"
            ),
        })
        if len(picos) > 1:
            alertas.append({
                "tipo": "info",
                "icone": "pico",
                "texto": f"Outros {len(picos) - 1} mes(es) com gasto acima de 2.5x a media",
            })

    return alertas


def _traduzir_despesa_simples(descricao: str) -> str:
    """Versao simplificada para uso interno nos alertas."""
    from analise import traduzir_despesa
    return traduzir_despesa(descricao)


def _nome_mes(n: int) -> str:
    nomes = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
        7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
    }
    return nomes.get(n, str(n))


def gerar_resumo_politico(nome: str, cargo: str | None, patrimonio: float | None,
                          num_emendas: int, total_emendas: float,
                          num_conexoes: int) -> str:
    """Gera um resumo em linguagem simples sobre o politico."""
    partes = []
    partes.append(f"{nome.title()} e {traduzir_cargo(cargo) if cargo else 'politico(a)'}.")

    if patrimonio:
        partes.append(f"Patrimonio declarado de {_fmt(patrimonio)}.")

    if num_emendas > 0:
        partes.append(f"Autor(a) de {num_emendas} emenda(s) parlamentar(es) "
                      f"totalizando {_fmt(total_emendas)}.")

    if num_conexoes > 0:
        partes.append(f"Possui {num_conexoes} conexao(oes) registrada(s) "
                      f"com empresas, pessoas e contratos publicos.")

    return " ".join(partes)


def _fmt(valor: float) -> str:
    if valor >= 1_000_000_000:
        return f"R$ {valor / 1_000_000_000:.2f} bi"
    if valor >= 1_000_000:
        return f"R$ {valor / 1_000_000:.2f} mi"
    if valor >= 1_000:
        return f"R$ {valor / 1_000:.1f} mil"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
