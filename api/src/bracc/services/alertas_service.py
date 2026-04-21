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

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

from bracc.services.formatacao_service import fmt_brl, nomear_mes
from bracc.services.traducao_service import (
    _sem_acento,
    traduzir_cargo,
    traduzir_despesa,
)

if TYPE_CHECKING:
    from bracc.models.perfil import (
        DoadorEmpresa,
        Emenda,
        RedFlagsSummary,
        SocioConectado,
        TetoGastos,
    )


class _PerfilComEmpresas(Protocol):
    """Duck-typed shape que ``analisar_cnpj_baixados`` consome.

    Aceita :class:`PerfilPolitico` (tem ambos os atributos) e também
    :class:`bracc.services.conexoes_service.ConexoesClassificadas`
    (mesmo shape) — o serviço de perfil chama antes do model final
    estar montado, então duck-typing simplifica o call site.
    """

    doadores_empresa: list[DoadorEmpresa]
    socios: list[SocioConectado]

# Situações RFB não-operacionais levantam alerta grave em doadores/sócios.
# Constantes canônicas centralizadas em `rfb_status`.
from bracc.services.rfb_status import SITUACOES_GRAVES as _SITUACOES_GRAVES  # noqa: E402

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
            "tipo": "atencao",
            "icone": "emenda",
            "texto": (
                f"{len(nao_pagas)} emenda(s) empenhada(s) que demoram muito "
                f"para serem concluidas: {fmt_brl(total_nao_pago)} "
                f"reservados mas ainda nao pagos{local_txt}"
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


def analisar_teto_gastos(
    teto: TetoGastos | None,
) -> list[dict[str, str]]:
    """Alerta baseado no teto legal de gastos de campanha (TSE).

    Fonte legal: Resolução TSE nº 23.607/2019 (e atualizações). Entrada
    produzida por ``bracc.services.teto_service.calcular_teto``.

    Severidade:

    * ``ultrapassou`` (> 100%) → alerta ``grave`` (infração eleitoral
      sujeita a multa, cassação e desaprovação de contas).
    * ``limite`` (90-100%)     → alerta ``atencao``.
    * ``alto`` (70-90%)        → alerta ``info``.
    * ``ok`` (< 70%)           → lista vazia (não gera ruído).
    * ``None``                  → lista vazia (cargo/UF não mapeados).
    """
    if teto is None:
        return []

    classificacao = teto.classificacao
    if classificacao == "ultrapassou":
        excedente = teto.valor_gasto - teto.valor_limite
        return [{
            "tipo": "grave",
            "icone": "teto",
            "texto": (
                f"Ultrapassou o teto legal de gastos de campanha para "
                f"{teto.cargo} ({teto.ano_eleicao}): gastou "
                f"{teto.valor_gasto_fmt} — {teto.pct_usado_fmt} do limite "
                f"de {teto.valor_limite_fmt} ({fmt_brl(excedente)} acima). "
                f"Infracao eleitoral grave conforme {teto.fonte_legal}"
            ),
        }]
    if classificacao == "limite":
        return [{
            "tipo": "atencao",
            "icone": "teto",
            "texto": (
                f"No limite do teto legal: gastou {teto.valor_gasto_fmt} "
                f"({teto.pct_usado_fmt} dos {teto.valor_limite_fmt} permitidos "
                f"para {teto.cargo} em {teto.ano_eleicao})"
            ),
        }]
    if classificacao == "alto":
        return [{
            "tipo": "info",
            "icone": "teto",
            "texto": (
                f"Gastou {teto.pct_usado_fmt} do teto legal de campanha "
                f"({teto.valor_gasto_fmt} de {teto.valor_limite_fmt})"
            ),
        }]
    return []


def analisar_cnpj_baixados(
    perfil: _PerfilComEmpresas,
) -> list[dict[str, str]]:
    """Alerta grave: empresas doadoras ou sócias com situação não-operacional.

    Conta CNPJs em ``perfil.doadores_empresa`` e ``perfil.socios`` cuja
    ``situacao`` está em ``_SITUACOES_GRAVES`` (BAIXADA / SUSPENSA /
    INAPTA). Uma única ocorrência já dispara alerta ``grave`` — esses
    são sinais clássicos de laranja / caixa 2 / fraude documental que o
    investigador precisa ver no topo do perfil.

    Empresas ``ATIVA``, ``NULA`` ou sem ``situacao`` (ainda não
    verificadas pelo pipeline ``brasilapi_cnpj_status``) não geram
    ruído — ausência do alerta não significa empresa limpa, só que o
    dado ainda não foi verificado.

    Aceita duck-typing: qualquer objeto com atributos
    ``doadores_empresa`` e ``socios`` (ex.:
    :class:`bracc.services.conexoes_service.ConexoesClassificadas`) é
    aceito — útil pra chamar antes do ``PerfilPolitico`` final estar
    montado.
    """
    doadores_empresa_raw = getattr(perfil, "doadores_empresa", []) or []
    socios_raw = getattr(perfil, "socios", []) or []
    doadores_baixados: list[DoadorEmpresa] = [
        d for d in doadores_empresa_raw
        if getattr(d, "situacao", None) in _SITUACOES_GRAVES
    ]
    socios_baixados: list[SocioConectado] = [
        s for s in socios_raw
        if getattr(s, "situacao", None) in _SITUACOES_GRAVES
    ]
    total = len(doadores_baixados) + len(socios_baixados)
    if total == 0:
        return []

    partes: list[str] = []
    if doadores_baixados:
        partes.append(
            f"{len(doadores_baixados)} empresa(s) doadora(s)"
            if len(doadores_baixados) != 1
            else "1 empresa doadora"
        )
    if socios_baixados:
        partes.append(
            f"{len(socios_baixados)} empresa(s) em que e socio(a)"
            if len(socios_baixados) != 1
            else "1 empresa em que e socio(a)"
        )
    descricao = " e ".join(partes)
    return [{
        "tipo": "grave",
        "icone": "empresa",
        "texto": (
            f"{descricao} estao baixadas/suspensas/inaptas na Receita "
            f"Federal — sinal de possivel laranja, caixa 2 ou fraude "
            f"documental (total: {total})."
        ),
    }]


def _cnpj_digitos(cnpj: str | None) -> str | None:
    """Normaliza CNPJ removendo tudo que não é dígito.

    Usado no cross-check doador↔beneficiário pra casar CNPJs que podem
    ter vindo formatados ("11.111.111/0001-11") ou crus ("11111111000111")
    de pipelines diferentes. Retorna ``None`` pra entrada vazia/``None``
    ou que não contém nenhum dígito — evita match espúrio de strings
    vazias entre doadores e beneficiários sem CNPJ.
    """
    if not cnpj:
        return None
    digitos = "".join(ch for ch in cnpj if ch.isdigit())
    return digitos or None


def _parse_data_abertura(raw: str | None) -> date | None:
    """Parse ISO ``YYYY-MM-DD`` (formato BrasilAPI) pro tipo ``date``.

    Retorna ``None`` em qualquer input inválido — sinal de CNPJ legado
    sem o pipeline ``brasilapi_cnpj_status`` ter rodado, não erro.
    """
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


# Limiares do alerta "CNPJ novinho recebendo emenda".
#
# * ``BENEFICIARIO_NOVO_ANOS``: janela de "juventude" do CNPJ. 2 anos é o
#   corte canônico em investigações de ONGs e empresas de fachada —
#   abaixo disso a empresa raramente consegue ter CNAE operacional,
#   acervo técnico e estrutura pra entregar obra/serviço de vulto. Acima
#   disso a presunção de capacidade sobe e o sinal vira ruído.
# * ``BENEFICIARIO_NOVO_VALOR_MIN``: R$ 500k filtra ruído. Emenda de
#   R$ 10k pra CNPJ novinho é muito comum (prestação de serviço pontual)
#   e não justifica alerta. Acima disso a assimetria entre valor e
#   maturidade da empresa começa a pesar.
BENEFICIARIO_NOVO_ANOS = 2
BENEFICIARIO_NOVO_VALOR_MIN = 500_000.0

# Limiares do alerta "emenda travada".
#
# * ``EMENDA_TRAVADA_ANOS``: emenda empenhada há 3 anos ou mais sem
#   execução significativa. 3 anos é o limite prático — dentro disso
#   ainda é razoável atribuir a timing/burocracia; acima começa a
#   sugerir problema estrutural (município incapaz, prestação de contas
#   pendente, intenção original enganosa).
# * ``EMENDA_TRAVADA_PAGO_MAX_PCT``: tolera até 10% de execução como
#   "travada efetiva" — pagamento simbólico não apaga o problema.
# * ``EMENDA_TRAVADA_VALOR_MIN``: R$ 100k filtra emendas pequenas onde
#   atraso é normal.
EMENDA_TRAVADA_ANOS = 3
EMENDA_TRAVADA_PAGO_MAX_PCT = 0.10
EMENDA_TRAVADA_VALOR_MIN = 100_000.0

# Limiares do alerta "beneficiário recorrente".
#
# * ``BENEFICIARIO_RECORRENTE_MIN_EMENDAS``: 3 ou mais emendas pro mesmo
#   CNPJ do mesmo parlamentar sai do padrão. 2 ainda pode ser coincidência
#   (convênio dividido em fases); 3+ sugere relação cativa.
# * ``BENEFICIARIO_RECORRENTE_VALOR_MIN``: R$ 300k agregados é o piso —
#   abaixo disso o padrão é ruído de serviço pontual repetido.
BENEFICIARIO_RECORRENTE_MIN_EMENDAS = 3
BENEFICIARIO_RECORRENTE_VALOR_MIN = 300_000.0

# Limiares do alerta "emenda fora da base eleitoral".
#
# Aproximação MVP: base eleitoral = UF do parlamentar. O ideal seria
# cruzar com votação por município do TSE (DivulgaCand/boletim de urna),
# mas o dado ainda não é consumido nos pipelines — a aproximação por UF
# cobre o caso mais comum (deputado de GO propondo emenda pra BA).
#
# * ``FORA_BASE_VALOR_MIN``: R$ 500k agregados fora da UF é o piso.
#   Abaixo vira ruído (ajustes pontuais são legítimos).
# * ``FORA_BASE_PCT_MIN``: 20% do total — se menos que isso, ainda é
#   a excecao à base; acima vira padrao deliberado.
FORA_BASE_VALOR_MIN = 500_000.0
FORA_BASE_PCT_MIN = 0.20


def analisar_beneficiario_novo(
    emendas: list[Emenda],
    referencia: date | None = None,
) -> list[dict[str, str]]:
    """Alerta: CNPJ recém-aberto (< 2 anos) recebendo emenda relevante.

    ONGs/empresas criadas pouco antes de ganhar contrato público são um
    sinal clássico de fachada/laranja — ainda mais quando o volume é
    desproporcional à maturidade (empresa que mal teve tempo de
    estruturar CNAE operacional recebendo meio milhão de reais).

    Parâmetros
    ----------
    emendas:
        Lista tipada com ``beneficiario_data_abertura`` preenchida
        (vem do pipeline ``brasilapi_cnpj_status`` no nó :Company).
    referencia:
        Data de referência pro cálculo de "anos desde abertura". Default
        é ``date.today()`` — aceita override só pra tornar o teste
        determinístico.

    Aparecer aqui não prova irregularidade — empresa nova pode ser
    legítima. É um sinal que combinado com outros (doador-beneficiário,
    sócio comum, etc.) forma o caso investigável. Severidade ``atencao``
    (não ``grave``), pra refletir que o sinal sozinho é fraco.
    """
    if not emendas:
        return []

    hoje = referencia or date.today()
    # Agrega primeiro por CNPJ, filtra por valor depois. Motivo: a mesma
    # empresa pode receber 3 emendas de R$ 300k — individualmente cada
    # uma é "serviço pontual", mas a soma (R$ 900k) pra empresa nova é
    # o padrão que queremos pegar. Filtrar antes de agregar subestima.
    agregados: dict[str, tuple[str, date, float]] = {}
    for emenda in emendas:
        abertura = _parse_data_abertura(emenda.beneficiario_data_abertura)
        if abertura is None:
            continue
        anos = (hoje - abertura).days / 365.25
        if anos >= BENEFICIARIO_NOVO_ANOS:
            continue
        valor = emenda.valor_pago or emenda.valor_empenhado or 0.0
        if valor <= 0:
            continue
        chave = emenda.beneficiario_cnpj or emenda.beneficiario_nome or emenda.id
        nome, _abertura, acumulado = agregados.get(
            chave, (emenda.beneficiario_nome or "Empresa sem nome", abertura, 0.0),
        )
        agregados[chave] = (nome, abertura, acumulado + valor)

    matches = {
        k: v for k, v in agregados.items()
        if v[2] >= BENEFICIARIO_NOVO_VALOR_MIN
    }
    if not matches:
        return []

    alertas: list[dict[str, str]] = []
    for nome, abertura, valor_total in matches.values():
        meses = (hoje.year - abertura.year) * 12 + (hoje.month - abertura.month)
        idade_txt = (
            f"{meses} mes(es)"
            if meses < 24
            else f"{meses // 12} ano(s) e {meses % 12} mes(es)"
        )
        alertas.append({
            "tipo": "atencao",
            "icone": "empresa",
            "texto": (
                f"{nome} recebeu {fmt_brl(valor_total)} em emenda(s) com "
                f"CNPJ aberto ha {idade_txt} (aberto em "
                f"{abertura.strftime('%d/%m/%Y')}) — empresa nova recebendo "
                "valor relevante merece conferencia de capacidade tecnica "
                "e acervo pra executar o objeto"
            ),
        })
    return alertas


def analisar_beneficiario_recorrente(
    emendas: list[Emenda],
) -> list[dict[str, str]]:
    """Alerta: mesmo CNPJ beneficiado em 3+ emendas do mesmo parlamentar.

    Relação cativa entre parlamentar e beneficiário. Por si só não prova
    nada (pode ser uma entidade legítima muito ativa na área do
    parlamentar), mas é sinal que merece conferência: mesma ONG
    recebendo repetidamente de uma só fonte vale olhar estatuto,
    diretoria, outros financiadores.

    Severidade ``atencao``.
    """
    if not emendas:
        return []

    por_cnpj: dict[str, tuple[str, list[Emenda]]] = {}
    for emenda in emendas:
        cnpj = _cnpj_digitos(emenda.beneficiario_cnpj)
        if cnpj is None:
            continue
        nome, lista = por_cnpj.get(
            cnpj, (emenda.beneficiario_nome or "Empresa sem nome", []),
        )
        lista.append(emenda)
        por_cnpj[cnpj] = (nome, lista)

    alertas: list[dict[str, str]] = []
    for nome, lista in por_cnpj.values():
        if len(lista) < BENEFICIARIO_RECORRENTE_MIN_EMENDAS:
            continue
        total = sum(e.valor_pago or e.valor_empenhado or 0 for e in lista)
        if total < BENEFICIARIO_RECORRENTE_VALOR_MIN:
            continue
        alertas.append({
            "tipo": "atencao",
            "icone": "empresa",
            "texto": (
                f"{nome} foi beneficiada em {len(lista)} emenda(s) do mesmo "
                f"parlamentar, totalizando {fmt_brl(total)} — relacao "
                "recorrente merece olhar no estatuto/diretoria e em "
                "outros financiadores da entidade"
            ),
        })
    return alertas


def analisar_emendas_travadas(
    emendas: list[Emenda],
    referencia: date | None = None,
) -> list[dict[str, str]]:
    """Alerta: emenda empenhada há >= 3 anos sem execução significativa.

    "Stuck money" — recurso reservado, sem entregar. Pode significar
    município incapaz de executar, pendência em prestação de contas, ou
    intenção original de "marcar" recurso sem real interesse na entrega.
    Valor reservado sem retorno é um dos sintomas clássicos de má
    qualidade do gasto público.

    Severidade ``atencao``: sozinho o sinal é fraco (burocracia bate
    tudo), mas junto com outros (CNPJ novinho, doador-beneficiário etc)
    fortalece o caso.
    """
    if not emendas:
        return []

    hoje = referencia or date.today()
    ano_corte = hoje.year - EMENDA_TRAVADA_ANOS
    travadas: list[Emenda] = []
    for emenda in emendas:
        if emenda.ano is None or emenda.ano > ano_corte:
            continue
        empenhado = emenda.valor_empenhado or 0.0
        if empenhado < EMENDA_TRAVADA_VALOR_MIN:
            continue
        pago = emenda.valor_pago or 0.0
        if pago > empenhado * EMENDA_TRAVADA_PAGO_MAX_PCT:
            continue
        travadas.append(emenda)

    if not travadas:
        return []

    total_travado = sum(e.valor_empenhado for e in travadas)
    return [{
        "tipo": "atencao",
        "icone": "emenda",
        "texto": (
            f"{len(travadas)} emenda(s) empenhada(s) ha {EMENDA_TRAVADA_ANOS}+ "
            f"anos sem execucao relevante — {fmt_brl(total_travado)} "
            "reservados mas parados; vale conferir prestacao de contas do "
            "tomador ou capacidade do municipio pra executar"
        ),
    }]


def analisar_emendas_fora_base(
    politico_uf: str | None,
    emendas: list[Emenda],
) -> list[dict[str, str]]:
    """Alerta: parcela relevante das emendas vai pra outra UF.

    Parlamentar tende a concentrar emenda na sua base eleitoral (mesma
    UF). Quando uma fatia relevante vai pra outro estado, é sinal de
    padrão deliberado — pode ser barganha política entre bancadas,
    favor pessoal, ou ligação com beneficiário específico fora da base.
    Sinal isolado é fraco; severidade ``atencao``.

    Aproximação MVP: ``politico_uf`` como proxy da base. Ideal futuro é
    cruzar com TSE (votação por município) pra capturar o caso em que
    o parlamentar tem base real em dois estados (ex: fronteiriço).
    """
    if not emendas or not politico_uf:
        return []

    base = politico_uf.strip().upper()
    if not base:
        return []

    total = 0.0
    fora_por_uf: dict[str, float] = {}
    for emenda in emendas:
        valor = emenda.valor_pago or emenda.valor_empenhado or 0.0
        if valor <= 0:
            continue
        total += valor
        uf = (emenda.uf or "").strip().upper()
        if not uf or uf == base:
            continue
        fora_por_uf[uf] = fora_por_uf.get(uf, 0.0) + valor

    if total <= 0 or not fora_por_uf:
        return []

    total_fora = sum(fora_por_uf.values())
    pct = total_fora / total
    if total_fora < FORA_BASE_VALOR_MIN or pct < FORA_BASE_PCT_MIN:
        return []

    ufs_ordenadas = sorted(fora_por_uf.items(), key=lambda kv: -kv[1])
    detalhe = ", ".join(f"{uf} ({fmt_brl(v)})" for uf, v in ufs_ordenadas[:3])
    return [{
        "tipo": "atencao",
        "icone": "emenda",
        "texto": (
            f"{pct * 100:.0f}% das emendas ({fmt_brl(total_fora)}) foram "
            f"pra fora de {base} — principais destinos: {detalhe}; "
            "parlamentar costuma concentrar emenda na base, fuga desse "
            "padrao merece olhar no objeto do convenio"
        ),
    }]


def analisar_socio_beneficiario(
    perfil: _PerfilComEmpresas,
    emendas: list[Emenda],
) -> list[dict[str, str]]:
    """Alerta grave: empresa em que o parlamentar é sócio recebeu emenda dele.

    Esse é o conflito de interesse mais direto possível — o parlamentar
    propôs uma emenda e o beneficiário é uma empresa que ele mesmo
    co-possui (via ``:SOCIO_DE`` no grafo). Mesmo que legalmente não
    configure crime per se, é o tipo de padrão que investigação de
    enriquecimento ilícito rastreia. Severidade ``grave``.
    """
    if not emendas:
        return []

    socios_raw = getattr(perfil, "socios", []) or []
    socios_por_cnpj: dict[str, SocioConectado] = {}
    for socio in socios_raw:
        chave = _cnpj_digitos(getattr(socio, "cnpj", None))
        if chave is not None:
            socios_por_cnpj[chave] = socio
    if not socios_por_cnpj:
        return []

    matches: dict[str, tuple[SocioConectado, float]] = {}
    for emenda in emendas:
        chave = _cnpj_digitos(emenda.beneficiario_cnpj)
        if chave is None or chave not in socios_por_cnpj:
            continue
        valor = emenda.valor_pago or emenda.valor_empenhado or 0.0
        _socio, acumulado = matches.get(chave, (socios_por_cnpj[chave], 0.0))
        matches[chave] = (socios_por_cnpj[chave], acumulado + valor)

    if not matches:
        return []

    alertas: list[dict[str, str]] = []
    for socio, valor_emendas in matches.values():
        nome = (socio.nome or "").strip() or "Empresa sem nome"
        alertas.append({
            "tipo": "grave",
            "icone": "empresa",
            "texto": (
                f"Parlamentar e socio(a) em {nome} — empresa recebeu "
                f"{fmt_brl(valor_emendas)} em emenda(s) proposta(s) pelo "
                "proprio parlamentar (conflito de interesse direto via "
                ":SOCIO_DE no grafo); padrao classico de enriquecimento "
                "ilicito, exige verificacao imediata"
            ),
        })
    return alertas


def analisar_doador_beneficiario(
    perfil: _PerfilComEmpresas,
    emendas: list[Emenda],
) -> list[dict[str, str]]:
    """Alerta grave: empresa que doou pra campanha virou beneficiária de emenda.

    Este é o red flag clássico de troca de favor eleitoral. A empresa
    financiou a campanha do parlamentar (doador registrado no TSE) e o
    mesmo parlamentar propôs emenda que acabou beneficiando-a
    (relação ``(:Amendment)-[:BENEFICIOU]->(:Company)`` no grafo).

    Comparação é feita por CNPJ normalizado (só dígitos) pra robustez
    entre pipelines TSE (doações) e Transferegov (beneficiários de
    emenda) que podem carimbar o documento em formatos diferentes.

    Aparecer aqui não é prova de crime — a lei permite doação e emenda.
    Mas a correlação é um sinal forte o bastante pra valer uma olhada
    no objeto do convênio, no processo licitatório e na execução da
    obra/serviço. Por isso severidade ``grave``.
    """
    if not emendas:
        return []

    doadores_raw = getattr(perfil, "doadores_empresa", []) or []
    doadores_por_cnpj: dict[str, DoadorEmpresa] = {}
    for doador in doadores_raw:
        chave = _cnpj_digitos(getattr(doador, "cnpj", None))
        if chave is not None:
            doadores_por_cnpj[chave] = doador
    if not doadores_por_cnpj:
        return []

    matches: dict[str, tuple[DoadorEmpresa, float]] = {}
    for emenda in emendas:
        chave = _cnpj_digitos(emenda.beneficiario_cnpj)
        if chave is None or chave not in doadores_por_cnpj:
            continue
        valor = emenda.valor_pago or emenda.valor_empenhado or 0.0
        _doador, acumulado = matches.get(
            chave, (doadores_por_cnpj[chave], 0.0),
        )
        matches[chave] = (doadores_por_cnpj[chave], acumulado + valor)

    if not matches:
        return []

    alertas: list[dict[str, str]] = []
    for _chave, (doador, valor_emendas) in matches.items():
        nome = (doador.nome or "").strip() or "Empresa sem nome"
        alertas.append({
            "tipo": "grave",
            "icone": "empresa",
            "texto": (
                f"{nome} doou {doador.valor_total_fmt} para a campanha e "
                f"recebeu {fmt_brl(valor_emendas)} em emenda(s) proposta(s) "
                "pelo parlamentar — cruzamento TSE (doacoes) x Transferegov "
                "(beneficiarios) sugere possivel troca de favor; vale "
                "investigar objeto do convenio e processo licitatorio"
            ),
        })
    return alertas


# --- Score consolidado ------------------------------------------------------

_RED_FLAG_PESOS: dict[str, int] = {
    "grave": 10,
    "atencao": 3,
    "info": 1,
    "ok": 0,
}


def calcular_red_flags_summary(
    alertas: list[dict[str, str]],
) -> RedFlagsSummary:
    """Agrega alertas em um score numérico + classificação legível.

    Pesos: grave=10, atencao=3, info=1, ok=0. Classificação:
    >=10 crítico, 5-9 alto, 1-4 médio, 0 baixo. Alertas informativos
    genéricos ("Avaliação indisponível") são ignorados pra não inflar
    a pontuação quando o perfil simplesmente não tem dados.
    """
    # Import tardio pra evitar ciclo alertas_service ↔ models.perfil
    # (models.perfil é referenciado só em TYPE_CHECKING no topo).
    from bracc.models.perfil import RedFlagsSummary as _RedFlagsSummary

    num_grave = 0
    num_atencao = 0
    num_info = 0
    pontos = 0
    for a in alertas:
        texto = a.get("texto", "")
        if "Avaliação indisponível" in texto or "Avaliacao indisponivel" in texto:
            continue
        tipo = a.get("tipo", "info")
        pontos += _RED_FLAG_PESOS.get(tipo, 0)
        if tipo == "grave":
            num_grave += 1
        elif tipo == "atencao":
            num_atencao += 1
        elif tipo == "info":
            num_info += 1

    classificacao: Literal["baixo", "medio", "alto", "critico"]
    if pontos >= 10:
        classificacao = "critico"
    elif pontos >= 5:
        classificacao = "alto"
    elif pontos >= 1:
        classificacao = "medio"
    else:
        classificacao = "baixo"

    partes: list[str] = []
    if num_grave:
        partes.append(
            f"{num_grave} alerta(s) grave(s)" if num_grave > 1 else "1 alerta grave",
        )
    if num_atencao:
        partes.append(f"{num_atencao} de atencao")
    if num_info:
        partes.append(f"{num_info} informativo(s)")
    resumo = ", ".join(partes) if partes else "nenhum alerta relevante"
    texto = f"{pontos} pontos de risco ({resumo})"

    return _RedFlagsSummary(
        pontos=pontos,
        classificacao=classificacao,
        num_grave=num_grave,
        num_atencao=num_atencao,
        num_info=num_info,
        texto=texto,
    )


# --- Orquestração -----------------------------------------------------------


def gerar_alertas_completos(
    entidade: dict[str, Any],
    conexoes_raw: list[dict[str, Any]],
    entidades_conectadas: dict[str, dict[str, Any]],
    emendas_raw: list[dict[str, Any]],
    perfil: _PerfilComEmpresas | None = None,
    emendas_tipadas: list[Emenda] | None = None,
    politico_uf: str | None = None,
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

    # Alerta grave: doadores/sócios com CNPJ BAIXADA/SUSPENSA/INAPTA.
    # Só roda quando o chamador passa o ``perfil`` já classificado — é
    # opt-in pra não quebrar chamadas antigas que passam só ``entidade``.
    if perfil is not None:
        alertas.extend(analisar_cnpj_baixados(perfil))
        if emendas_tipadas:
            alertas.extend(analisar_doador_beneficiario(perfil, emendas_tipadas))
            alertas.extend(analisar_socio_beneficiario(perfil, emendas_tipadas))
    if emendas_tipadas:
        alertas.extend(analisar_beneficiario_novo(emendas_tipadas))
        alertas.extend(analisar_emendas_travadas(emendas_tipadas))
        alertas.extend(analisar_beneficiario_recorrente(emendas_tipadas))
        alertas.extend(analisar_emendas_fora_base(politico_uf, emendas_tipadas))

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
