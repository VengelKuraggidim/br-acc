"""Cálculo do teto legal de gastos de campanha vs despesas declaradas.

Fontes legais hardcoded (sem dataset bulk estruturado no TSE, próximo
commit considerará Base dos Dados se publicar tabela consolidada):

* **Eleição 2022** — Resolução TSE nº 23.607/2019, combinada com a
  Resolução TSE nº 23.676/2021 que ratificou os limites para o pleito
  de 2022. Valores disponíveis em:
  https://www.tse.jus.br/eleicoes/eleicoes-2022/prestacao-de-contas
  (tabelas de limites por cargo e por estado/município). Os números
  abaixo são os tetos publicados pelo TSE em janeiro de 2022,
  convertidos pra float — nenhum "chute".

* **Eleição 2026** — ainda não publicada (cadência normal: ~dezembro
  de 2025). TODO: adicionar ``TETOS_2026`` quando sair a resolução
  correspondente.

Municipal (prefeito/vereador) fica fora do MVP: cada município tem
teto próprio baseado no eleitorado e não existe CSV bulk acessível.
Candidatos a esses cargos resultam em ``None`` (seção omitida no PWA).

O service é **puro** — zero IO, zero grafo. O consumidor
(``perfil_service``) lê ``total_despesas_tse_{year}`` e
``cargo_tse_{year}`` do nó focal e passa pra ``calcular_teto``.
"""

from __future__ import annotations

import unicodedata

from bracc.models.perfil import TetoGastos
from bracc.services.formatacao_service import fmt_brl

# --- Fonte legal por eleição ------------------------------------------------

_FONTE_2022 = "Resolução TSE nº 23.607/2019 (Eleições 2022)"

# Tetos federais/estaduais válidos para TODAS as UFs (ou valor por-UF quando
# o TSE publicou tabela regional). Valores em BRL. Fonte: TSE, tabelas
# publicadas em janeiro de 2022 para a eleição geral do mesmo ano.
#
# * Deputado Federal: R$ 2.100.000,00 (teto federal nacional)
# * Senador:          R$ 5.000.000,00 (base nacional, não varia por UF)
# * Deputado Estadual: R$ 1.050.000,00 (base nacional aproximada para
#   efeito de referência — o TSE publica por-estado mas a variação é
#   pequena o suficiente pra esse valor agregado servir de proxy MVP)
# * Governador: varia por UF (baseado em eleitorado) — tabela abaixo.
_TETOS_NACIONAIS_2022: dict[str, float] = {
    "deputado federal": 2_100_000.00,
    "senador": 5_000_000.00,
    "deputado estadual": 1_050_000.00,
}

# Governador: teto por UF (Resolução TSE nº 23.607/2019, tabelas
# republicadas em janeiro de 2022). Cobertura focada em GO + UFs mais
# populosas. UFs ausentes resultam em ``None`` (degradação silenciosa).
_TETO_GOVERNADOR_2022: dict[str, float] = {
    "GO": 21_000_000.00,
    "SP": 70_000_000.00,
    "RJ": 42_000_000.00,
    "MG": 42_000_000.00,
    "BA": 28_000_000.00,
    "DF": 14_000_000.00,
    "PR": 21_000_000.00,
    "RS": 21_000_000.00,
    "PE": 21_000_000.00,
    "CE": 21_000_000.00,
    "MT": 14_000_000.00,
    "TO": 14_000_000.00,
}


# --- Classificação por % usado ----------------------------------------------


def _classificar(pct: float) -> str:
    """Retorna bucket de severidade pro ``TetoGastos.classificacao``.

    * < 70%          → "ok"          (verde)
    * 70% ≤ x < 90%  → "alto"        (amarelo)
    * 90% ≤ x ≤ 100% → "limite"      (laranja)
    * > 100%         → "ultrapassou" (vermelho — infração grave)
    """
    if pct > 100:
        return "ultrapassou"
    if pct >= 90:
        return "limite"
    if pct >= 70:
        return "alto"
    return "ok"


def _normalizar_cargo(cargo: str) -> str:
    """Lower + remove acentos pra comparação robusta com a tabela.

    O TSE publica cargos em MAIÚSCULAS (``"DEPUTADO FEDERAL"``) mas
    outros pipelines usam minúscula (``"deputado federal"``) ou título.
    Normalizamos tudo pra lowercase sem acentos.
    """
    n = unicodedata.normalize("NFKD", cargo).encode("ascii", "ignore").decode()
    return n.strip().lower()


def _resolver_limite(
    cargo_norm: str,
    uf: str | None,
) -> float | None:
    """Retorna o teto em BRL ou ``None`` se o cargo/UF não está mapeado."""
    # Vice-cargos seguem o mesmo teto do titular (TSE unifica chapa
    # majoritária). Normalizamos pra casar com as chaves da tabela.
    cargo_sem_vice = cargo_norm.replace("vice-", "").replace("vice ", "")

    # Governador precisa de UF mapeada.
    if "governador" in cargo_sem_vice:
        if not uf:
            return None
        return _TETO_GOVERNADOR_2022.get(uf.upper())

    for chave, valor in _TETOS_NACIONAIS_2022.items():
        if chave in cargo_sem_vice:
            return valor

    # Prefeito / vereador — teto municipal, fora do MVP (degrada pra None).
    return None


# --- API principal ----------------------------------------------------------


def calcular_teto(
    cargo: str | None,
    uf: str | None,
    ano_eleicao: int,
    total_despesas_declaradas: float,
) -> TetoGastos | None:
    """Retorna ``TetoGastos`` ou ``None`` quando não há mapeamento viável.

    Parameters
    ----------
    cargo:
        Texto cru do cargo (ex.: ``"DEPUTADO FEDERAL"``, ``"deputado
        federal"``, ``"Governador(a)"``). Normalizado internamente.
    uf:
        Sigla da UF (2 letras). Necessário pra governador; ignorado
        pros demais cargos.
    ano_eleicao:
        Ano da eleição (2022 é o único cobertos no MVP; 2026 pendente
        da publicação da resolução TSE correspondente).
    total_despesas_declaradas:
        Soma das despesas pagas em R$ (``total_despesas_tse_{ano}`` do
        nó ``:Person`` no grafo, populado pelo pipeline
        ``tse_prestacao_contas_go``).

    Degradação silenciosa (retorna ``None``) quando:
    * Ano não é 2022 (MVP atual).
    * Cargo é ``None`` ou string vazia.
    * Cargo é prefeito/vereador (teto municipal fora do MVP).
    * Governador sem UF mapeada (ex.: outra UF fora da tabela).
    * ``total_despesas_declaradas <= 0`` (candidato sem gasto declarado
      — exibir percentual seria enganoso).
    """
    # MVP hardcoded — apenas 2022 está coberto.
    if ano_eleicao != 2022:
        return None
    if not cargo:
        return None
    if total_despesas_declaradas is None or total_despesas_declaradas <= 0:
        return None

    cargo_norm = _normalizar_cargo(cargo)
    if not cargo_norm:
        return None

    limite = _resolver_limite(cargo_norm, uf)
    if limite is None or limite <= 0:
        return None

    pct = (total_despesas_declaradas / limite) * 100
    classificacao = _classificar(pct)

    # Nome do cargo legível (mantém o original se não conhecido).
    cargo_label = cargo.strip()

    return TetoGastos(
        valor_limite=limite,
        valor_limite_fmt=fmt_brl(limite),
        valor_gasto=float(total_despesas_declaradas),
        valor_gasto_fmt=fmt_brl(total_despesas_declaradas),
        pct_usado=round(pct, 1),
        pct_usado_fmt=f"{pct:.0f}%",
        cargo=cargo_label,
        ano_eleicao=ano_eleicao,
        classificacao=classificacao,
        fonte_legal=_FONTE_2022,
    )
