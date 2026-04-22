"""Cálculo do teto legal de gastos de campanha vs despesas declaradas.

Fontes legais hardcoded (sem dataset bulk estruturado no TSE):

* **Eleição 2022** — Resolução TSE nº 23.607/2019 fixa o critério;
  Portaria TSE nº 647/2022 (publicada no DJE em 19/07/2022) fixou os
  valores finais, iguais aos de 2018 atualizados pelo IPCA (out/2018
  a jun/2022). Valores oficiais:
  https://www.tse.jus.br/eleicoes/eleicoes-2022/prestacao-de-contas

* **Eleição 2026** — ainda não publicada (cadência normal: ~dezembro
  de 2025). TODO: adicionar ``TETOS_2026`` quando sair a resolução
  correspondente.

Estrutura dos tetos por cargo (2022):
* Deputado Federal: valor **único nacional** (mesmo pra todas as UFs).
* Governador, Senador, Deputado Estadual: variam **por UF** (base:
  eleitorado). Cobertura inicial focada em GO (produto GO-only);
  outras UFs adicionar sob demanda — degradam pra ``None``.

Municipal (prefeito/vereador) fica fora do MVP: cada município tem
teto próprio baseado no eleitorado e não existe CSV bulk acessível.
Candidatos a esses cargos resultam em ``None`` (seção omitida no PWA).

O service é **puro** — zero IO, zero grafo. O consumidor
(``perfil_service``) lê ``total_despesas_tse_{year}`` e
``cargo_tse_{year}`` do nó focal e passa pra ``calcular_teto``.
"""

from __future__ import annotations

import logging
import unicodedata

from bracc.models.perfil import TetoGastos
from bracc.services.formatacao_service import fmt_brl

logger = logging.getLogger(__name__)

# Ano único coberto no MVP. Quando o TSE publicar a Resolução de 2026,
# adicionar ``_TETOS_NACIONAIS_2026`` + ``_TETO_GOVERNADOR_2026`` seguindo
# o padrão abaixo e expandir a checagem em :func:`calcular_teto`.
_ANOS_COBERTOS: frozenset[int] = frozenset({2022})

# --- Fonte legal por eleição ------------------------------------------------

_FONTE_2022 = (
    "Resolução TSE nº 23.607/2019 + Portaria TSE nº 647/2022 (Eleições 2022)"
)

# Tetos nacionais únicos — um só valor em todas as UFs.
#
# Deputado Federal é o único cargo com teto genuinamente nacional em
# 2022: R$ 3.176.572,53 (fonte: Portaria TSE 647/2022, item referente
# a deputado federal).
_TETOS_NACIONAIS_2022: dict[str, float] = {
    "deputado federal": 3_176_572.53,
}

# Tetos por UF — Governador, Senador, Deputado Estadual variam com o
# eleitorado do estado. Cobertura inicial focada em Goiás (produto
# GO-only); outras UFs adicionar sob demanda usando a Portaria TSE
# 647/2022 como fonte. UFs ausentes → ``None`` (degradação silenciosa).
#
# Valores GO 2022 (Portaria TSE 647/2022):
# * Governador:        R$ 11.480.000 (1º turno; 2º turno é +50%,
#                       fora do escopo do MVP — somamos só 1º turno)
# * Senador:           R$ 4.400.000
# * Deputado Estadual: R$ 1.260.000
_TETOS_POR_UF_2022: dict[str, dict[str, float]] = {
    "governador": {
        "GO": 11_480_000.00,
    },
    "senador": {
        "GO": 4_400_000.00,
    },
    "deputado estadual": {
        "GO": 1_260_000.00,
    },
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

    # Tetos nacionais (Dep. Federal) — UF não importa.
    for chave, valor in _TETOS_NACIONAIS_2022.items():
        if chave in cargo_sem_vice:
            return valor

    # Tetos por UF (Governador, Senador, Dep. Estadual) — precisa de UF.
    for chave, tabela_uf in _TETOS_POR_UF_2022.items():
        if chave in cargo_sem_vice:
            if not uf:
                return None
            return tabela_uf.get(uf.upper())

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
    * Ano não está em :data:`_ANOS_COBERTOS` (hoje só 2022). **Loga
      warning explícito** apontando que a tabela do ano correspondente
      ainda não foi adicionada — não quebra produção mas deixa trilha
      pro operador notar antes de 2026 (senão o card "teto" some do
      PWA silenciosamente quando o pipeline começar a alimentar
      ``ano_eleicao=2026``).
    * Cargo é ``None`` ou string vazia.
    * Cargo é prefeito/vereador (teto municipal fora do MVP).
    * Governador sem UF mapeada (ex.: outra UF fora da tabela).
    * ``total_despesas_declaradas <= 0`` (candidato sem gasto declarado
      — exibir percentual seria enganoso).
    """
    # MVP hardcoded — fail-loud no log, degrada pra None silenciosamente.
    # Rationale: raise NotImplementedError quebraria o PerfilService
    # inteiro quando 2026 chegar; devolver None só oculta o card teto
    # (graceful) e o warning garante que o operador veja o gap antes.
    if ano_eleicao not in _ANOS_COBERTOS:
        logger.warning(
            "[teto_service] ano_eleicao=%s sem tabela hardcoded; "
            "adicionar TETOS_%s + TETO_GOVERNADOR_%s seguindo o padrao "
            "TETOS_2022. Retornando None (card 'teto' sera omitido).",
            ano_eleicao, ano_eleicao, ano_eleicao,
        )
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
