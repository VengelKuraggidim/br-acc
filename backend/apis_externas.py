"""Busca em tempo real nas APIs publicas do governo brasileiro.

Fontes:
- Camara dos Deputados (aberta, sem autenticacao)
- Portal da Transparencia (requer chave gratuita para emendas)
"""

from __future__ import annotations

import logging
import os
import unicodedata
from datetime import datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

CAMARA_API = "https://dadosabertos.camara.leg.br/api/v2"
TRANSPARENCIA_API = "https://api.portaldatransparencia.gov.br/api-de-dados"
TRANSPARENCIA_API_KEY = os.getenv("TRANSPARENCIA_API_KEY", "")

_TIMEOUT = 15.0


def _so_digitos(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def _normalizar_nome(nome: str) -> str:
    nfkd = unicodedata.normalize("NFKD", nome or "")
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(sem_acento.lower().split())


_PARTICULAS_NOME = {"de", "da", "do", "das", "dos", "e", "di", "del"}


def _tokens_significativos(nome: str) -> set[str]:
    return {
        t for t in _normalizar_nome(nome).split()
        if t and t not in _PARTICULAS_NOME
    }


def _nome_compativel(nome_buscado: str, dados_deputado: dict[str, Any]) -> bool:
    """Confere se o deputado retornado casa razoavelmente com o nome
    buscado. Compara tokens contra nome civil, nome parlamentar atual e
    o nome curto da lista. Evita que um fallback por sobrenome sirva a
    foto de outra pessoa (ex.: buscar 'Clecio Antonio Alves' e cair em
    'Silvye Alves', que e a unica 'Alves' de GO na Camara).
    """
    tokens_buscado = _tokens_significativos(nome_buscado)
    if not tokens_buscado:
        return False
    necessarios = min(2, len(tokens_buscado))
    candidatos = [
        dados_deputado.get("nomeCivil"),
        dados_deputado.get("ultimoStatus", {}).get("nome"),
        dados_deputado.get("nome"),
    ]
    for cand in candidatos:
        if not cand:
            continue
        comum = tokens_buscado & _tokens_significativos(cand)
        if len(comum) >= necessarios:
            return True
    return False


async def buscar_deputado_camara(
    nome: str,
    cpf: str | None = None,
    uf: str | None = None,
) -> dict[str, Any] | None:
    """Busca um deputado na API da Camara pelo nome, com desambiguacao.

    Regras para evitar foto/perfil errado quando ha homonimos:
    - Se CPF for informado e nenhum candidato bater com ele, retorna None.
    - Sem CPF: so retorna quando ha candidato unico ou match exato de
      nome (normalizado) E os tokens do nome buscado aparecem no nome
      civil/parlamentar do candidato. Caso contrario retorna None para
      nao servir a foto de outra pessoa.
    """
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        dados = await _buscar_deputados(client, nome, uf)

        if not dados:
            partes = nome.split()
            if len(partes) >= 2:
                for tentativa in [partes[-1], partes[0]]:
                    dados = await _buscar_deputados(client, tentativa, uf)
                    if dados:
                        break

        if not dados:
            return None

        if cpf:
            cpf_limpo = _so_digitos(cpf)
            for dep in dados:
                detalhe = await _detalhe_deputado(client, dep["id"])
                if not detalhe:
                    continue
                cpf_dep = _so_digitos(detalhe.get("cpf") or "")
                if cpf_dep and cpf_dep == cpf_limpo:
                    return detalhe
            # CPF nao bateu com nenhum candidato: nao retorna outro pra
            # evitar foto errada.
            return None

        candidato: dict[str, Any] | None = None
        if len(dados) == 1:
            candidato = dados[0]
        else:
            nome_norm = _normalizar_nome(nome)
            candidatos_exatos = [
                d for d in dados
                if _normalizar_nome(d.get("nome", "")) == nome_norm
            ]
            if len(candidatos_exatos) == 1:
                candidato = candidatos_exatos[0]

        if candidato is None:
            return None

        detalhe = await _detalhe_deputado(client, candidato["id"])
        dados_completos = detalhe or candidato
        if not _nome_compativel(nome, dados_completos):
            return None
        return dados_completos


async def _buscar_deputados(
    client: httpx.AsyncClient, nome: str, uf: str | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"nome": nome}
    if uf:
        params["siglaUf"] = uf.upper()
    try:
        resp = await client.get(
            f"{CAMARA_API}/deputados", params=params,
        )
        resp.raise_for_status()
        return resp.json().get("dados", [])
    except (httpx.HTTPError, ValueError) as e:
        logger.warning("Erro ao buscar deputado na Camara: %s", e)
        return []


async def _detalhe_deputado(
    client: httpx.AsyncClient, dep_id: int,
) -> dict[str, Any] | None:
    try:
        resp = await client.get(f"{CAMARA_API}/deputados/{dep_id}")
        resp.raise_for_status()
        return resp.json().get("dados")
    except (httpx.HTTPError, ValueError):
        return None


async def buscar_despesas_deputado(
    deputado_id: int,
    anos: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Busca despesas CEAP (cota parlamentar) de um deputado."""
    if anos is None:
        ano_atual = datetime.now().year
        anos = [ano_atual, ano_atual - 1]

    todas_despesas: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for ano in anos:
            pagina = 1
            while pagina <= 5:
                try:
                    resp = await client.get(
                        f"{CAMARA_API}/deputados/{deputado_id}/despesas",
                        params={"ano": ano, "pagina": pagina, "itens": 100},
                    )
                    resp.raise_for_status()
                    dados = resp.json().get("dados", [])
                    if not dados:
                        break
                    todas_despesas.extend(dados)
                    pagina += 1
                except (httpx.HTTPError, ValueError) as e:
                    logger.warning(
                        "Erro ao buscar despesas (ano=%d, pag=%d): %s",
                        ano, pagina, e,
                    )
                    break

    return todas_despesas


async def buscar_emendas_transparencia(
    nome_autor: str,
    ano: int | None = None,
) -> list[dict[str, Any]]:
    """Busca emendas parlamentares no Portal da Transparencia.

    Requer TRANSPARENCIA_API_KEY configurada no .env.
    """
    if not TRANSPARENCIA_API_KEY:
        return []

    if ano is None:
        ano = datetime.now().year

    todas_emendas: list[dict[str, Any]] = []
    anos = [ano, ano - 1, ano - 2]

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for a in anos:
            pagina = 1
            while pagina <= 10:
                try:
                    resp = await client.get(
                        f"{TRANSPARENCIA_API}/emendas",
                        params={
                            "nomeAutor": nome_autor,
                            "ano": a,
                            "pagina": pagina,
                        },
                        headers={
                            "chave-api-dados": TRANSPARENCIA_API_KEY,
                            "Accept": "application/json",
                        },
                    )
                    if resp.status_code == 401:
                        logger.warning("Chave do Portal da Transparencia invalida")
                        return todas_emendas
                    resp.raise_for_status()
                    dados = resp.json()
                    if not dados:
                        break
                    if isinstance(dados, list):
                        todas_emendas.extend(dados)
                    else:
                        break
                    pagina += 1
                except (httpx.HTTPError, ValueError) as e:
                    logger.warning(
                        "Erro ao buscar emendas (ano=%d, pag=%d): %s",
                        a, pagina, e,
                    )
                    break

    return todas_emendas


async def buscar_media_despesas_estado(
    uf: str,
    anos: list[int] | None = None,
) -> float:
    """Calcula a media de gastos CEAP dos deputados de um estado.

    Busca uma amostra dos deputados do estado e calcula a media de gasto total.
    Retorna 0 se nao conseguir calcular.
    """
    if anos is None:
        ano_atual = datetime.now().year
        anos = [ano_atual, ano_atual - 1]

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        # Buscar deputados do estado
        try:
            resp = await client.get(
                f"{CAMARA_API}/deputados",
                params={"siglaUf": uf.upper(), "itens": 30, "ordem": "ASC", "ordenarPor": "nome"},
            )
            resp.raise_for_status()
            deputados = resp.json().get("dados", [])
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Erro ao buscar deputados de %s: %s", uf, e)
            return 0

        if not deputados:
            return 0

        # Limitar a 10 deputados para nao sobrecarregar a API
        amostra = deputados[:10]
        totais = []

        for dep in amostra:
            dep_id = dep.get("id")
            if not dep_id:
                continue
            total_dep = 0.0
            for ano in anos:
                try:
                    resp = await client.get(
                        f"{CAMARA_API}/deputados/{dep_id}/despesas",
                        params={"ano": ano, "pagina": 1, "itens": 100},
                    )
                    resp.raise_for_status()
                    dados = resp.json().get("dados", [])
                    total_dep += sum(d.get("valorLiquido", 0) or 0 for d in dados)
                except (httpx.HTTPError, ValueError):
                    continue
            totais.append(total_dep)

        if not totais:
            return 0

        return sum(totais) / len(totais)


def agrupar_despesas_por_tipo(despesas: list[dict]) -> list[dict[str, Any]]:
    """Agrupa despesas CEAP por tipo e calcula totais."""
    por_tipo: dict[str, float] = {}
    for d in despesas:
        tipo = d.get("tipoDespesa", "Outros")
        valor = d.get("valorLiquido", 0) or 0
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor

    resultado = []
    for tipo, total in sorted(por_tipo.items(), key=lambda x: -x[1]):
        resultado.append({"tipo": tipo, "total": total})
    return resultado


def converter_emendas_transparencia(emendas_raw: list[dict]) -> list[dict[str, Any]]:
    """Converte emendas do Portal da Transparencia pro formato interno."""
    emendas = []
    for e in emendas_raw:
        emendas.append({
            "amendment_id": str(e.get("codigo", e.get("codigoEmenda", ""))),
            "type": e.get("tipoEmenda", ""),
            "function": e.get("nomeAreaTematica", e.get("funcao", "")),
            "municipality": e.get("nomeLocalidadeGasto", ""),
            "uf": e.get("ufGasto", ""),
            "value_committed": e.get("valorEmpenhado", 0) or 0,
            "value_paid": e.get("valorPago", 0) or 0,
        })
    return emendas
