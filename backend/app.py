from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from analise import (
    analisar_conexoes,
    analisar_despesas_gabinete,
    analisar_despesas_vs_cidadao,
    analisar_despesas_vs_media,
    analisar_emendas,
    analisar_picos_mensais,
    analisar_patrimonio,
    gerar_resumo_politico,
    traduzir_cargo,
    traduzir_despesa,
    traduzir_funcao_emenda,
    traduzir_tipo_emenda,
)
from apis_externas import (
    agrupar_despesas_por_tipo,
    buscar_deputado_camara,
    buscar_despesas_deputado,
    buscar_emendas_transparencia,
    buscar_media_despesas_estado,
    converter_emendas_transparencia,
)

load_dotenv()

BRACC_API_URL = os.getenv("BRACC_API_URL", "http://localhost:8000")


# --- Modelos ---


class PoliticoResumo(BaseModel):
    id: str
    nome: str
    cpf: str | None = None
    patrimonio: float | None = None
    patrimonio_formatado: str | None = None
    is_pep: bool = False
    partido: str | None = None
    cargo: str | None = None
    uf: str | None = None
    score: float = 0
    foto_url: str | None = None


class Emenda(BaseModel):
    id: str
    tipo: str
    funcao: str
    municipio: str | None = None
    uf: str | None = None
    valor_empenhado: float
    valor_empenhado_fmt: str
    valor_pago: float
    valor_pago_fmt: str


class EmpresaConectada(BaseModel):
    nome: str
    cnpj: str | None = None
    relacao: str
    capital_social: float | None = None
    capital_social_fmt: str | None = None


class DoadorEmpresa(BaseModel):
    nome: str
    cnpj: str | None = None
    valor_total: float
    valor_total_fmt: str
    n_doacoes: int


class DoadorPessoa(BaseModel):
    nome: str
    cpf_mascarado: str | None = None
    valor_total: float
    valor_total_fmt: str
    n_doacoes: int


class SocioConectado(BaseModel):
    nome: str
    cnpj: str | None = None
    capital_social_fmt: str | None = None


class FamiliarConectado(BaseModel):
    nome: str
    cpf_mascarado: str | None = None
    relacao: str


class ValidacaoTSE(BaseModel):
    """Cross-check: valor declarado ao TSE vs o que o sistema ingeriu."""
    ano_eleicao: int
    total_declarado_tse: float
    total_declarado_tse_fmt: str
    total_ingerido: float
    total_ingerido_fmt: str
    divergencia_valor: float
    divergencia_valor_fmt: str
    divergencia_pct: float
    breakdown_tse: list[dict[str, str]]  # [{"origem": "Partido", "valor_fmt": "R$ X mi"}, ...]
    status: str  # "ok" (<5%), "atencao" (5-20%), "divergente" (>20%)


class ContratoConectado(BaseModel):
    objeto: str
    valor: float
    valor_fmt: str
    orgao: str | None = None
    data: str | None = None


class DespesaGabinete(BaseModel):
    tipo: str
    total: float
    total_fmt: str


class ComparacaoCidada(BaseModel):
    categoria: str
    total_politico_fmt: str
    media_mensal_politico_fmt: str
    referencia_cidadao_fmt: str
    razao: float | None = None
    razao_texto: str
    classificacao: str  # "normal", "elevado", "abusivo"


class PerfilPolitico(BaseModel):
    politico: PoliticoResumo
    resumo: str
    emendas: list[Emenda]
    total_emendas_valor: float
    total_emendas_valor_fmt: str
    empresas: list[EmpresaConectada]
    contratos: list[ContratoConectado]
    despesas_gabinete: list[DespesaGabinete] = []
    total_despesas_gabinete: float = 0
    total_despesas_gabinete_fmt: str = "R$ 0,00"
    comparacao_cidada: list[ComparacaoCidada] = []
    comparacao_cidada_resumo: str = ""
    alertas: list[dict[str, str]]
    conexoes_total: int
    fonte_emendas: str | None = None
    descricao_conexoes: str = ""
    doadores_empresa: list[DoadorEmpresa] = []
    doadores_pessoa: list[DoadorPessoa] = []
    total_doacoes: float = 0
    total_doacoes_fmt: str = "R$ 0,00"
    socios: list[SocioConectado] = []
    familia: list[FamiliarConectado] = []
    aviso_despesas: str = ""
    validacao_tse: ValidacaoTSE | None = None


class ServidorResumo(BaseModel):
    id: str
    nome: str
    cargo: str | None = None
    orgao: str | None = None
    salario_bruto: float | None = None
    salario_bruto_fmt: str | None = None
    is_comissionado: bool = False


class MunicipioResumo(BaseModel):
    id: str
    nome: str
    populacao: int | None = None
    receita_total: float | None = None
    receita_total_fmt: str | None = None
    despesa_total: float | None = None
    despesa_total_fmt: str | None = None


class LicitacaoGO(BaseModel):
    id: str
    orgao: str
    cnpj_orgao: str | None = None
    objeto: str
    modalidade: str | None = None
    valor_estimado: float | None = None
    valor_estimado_fmt: str | None = None
    data_publicacao: str | None = None
    municipio: str | None = None


class NomeacaoGO(BaseModel):
    id: str
    nome_pessoa: str
    cargo: str | None = None
    orgao: str | None = None
    data: str | None = None
    tipo: str  # nomeacao or exoneracao
    fonte_diario: str | None = None


class VereadorResumo(BaseModel):
    id: str
    nome: str
    partido: str | None = None
    municipio: str = "Goiania"
    total_despesas: float | None = None
    total_despesas_fmt: str | None = None
    proposicoes: int = 0


class StatusResponse(BaseModel):
    status: str
    bracc_conectado: bool
    total_nos: int
    total_relacionamentos: int
    deputados_federais: int
    deputados_estaduais: int
    senadores: int
    servidores_estaduais: int = 0
    cargos_comissionados: int = 0
    municipios_go: int = 0
    licitacoes_go: int = 0
    nomeacoes_go: int = 0
    vereadores_goiania: int = 0


# --- Helpers ---


def fmt_brl(valor: float) -> str:
    if valor >= 1_000_000_000:
        return f"R$ {valor / 1_000_000_000:.2f} bi"
    if valor >= 1_000_000:
        return f"R$ {valor / 1_000_000:.2f} mi"
    if valor >= 1_000:
        return f"R$ {valor / 1_000:.1f} mil"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def traduzir_relacao(rel_type: str) -> str:
    traducoes = {
        "SOCIO_DE": "Socio(a) de",
        "CONJUGE_DE": "Conjuge de",
        "PARENTE_DE": "Parente de",
        "VENCEU": "Venceu licitacao",
        "SANCIONADA": "Sancionada",
        "DOOU": "Doou para campanha",
        "CANDIDATO_EM": "Candidato(a) em",
        "RECEBEU_SALARIO": "Recebe salario de",
        "AUTOR_EMENDA": "Autor de emenda",
        "CONTRATADA_POR": "Contratada por",
    "LOTADO_EM": "Lotado(a) em",
    "FORNECEU_GO": "Forneceu em licitacao GO",
    "CONTRATOU_GO": "Contratou (orgao GO)",
    "PUBLICADO_EM": "Publicado em diario oficial",
    "MENCIONADA_EM_GO": "Mencionada em diario oficial GO",
    "ARRECADOU": "Arrecadou",
    "GASTOU": "Gastou",
    "AUTOR_DE": "Autor(a) de projeto de lei",
    "DESPESA_GABINETE": "Despesa de gabinete",
    }
    return traducoes.get(rel_type, rel_type.replace("_", " ").capitalize())


def gerar_alertas_completos(
    entidade: dict,
    conexoes_raw: list,
    entidades_conectadas: dict,
    emendas_raw: list,
) -> list[dict[str, str]]:
    """Gera alertas usando o modulo de analise inteligente."""
    alertas = []
    props = entidade.get("properties", {})

    # Analise de patrimonio
    alerta_pat = analisar_patrimonio(
        props.get("patrimonio_declarado"),
        props.get("role") or props.get("cargo"),
    )
    if alerta_pat:
        alertas.append(alerta_pat)

    # Analise de emendas
    alertas.extend(analisar_emendas(emendas_raw))

    # Analise de conexoes
    alertas.extend(analisar_conexoes(conexoes_raw, entidades_conectadas))

    if not alertas:
        alertas.append({
            "tipo": "info",
            "icone": "info",
            "texto": (
                "Avaliação indisponível no momento. "
                "Não foi possível obter dados suficientes para analisar esta entidade. "
                "A ausência de alertas não significa que não existam irregularidades."
            ),
        })

    return alertas


# --- Cliente BRACC ---


class BraccClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=60.0)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("BraccClient not started")
        return self._client

    async def health(self) -> bool:
        try:
            resp = await self.client.get("/health")
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def meta(self) -> dict[str, Any]:
        resp = await self.client.get("/api/v1/meta/stats")
        resp.raise_for_status()
        return resp.json()

    async def contar_politicos(self, uf: str = "GO") -> dict[str, int]:
        resp = await self.client.get("/api/v1/meta/person-count", params={"uf": uf})
        resp.raise_for_status()
        return resp.json()

    async def buscar(self, query: str, tipo: str | None = None, page: int = 1, size: int = 20) -> dict[str, Any]:
        params: dict[str, Any] = {"q": query, "page": page, "size": size}
        if tipo:
            params["type"] = tipo
        resp = await self.client.get("/api/v1/search", params=params)
        resp.raise_for_status()
        return resp.json()

    async def entidade(self, cpf_ou_cnpj: str) -> dict[str, Any]:
        resp = await self.client.get(f"/api/v1/entity/{cpf_ou_cnpj}")
        resp.raise_for_status()
        return resp.json()

    async def conexoes(self, entity_id: str, depth: int = 1) -> dict[str, Any]:
        # depth=1 por padrao pra evitar queries explosivas no grafo grande
        try:
            resp = await self.client.get(
                f"/api/v1/entity/{entity_id}/connections",
                params={"depth": depth},
            )
            resp.raise_for_status()
            return resp.json()
        except (httpx.TimeoutException, httpx.ReadError):
            # Retorna estrutura vazia pra que o perfil ainda funcione
            # com dados das APIs externas (Camara, Transparencia)
            return {"entity": {}, "connections": [], "connected_entities": []}

    async def timeline(self, entity_id: str) -> dict[str, Any]:
        resp = await self.client.get(f"/api/v1/entity/{entity_id}/timeline")
        resp.raise_for_status()
        return resp.json()

    async def buscar_servidores_go(self, query: str, limit: int = 20) -> list[dict]:
        """Search state employees in GO."""
        resp = await self.client.get(
            "/api/v1/go/employees",
            params={"q": query, "limit": limit},
        )
        if resp.status_code == 200:
            return resp.json().get("results", [])
        return []

    async def buscar_municipios_go(self) -> list[dict]:
        """Get all GO municipalities via the dedicated /go/ router."""
        resp = await self.client.get("/api/v1/go/municipalities")
        if resp.status_code == 200:
            return resp.json().get("results", [])
        return []

    async def buscar_licitacoes_go(self, query: str = "", limit: int = 20) -> list[dict]:
        """Search GO procurements."""
        resp = await self.client.get(
            "/api/v1/go/procurements",
            params={"q": query, "limit": limit},
        )
        if resp.status_code == 200:
            return resp.json().get("results", [])
        return []

    async def buscar_nomeacoes_go(self, query: str = "", limit: int = 20) -> list[dict]:
        """Search GO appointments."""
        resp = await self.client.get(
            "/api/v1/search",
            params={"q": query or "*", "type": "go_appointment", "size": limit},
        )
        if resp.status_code == 200:
            return resp.json().get("results", [])
        return []

    async def buscar_vereadores_goiania(self) -> list[dict]:
        """Get Goiania city council members."""
        resp = await self.client.get(
            "/api/v1/search",
            params={"q": "*", "type": "go_vereador", "size": 100},
        )
        if resp.status_code == 200:
            return resp.json().get("results", [])
        return []

    async def contagem_go(self) -> dict[str, int]:
        """Get counts for all GO-specific node types."""
        try:
            resp = await self.client.get("/api/v1/go/counts")
            if resp.status_code != 200:
                return {}
            data = resp.json()
        except httpx.HTTPError:
            return {}
        return {
            "servidores_estaduais": int(data.get("state_employees", 0)),
            "cargos_comissionados": int(data.get("commissioned", 0)),
            "municipios_go": int(data.get("municipalities", 0)),
            "licitacoes_go": int(data.get("procurements", 0)),
            "nomeacoes_go": int(data.get("appointments", 0)),
        }


bracc = BraccClient(BRACC_API_URL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await bracc.start()
    yield
    await bracc.close()


# --- App ---

app = FastAPI(
    title="Fiscal Cidadao - Goiania",
    description="Monitor de politicos e gastos publicos em linguagem simples",
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Rotas ---


@app.get("/status", response_model=StatusResponse)
async def status():
    conectado = await bracc.health()
    meta: dict[str, Any] = {}
    contagem: dict[str, int] = {}
    if conectado:
        try:
            meta = await bracc.meta()
        except httpx.HTTPError:
            pass
        try:
            contagem = await bracc.contagem_go()
        except httpx.HTTPError:
            pass
    # Contagens de politicos de GO via BRACC
    politicos: dict[str, int] = {}
    if conectado:
        try:
            politicos = await bracc.contar_politicos(UF_FILTRO)
        except httpx.HTTPError:
            pass
    return StatusResponse(
        status="online",
        bracc_conectado=conectado,
        total_nos=meta.get("total_nodes", 0),
        total_relacionamentos=meta.get("total_relationships", 0),
        deputados_federais=politicos.get("deputados_federais", 0),
        deputados_estaduais=politicos.get("deputados_estaduais", 0),
        senadores=politicos.get("senadores", 0),
        vereadores_goiania=politicos.get("vereadores", 0),
        servidores_estaduais=contagem.get("servidores_estaduais", 0),
        cargos_comissionados=contagem.get("cargos_comissionados", 0),
        municipios_go=contagem.get("municipios_go", 0),
        licitacoes_go=contagem.get("licitacoes_go", 0),
        nomeacoes_go=contagem.get("nomeacoes_go", 0),
    )


UF_FILTRO = "GO"


@app.get("/buscar", response_model=list[PoliticoResumo])
async def buscar_politico(
    nome: str = Query(min_length=2, max_length=200, description="Nome do politico"),
):
    """Busca politicos por nome. Filtra apenas politicos de Goias (GO)."""
    try:
        resultado = await bracc.buscar(nome, tipo="person")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro na busca: {e}") from e

    politicos = []
    for r in resultado.get("results", []):
        props = r.get("properties", {})
        uf = props.get("uf", "")

        # Filtrar: so mostra politicos de GO (ignora quem nao tem UF - nao e politico)
        if not uf or uf.upper() != UF_FILTRO:
            continue

        patrimonio = props.get("patrimonio_declarado")
        politicos.append(PoliticoResumo(
            id=r["id"],
            nome=r.get("name", ""),
            cpf=r.get("document") or props.get("cpf"),
            patrimonio=patrimonio,
            patrimonio_formatado=fmt_brl(patrimonio) if patrimonio else None,
            is_pep=props.get("is_pep", False),
            partido=props.get("partido"),
            cargo=props.get("role") or props.get("cargo"),
            uf=uf or None,
            score=r.get("score", 0),
        ))

    return politicos


@app.get("/politico/{entity_id:path}", response_model=PerfilPolitico)
async def perfil_politico(entity_id: str):
    """Perfil completo de um politico com conexoes, emendas e alertas."""
    # Buscar conexoes (depth=1 pra performance em grafos grandes)
    try:
        dados_conexoes = await bracc.conexoes(entity_id, depth=1)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Politico nao encontrado") from e
        raise HTTPException(status_code=502, detail="Erro ao buscar dados") from e

    entidade = dados_conexoes.get("entity", {})
    props = entidade.get("properties", {})

    # Se conexoes deram timeout, buscar dados basicos pelo element-id
    if not props:
        try:
            resp = await bracc.client.get(
                f"/api/v1/entity/by-element-id/{entity_id}",
            )
            if resp.status_code == 200:
                entity_data = resp.json()
                props = entity_data.get("properties", {})
                entidade = entity_data
        except httpx.HTTPError:
            pass
    conexoes_raw = dados_conexoes.get("connections", [])
    entidades_conectadas = {
        e["id"]: e for e in dados_conexoes.get("connected_entities", [])
    }

    # Montar politico
    patrimonio = props.get("patrimonio_declarado")
    cargo_raw = props.get("role") or props.get("cargo")
    politico = PoliticoResumo(
        id=entity_id,
        nome=props.get("name", ""),
        cpf=props.get("cpf"),
        patrimonio=patrimonio,
        patrimonio_formatado=fmt_brl(patrimonio) if patrimonio else None,
        is_pep=props.get("is_pep", False),
        partido=props.get("partido"),
        cargo=traduzir_cargo(cargo_raw) if cargo_raw else None,
        uf=props.get("uf"),
    )

    # Classificar conexoes em categorias separadas
    emendas = []
    empresas: list[EmpresaConectada] = []  # empresas ligadas por sociedade/contrato
    contratos = []
    # Agregacao de doacoes por documento (1 doador pode fazer varias doacoes)
    doacoes_empresa_raw: dict[str, dict] = {}
    doacoes_pessoa_raw: dict[str, dict] = {}
    socios: list[SocioConectado] = []
    familia: list[FamiliarConectado] = []

    for conn in conexoes_raw:
        # Pegar a "outra ponta" da conexao — o politico pode estar como source OU target
        # (ex: (Doador)-[:DOOU]->(Politico) tem o politico como target)
        if conn["source_id"] == entity_id:
            target_id = conn["target_id"]
            politico_is_source = True
        elif conn["target_id"] == entity_id:
            target_id = conn["source_id"]
            politico_is_source = False
        else:
            continue
        target = entidades_conectadas.get(target_id, {})
        target_type = target.get("type", "")
        target_props = target.get("properties", {})
        rel_type = conn.get("relationship_type", "")
        rel_props = conn.get("properties", {}) or {}

        if target_type == "amendment":
            val_committed = target_props.get("value_committed", 0) or 0
            val_paid = target_props.get("value_paid", 0) or 0
            emendas.append(Emenda(
                id=target_props.get("amendment_id", target_id),
                tipo=traduzir_tipo_emenda(target_props.get("type", "")),
                funcao=traduzir_funcao_emenda(target_props.get("function", "")),
                municipio=target_props.get("municipality"),
                uf=target_props.get("uf"),
                valor_empenhado=val_committed,
                valor_empenhado_fmt=fmt_brl(val_committed),
                valor_pago=val_paid,
                valor_pago_fmt=fmt_brl(val_paid),
            ))
        elif rel_type == "DOOU" and not politico_is_source:
            # Doador de campanha (inbound): alguem doou dinheiro pro politico
            valor_doacao = float(rel_props.get("valor") or rel_props.get("amount") or 0)
            if target_type == "company":
                doc = target_props.get("cnpj") or f"empresa_{target_id}"
                nome = target_props.get("razao_social") or target_props.get("name", "")
                reg = doacoes_empresa_raw.setdefault(doc, {"nome": nome, "cnpj": target_props.get("cnpj"), "total": 0.0, "n": 0})
                reg["total"] += valor_doacao
                reg["n"] += 1
            elif target_type == "person":
                cpf = target_props.get("cpf") or target_props.get("cpf_partial") or f"pessoa_{target_id}"
                nome = target_props.get("name", "")
                reg = doacoes_pessoa_raw.setdefault(cpf, {"nome": nome, "cpf": target_props.get("cpf"), "total": 0.0, "n": 0})
                reg["total"] += valor_doacao
                reg["n"] += 1
        elif rel_type == "SOCIO_DE" and target_type == "company":
            cap = target_props.get("capital_social")
            socios.append(SocioConectado(
                nome=target_props.get("razao_social") or target_props.get("name", ""),
                cnpj=target_props.get("cnpj"),
                capital_social_fmt=fmt_brl(cap) if cap else None,
            ))
        elif rel_type in ("CONJUGE_DE", "PARENTE_DE") and target_type == "person":
            familia.append(FamiliarConectado(
                nome=target_props.get("name", ""),
                cpf_mascarado=target_props.get("cpf"),
                relacao="Cônjuge" if rel_type == "CONJUGE_DE" else "Parente",
            ))
        elif target_type == "company":
            # Relacao empresarial direta nao-doacao nao-socio (ex: contratante)
            cap = target_props.get("capital_social")
            empresas.append(EmpresaConectada(
                nome=target_props.get("razao_social") or target_props.get("name", ""),
                cnpj=target_props.get("cnpj"),
                relacao=traduzir_relacao(rel_type),
                capital_social=cap,
                capital_social_fmt=fmt_brl(cap) if cap else None,
            ))
        elif target_type == "contract":
            valor = target_props.get("value", 0) or 0
            contratos.append(ContratoConectado(
                objeto=target_props.get("object", "Nao informado"),
                valor=valor,
                valor_fmt=fmt_brl(valor),
                orgao=target_props.get("contracting_org"),
                data=target_props.get("date"),
            ))
        elif target_type == "go_procurement":
            valor = target_props.get("amount_estimated", 0) or 0
            contratos.append(ContratoConectado(
                objeto=target_props.get("object", "Licitacao estadual/municipal"),
                valor=valor,
                valor_fmt=fmt_brl(valor),
                orgao=target_props.get("agency_name"),
                data=target_props.get("published_at"),
            ))
        elif target_type == "state_agency":
            empresas.append(EmpresaConectada(
                nome=target_props.get("name", ""),
                cnpj=None,
                relacao="Lotado em (orgao estadual)",
            ))
        # election, go_gazette_act, person (sem rel familiar/doacao) — informativos, ignorados

    # Materializar listas agregadas de doadores, ordenadas por maior valor
    doadores_empresa = [
        DoadorEmpresa(
            nome=r["nome"],
            cnpj=r["cnpj"],
            valor_total=r["total"],
            valor_total_fmt=fmt_brl(r["total"]),
            n_doacoes=r["n"],
        )
        for r in doacoes_empresa_raw.values()
    ]
    doadores_empresa.sort(key=lambda x: -x.valor_total)
    doadores_pessoa = [
        DoadorPessoa(
            nome=r["nome"],
            cpf_mascarado=r["cpf"],
            valor_total=r["total"],
            valor_total_fmt=fmt_brl(r["total"]),
            n_doacoes=r["n"],
        )
        for r in doacoes_pessoa_raw.values()
    ]
    doadores_pessoa.sort(key=lambda x: -x.valor_total)
    total_doacoes = sum(d.valor_total for d in doadores_empresa) + sum(d.valor_total for d in doadores_pessoa)

    total_emendas_valor = sum(e.valor_pago or e.valor_empenhado for e in emendas)

    # --- Busca em tempo real nas APIs publicas ---
    fonte_emendas = "bracc" if emendas else None
    despesas_gabinete: list[DespesaGabinete] = []
    total_despesas = 0.0
    despesas_raw: list[dict] = []

    # Se nao encontrou emendas no BRACC, buscar nas APIs externas
    nome_politico = props.get("name", "")
    cpf_politico = props.get("cpf")

    # Buscar deputado na Camara (UF ajuda a desambiguar homonimos)
    uf_politico = props.get("uf") or politico.uf
    deputado = await buscar_deputado_camara(nome_politico, cpf_politico, uf_politico)
    if deputado:
        foto = deputado.get("ultimoStatus", {}).get("urlFoto") or deputado.get("urlFoto")
        if foto:
            politico.foto_url = foto
        dep_id = deputado.get("id")
        if dep_id:
            # Buscar despesas CEAP (cota parlamentar)
            despesas_raw = await buscar_despesas_deputado(dep_id)
            if despesas_raw:
                agrupadas = agrupar_despesas_por_tipo(despesas_raw)
                for item in agrupadas:
                    despesas_gabinete.append(DespesaGabinete(
                        tipo=traduzir_despesa(item["tipo"]),
                        total=item["total"],
                        total_fmt=fmt_brl(item["total"]),
                    ))
                total_despesas = sum(d.total for d in despesas_gabinete)

    # Buscar emendas no Portal da Transparencia (se nao tem do BRACC)
    if not emendas and nome_politico:
        emendas_ext = await buscar_emendas_transparencia(nome_politico)
        if emendas_ext:
            emendas_convertidas = converter_emendas_transparencia(emendas_ext)
            for e_raw in emendas_convertidas:
                val_committed = e_raw.get("value_committed", 0) or 0
                val_paid = e_raw.get("value_paid", 0) or 0
                emendas.append(Emenda(
                    id=e_raw.get("amendment_id", ""),
                    tipo=traduzir_tipo_emenda(e_raw.get("type", "")),
                    funcao=traduzir_funcao_emenda(e_raw.get("function", "")),
                    municipio=e_raw.get("municipality") or None,
                    uf=e_raw.get("uf") or None,
                    valor_empenhado=val_committed,
                    valor_empenhado_fmt=fmt_brl(val_committed),
                    valor_pago=val_paid,
                    valor_pago_fmt=fmt_brl(val_paid),
                ))
            total_emendas_valor = sum(
                e.valor_pago or e.valor_empenhado for e in emendas
            )
            fonte_emendas = "transparencia"

    # Gerar alertas com analise inteligente
    emendas_raw_alertas = [
        entidades_conectadas.get(c["target_id"], {}).get("properties", {})
        for c in conexoes_raw
        if entidades_conectadas.get(c["target_id"], {}).get("type") == "amendment"
    ]
    # Se as emendas vieram da API externa, usar elas nos alertas tambem
    if fonte_emendas == "transparencia" and not emendas_raw_alertas:
        emendas_raw_alertas = [
            {
                "value_paid": e.valor_pago,
                "value_committed": e.valor_empenhado,
                "municipality": e.municipio or "",
                "type": e.tipo,
            }
            for e in emendas
        ]
    alertas = gerar_alertas_completos(
        entidade, conexoes_raw, entidades_conectadas, emendas_raw_alertas,
    )

    # Analises de despesas de gabinete (comparacao e picos)
    uf_deputado = props.get("uf") or politico.uf
    comparacoes_cidada: list[ComparacaoCidada] = []
    comparacao_cidada_resumo = ""
    if despesas_raw:
        alertas.extend(analisar_despesas_gabinete(despesas_raw, uf_deputado))
        alertas.extend(analisar_picos_mensais(despesas_raw))

        # Comparacao com referencia cidada
        resultado_cidadao = analisar_despesas_vs_cidadao(despesas_raw)
        alertas.extend(resultado_cidadao["alertas"])
        comparacao_cidada_resumo = resultado_cidadao["resumo"]
        for comp in resultado_cidadao["comparacoes"]:
            comparacoes_cidada.append(ComparacaoCidada(
                categoria=comp["categoria"],
                total_politico_fmt=comp["total_politico_fmt"],
                media_mensal_politico_fmt=comp["media_mensal_politico_fmt"],
                referencia_cidadao_fmt=comp["referencia_cidadao_fmt"],
                razao=comp["razao"],
                razao_texto=comp["razao_texto"],
                classificacao=comp["classificacao"],
            ))

        # Comparar com media dos deputados do mesmo estado
        if uf_deputado and total_despesas > 0:
            try:
                media = await buscar_media_despesas_estado(uf_deputado)
                alerta_media = analisar_despesas_vs_media(
                    total_despesas, media, uf_deputado,
                )
                if alerta_media:
                    alertas.append(alerta_media)
            except Exception:
                pass  # nao bloqueia o perfil se a comparacao falhar

    # Remover alerta generico "ok" se agora temos alertas reais
    if len(alertas) > 1:
        alertas = [a for a in alertas if a["tipo"] != "ok"]

    # Ordenar por severidade: grave > atencao > info > ok — para que os
    # problemas mais serios apareçam antes do "dobra" na tela
    _severidade = {"grave": 0, "atencao": 1, "info": 2, "ok": 3}
    alertas.sort(key=lambda a: _severidade.get(a.get("tipo", "info"), 2))

    # Se achamos qualquer dado util sobre o politico, nao mostrar o alerta
    # generico "Avaliacao indisponivel" — ele soh faz sentido se o perfil
    # esta completamente vazio.
    tem_dados = bool(
        emendas or doadores_empresa or doadores_pessoa
        or socios or familia or empresas or contratos
        or despesas_gabinete
    )
    if tem_dados:
        alertas = [
            a for a in alertas
            if "Avaliação indisponível" not in a.get("texto", "")
        ]

    # Gerar resumo em linguagem simples
    resumo = gerar_resumo_politico(
        nome=politico.nome,
        cargo=cargo_raw,
        patrimonio=patrimonio,
        num_emendas=len(emendas),
        total_emendas=total_emendas_valor,
        num_conexoes=len(conexoes_raw),
    )

    # Descricao leiga do que o usuario vai ver nos cards abaixo.
    cats = []
    if doadores_empresa:
        cats.append(f"{len(doadores_empresa)} empresa(s) que doaram para a campanha")
    if doadores_pessoa:
        cats.append(f"{len(doadores_pessoa)} pessoa(s) que doaram para a campanha")
    if socios:
        cats.append(f"{len(socios)} empresa(s) em que o(a) politico(a) aparece como socio(a)")
    if familia:
        cats.append(f"{len(familia)} familiar(es) com ligacao politica")

    if cats:
        descricao_conexoes = (
            "Encontramos: " + "; ".join(cats) + ". "
            "Esses dados vem da Justica Eleitoral (TSE) e da Receita Federal — "
            "sao publicos. Aparecer aqui nao quer dizer que tem algo errado; "
            "e so pra voce saber com quem o(a) politico(a) se relaciona."
        )
    else:
        descricao_conexoes = ""

    # Validacao cruzada: total declarado ao TSE vs total que temos ingerido.
    validacao_tse: ValidacaoTSE | None = None
    total_tse = props.get("total_tse_2022")
    if total_tse:
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
        breakdown = []
        for label, key in [
            ("Partido político (fundo partidário + FEFC)", "tse_2022_partido"),
            ("Pessoas físicas", "tse_2022_pessoa_fisica"),
            ("Recursos próprios (autofinanciamento)", "tse_2022_proprios"),
            ("Financiamento coletivo (vaquinha)", "tse_2022_fin_coletivo"),
        ]:
            v = props.get(key)
            if v and float(v) > 0:
                breakdown.append({"origem": label, "valor_fmt": fmt_brl(float(v))})
        validacao_tse = ValidacaoTSE(
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

    # Aviso explicando pq nao tem despesas CEAP quando o politico nao e
    # deputado federal (so a Camara Federal tem essa cota com dados publicos).
    aviso_despesas = ""
    if not despesas_gabinete and not deputado:
        aviso_despesas = (
            "Este(a) politico(a) nao e deputado(a) federal. "
            "A cota parlamentar (CEAP) — com gastos de gabinete, telefone, "
            "combustivel e aluguel de escritorio — so existe na Camara Federal. "
            "Deputados estaduais e vereadores tem verbas parecidas, mas ainda "
            "nao temos esses dados no sistema."
        )

    return PerfilPolitico(
        politico=politico,
        resumo=resumo,
        emendas=emendas,
        total_emendas_valor=total_emendas_valor,
        total_emendas_valor_fmt=fmt_brl(total_emendas_valor),
        empresas=empresas,
        contratos=contratos,
        despesas_gabinete=despesas_gabinete,
        total_despesas_gabinete=total_despesas,
        total_despesas_gabinete_fmt=fmt_brl(total_despesas),
        comparacao_cidada=comparacoes_cidada,
        comparacao_cidada_resumo=comparacao_cidada_resumo,
        alertas=alertas,
        conexoes_total=len(conexoes_raw),
        fonte_emendas=fonte_emendas,
        descricao_conexoes=descricao_conexoes,
        doadores_empresa=doadores_empresa,
        doadores_pessoa=doadores_pessoa,
        total_doacoes=total_doacoes,
        total_doacoes_fmt=fmt_brl(total_doacoes),
        socios=socios,
        familia=familia,
        aviso_despesas=aviso_despesas,
        validacao_tse=validacao_tse,
    )


@app.get("/buscar-tudo")
async def buscar_tudo(
    q: str = Query(min_length=2, max_length=200),
    page: int = Query(default=1, ge=1),
):
    """Busca geral - politicos, empresas, contratos."""
    # BRACC ranqueia empresas primeiro por fulltext; precisamos buscar
    # pessoas separadamente pra que politicos de GO nao sejam soterrados.
    try:
        pessoas_res, outros_res = await asyncio.gather(
            bracc.buscar(q, tipo="person", page=page, size=30),
            bracc.buscar(q, page=page, size=20),
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    go_types = {"state_employee", "go_procurement", "go_appointment", "go_vereador"}

    # Unir mantendo dedup por id e preservando a maior score
    combined: dict[str, dict] = {}
    for r in pessoas_res.get("results", []) + outros_res.get("results", []):
        existing = combined.get(r["id"])
        if not existing or (r.get("score", 0) > existing.get("score", 0)):
            combined[r["id"]] = r

    total = max(pessoas_res.get("total", 0), outros_res.get("total", 0))

    items = []
    for r in sorted(combined.values(), key=lambda x: -x.get("score", 0)):
        props = r.get("properties", {})
        tipo = r.get("type", "")

        # Filtrar: pessoas so de GO; tipos GO-especificos passam direto
        if tipo == "person":
            uf = props.get("uf", "")
            if not uf or uf.upper() != UF_FILTRO:
                continue
        elif tipo not in go_types:
            continue

        item = {
            "id": r["id"],
            "tipo": tipo,
            "nome": r.get("name", ""),
            "documento": r.get("document"),
            "score": r.get("score", 0),
        }

        if tipo == "person":
            item["icone"] = "pessoa"
            patrimonio = props.get("patrimonio_declarado")
            item["detalhe"] = f"Patrimonio: {fmt_brl(patrimonio)}" if patrimonio else "Pessoa publica"
            item["is_pep"] = props.get("is_pep", False)
        elif tipo == "state_employee":
            item["icone"] = "servidor"
            salario = props.get("salary_gross")
            cargo = props.get("role", "")
            item["detalhe"] = f"{cargo} - {fmt_brl(salario)}/mes" if salario else cargo or "Servidor estadual"
            item["is_comissionado"] = props.get("is_commissioned", False)
        elif tipo == "go_procurement":
            item["icone"] = "licitacao"
            valor = props.get("amount_estimated", 0)
            item["detalhe"] = f"Licitacao: {fmt_brl(valor)}" if valor else props.get("object", "Licitacao")
        elif tipo == "go_appointment":
            item["icone"] = "nomeacao"
            item["detalhe"] = f"{props.get('appointment_type', 'Nomeacao').title()}: {props.get('role', '')}"
        elif tipo == "go_vereador":
            item["icone"] = "vereador"
            item["detalhe"] = f"Vereador(a) - {props.get('party', '')}"
        elif tipo == "company":
            item["icone"] = "empresa"
            item["detalhe"] = props.get("razao_social", "")
        elif tipo == "contract":
            item["icone"] = "contrato"
            valor = props.get("value", 0)
            item["detalhe"] = fmt_brl(valor) if valor else "Contrato publico"
        elif tipo == "amendment":
            item["icone"] = "emenda"
            valor = props.get("value_paid") or props.get("value_committed") or 0
            item["detalhe"] = f"Emenda: {fmt_brl(valor)}" if valor else "Emenda parlamentar"
        else:
            item["icone"] = "outro"
            item["detalhe"] = tipo.capitalize()

        items.append(item)

    return {
        "resultados": items,
        "total": total,
        "pagina": page,
    }


@app.get("/servidores", response_model=list[ServidorResumo])
async def buscar_servidores(
    nome: str = Query(min_length=2, max_length=200, description="Nome do servidor"),
):
    """Busca servidores estaduais de Goias por nome."""
    try:
        resultados = await bracc.buscar_servidores_go(nome)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    servidores = []
    for r in resultados:
        props = r.get("properties", {})
        salario = props.get("salary_gross")
        servidores.append(ServidorResumo(
            id=r.get("id", props.get("employee_id", "")),
            nome=props.get("name", ""),
            cargo=props.get("role"),
            orgao=props.get("agency"),
            salario_bruto=salario,
            salario_bruto_fmt=fmt_brl(salario) if salario else None,
            is_comissionado=props.get("is_commissioned", False),
        ))
    return servidores


@app.get("/municipios", response_model=list[MunicipioResumo])
async def listar_municipios():
    """Lista municipios de Goias com dados fiscais resumidos."""
    try:
        resultados = await bracc.buscar_municipios_go()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    municipios = []
    for r in resultados:
        props = r.get("properties", {})
        receita = props.get("total_revenue")
        despesa = props.get("total_expenditure")
        pop = props.get("population")
        municipios.append(MunicipioResumo(
            id=r.get("id", props.get("municipality_id", "")),
            nome=props.get("name", ""),
            populacao=int(pop) if pop else None,
            receita_total=receita,
            receita_total_fmt=fmt_brl(receita) if receita else None,
            despesa_total=despesa,
            despesa_total_fmt=fmt_brl(despesa) if despesa else None,
        ))
    return municipios


@app.get("/licitacoes", response_model=list[LicitacaoGO])
async def buscar_licitacoes(
    q: str = Query(default="", max_length=200, description="Termo de busca (vazio = todas)"),
    limit: int = Query(default=20, ge=1, le=100),
):
    """Busca licitacoes estaduais e municipais de Goias."""
    try:
        resultados = await bracc.buscar_licitacoes_go(q, limit)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    licitacoes = []
    for r in resultados:
        props = r.get("properties", {})
        valor = props.get("amount_estimated")
        licitacoes.append(LicitacaoGO(
            id=r.get("id", props.get("procurement_id", "")),
            orgao=props.get("agency_name", ""),
            cnpj_orgao=props.get("cnpj_agency"),
            objeto=props.get("object", ""),
            modalidade=props.get("modality"),
            valor_estimado=valor,
            valor_estimado_fmt=fmt_brl(valor) if valor else None,
            data_publicacao=props.get("published_at"),
            municipio=props.get("municipality"),
        ))
    return licitacoes


@app.get("/nomeacoes", response_model=list[NomeacaoGO])
async def buscar_nomeacoes(
    nome: str = Query(default="", max_length=200, description="Nome da pessoa nomeada"),
    limit: int = Query(default=20, ge=1, le=100),
):
    """Busca nomeacoes e exoneracoes em diarios oficiais de Goias."""
    try:
        resultados = await bracc.buscar_nomeacoes_go(nome, limit)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    nomeacoes = []
    for r in resultados:
        props = r.get("properties", {})
        nomeacoes.append(NomeacaoGO(
            id=r.get("id", props.get("appointment_id", "")),
            nome_pessoa=props.get("person_name", ""),
            cargo=props.get("role"),
            orgao=props.get("agency"),
            data=props.get("act_date"),
            tipo=props.get("appointment_type", "nomeacao"),
            fonte_diario=props.get("territory_name"),
        ))
    return nomeacoes


@app.get("/vereadores", response_model=list[VereadorResumo])
async def listar_vereadores():
    """Lista vereadores da Camara Municipal de Goiania."""
    try:
        resultados = await bracc.buscar_vereadores_goiania()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    vereadores = []
    for r in resultados:
        props = r.get("properties", {})
        total_desp = props.get("total_expenses")
        vereadores.append(VereadorResumo(
            id=r.get("id", props.get("vereador_id", "")),
            nome=props.get("name", ""),
            partido=props.get("party"),
            municipio=props.get("municipality", "Goiania"),
            total_despesas=total_desp,
            total_despesas_fmt=fmt_brl(total_desp) if total_desp else None,
            proposicoes=props.get("proposals_count", 0),
        ))
    return vereadores


# --- Static PWA ---
# Mount last so API routes above take precedence over static paths.
_PWA_DIR = os.getenv(
    "PWA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pwa"),
)
if os.path.isdir(_PWA_DIR):
    app.mount("/", StaticFiles(directory=_PWA_DIR, html=True), name="pwa")
