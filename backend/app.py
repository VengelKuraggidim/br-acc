from __future__ import annotations

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
            "tipo": "ok",
            "icone": "ok",
            "texto": "Nenhuma irregularidade aparente encontrada",
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
        counts = {}
        for node_type, key in [
            ("state_employee", "servidores_estaduais"),
            ("go_municipality", "municipios_go"),
            ("go_procurement", "licitacoes_go"),
            ("go_appointment", "nomeacoes_go"),
            ("go_vereador", "vereadores_goiania"),
        ]:
            try:
                resp = await self.client.get(
                    "/api/v1/search",
                    params={"q": "*", "type": node_type, "size": 1},
                )
                if resp.status_code == 200:
                    counts[key] = resp.json().get("total", 0)
                else:
                    counts[key] = 0
            except httpx.HTTPError:
                counts[key] = 0
        # Commissioned positions
        try:
            resp = await self.client.get(
                "/api/v1/search",
                params={"q": "comissionado", "type": "state_employee", "size": 1},
            )
            if resp.status_code == 200:
                counts["cargos_comissionados"] = resp.json().get("total", 0)
            else:
                counts["cargos_comissionados"] = 0
        except httpx.HTTPError:
            counts["cargos_comissionados"] = 0
        return counts


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

    # Classificar conexoes
    emendas = []
    empresas = []
    contratos = []

    for conn in conexoes_raw:
        # So processar conexoes diretas deste politico
        if conn["source_id"] != entity_id:
            continue
        target_id = conn["target_id"]
        target = entidades_conectadas.get(target_id, {})
        target_type = target.get("type", "")
        target_props = target.get("properties", {})

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
        elif target_type == "company":
            cap = target_props.get("capital_social")
            empresas.append(EmpresaConectada(
                nome=target_props.get("razao_social", target_props.get("name", "")),
                cnpj=target_props.get("cnpj"),
                relacao=traduzir_relacao(conn.get("relationship_type", "")),
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
        elif target_type == "election":
            # Registro eleitoral - nao e emenda, ignorar aqui
            pass
        elif target_type == "person":
            # Familiar ou conexao pessoal
            empresas.append(EmpresaConectada(
                nome=target_props.get("name", ""),
                cnpj=target_props.get("cpf"),
                relacao=traduzir_relacao(conn.get("relationship_type", "")),
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
        elif target_type == "go_gazette_act":
            # Gazette mentions are informational
            pass
        elif target_type == "state_agency":
            empresas.append(EmpresaConectada(
                nome=target_props.get("name", ""),
                cnpj=None,
                relacao="Lotado em (orgao estadual)",
            ))

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

    # Gerar resumo em linguagem simples
    resumo = gerar_resumo_politico(
        nome=politico.nome,
        cargo=cargo_raw,
        patrimonio=patrimonio,
        num_emendas=len(emendas),
        total_emendas=total_emendas_valor,
        num_conexoes=len(conexoes_raw),
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
    )


@app.get("/buscar-tudo")
async def buscar_tudo(
    q: str = Query(min_length=2, max_length=200),
    page: int = Query(default=1, ge=1),
):
    """Busca geral - politicos, empresas, contratos."""
    try:
        resultado = await bracc.buscar(q, page=page)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erro: {e}") from e

    go_types = {"state_employee", "go_procurement", "go_appointment", "go_vereador"}

    items = []
    for r in resultado.get("results", []):
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
        "total": resultado.get("total", 0),
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
