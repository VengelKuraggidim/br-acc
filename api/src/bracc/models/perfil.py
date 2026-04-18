"""Modelos Pydantic do perfil político (endpoint /politico/{entity_id}).

Portados do Flask (`backend/app.py` linhas 46-170) como parte da Fase 04.A
da consolidação FastAPI. Campo `capital_social` foi removido de
`EmpresaConectada` e `SocioConectado` porque o pipeline RFB/QSA ainda não
existe. Quando o pipeline voltar, reintroduzir os campos sem breaking change
(tornando-os opcionais por default).
"""

from __future__ import annotations

from pydantic import BaseModel

from bracc.models.entity import ProvenanceBlock  # noqa: TC001 (pydantic runtime annotation)


class PoliticoResumo(BaseModel):
    """Identificação básica de um político para exibição em card/resumo."""

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
    """Emenda parlamentar (individual, bancada, comissão, relator, pix)."""

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
    """Empresa ligada ao político por relação qualquer (exceto DOOU/SOCIO_DE).

    Campo `capital_social` foi removido nesta fase (04.A) porque o pipeline
    RFB/QSA ainda não existe. Volta quando o pipeline for implementado.
    """

    nome: str
    cnpj: str | None = None
    relacao: str


class DoadorEmpresa(BaseModel):
    """Pessoa jurídica agregada por CNPJ que doou para a campanha."""

    nome: str
    cnpj: str | None = None
    valor_total: float
    valor_total_fmt: str
    n_doacoes: int


class DoadorPessoa(BaseModel):
    """Pessoa física agregada por CPF que doou para a campanha.

    `cpf_mascarado` deve ser sempre mascarado por `FormatacaoService` antes
    de entrar aqui — CPF pleno é violação LGPD.
    """

    nome: str
    cpf_mascarado: str | None = None
    valor_total: float
    valor_total_fmt: str
    n_doacoes: int


class SocioConectado(BaseModel):
    """Empresa em que o político aparece como sócio (rel=SOCIO_DE).

    Campo `capital_social_fmt` foi removido nesta fase (04.A) — pipeline
    RFB/QSA ausente. Volta quando o pipeline existir.
    """

    nome: str
    cnpj: str | None = None


class FamiliarConectado(BaseModel):
    """Familiar ligado ao político (cônjuge/parente) com CPF mascarado."""

    nome: str
    cpf_mascarado: str | None = None
    relacao: str


class ValidacaoTSE(BaseModel):
    """Cross-check entre o valor declarado ao TSE e o que BRACC ingeriu."""

    ano_eleicao: int
    total_declarado_tse: float
    total_declarado_tse_fmt: str
    total_ingerido: float
    total_ingerido_fmt: str
    divergencia_valor: float
    divergencia_valor_fmt: str
    divergencia_pct: float
    breakdown_tse: list[dict[str, str]]
    status: str  # "ok" (<5%), "atencao" (5-20%), "divergente" (>=20%)


class ContratoConectado(BaseModel):
    """Contrato ou licitação (federal ou GO) ligado ao político."""

    objeto: str
    valor: float
    valor_fmt: str
    orgao: str | None = None
    data: str | None = None


class DespesaGabinete(BaseModel):
    """Despesa CEAP agregada por tipo (combustível, telefone, etc)."""

    tipo: str
    total: float
    total_fmt: str


class ComparacaoCidada(BaseModel):
    """Comparação de uma categoria CEAP contra a referência do cidadão comum."""

    categoria: str
    total_politico_fmt: str
    media_mensal_politico_fmt: str
    referencia_cidadao_fmt: str
    razao: float | None = None
    razao_texto: str
    classificacao: str  # "normal" | "elevado" | "abusivo"


class PerfilPolitico(BaseModel):
    """Response principal do endpoint /politico/{entity_id} (22 campos top-level).

    `provenance` carrega origem rastreável (source_url, run_id, ingested_at)
    conforme `docs/provenance.md`. `None` para agregações que ainda não
    carimbam a origem — será preenchido em fases posteriores.
    """

    provenance: ProvenanceBlock | None = None
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
