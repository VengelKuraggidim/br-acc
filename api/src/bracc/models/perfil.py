"""Modelos Pydantic do perfil político (endpoint /politico/{entity_id}).

Portados do Flask (`backend/app.py` linhas 46-170) como parte da Fase 04.A
da consolidação FastAPI. Campo `capital_social` foi removido de
`EmpresaConectada` e `SocioConectado` porque o pipeline RFB/QSA ainda não
existe. Quando o pipeline voltar, reintroduzir os campos sem breaking change
(tornando-os opcionais por default).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from bracc.models.entity import ProvenanceBlock  # noqa: TC001 (pydantic runtime annotation)

_PERFIL_MODEL_CONFIG = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
)


class PoliticoResumo(BaseModel):
    """Identificação básica de um político para exibição em card/resumo."""

    model_config = _PERFIL_MODEL_CONFIG


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
    """Emenda parlamentar (individual, bancada, comissão, relator, pix).

    ``provenance`` carrega origem rastreável dos props do nó :Amendment no
    grafo (Siop / Transparência / Câmara). ``None`` quando o nó é legado e
    ainda não foi re-ingerido sob o contrato de proveniência.

    ``beneficiario_cnpj`` e ``beneficiario_nome`` surfaceiam a empresa
    favorecida via ``(:Amendment)-[:BENEFICIOU]->(:Company)`` (populada
    pelo pipeline ``transferegov``). Ausentes quando a emenda ainda não
    tem beneficiário carimbado no grafo — comum em emendas recentes onde
    o convênio não saiu. ``beneficiario_cnpj`` é armazenado no formato
    do grafo (só dígitos); comparadores devem normalizar antes de
    cruzar com outras bases.
    """

    model_config = _PERFIL_MODEL_CONFIG

    id: str
    tipo: str
    funcao: str
    municipio: str | None = None
    uf: str | None = None
    valor_empenhado: float
    valor_empenhado_fmt: str
    valor_pago: float
    valor_pago_fmt: str
    beneficiario_cnpj: str | None = None
    beneficiario_nome: str | None = None
    beneficiario_data_abertura: str | None = None
    provenance: ProvenanceBlock | None = None


class EmpresaConectada(BaseModel):
    """Empresa ligada ao político por relação qualquer (exceto DOOU/SOCIO_DE).

    Campo `capital_social` foi removido nesta fase (04.A) porque o pipeline
    RFB/QSA ainda não existe. Volta quando o pipeline for implementado.

    `situacao` vem da RFB via pipeline ``brasilapi_cnpj_status`` (ou do
    bulk ``cnpj`` quando rodar). Valores brutos: ATIVA / BAIXADA /
    SUSPENSA / INAPTA / NULA / ``None``. ``situacao_fmt`` é a versão
    leiga pra exibir (Ativa / Baixada / etc.).
    """

    model_config = _PERFIL_MODEL_CONFIG

    nome: str
    cnpj: str | None = None
    relacao: str
    situacao: str | None = None
    situacao_fmt: str | None = None
    situacao_verified_at: str | None = None


class DoacaoItem(BaseModel):
    """Uma doação individual (linha do CSV TSE) dentro de um doador agregado.

    Cada item carrega valor + data + proveniência da doação específica.
    Ordenado cronologicamente por ``data_doacao`` (ISO → ordenação
    lexicográfica) quando emitido na lista ``doacoes`` dos modelos doador.
    """

    model_config = _PERFIL_MODEL_CONFIG

    valor: float
    valor_fmt: str
    data_doacao: str | None = None
    data_doacao_fmt: str | None = None
    provenance: ProvenanceBlock | None = None


class DoadorEmpresa(BaseModel):
    """Pessoa jurídica agregada por CNPJ que doou para a campanha.

    `situacao` vem do mesmo lugar que em :class:`EmpresaConectada`
    (pipeline ``brasilapi_cnpj_status``). Quando a empresa está
    BAIXADA/SUSPENSA/INAPTA, o ``alertas_service`` levanta alerta grave.

    ``provenance`` é agregado: quando múltiplas doações viram 1 doador, o
    service escolhe a proveniência da doação mais recente por
    ``ingested_at``. ``None`` quando nenhuma das doações agregadas trouxe
    os 4 campos obrigatórios (``source_id``/``source_url``/``ingested_at``/
    ``run_id``).

    ``data_primeira_doacao``/``data_ultima_doacao`` são ISO ``YYYY-MM-DD``
    do menor/maior ``donated_at`` visto entre as doações agregadas. ``None``
    quando nenhuma doação trouxe a data (legado pré-DT_RECEITA). ``*_fmt``
    é o mesmo valor em ``DD/MM/YYYY`` pra display direto no PWA.

    ``doacoes`` é a lista detalhada de :class:`DoacaoItem` — uma entry por
    linha do CSV TSE — ordenada por data. Permite ao PWA expandir o card e
    mostrar "DD/MM/YYYY — R$ X · Ver fonte" pra cada doação com rastreio
    individual. Vazia quando o pipeline fonte não carimbou ``donated_at``
    (legado pré-DT_RECEITA).
    """

    model_config = _PERFIL_MODEL_CONFIG

    nome: str
    cnpj: str | None = None
    valor_total: float
    valor_total_fmt: str
    n_doacoes: int
    situacao: str | None = None
    situacao_fmt: str | None = None
    situacao_verified_at: str | None = None
    data_primeira_doacao: str | None = None
    data_primeira_doacao_fmt: str | None = None
    data_ultima_doacao: str | None = None
    data_ultima_doacao_fmt: str | None = None
    doacoes: list[DoacaoItem] = []
    provenance: ProvenanceBlock | None = None


class DoadorPessoa(BaseModel):
    """Pessoa física agregada por CPF que doou para a campanha.

    `cpf_mascarado` deve ser sempre mascarado por `FormatacaoService` antes
    de entrar aqui — CPF pleno é violação LGPD.

    ``provenance`` segue a mesma regra de agregação de
    :class:`DoadorEmpresa` (doação mais recente por ``ingested_at``). LGPD:
    ``source_record_id`` NUNCA é populado aqui — no TSE o record_id
    normalmente é o CPF do doador, e surfar isso no chip de fonte violaria
    a máscara de CPF que o service aplica no próprio ``cpf_mascarado``. O
    restante dos campos (URLs públicas, timestamps, run_id) é preservado.

    Campos de data seguem a mesma semântica de :class:`DoadorEmpresa`.
    """

    model_config = _PERFIL_MODEL_CONFIG

    nome: str
    cpf_mascarado: str | None = None
    valor_total: float
    valor_total_fmt: str
    n_doacoes: int
    data_primeira_doacao: str | None = None
    data_primeira_doacao_fmt: str | None = None
    data_ultima_doacao: str | None = None
    data_ultima_doacao_fmt: str | None = None
    doacoes: list[DoacaoItem] = []
    provenance: ProvenanceBlock | None = None


class SocioConectado(BaseModel):
    """Empresa em que o político aparece como sócio (rel=SOCIO_DE).

    Campo `capital_social_fmt` foi removido nesta fase (04.A) — pipeline
    RFB/QSA ausente. Volta quando o pipeline existir.

    `situacao` idem :class:`DoadorEmpresa` — sócio de empresa BAIXADA
    também alimenta o alerta grave de ``alertas_service``.

    ``provenance`` carrega origem rastreável dos props do nó :Company no
    grafo (RFB/QSA). ``None`` quando o nó é legado e ainda não foi
    re-ingerido sob o contrato de proveniência. Como CNPJ é dado público,
    ``source_record_id`` é preservado (sem risco LGPD).
    """

    model_config = _PERFIL_MODEL_CONFIG

    nome: str
    cnpj: str | None = None
    situacao: str | None = None
    situacao_fmt: str | None = None
    situacao_verified_at: str | None = None
    provenance: ProvenanceBlock | None = None


class FamiliarConectado(BaseModel):
    """Familiar ligado ao político (cônjuge/parente) com CPF mascarado.

    ``provenance`` carrega origem rastreável dos props do nó :Person no
    grafo. LGPD: ``source_record_id`` NUNCA é populado aqui — o record_id
    do nó Person pode carregar o CPF pleno, e surfar isso no chip de
    fonte violaria a máscara que o service aplica no ``cpf_mascarado``.
    ``None`` quando o nó é legado e ainda não foi re-ingerido sob o
    contrato de proveniência.
    """

    model_config = _PERFIL_MODEL_CONFIG

    nome: str
    cpf_mascarado: str | None = None
    relacao: str
    provenance: ProvenanceBlock | None = None


class ValidacaoTSE(BaseModel):
    """Cross-check entre o valor declarado ao TSE e o que BRACC ingeriu.

    ``direcao`` distingue os dois modos de divergência para o PWA escolher
    a mensagem correta:

    * ``gap_ingestao``      — ``declarado > ingerido`` (faltaram doações
      no nosso banco; valor oficial é o do TSE).
    * ``excesso_ingestao``  — ``ingerido >= declarado`` (agregamos mais
      que o TSE declarou; provável duplicação nossa, não acusação).

    ``divergencia_valor`` preserva o sinal (positivo em gap, negativo em
    excesso); ``divergencia_valor_fmt`` sempre mostra o módulo pra
    display. ``divergencia_pct`` é magnitude (sempre >= 0).
    """

    model_config = _PERFIL_MODEL_CONFIG

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
    direcao: Literal["gap_ingestao", "excesso_ingestao"]


class ComparacaoContas(BaseModel):
    """Cross-check TSE-interno: receitas declaradas vs despesas pagas declaradas.

    Fase 1 do roadmap "cross-check de perspectivas TSE" — compara duas
    declarações do próprio candidato (``total_tse_{ano}`` vs
    ``total_despesas_tse_{ano}``) vindas do mesmo ``:Person`` no grafo.

    ``direcao`` indica qual lado ficou maior:

    * ``despesas_excedem`` — candidato declarou ter gasto mais do que
      arrecadou (pode ter explicação legítima: empréstimo, doação
      posterior, recursos próprios).
    * ``receitas_excedem`` — candidato declarou ter arrecadado mais do
      que gastou (sobra tem destino regulamentado: devolução ao TSE ou
      transferência ao partido).

    ``status`` segue a severidade:

    * ``ok``         — divergência < 5%
    * ``atencao``    — 5% <= divergência < 20%
    * ``divergente`` — divergência >= 20%

    ``divergencia_valor`` preserva sinal (positivo → sobra; negativo →
    estouro); ``divergencia_valor_fmt`` é o valor absoluto formatado
    pra exibição.
    """

    model_config = _PERFIL_MODEL_CONFIG

    ano_eleicao: int
    total_receitas: float
    total_receitas_fmt: str
    total_despesas: float
    total_despesas_fmt: str
    divergencia_valor: float
    divergencia_valor_fmt: str
    divergencia_pct: float
    direcao: Literal["despesas_excedem", "receitas_excedem"]
    status: Literal["ok", "atencao", "divergente"]


class ContratoConectado(BaseModel):
    """Contrato ou licitação (federal ou GO) ligado ao político.

    ``provenance`` carrega origem rastreável dos props do nó :Contract ou
    :Go_procurement no grafo (Portal da Transparência / PNCP / GO).
    ``None`` quando o nó é legado e ainda não foi re-ingerido sob o
    contrato de proveniência. Identificador do contrato/licitação é
    público — ``source_record_id`` é preservado.
    """

    model_config = _PERFIL_MODEL_CONFIG

    objeto: str
    valor: float
    valor_fmt: str
    orgao: str | None = None
    data: str | None = None
    provenance: ProvenanceBlock | None = None


class DespesaGabinete(BaseModel):
    """Despesa CEAP agregada por tipo (combustível, telefone, etc)."""

    model_config = _PERFIL_MODEL_CONFIG

    tipo: str
    total: float
    total_fmt: str


class ComparacaoCidada(BaseModel):
    """Comparação de uma categoria CEAP contra a referência do cidadão comum."""

    model_config = _PERFIL_MODEL_CONFIG

    categoria: str
    total_politico_fmt: str
    media_mensal_politico_fmt: str
    referencia_cidadao_fmt: str
    razao: float | None = None
    razao_texto: str
    classificacao: str  # "normal" | "elevado" | "abusivo"


class TetoGastos(BaseModel):
    """Comparação entre o gasto declarado ao TSE e o teto legal do cargo.

    Usa a Resolução TSE nº 23.607/2019 (com atualizações) para eleições 2022.
    Consumido por ``teto_service.calcular_teto`` — retorna ``None`` quando
    o cargo/UF não está mapeado (degradação silenciosa, seção omitida no PWA).

    ``classificacao`` segue a severidade:

    * ``ok``          — <70% do teto (verde)
    * ``alto``        — 70-90% do teto (amarelo)
    * ``limite``      — 90-100% do teto (laranja)
    * ``ultrapassou`` — >100% do teto (vermelho — infração eleitoral grave)
    """

    model_config = _PERFIL_MODEL_CONFIG

    valor_limite: float
    valor_limite_fmt: str
    valor_gasto: float
    valor_gasto_fmt: str
    pct_usado: float
    pct_usado_fmt: str  # ex.: "87%"
    cargo: str
    ano_eleicao: int
    classificacao: str  # "ok" | "alto" | "limite" | "ultrapassou"
    fonte_legal: str  # ex.: "Resolução TSE nº 23.607/2019"


class PerfilPolitico(BaseModel):
    """Response principal do endpoint /politico/{entity_id} (22 campos top-level).

    `provenance` carrega origem rastreável (source_url, run_id, ingested_at)
    conforme `docs/provenance.md`. `None` para agregações que ainda não
    carimbam a origem — será preenchido em fases posteriores.
    """

    model_config = _PERFIL_MODEL_CONFIG

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
    contas_campanha: ComparacaoContas | None = None
    teto_gastos: TetoGastos | None = None
