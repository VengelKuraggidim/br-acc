"""Traduções de termos técnicos para linguagem leiga.

Portado do Flask (`backend/analise.py` linhas 14-133 e
`backend/app.py::traduzir_relacao` linhas 254-276) como parte da fase 04.A
da consolidação FastAPI. Stateless — apenas dicts + funções puras.

Convenção: módulo com funções puras (igual a `public_guard.py`). Refactor
de CONTEÚDO dos dicts é fora de escopo desta fase — preservar mapeamentos
existentes.
"""

from __future__ import annotations

import unicodedata

# --- Dicionários de tradução ------------------------------------------------

CARGOS: dict[str, str] = {
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

FUNCOES_EMENDA: dict[str, str] = {
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

TIPOS_DESPESA: dict[str, str] = {
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

TIPOS_EMENDA: dict[str, str] = {
    "individual": "Emenda individual (feita por um unico parlamentar)",
    "bancada": "Emenda de bancada (feita pelo grupo do partido/estado)",
    "comissao": "Emenda de comissao (feita por comissao do Congresso)",
    "relator": "Emenda de relator (orcamento secreto - extinto)",
    "pix": "Emenda Pix (transferencia direta pra municipio)",
}

RELACOES: dict[str, str] = {
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


# --- Helpers ---------------------------------------------------------------


def _sem_acento(texto: str) -> str:
    """Remove acentos da string (NFKD + filtra combining chars)."""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# --- Funções públicas de tradução -----------------------------------------


def traduzir_cargo(cargo: str | None) -> str:
    """Retorna o cargo em português. Substring match (case-insensitive)."""
    if not cargo:
        return ""
    cargo_lower = cargo.lower().strip()
    for chave, traducao in CARGOS.items():
        if chave in cargo_lower:
            return traducao
    return cargo.title()


def traduzir_funcao_emenda(funcao: str | None) -> str:
    """Traduz função orçamentária (saude, educacao, etc)."""
    if not funcao:
        return "Nao informada"
    funcao_lower = funcao.lower().strip()
    for chave, traducao in FUNCOES_EMENDA.items():
        if chave in funcao_lower:
            return traducao
    return funcao.title()


def traduzir_tipo_emenda(tipo: str | None) -> str:
    """Traduz tipo de emenda (individual/bancada/comissao/relator/pix)."""
    if not tipo:
        return "Nao informado"
    tipo_lower = tipo.lower().strip()
    for chave, traducao in TIPOS_EMENDA.items():
        if chave in tipo_lower:
            return traducao
    return tipo.title()


def traduzir_despesa(descricao: str | None) -> str:
    """Traduz tipoDespesa CEAP (acento-insensitive)."""
    if not descricao:
        return "Despesa nao especificada"
    desc_lower = _sem_acento(descricao.lower().strip())
    for chave, traducao in TIPOS_DESPESA.items():
        if chave in desc_lower:
            return traducao
    return descricao.title()


def traduzir_relacao(rel_type: str) -> str:
    """Traduz rel_type BRACC (SOCIO_DE, DOOU, etc) pra texto leigo."""
    return RELACOES.get(rel_type, rel_type.replace("_", " ").capitalize())
