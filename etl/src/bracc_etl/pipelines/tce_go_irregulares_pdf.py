"""Parser dos PDFs anuais de "Contas Julgadas Irregulares" do TCE-GO.

Cada PDF (8 no total, um por ano par 2010..2024) lista os servidores
estaduais/municipais cujas contas foram julgadas irregulares pelo
Tribunal. O texto é estruturado (não scan) e ``pypdf`` extrai limpo.

Dois formatos no acervo:

- **Formato 2010** (outlier, sem CPF): tabela com colunas Nome | Cargo
  | Exercício | Nº do Acórdão e data | Fundamento Legal. CPF não estava
  no schema da época.
- **Formato 2012+** ("Relação de Responsáveis"): Nome | CPF | Processo
  | Cargo | Data de Julgamento | Assunto. **2022 mascara o CPF** por
  LGPD (e.g., ``836.XXX.XXX-34`` — só primeiros 3 e últimos 2 dígitos
  visíveis), os outros anos liberam dígitos completos.

O parser é unificado: ancora em padrão de CPF (full ou masked) quando
disponível, senão faz fallback line-based pra 2010. Saída é
``list[dict]`` com keys ``nome``, ``cpf`` (vazio quando ausente),
``cpf_masked`` (bool), ``processo``, ``cargo``, ``julgamento``, ``ano``
— pronta pra alimentar ``TceGoPipeline._transform_irregular`` via
``irregulares.csv``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# CPF: aceita formato completo (\d{3}.\d{3}.\d{3}-\d{2}) E mascarado
# (\d{3}.XXX.XXX-\d{2}) — TCE-GO mascarou em 2022 por LGPD/Lei Geral
# de Proteção de Dados (mantendo só primeiros e últimos blocos).
_CPF_RE = re.compile(r"\b(\d{3})\.([\dX]{3})\.([\dX]{3})-(\d{2})\b")

# "Acórdão nº NNNN" / "Processo NNNN" — usado pra ancorar o formato 2010
# que não tem CPF, e como sinal extra nos demais anos.
_ACORDAO_RE = re.compile(r"Acórdão\s*(?:nº)?\s*(\d+)", re.IGNORECASE)
_PROCESSO_RE = re.compile(r"Processo\s*(\d{6,})", re.IGNORECASE)

# Data DD/MM/YYYY ou DD/MM/YY
_DATA_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/(?:\d{2}|\d{4}))\b")

# Linha de cabeçalho/rodapé que aparece em vários PDFs e atrapalha
# o parsing line-based — descartar.
_NOISE_LINES = {
    "tribunal de contas do estado de goiás",
    "gabinete da presidência",
    "relação de responsáveis com contas julgadas irregulares",
    "relação de contas julgadas irregulares e tre",
    "lista das autoridades/servidores",
    "cujas contas foram julgadas irregulares",
    "lista das servidoras",  # 2010 typo na própria fonte
    "lista das servidores",
    "lista dos servidores",
}


def _normalize_text(raw: str) -> str:
    """Junta linhas quebradas e colapsa whitespace duplicado.

    Pypdf às vezes quebra ``"Iranildo Rodrigues\\nValença"`` em duas
    linhas (texto em PDF coluna estreita). Reglue quando a linha atual
    termina em preposição típica de nome próprio em PT-BR (``de``,
    ``da``, ``do``, ``dos``, ``das``) — bem específico, não toca outros
    casos.
    """
    lines = [line.strip() for line in raw.split("\n") if line.strip()]
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Junta com próxima se termina em preposição e próxima começa
        # com letra maiúscula (continuação de nome próprio)
        while (i + 1 < len(lines)
               and re.search(r"\b(de|da|do|dos|das|e)$", line)
               and re.match(r"^[A-ZÁÉÍÓÚÂÊÔÃÕÇ]", lines[i + 1])):
            line = f"{line} {lines[i + 1]}"
            i += 1
        out.append(line)
        i += 1
    return "\n".join(out)


def _is_noise(line: str) -> bool:
    low = line.lower().strip()
    if not low:
        return True
    for marker in _NOISE_LINES:
        if marker in low:
            return True
    if low.startswith("página ") or low.startswith("pagina "):
        return True
    if low.startswith("período de ") or low.startswith("base legal"):
        return True
    if low.startswith("nome ") or low.startswith("nome completo"):
        return True
    if low.startswith("nº do processo") or low.startswith("responsável"):
        return True
    return low in {"cpf", "cargo", "exercício", "fundamento legal"}


def _looks_like_name(text: str) -> bool:
    """Heurística: nome próprio começa com maiúscula e tem pelo menos
    duas palavras com letras (não puro número/data/processo)."""
    if not text or len(text) < 4:
        return False
    if any(ch.isdigit() for ch in text):
        return False
    parts = [w for w in text.split() if any(c.isalpha() for c in w)]
    return len(parts) >= 2


def _cpf_from_match(match: re.Match[str]) -> tuple[str, bool]:
    """Returns (cpf_string, is_masked). cpf_string conserva o formato
    visto (com X quando mascarado) — sanitização vira responsabilidade
    do consumer (strip_document filtra X)."""
    full = f"{match.group(1)}.{match.group(2)}.{match.group(3)}-{match.group(4)}"
    is_masked = "X" in match.group(2) or "X" in match.group(3)
    return full, is_masked


def parse_irregulares_pdf(text: str, year: str) -> list[dict[str, Any]]:
    """Parse o texto extraído de um PDF anual em rows de servidores.

    Args:
        text: texto cru extraído via pypdf (concat de todas as páginas).
        year: ano do PDF (2010, 2012, ...) — usado quando o PDF não traz
            data de julgamento por linha.

    Returns:
        Lista de dicts. Schema do CSV downstream:
        ``nome``, ``cpf``, ``cpf_masked``, ``processo``, ``cargo``,
        ``julgamento``, ``ano``.
    """
    text = _normalize_text(text)
    cpf_matches = list(_CPF_RE.finditer(text))
    if cpf_matches:
        return _parse_cpf_anchored(text, cpf_matches, year)
    # Fallback: 2010-style sem CPF — anchora em "Acórdão nº NNNN"
    return _parse_acordao_anchored(text, year)


def _parse_cpf_anchored(
    text: str, matches: list[re.Match[str]], year: str,
) -> list[dict[str, Any]]:
    """Cada CPF identifica um registro. Nome vem antes, processo/data depois."""
    rows: list[dict[str, Any]] = []
    for i, m in enumerate(matches):
        cpf, masked = _cpf_from_match(m)
        # Janela à esquerda do CPF até o limite anterior (CPF-1 ou início)
        left_start = matches[i - 1].end() if i > 0 else 0
        left_text = text[left_start:m.start()].strip()
        # Janela à direita até o próximo CPF (ou fim) — limita a 400 chars
        right_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        right_text = text[m.end():right_end][:400].strip()

        nome = _extract_name_before_cpf(left_text)
        processo = _extract_processo(right_text) or _extract_processo(left_text)
        julgamento = _extract_data(right_text)
        cargo = _extract_cargo(right_text)

        if not nome:
            logger.debug("[tce_irreg_pdf] CPF %s sem nome resolvível: %r",
                         cpf, left_text[-100:])
            continue
        rows.append({
            "nome": nome,
            "cpf": cpf,
            "cpf_masked": masked,
            "processo": processo,
            "cargo": cargo,
            "julgamento": julgamento,
            "ano": year,
        })
    return rows


def _extract_name_before_cpf(left: str) -> str:
    """Pega o nome próprio imediatamente antes do CPF.

    O texto bruto tipicamente termina com ``...\\n<NOME COMPLETO>`` ou
    ``\\n<NOME>`` direto colado no CPF. Walk back nas linhas até achar
    a primeira que parece nome.

    Stop words: além de cargo/data, palavras de cabeçalho ("Trânsito",
    "Julgado", "CPF", etc) que aparecem coladas no nome quando o pypdf
    junta o header da coluna com a primeira data row de uma página nova.
    Stop words legais ("inelegibilidade", "gerou") também — frase
    explicativa do dispositivo que precede algumas tabelas.
    """
    cleaned = re.sub(r"\s+", " ", left).strip()
    if not cleaned:
        return ""
    tokens = cleaned.split()
    name_tokens: list[str] = []
    for tok in reversed(tokens):
        if any(c.isdigit() for c in tok):
            break
        low = tok.lower().strip(",.:;()")
        # Stop words: cargo, header, frase legal, palavras conjuntivas
        # de notas ("gerou inelegibilidade", "trânsito em julgado")
        if low in _NAME_STOP_WORDS:
            break
        name_tokens.append(tok)
        if len(name_tokens) >= 7:
            break
    name = " ".join(reversed(name_tokens)).strip(" ,.:;")
    if not _looks_like_name(name):
        return ""
    return name


# Palavras que NUNCA fazem parte de nome próprio na lista TCE — usadas
# pra parar a janela retroativa quando o pypdf cola header de coluna ou
# texto explicativo no início de uma row.
_NAME_STOP_WORDS = frozenset({
    # Cargo / função
    "estadual", "estaduais", "municipal", "municipais",
    "servidor", "servidora", "presidente", "presidenta", "autarquia",
    "função", "funcao", "cargo",
    # Cabeçalhos de coluna
    "cpf", "trânsito", "transito", "julgado", "julgada",
    "responsável", "responsavel", "eleitor", "decisão", "decisao",
    "processo", "acórdão", "acordao", "data",
    # Texto introdutório / nota legal
    "inelegibilidade", "gerou", "rol", "lei", "complementar",
    "complementarinciso", "alínea", "alinea",
    # Conjunções/preposições isoladas (raro mas defensivo)
    "para", "que", "qual", "esta", "este", "estado",
})


def _extract_processo(snippet: str) -> str:
    m = _PROCESSO_RE.search(snippet)
    if m:
        return m.group(1)
    # Padrão alternativo: número no início da linha (formato 2014)
    m2 = re.search(r"\b(\d{8,})\b", snippet)
    if m2:
        return m2.group(1)
    return ""


def _extract_data(snippet: str) -> str:
    m = _DATA_RE.search(snippet)
    return m.group(1) if m else ""


_CARGO_KEYWORDS = (
    "Servidor Estadual", "Servidora Estadual", "Servidor Municipal",
    "Servidor estadual", "Servidor municipal", "Presidente",
    "Tem QOAPM", "Auxiliar", "Diretor", "Secretário", "Secretária",
    "Prefeito", "Prefeita", "Vereador", "Vereadora",
)


def _extract_cargo(snippet: str) -> str:
    for kw in _CARGO_KEYWORDS:
        if kw in snippet:
            # Pega da palavra-chave até o fim da "frase" (período/quebra dupla)
            idx = snippet.find(kw)
            tail = snippet[idx:idx + 120].split(".")[0]
            return re.sub(r"\s+", " ", tail).strip()
    return ""


def _parse_acordao_anchored(text: str, year: str) -> list[dict[str, Any]]:
    """Fallback pro formato 2010 (sem CPF): cada "Acórdão nº NNNN" + data
    é um registro; o nome vem nas 1-3 linhas anteriores."""
    rows: list[dict[str, Any]] = []
    lines = [ln for ln in text.split("\n") if ln.strip() and not _is_noise(ln)]
    for i, line in enumerate(lines):
        m = _ACORDAO_RE.search(line)
        if not m:
            continue
        # Nome: walk back até achar uma linha que pareça nome
        nome = ""
        for j in range(i - 1, max(-1, i - 6), -1):
            cand = lines[j].strip()
            # Cargo line ("Servidor estadual") sozinha não é nome
            if cand.lower() in {"servidor estadual", "servidor municipal",
                                "presidente de autarquia"}:
                continue
            # Linha que começa com data não é nome
            if _DATA_RE.match(cand):
                continue
            # No PDF de 2010 o pypdf às vezes funde nome + primeira
            # palavra do cargo ("Delaide Luiz Machado Servidor"); strip
            # tail words que são cargo isolado.
            # No PDF de 2010 o pypdf às vezes funde nome + primeira
            # palavra(s) do cargo ("Delaide Luiz Machado Servidor",
            # "Nasr Nagib Fayad Chaul Presidente de"); strip tail.
            cand = re.sub(
                r"\s+(Servidor|Servidora|Presidente|Cargo|Função)"
                r"(\s+(de|da|do|estadual|municipal))*\s*$",
                "", cand,
            ).strip()
            if _looks_like_name(cand):
                nome = cand
                break
        if not nome:
            continue
        julgamento = _extract_data(line)
        cargo = ""
        # Cargo geralmente está na linha SEGUINTE ao nome no 2010
        for j in range(i - 1, max(-1, i - 4), -1):
            low = lines[j].strip().lower()
            if any(low.startswith(p) for p in
                   ("servidor estadual", "servidor municipal",
                    "presidente de autarquia")):
                cargo = lines[j].strip()
                break
        rows.append({
            "nome": nome,
            "cpf": "",
            "cpf_masked": False,
            "processo": f"Acórdão {m.group(1)}",
            "cargo": cargo,
            "julgamento": julgamento,
            "ano": year,
        })
    return rows


def parse_pdf_file(pdf_path: Path | str, year: str) -> list[dict[str, Any]]:
    """Convenience: lê o PDF do disco via pypdf e roda o parser."""
    import pypdf
    pdf_path = Path(pdf_path)
    reader = pypdf.PdfReader(str(pdf_path))
    text = "\n".join(p.extract_text() or "" for p in reader.pages)
    rows = parse_irregulares_pdf(text, year)
    logger.info("[tce_irreg_pdf] %s: extraiu %d servidores",
                pdf_path.name, len(rows))
    return rows
