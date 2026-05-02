"""Entity resolution de políticos GO — cargo ↔ Person cross-label.

Problema
--------
O grafo acumula múltiplos nós pra mesma pessoa física. Exemplo real
medido em 2026-04-18: Jorge Kajuru aparece como 2 ``:Person`` (um com
CPF e sq_candidato, outro apenas com ``name``) + 1 ``:Senator``. A busca
PWA retorna só o primeiro ``:Person`` (partido antigo, sem foto) porque
nada liga os 3. Problema análogo atinge 17 ``:FederalLegislator`` e, quando
o pipeline ``alego`` rodar, ``:StateLegislator``. Ver a investigação em
``docs/entity_resolution.md``.

Estratégia
----------
Estratégia **C** (CanonicalPerson layer): este pipeline cria nós
``:CanonicalPerson`` representando a "pessoa real" e arestas ``:REPRESENTS``
apontando pros nós-fonte preservados. Proveniência por pipeline continua
intacta (nada é mergeado/deletado), e queries novas no grafo passam a
pivotar pela layer canônica.

Regras de matching (ordem decrescente de confiança; só a primeira que
resolve sem ambiguidade vence):

1. **cpf_exact** — ``:Person.cpf == :StateLegislator.cpf`` (dígitos,
   normalizados). Conf 1.00. ``:Senator`` e ``:FederalLegislator`` não
   entram aqui porque no grafo atual o primeiro não tem CPF e o segundo
   traz CPF mascarado vindo da Câmara.
2. **name_exact** — cargo.name normalizado (upper + sem acento, espaço
   colapsado) == Person.name normalizado, dentro do escopo ``uf='GO'``
   do Person. Ambiguidade (>1 Person GO com mesmo nome) vira audit-log
   e skip. Conf 0.95.
3. **name_stripped** — mesmo de (2) aplicado após tirar prefixos
   honoríficos (``DR. `` / ``DRA. `` / ``CEL. `` / ``DEP. `` / ``SEN. ``)
   e sufixos patronímicos (``JUNIOR`` / ``FILHO`` / ``NETO``) de qualquer
   ponta. Cobre "DR. ISMAEL ALEXANDRINO" ↔ "ISMAEL ALEXANDRINO JUNIOR".
   Conf 0.85.

Pós-resolução inicial, duas fases adicionais anexam nós-fonte que as
regras 1-3 não pegam:

* **cpf_suffix_name** (fase 3) — cargo com CPF mascarado
  (``***.***.*NN-NN``, formato da Câmara) contra ``:Person`` GO com CPF
  pleno cujo último 4 dígitos bate **e** cujo nome contém todos os tokens
  contentfuls (≥3 chars, não-stopwords) do nome do cargo. Cobre o caso
  "FLAVIA MORAIS" (parlamentar) ↔ "FLAVIA CARREIRO ALBUQUERQUE MORAIS"
  (TSE) onde nome abreviado + CPF mascarado quebram as 3 primeiras
  regras. Conf 0.92. Múltiplos matches → audit + skip.

* **cpf_suffix_token_overlap** (fase 3.5) — relaxa fase 3 substituindo
  "todos os tokens contentfuls do cargo no Person" por "≥1 token em
  comum" **mais** o filtro adicional de ``cargo_tse_{YYYY}`` no mesmo
  nível do cargo (``Deputado Federal`` ↔ ``:FederalLegislator``, etc.).
  Cobre nomes de campanha radicalmente reescritos onde o sufixo CPF
  sozinho gera ambiguidade entre 2-4 candidatos do mesmo cargo TSE,
  mas só um compartilha um token contentful com o nome do parlamentar:
  "ADRIANO DO BALDY" ↔ "ADRIANO ANTONIO AVELAR" (3 candidatos
  Deputado Federal sufixo 3153, só um com token "ADRIANO"); "DANIEL
  AGROBOM" ↔ "DANIEL VIEIRA RAMOS"; "DR. ZACHARIAS CALIL" ↔ "ZACARIAS
  CALIL HAMU" (4 candidatos sufixo 0100, só um com token "CALIL").
  Conf 0.88 — entre cpf_suffix_name (0.92) e cpf_suffix_cargo (0.85),
  porque a evidência de identidade é mais fraca que tokens-subset mas
  mais forte que sufixo+cargo sem nome. Match único → attach;
  múltiplos → audit. Tokens curtos (<3 chars) já caem no filtro de
  ``_contentful_tokens``; tokens stopwords/honoríficos também.

* **cpf_suffix_cargo** (fase 4) — fallback mais fraco pra quando
  ``cpf_suffix_name`` E ``cpf_suffix_token_overlap`` falham (zero
  tokens em comum entre nome cargo e nome Person). Casa ``:Person`` GO
  cujo ``cargo_tse_{YYYY}`` corresponde ao label do cargo (``Deputado
  Federal`` ↔ ``:FederalLegislator``, etc.) **e** cujos 4 últimos dígitos
  do CPF batem com o sufixo mascarado do cargo. Sem validar nome.
  Cobre "PROFESSOR ALCIDES" ↔ "ALCIDES RIBEIRO FILHO" (resolvido pela
  3.5 quando há token "ALCIDES" comum) e nomes completamente
  reescritos como "GLAUSTIN DA FOKUS" ↔ "GLAUSKSTON BATISTA RIOS"
  (sem token comum, só sufixo + cargo). Conf 0.85. Trava de match
  único é obrigatória — sufixo de 4 dígitos sozinho tem ~1/10000
  colisão e acumulado em ~400-1000 candidatos GO por cargo gera
  ambiguidade frequente; audit+skip quando >1.

* **name_partido_multi** (fase 4.5) — pra cargos SEM CPF publicado
  (Senadores do pipeline ``senado_senadores_foto``; ``:StateLegislator``
  quando a fonte ALEGO não traz CPF) onde ``name_exact`` retornou N>1
  hits e ``_disambiguate_by_partido`` devolveu None. Anexa TODOS os
  ``:Person`` GO com ``(name_normalized, partido, uf)`` idênticos ao
  cargo. Justificativa: homonimia completa (mesmo nome + mesmo
  partido + mesma UF) entre pessoas reais distintas é virtualmente
  inexistente em cargos legislativos ativos — os N Persons são
  registros TSE do mesmo candidato em anos diferentes (receitas,
  bens, candidato). Conf 0.78 (mais baixa que name_exact). Sem essa
  fase, o Senador Vanderlan Vieira Cardoso (PSD/GO) fica orfão com
  ≥1 ``:Person`` GO PSD/GO com o mesmo nome e o perfil perde o
  cluster canônico.

* **name_municipio_vereador** (fase 4.7) — pra duplicatas de vereador GO
  onde o TSE 2024 não publica CPF. ``:Person`` GO **sem CPF** que é
  ``MEMBRO_DE :CamaraMunicipal {uf:'GO', municipio:M}`` é anexado ao
  cluster ancorado num ``:Person`` GO **com CPF** que tem o mesmo
  ``name_normalized`` E também é membro da mesma Câmara M. Caso
  canônico: "ROMARIO BARBOSA POLICARPO" GOIANIA — id 8071 com CPF
  (criado por TSE 2020+2022, recebeu enriquecimento 2024) e id 501376
  sem CPF (criado por tse_bens 2024). Único path do pipeline que cria
  ``:CanonicalPerson`` ancorado em Person fora de cargo Senador/Fed/State,
  porque vereador não tem label de cargo no grafo (sem ``:Vereador``;
  só ``:MEMBRO_DE :CamaraMunicipal``). Travas: LHS único (>1 Person
  com CPF mesmo nome no mesmo município = audit ``municipal_lhs_ambiguous``
  + skip — pai+filho homônimos sentando juntos é raro mas possível);
  filtro estrito de município (``JOAO BATISTA DA SILVA`` em 8 cidades
  diferentes nunca colide); só :Person, não cruza com Senator/Fed/State
  (esses já passaram). Conf 0.90 — entre cpf_suffix_name (0.92) e
  cpf_suffix_token_overlap (0.88). Justificativa: nome exato + município
  é evidência forte (município é discriminador local), mas RHS sem
  CPF impede confirmação por documento.

* **shadow_name_exact** (fase 5) — ``:Person`` sem CPF, sem UF (nós bare
  "só name" originados de referências em outros pipelines, ex.: autores
  de inquéritos) com nome normalizado batendo exatamente com UM dos
  nomes já presentes no cluster canônico → REPRESENTS adicional. Conf
  0.80. Ambiguidade = skip+log.

* **shadow_prefix_match** (fase 5.5) — pra shadows que ``shadow_name_exact``
  não pegou (zero clusters com nome igual). Casa shadow cujo
  ``name_normalized`` é prefix exato (sequência de tokens iniciais) do
  nome de algum source num cluster já resolvido. Caso canônico:
  shadow ``"JORGE KAJURU"`` (2 tokens) ↔ Senator ``"JORGE KAJURU REIS
  DA COSTA NASSER"`` (6 tokens). Gating conservador: shadow precisa
  ter ≥2 tokens (1 token = sobrenome solto, genérico demais), e o
  prefix precisa bater EXATAMENTE 1 cluster — múltiplos clusters
  compartilhando o mesmo prefix curto vira audit. Conf 0.70 (mais
  baixa que ``shadow_name_exact`` porque a evidência de identidade é
  mais fraca).

* **shadow_first_last_match** (fase 5.6, opt-in via
  ``enable_first_last_match=True``) — pra shadows que ``shadow_prefix_match``
  não pegou. Casa shadow de **exatamente 2 tokens** com cluster cujo
  source tem ≥3 tokens compartilhando primeiro **e** último token com
  o shadow. Caso canônico: shadow ``"KARLOS CABRAL"`` (2 tokens) ↔
  cluster com source ``"KARLOS MARCIO VIEIRA CABRAL"`` (4 tokens,
  ``[0]==shadow[0]`` e ``[-1]==shadow[-1]``). Conf 0.65 (mais baixa
  que ``shadow_prefix_match`` — tokens do meio podem divergir).
  Default OFF + ``first_last_audit_only=True`` → 1ª passagem só popula
  audit pra spot-check humano antes de promover (homonímia 2-token no
  Brasil é alta).

* **manual_override** (fase 6, opcional) — última camada, lê CSV
  versionado em ``docs/entity_resolution_overrides.csv`` (path
  configurável via env ``BRACC_ER_OVERRIDES_PATH``) com linhas
  ``canonical_id,target_kind,target_key,confidence,notes,added_by,
  added_at``. Cada linha é uma afirmação humana: "anexe este nó
  (identificado por chave estável: ``sq_candidato``/``id_senado``/
  ``id_camara``/``legislator_id``/``cpf``) ao cluster ``canonical_id``
  com método ``manual_override``". Conf default 1.0. Skippar (audit)
  quando: (a) cluster não existe; (b) target não bate nenhum nó; (c)
  target já anexado a OUTRO cluster (conflito, requer review humana).
  Idempotente quando target já está no mesmo cluster (no-op). CSV
  ausente é OK — fase desativa silenciosamente.

Stop on ambiguidade é política do projeto (CLAUDE.md §3). Audit log em
``data/entity_resolution_politicos_go/audit_{run_id}.jsonl`` lista todos
os casos puláveis pra revisão humana.

Saída no grafo
--------------
Nó ``:CanonicalPerson`` com ``canonical_id`` estável por cluster. Prioridade
do canonical_id:

1. ``canon_senado_{id_senado}``
2. ``canon_camara_{id_camara}``
3. ``canon_alego_{legislator_id_digits}`` (pipeline ``alego``)
4. ``canon_cpf_{cpf_digits}`` (Person com CPF mas sem cargo ativo)

Props no nó (além de proveniência):

* ``display_name``: nome do cargo mais oficial (Senator > Fed > State >
  Person com CPF).
* ``cargo_ativo``: ``"senador"`` / ``"deputado_federal"`` /
  ``"deputado_estadual"`` / ``None``.
* ``uf``: sempre ``"GO"`` (escopo do pipeline).
* ``partido``: do cargo ativo (mais recente).
* ``num_sources``: tamanho do cluster.
* ``confidence_min``: menor confidence entre os REPRESENTS do cluster —
  útil pro frontend sinalizar "match com dúvida".

Arestas ``:REPRESENTS`` (1 por nó-fonte), direcionadas
``(:CanonicalPerson)-[:REPRESENTS]->(sourceNode)``. Props:

* ``method``: ``"cpf_exact" | "name_exact" | "name_exact_partido" |
  "name_stripped" | "cpf_suffix_name" | "cpf_suffix_token_overlap" |
  "cpf_suffix_cargo" | "name_partido_multi" | "name_municipio_vereador" |
  "shadow_name_exact" | "shadow_prefix_match" | "shadow_first_last_match" |
  "manual_override" |
  "cargo_root"``.
* ``confidence``: float [0, 1].
* Proveniência do próprio pipeline (source_id, run_id, source_url,
  ingested_at, source_record_id).

Idempotência
------------
``MERGE`` em ``canonical_id`` + ``MERGE`` em ``(canonical)-[r:REPRESENTS]->
(source)``. Re-runs atualizam props (``SET r.method = ...``) mas não
duplicam. Pipelines-fonte (tse, senado_senadores_foto, camara_politicos_go,
alego) são **desacoplados**: rodar este pipeline não altera os nós-fonte.

Sem archival: este pipeline não busca dados externos. ``source_url`` é
o próprio código do pipeline no repo público — honesto: a "fonte"
desta derivação *é* a lógica de resolução versionada em git.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


_SOURCE_ID = "entity_resolution_politicos_go"

# Escopo GO-only (alinha com o produto Fiscal Cidadão). Cargos Senate,
# Fed, State: os 3 labels que este pipeline liga a Person.
_TARGET_UF = "GO"

# Prefixos honoríficos/de ocupação que aparecem em nomes de campanha
# ("DR. ISMAEL ALEXANDRINO") mas não constam do registro TSE canônico
# ("ISMAEL ALEXANDRINO JUNIOR"). Removidos na fase ``name_stripped``.
# Inclui ponto opcional e variações masculino/feminino.
_HONORIFIC_PREFIXES = frozenset({
    "DR", "DRA", "DR.", "DRA.",
    "PROF", "PROFA", "PROF.", "PROFA.",
    "CEL", "CEL.", "GEN", "GEN.", "SGT", "SGT.",
    "DEP", "DEP.", "SEN", "SEN.", "VER", "VER.",
    "PASTOR", "PADRE", "IRMAO", "IRMÃO", "DELEGADO", "DELEGADA",
})
# Sufixos patronímicos (cargo registro TSE "MARCONI PERILLO JUNIOR"
# x label social "MARCONI PERILLO"). Replica _HONORIFIC_SUFFIXES do
# pipeline wikidata_politicos_foto; fonte unificada ficaria boa como
# follow-up.
_HONORIFIC_SUFFIXES = frozenset({
    "JUNIOR", "JR", "FILHO", "NETO", "SOBRINHO", "SEGUNDO",
})

# Stopwords portuguesas descartadas na comparação por "tokens contentfuls"
# usada pela regra ``cpf_suffix_name``. Preservar "DE", "DA", "DOS" etc.
# inflaria falsos positivos — um "FLAVIA DE MORAIS" não deveria casar com
# "FLAVIA MORAIS" por tokens. Tokens ≤2 chars já caem no filtro de
# tamanho mínimo (ver ``_contentful_tokens``).
_NAME_STOPWORDS = frozenset({
    "DE", "DA", "DO", "DAS", "DOS", "E",
})

_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")
_MULTI_SPACE = re.compile(r"\s+")

# Cargo ranking — quem "ganha" como display_name quando um cluster tem
# múltiplas fontes. Mais oficial primeiro.
_CARGO_RANK: dict[str, int] = {
    "Senator": 0,
    "FederalLegislator": 1,
    "StateLegislator": 2,
    "Person": 3,
}

_CARGO_ATIVO_LABEL: dict[str, str] = {
    "Senator": "senador",
    "FederalLegislator": "deputado_federal",
    "StateLegislator": "deputado_estadual",
}

# Label do cargo ↔ string canônica que aparece em ``cargo_tse_{YYYY}`` no
# ``:Person`` (CSV TSE grava em Title Case; normalizamos pra UPPER sem
# acento antes de comparar). Usado pela fase 4 ``cpf_suffix_cargo`` pra
# filtrar candidatos TSE do mesmo nível de cargo. Governador e cargos
# municipais (prefeito, vereador) ficam de fora — o ER só cobre
# Senador/Fed/State e o objetivo é Pessoa com mandato federal/estadual
# ativo hoje.
_CARGO_LABEL_TO_TSE: dict[str, str] = {
    "Senator": "SENADOR",
    "FederalLegislator": "DEPUTADO FEDERAL",
    "StateLegislator": "DEPUTADO ESTADUAL",
}

# Chaves estáveis aceitas em ``docs/entity_resolution_overrides.csv``.
# ``cpf`` é normalizado pra dígitos antes de comparar; os outros são
# comparados como string crua. Não inclui ``element_id`` propositalmente
# — esses IDs são do Neo4j local e não sobrevivem re-ingestão.
_OVERRIDE_TARGET_KINDS = frozenset({
    "sq_candidato", "id_senado", "id_camara", "legislator_id", "cpf",
})

_OVERRIDES_ENV_VAR = "BRACC_ER_OVERRIDES_PATH"
_DEFAULT_OVERRIDES_PATH = Path("docs/entity_resolution_overrides.csv")


def _resolve_overrides_path() -> Path:
    """Resolve o path do CSV de overrides — env var sobrepõe o default."""
    override = os.environ.get(_OVERRIDES_ENV_VAR)
    if override:
        return Path(override)
    return _DEFAULT_OVERRIDES_PATH


def _load_overrides_csv(path: Path) -> list[dict[str, str]]:
    """Lê o CSV de overrides; retorna lista de dicts ou [] se não existe.

    Schema: ``canonical_id,target_kind,target_key,confidence,notes,added_by,
    added_at``. Apenas ``canonical_id``, ``target_kind`` e ``target_key``
    são obrigatórios; o resto é opcional (``confidence`` default 1.0).
    """
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cleaned = {k: (v.strip() if v else "") for k, v in row.items()}
            if not cleaned.get("canonical_id"):
                continue
            if not cleaned.get("target_kind"):
                continue
            if not cleaned.get("target_key"):
                continue
            rows.append(cleaned)
    return rows


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_name(raw: str | None) -> str:
    """Upper + sem acento + sem pontuação + whitespace colapsado."""
    if not raw:
        return ""
    base = _strip_accents(str(raw)).upper()
    base = _NON_ALNUM.sub(" ", base)
    return _MULTI_SPACE.sub(" ", base).strip()


def _strip_honorifics(normalized: str) -> str:
    """Remove honoríficos/sufixos das pontas (já com ``_normalize_name``).

    - Prefixos: "DR", "DRA", "PROF", "CEL", "DEP", "SEN", "VER", "PASTOR",
      etc. — só da primeira palavra (evita tirar "DEP" do meio do nome).
    - Sufixos: "JUNIOR", "JR", "FILHO", "NETO" — só da última palavra.

    Os conjuntos cobrem os 2 deputados federais GO cujo nome de campanha
    diverge do TSE ("DR. ISMAEL ALEXANDRINO" → "ISMAEL ALEXANDRINO";
    pareado via ``name_stripped`` contra "ISMAEL ALEXANDRINO JUNIOR"
    depois que este também perde o "JUNIOR").
    """
    if not normalized:
        return ""
    parts = normalized.split(" ")
    # Prefix strip — até 2 tokens honoríficos encadeados ("DR CEL ...").
    while parts and parts[0] in _HONORIFIC_PREFIXES:
        parts.pop(0)
    # Suffix strip — última palavra.
    while parts and parts[-1] in _HONORIFIC_SUFFIXES:
        parts.pop()
    return " ".join(parts)


def _digits_only(raw: str | None) -> str:
    if not raw:
        return ""
    return "".join(ch for ch in str(raw) if ch.isdigit())


def _is_masked_cpf(raw: str | None) -> bool:
    """Retorna True se o CPF tem ``*`` — format de mascaramento LGPD.

    ``camara_deputados`` grava CPF mascarado (`***.***.*31-53`). Não dá
    pra comparar com CPFs plenos do TSE — pulamos esses cases no path
    ``cpf_exact`` (vão pro ``name_*`` depois).
    """
    return bool(raw) and "*" in str(raw)


def _is_sq_sentinel_cpf(raw: str | None) -> bool:
    """Retorna True se o campo ``cpf`` é o sentinel ``sq:{sq_candidato}``.

    O pipeline ``tse_prestacao_contas_go`` grava ``cpf="sq:{sq}"`` quando
    o TSE 2024+ mascara o CPF. Esse valor não é um CPF real — ``_digits_only``
    extrai os dígitos do sq como se fosse CPF, o que polui o índice
    ``persons_by_cpf`` do ER com fake-CPFs que podem colidir com CPFs
    plenos de outras pessoas (1/10^11, raro mas não zero). Detectamos
    pelo prefixo ``sq:`` antes de normalizar pra dígitos.
    """
    return bool(raw) and str(raw).startswith("sq:")


def _visible_cpf_suffix(raw: str | None) -> str:
    """Extrai os 4 dígitos visíveis do CPF (funciona mascarado ou pleno).

    Câmara mascara no formato ``***.***.*NN-NN`` — revela exatamente os 4
    últimos dígitos (posições 10, 11, 13, 14 de ``AAA.BBB.CCC-DD``). CPF
    pleno do TSE também devolve os últimos 4. Retorna ``""`` quando a
    entrada é inconclusiva (sem 4 dígitos parseáveis).

    Uso: casa ``:Person`` TSE com CPF pleno contra ``:FederalLegislator``
    com CPF mascarado quando o nome TSE ("FLAVIA CARREIRO ALBUQUERQUE
    MORAIS") não bate exato com o nome parlamentar ("FLAVIA MORAIS").
    """
    digits = _digits_only(raw)
    if len(digits) < 4:
        return ""
    return digits[-4:]


def _contentful_tokens(name_normalized: str) -> list[str]:
    """Tokens significativos do nome — exclui stopwords, honoríficos e tokens curtos.

    Filtros (ordem):
    * tamanho ≥ 3 chars (descarta "E", iniciais, artigos);
    * não é stopword (``DE``/``DA``/``DO``/``DAS``/``DOS``/``E``);
    * não é honorífico prefixo ou sufixo (``DR``, ``JUNIOR``, ...).

    Usado pela regra ``cpf_suffix_name`` pra garantir que todos os tokens
    contentfuls do nome do cargo aparecem no nome do Person. Previne
    colisões de suffix em outros Persons GO que batem no suffix mas são
    pessoas diferentes ("CELIO ANTONIO DA SILVEIRA" passa; "WEBER TIAGO
    PIRES" falha apesar do suffix igual ao "CELIO SILVEIRA").
    """
    if not name_normalized:
        return []
    tokens: list[str] = []
    for tok in name_normalized.split(" "):
        if len(tok) < 3:
            continue
        if tok in _NAME_STOPWORDS:
            continue
        if tok in _HONORIFIC_PREFIXES or tok in _HONORIFIC_SUFFIXES:
            continue
        tokens.append(tok)
    return tokens


def _cargo_tokens_subset_of_person(
    cargo_name_normalized: str,
    person_name_normalized: str,
) -> bool:
    """True sse todos os tokens contentfuls do cargo estão no nome do Person.

    Subset de tokens (não substring) — preserva ordem livre e ignora
    tokens extras do Person (nomes do meio, patronímicos, etc.).
    Retorna False se o cargo não tem nenhum token contentful (nome vazio
    ou só stopwords — não dá pra validar).
    """
    cargo_tokens = _contentful_tokens(cargo_name_normalized)
    if not cargo_tokens:
        return False
    person_tokens = set(_contentful_tokens(person_name_normalized))
    return all(tok in person_tokens for tok in cargo_tokens)


def _cargo_person_share_token(
    cargo_name_normalized: str,
    person_name_normalized: str,
) -> bool:
    """True sse cargo e Person compartilham ≥1 token contentful.

    Mais frouxa que ``_cargo_tokens_subset_of_person`` — basta um token
    em comum (>= 3 chars, não-stopword, não-honorífico) pra retornar
    True. Usada pela fase 3.5 ``cpf_suffix_token_overlap`` quando o
    nome de campanha do parlamentar diverge muito do registro TSE
    ("ADRIANO DO BALDY" vs "ADRIANO ANTONIO AVELAR" — só "ADRIANO"
    em comum). Retorna False se um dos dois lados não tem nenhum
    token contentful.
    """
    cargo_tokens = set(_contentful_tokens(cargo_name_normalized))
    if not cargo_tokens:
        return False
    person_tokens = set(_contentful_tokens(person_name_normalized))
    if not person_tokens:
        return False
    return bool(cargo_tokens & person_tokens)


# Cypher: puxa todos os nós candidatos pro ER — os 3 cargos GO +
# Persons GO (com UF=GO) + shadow Persons (UF IS NULL, CPF IS NULL,
# só ``name``).  Formato flat pra simplificar parsing no Python.
_DISCOVERY_QUERY = """
CALL {
    MATCH (n:Senator)
    WHERE coalesce(n.uf, 'GO') = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.senator_id AS stable_key,
           n.id_senado AS id_senado,
           NULL AS id_camara,
           NULL AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           coalesce(n.uf, $target_uf) AS uf,
           [] AS cargo_tse_values,
           [] AS camara_municipios
UNION ALL
    MATCH (n:FederalLegislator)
    WHERE coalesce(n.uf, 'GO') = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.legislator_id AS stable_key,
           NULL AS id_senado,
           n.id_camara AS id_camara,
           n.legislator_id AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           coalesce(n.uf, $target_uf) AS uf,
           [] AS cargo_tse_values,
           [] AS camara_municipios
UNION ALL
    MATCH (n:StateLegislator)
    WHERE coalesce(n.uf, 'GO') = $target_uf
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.legislator_id AS stable_key,
           NULL AS id_senado,
           NULL AS id_camara,
           n.legislator_id AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           coalesce(n.uf, $target_uf) AS uf,
           [] AS cargo_tse_values,
           [] AS camara_municipios
UNION ALL
    MATCH (n:Person)
    WHERE n.uf = $target_uf
    OPTIONAL MATCH (n)-[:MEMBRO_DE]->(cam:CamaraMunicipal)
    WHERE coalesce(cam.uf, $target_uf) = $target_uf
    WITH n, collect(DISTINCT cam.municipio) AS munis
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           coalesce(n.cpf, n.name) AS stable_key,
           NULL AS id_senado,
           NULL AS id_camara,
           NULL AS legislator_id,
           n.sq_candidato AS sq_candidato,
           n.name AS name,
           n.cpf AS cpf,
           n.partido AS partido,
           n.uf AS uf,
           [k IN keys(n) WHERE k STARTS WITH 'cargo_tse_' | n[k]] AS cargo_tse_values,
           [m IN munis WHERE m IS NOT NULL AND m <> ''] AS camara_municipios
UNION ALL
    MATCH (n:Person)
    WHERE n.uf IS NULL AND n.cpf IS NULL AND coalesce(n.name, '') <> ''
    RETURN labels(n) AS labels,
           elementId(n) AS element_id,
           n.name AS stable_key,
           NULL AS id_senado,
           NULL AS id_camara,
           NULL AS legislator_id,
           NULL AS sq_candidato,
           n.name AS name,
           NULL AS cpf,
           NULL AS partido,
           NULL AS uf,
           [] AS cargo_tse_values,
           [] AS camara_municipios
}
RETURN labels, element_id, stable_key, id_senado, id_camara,
       legislator_id, sq_candidato, name, cpf, partido, uf,
       cargo_tse_values, camara_municipios
"""


def _primary_label(labels: list[str]) -> str:
    """Pega a label "mais específica" (menor rank no ``_CARGO_RANK``)."""
    known = [label for label in labels if label in _CARGO_RANK]
    if not known:
        return labels[0] if labels else "Person"
    return min(known, key=lambda lbl: _CARGO_RANK[lbl])


def _display_source_label(canonical: dict[str, Any]) -> str:
    """Label equivalente ao ``cargo_ativo`` atual do canonical node.

    Usado pra decidir se um novo nó-fonte desbanca o display_name
    corrente: Senator bate qualquer cargo; Fed bate State/Person; etc.
    """
    reverse = {v: k for k, v in _CARGO_ATIVO_LABEL.items()}
    cargo = canonical.get("cargo_ativo")
    if cargo and cargo in reverse:
        return reverse[cargo]
    return "Person"


def _canonical_id_for(primary_label: str, node: dict[str, Any]) -> str:
    """Deriva ``canonical_id`` estável a partir do nó âncora do cluster.

    Ordem:
    1. Senator → ``canon_senado_{id_senado}``.
    2. FederalLegislator → ``canon_camara_{id_camara}``.
    3. StateLegislator → ``canon_alego_{digits(legislator_id) or legislator_id}``.
    4. Person com CPF pleno → ``canon_cpf_{digits(cpf)}``.

    Person shadow (só name) nunca vira âncora — é sempre anexado a um
    cluster existente via shadow attach. Se escaparmos aqui, fica no
    audit-log como "shadow sem cluster".
    """
    if primary_label == "Senator" and node.get("id_senado"):
        return f"canon_senado_{node['id_senado']}"
    if primary_label == "FederalLegislator" and node.get("id_camara"):
        return f"canon_camara_{node['id_camara']}"
    if primary_label == "StateLegislator" and node.get("legislator_id"):
        leg_id = str(node["legislator_id"])
        digits = _digits_only(leg_id) or leg_id.replace(" ", "_")
        return f"canon_alego_{digits}"
    if primary_label == "Person":
        cpf_digits = _digits_only(node.get("cpf"))
        if cpf_digits and cpf_digits != "00000000000":
            return f"canon_cpf_{cpf_digits}"
    # Fallback defensivo (nunca deveria ocorrer dado o filtro de
    # ``extract``; se ocorrer, o pipeline levanta no transform pra não
    # criar canonical_id instável).
    raise ValueError(
        f"no stable canonical_id for label={primary_label} node={node}",
    )


class EntityResolutionPoliticosGoPipeline(Pipeline):
    """Liga ``:Senator`` / ``:FederalLegislator`` / ``:StateLegislator`` ↔ ``:Person``.

    Lê o grafo uma vez, aplica regras determinísticas de matching,
    grava ``:CanonicalPerson`` + ``:REPRESENTS``. Sem fetch externo.

    Cadência recomendada: diária ou sempre que um pipeline de cargo
    rodar (esquecer é de baixo risco — os nós-fonte continuam no grafo;
    só a camada canônica fica desatualizada até a próxima run).
    """

    name = "entity_resolution_politicos_go"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        enable_first_last_match: bool = False,
        first_last_audit_only: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        # Fase 5.6 (shadow_first_last_match): casa shadow de 2 tokens com
        # cluster cujo source tem ≥3 tokens com primeiro+último iguais ao
        # shadow. Caso canônico: KARLOS CABRAL ↔ KARLOS MARCIO VIEIRA
        # CABRAL. Default OFF (homonímia 2-token no Brasil é alta —
        # exige spot-check humano antes de promover). ``first_last_audit_only``
        # = só popula audit jsonl, sem gravar :REPRESENTS — usar pra
        # primeira passagem de validação.
        self.enable_first_last_match = enable_first_last_match
        self.first_last_audit_only = first_last_audit_only
        # Nós-fonte lidos do grafo, separados por primary label.
        self._nodes_by_label: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # Canonical clusters finais:
        # canonical_id → {"canonical": {...}, "edges": [rel_row, ...]}
        self._clusters: dict[str, dict[str, Any]] = {}
        self._audit_entries: list[dict[str, Any]] = []
        self.canonical_rows: list[dict[str, Any]] = []
        self.represents_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # extract — lê nós do grafo
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Puxa cargos + Persons GO + shadow Persons do grafo."""
        with self.driver.session(database=self.neo4j_database) as session:
            result = session.run(_DISCOVERY_QUERY, {"target_uf": _TARGET_UF})
            rows = [dict(record) for record in result]

        for row in rows:
            labels = list(row.get("labels") or [])
            primary = _primary_label(labels)
            cargo_tse_values = row.get("cargo_tse_values") or []
            camara_munis_raw = row.get("camara_municipios") or []
            node = {
                "labels": labels,
                "primary_label": primary,
                "element_id": row.get("element_id"),
                "stable_key": row.get("stable_key"),
                "id_senado": row.get("id_senado"),
                "id_camara": row.get("id_camara"),
                "legislator_id": row.get("legislator_id"),
                "sq_candidato": row.get("sq_candidato"),
                "name": row.get("name"),
                "cpf": row.get("cpf"),
                "partido": row.get("partido"),
                "uf": row.get("uf"),
                "name_normalized": _normalize_name(row.get("name")),
                # Set de cargos TSE normalizados (upper, sem acento) de
                # qualquer ano — usado pela fase 4 ``cpf_suffix_cargo`` pra
                # decidir se um Person é candidato TSE do nível de cargo
                # do cluster (Senador / Deputado Federal / Deputado
                # Estadual). Vazio em cargos/shadow (não entram como
                # "candidatos TSE"; são o próprio alvo do attach).
                "cargo_tse_set": frozenset(
                    _normalize_name(v) for v in cargo_tse_values if v
                ),
                # Municípios das CamaraMunicipal GO em que o Person é
                # MEMBRO_DE. Normalizados (upper + sem acento) pra casar
                # com a fase ``name_municipio_vereador``. Vazio fora do
                # branch :Person UF=GO do discovery (cargos federais/
                # estaduais não são membros de Câmara municipal).
                "camara_municipios": tuple(
                    _normalize_name(m) for m in camara_munis_raw if m
                ),
            }
            node["name_stripped"] = _strip_honorifics(
                str(node["name_normalized"] or ""),
            )
            self._nodes_by_label[primary].append(node)

        self.rows_in = sum(len(v) for v in self._nodes_by_label.values())
        logger.info(
            "[%s] extracted: %d senators, %d federal, %d state, %d persons GO, %d shadow",
            self.name,
            len(self._nodes_by_label.get("Senator", [])),
            len(self._nodes_by_label.get("FederalLegislator", [])),
            len(self._nodes_by_label.get("StateLegislator", [])),
            sum(1 for n in self._nodes_by_label.get("Person", []) if n["uf"] == _TARGET_UF),
            sum(1 for n in self._nodes_by_label.get("Person", []) if not n["uf"]),
        )

    # ------------------------------------------------------------------
    # transform — aplica regras de matching e monta clusters
    # ------------------------------------------------------------------

    def transform(self) -> None:
        persons_go = [
            n for n in self._nodes_by_label.get("Person", []) if n["uf"] == _TARGET_UF
        ]
        persons_shadow = [
            n for n in self._nodes_by_label.get("Person", []) if not n["uf"]
        ]

        # Índices pra lookup eficiente.
        persons_by_cpf: dict[str, list[dict[str, Any]]] = defaultdict(list)
        persons_by_name_norm: dict[str, list[dict[str, Any]]] = defaultdict(list)
        persons_by_name_stripped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        name_norm_counts: Counter[str] = Counter()
        for person in persons_go:
            cpf_raw = person["cpf"]
            # Exclui sentinel ``sq:{sq_cand}`` do tse_prestacao_contas 2024
            # e CPFs mascarados (``***.***.*NN-NN``) do índice CPF — não
            # são CPFs reais. Sem isso, ``sq:90002105951`` virava CPF fake
            # ``90002105951`` no índice.
            if _is_sq_sentinel_cpf(cpf_raw) or _is_masked_cpf(cpf_raw):
                cpf_digits = ""
            else:
                cpf_digits = _digits_only(cpf_raw)
            if cpf_digits and cpf_digits != "00000000000":
                persons_by_cpf[cpf_digits].append(person)
            if person["name_normalized"]:
                persons_by_name_norm[person["name_normalized"]].append(person)
                name_norm_counts[person["name_normalized"]] += 1
            if person["name_stripped"]:
                persons_by_name_stripped[person["name_stripped"]].append(person)

        # Rastreia quais Persons já foram anexados a algum cluster.
        person_elt_ids_in_cluster: set[str] = set()

        # ---- Fase 1: cada cargo vira um cluster; tenta anexar Person ----
        for cargo_label in ("Senator", "FederalLegislator", "StateLegislator"):
            for cargo in self._nodes_by_label.get(cargo_label, []):
                self._resolve_cargo(
                    cargo,
                    persons_by_cpf=persons_by_cpf,
                    persons_by_name_norm=persons_by_name_norm,
                    persons_by_name_stripped=persons_by_name_stripped,
                    name_norm_counts=name_norm_counts,
                    claimed=person_elt_ids_in_cluster,
                )

        # ---- Fase 3: cpf_suffix_name attach ----
        # Câmara mascara CPF como ``***.***.*NN-NN``. Persons TSE com CPF
        # pleno e nome estendido ("FLAVIA CARREIRO ALBUQUERQUE MORAIS") não
        # casam pelas 3 regras principais (cpf_exact pula mascarado;
        # name_exact/stripped requerem nome igual ao da Câmara "FLAVIA
        # MORAIS"). Aqui anexamos quando (a) os 4 últimos dígitos batem e
        # (b) todos os tokens contentfuls do nome do cargo aparecem no
        # nome do Person.
        self._attach_cpf_suffix_matches(
            persons_go=persons_go,
            claimed=person_elt_ids_in_cluster,
        )

        # ---- Fase 3.5: cpf_suffix_token_overlap attach ----
        # Pra clusters em que fase 3 falha porque o nome de campanha do
        # parlamentar diverge muito do registro TSE ("ADRIANO DO BALDY"
        # ↔ "ADRIANO ANTONIO AVELAR"; "DR. ZACHARIAS CALIL" ↔ "ZACARIAS
        # CALIL HAMU"). Relaxa o subset estrito pra "≥1 token contentful
        # comum" e adiciona filtro ``cargo_tse_*`` no mesmo nível pra
        # restringir candidatos. Resolve casos onde o sufixo CPF sozinho
        # ambigua entre 2-4 candidatos, mas só um compartilha um token
        # com o nome do parlamentar. Conf 0.88. Múltiplos → audit.
        self._attach_cpf_suffix_token_overlap_matches(
            persons_go=persons_go,
            claimed=person_elt_ids_in_cluster,
        )

        # ---- Fase 4: cpf_suffix_cargo attach ----
        # Fallback mais frouxo pra clusters que ainda não pegaram Person
        # TSE: sufixo CPF + cargo_tse_* do mesmo nível (Senador / Deputado
        # Federal / Deputado Estadual). Sem validar tokens de nome.
        # Conf 0.85, múltiplos → audit. Cobre cargo cujo nome de campanha
        # ("PROFESSOR ALCIDES") difere do registro TSE ("ALCIDES RIBEIRO
        # FILHO") por token honorífico não-padrão.
        self._attach_cpf_suffix_cargo_matches(
            persons_go=persons_go,
            claimed=person_elt_ids_in_cluster,
        )

        # ---- Fase 4.5: name + partido attach pra cargos sem CPF ----
        # Senadores (senado_senadores_foto não publica CPF) e clusters
        # de FederalLegislator onde o CPF não bateu pelas regras
        # cpf_suffix_* caem aqui. name_exact com múltiplos hits no mesmo
        # partido normalmente ia pro audit "name_ambiguous" e ficava
        # órfão; aqui anexamos TODOS os Person GO com (name, partido,
        # uf) identicos ao cargo. Justificativa: homonimia completa
        # (mesmo nome + mesmo partido + mesma UF) entre pessoas reais
        # distintas é virtualmente inexistente em cargos legislativos
        # ativos. O que geramos são duplicatas de registros TSE do
        # mesmo candidato em anos diferentes.
        # Conf 0.78 (mais baixa que name_exact=0.95 porque o trade-off
        # de ambiguidade é mais fraco — ``confidence_min`` no cluster
        # sinaliza pro PWA que o match foi mais permissivo).
        self._attach_name_partido_matches(
            persons_go=persons_go,
            claimed=person_elt_ids_in_cluster,
        )

        # ---- Fase 4.7: name_municipio_vereador ----
        # Resolve duplicatas de vereador GO criadas pelo TSE 2024 (que
        # não publica CPF) ou tse_bens 2024: o mesmo político vira N
        # :Person com mesmo nome e mesma CamaraMunicipal, um com CPF
        # (TSE pré-2024) e outros sem. Esta fase é o único path que cria
        # cluster ancorado em :Person fora de cargo Senator/Fed/State —
        # vereador não tem label de cargo no grafo.
        self._attach_municipal_name_matches(
            persons_go=persons_go,
            claimed=person_elt_ids_in_cluster,
        )

        # ---- Fase 5: shadow attach ----
        # Pra cada shadow, tenta anexar a um cluster existente por nome normalizado.
        cluster_names: dict[str, list[str]] = defaultdict(list)
        # Índice complementar pra fase 5.5 (shadow_prefix_match): chave
        # = prefix de tokens do nome do source (k em [2, len-1]); valor
        # = canonical_ids cujos sources começam com esse prefix.
        # k=len(tokens) é redundante (já coberto por cluster_names).
        cluster_prefixes: dict[str, set[str]] = defaultdict(set)
        for canonical_id, cluster in self._clusters.items():
            for edge in cluster["edges"]:
                src_name = edge.get("_source_name_norm") or ""
                if not src_name:
                    continue
                cluster_names[src_name].append(canonical_id)
                src_tokens = src_name.split()
                if len(src_tokens) < 3:
                    continue
                for k in range(2, len(src_tokens)):
                    prefix = " ".join(src_tokens[:k])
                    cluster_prefixes[prefix].add(canonical_id)

        attached_shadows: set[str] = set()

        for shadow in persons_shadow:
            name_norm = shadow["name_normalized"]
            if not name_norm:
                continue
            candidate_ids = cluster_names.get(name_norm, [])
            # Dedup — mesma canonical pode ter >1 source com nome igual.
            unique_canonicals = sorted(set(candidate_ids))
            if len(unique_canonicals) == 1:
                self._attach_source(
                    canonical_id=unique_canonicals[0],
                    node=shadow,
                    method="shadow_name_exact",
                    confidence=0.80,
                )
                attached_shadows.add(shadow["element_id"])
            elif len(unique_canonicals) > 1:
                self._audit_entries.append({
                    "type": "shadow_ambiguous",
                    "shadow_element_id": shadow["element_id"],
                    "shadow_name": shadow["name"],
                    "candidate_canonicals": unique_canonicals,
                })
                attached_shadows.add(shadow["element_id"])

        # ---- Fase 5.5: shadow_prefix_match ----
        # Shadow cujo nome é prefix exato (sequência de tokens iniciais)
        # do nome de UM source num cluster já resolvido. Conservador:
        # shadow precisa ter ≥2 tokens; prefix precisa bater 1 cluster
        # único. Caso canônico: shadow "JORGE KAJURU" ↔ Senator "JORGE
        # KAJURU REIS DA COSTA NASSER".
        for shadow in persons_shadow:
            if shadow["element_id"] in attached_shadows:
                continue
            name_norm = shadow["name_normalized"]
            if not name_norm:
                continue
            shadow_tokens = name_norm.split()
            if len(shadow_tokens) < 2:
                continue
            unique_canonicals = sorted(cluster_prefixes.get(name_norm, set()))
            if len(unique_canonicals) == 1:
                self._attach_source(
                    canonical_id=unique_canonicals[0],
                    node=shadow,
                    method="shadow_prefix_match",
                    confidence=0.70,
                )
                attached_shadows.add(shadow["element_id"])
            elif len(unique_canonicals) > 1:
                self._audit_entries.append({
                    "type": "shadow_prefix_ambiguous",
                    "shadow_element_id": shadow["element_id"],
                    "shadow_name": shadow["name"],
                    "candidate_canonicals": unique_canonicals,
                })
                attached_shadows.add(shadow["element_id"])

        # ---- Fase 5.6: shadow_first_last_match (opt-in, default OFF) ----
        # Casa shadow de exatamente 2 tokens (apelido + sobrenome) com
        # cluster cujo source tem ≥3 tokens com primeiro==shadow[0] e
        # último==shadow[-1]. Caso canônico: KARLOS CABRAL ↔ KARLOS
        # MARCIO VIEIRA CABRAL. Conf 0.65 (mais baixa que prefix_match
        # porque tokens do meio podem coincidir ou divergir). Audit-only
        # default — homonímia 2-token no BR é alta, primeiro run é só
        # spot-check humano.
        if self.enable_first_last_match:
            cluster_first_last: dict[tuple[str, str], set[str]] = defaultdict(set)
            for canonical_id, cluster in self._clusters.items():
                for edge in cluster["edges"]:
                    src_name = edge.get("_source_name_norm") or ""
                    src_tokens = src_name.split()
                    if len(src_tokens) < 3:
                        continue
                    cluster_first_last[
                        (src_tokens[0], src_tokens[-1])
                    ].add(canonical_id)

            for shadow in persons_shadow:
                if shadow["element_id"] in attached_shadows:
                    continue
                name_norm = shadow["name_normalized"]
                if not name_norm:
                    continue
                shadow_tokens = name_norm.split()
                # Gating: shadow precisa ter EXATAMENTE 2 tokens.
                # 1 token é genérico demais; 3+ já passou por prefix_match.
                if len(shadow_tokens) != 2:
                    continue
                key = (shadow_tokens[0], shadow_tokens[-1])
                unique_canonicals = sorted(cluster_first_last.get(key, set()))
                if len(unique_canonicals) == 1:
                    self._audit_entries.append({
                        "type": (
                            "shadow_first_last_match_audit"
                            if self.first_last_audit_only
                            else "shadow_first_last_match"
                        ),
                        "shadow_element_id": shadow["element_id"],
                        "shadow_name": shadow["name"],
                        "canonical_id": unique_canonicals[0],
                    })
                    if not self.first_last_audit_only:
                        self._attach_source(
                            canonical_id=unique_canonicals[0],
                            node=shadow,
                            method="shadow_first_last_match",
                            confidence=0.65,
                        )
                        attached_shadows.add(shadow["element_id"])
                elif len(unique_canonicals) > 1:
                    self._audit_entries.append({
                        "type": "shadow_first_last_ambiguous",
                        "shadow_element_id": shadow["element_id"],
                        "shadow_name": shadow["name"],
                        "candidate_canonicals": unique_canonicals,
                    })
                    # Mesmo audit-only marca attached pra não cair no
                    # shadow_no_match abaixo (semanticamente já tem hit).
                    attached_shadows.add(shadow["element_id"])

        # Shadows que nenhuma regra cobriu: cai pro audit shadow_no_match.
        # (Só name é pouco pra criar cluster canônico próprio.)
        for shadow in persons_shadow:
            if shadow["element_id"] in attached_shadows:
                continue
            name_norm = shadow["name_normalized"]
            if not name_norm:
                continue
            self._audit_entries.append({
                "type": "shadow_no_match",
                "shadow_element_id": shadow["element_id"],
                "shadow_name": shadow["name"],
            })

        # ---- Fase 6: manual_override (CSV) ----
        # Última camada — afirmações humanas via CSV versionado. Roda
        # depois de todas as regras automáticas pra que o operador
        # possa: (a) agregar match novo a cluster órfão; (b) ver no
        # audit se a override colide com regra automática (mesmo
        # cluster = no-op; outro cluster = audit conflict).
        self._apply_manual_overrides(claimed=person_elt_ids_in_cluster)

        # ---- Finaliza: materializa rows pra Neo4jBatchLoader ----
        for cluster in self._clusters.values():
            self.canonical_rows.append(cluster["canonical"])
            self.represents_rels.extend(cluster["edges"])

        # Drop campos de trabalho que não vão pro grafo.
        for edge in self.represents_rels:
            edge.pop("_source_name_norm", None)

        self.rows_loaded = len(self.canonical_rows) + len(self.represents_rels)
        logger.info(
            "[%s] transformed: %d canonical clusters, %d REPRESENTS edges, %d audit entries",
            self.name,
            len(self.canonical_rows),
            len(self.represents_rels),
            len(self._audit_entries),
        )

    def _resolve_cargo(
        self,
        cargo: dict[str, Any],
        *,
        persons_by_cpf: dict[str, list[dict[str, Any]]],
        persons_by_name_norm: dict[str, list[dict[str, Any]]],
        persons_by_name_stripped: dict[str, list[dict[str, Any]]],
        name_norm_counts: Counter[str],
        claimed: set[str],
    ) -> None:
        """Cria cluster pro cargo e anexa Person(GO) se match conservador existir."""
        primary_label = cargo["primary_label"]
        try:
            canonical_id = _canonical_id_for(primary_label, cargo)
        except ValueError as exc:
            # Cargo sem stable key — carga parcial do pipeline-fonte. Skip
            # e registra no audit pra o operador olhar.
            self._audit_entries.append({
                "type": "cargo_no_stable_key",
                "element_id": cargo["element_id"],
                "label": primary_label,
                "reason": str(exc),
            })
            return

        # Cluster vazio inicial (canonical + 1 edge pro cargo).
        canonical = self._build_canonical_row(canonical_id, cargo)
        # ``cargo`` guardado pra fase 3 (``_attach_cpf_suffix_matches``)
        # que precisa do CPF mascarado e do nome normalizado do cargo
        # pra casar com ``:Person`` GO pelos últimos 4 dígitos + tokens.
        self._clusters[canonical_id] = {
            "canonical": canonical,
            "edges": [],
            "cargo": cargo,
        }
        self._attach_source(
            canonical_id=canonical_id,
            node=cargo,
            method="cargo_root",
            confidence=1.00,
        )

        # Tenta anexar Person via cpf_exact / name_exact / name_stripped.
        matched_person: dict[str, Any] | None = None
        method: str | None = None
        confidence: float | None = None

        cargo_cpf_digits = (
            "" if _is_masked_cpf(cargo.get("cpf"))
            else _digits_only(cargo.get("cpf"))
        )
        if cargo_cpf_digits and cargo_cpf_digits != "00000000000":
            hits = [
                p for p in persons_by_cpf.get(cargo_cpf_digits, [])
                if p["element_id"] not in claimed
            ]
            if len(hits) == 1:
                matched_person, method, confidence = hits[0], "cpf_exact", 1.00
            elif len(hits) > 1:
                self._audit_entries.append({
                    "type": "cargo_cpf_ambiguous",
                    "cargo_element_id": cargo["element_id"],
                    "cargo_label": primary_label,
                    "cargo_name": cargo["name"],
                    "cpf_digits": cargo_cpf_digits,
                    "person_candidates": [p["element_id"] for p in hits],
                })

        if matched_person is None:
            name_norm = cargo["name_normalized"]
            if name_norm:
                hits = [
                    p for p in persons_by_name_norm.get(name_norm, [])
                    if p["element_id"] not in claimed
                ]
                if len(hits) == 1:
                    matched_person, method, confidence = hits[0], "name_exact", 0.95
                elif len(hits) > 1:
                    disambiguated = self._disambiguate_by_partido(cargo, hits)
                    if disambiguated is not None:
                        matched_person = disambiguated
                        method, confidence = "name_exact_partido", 0.90
                    else:
                        self._audit_entries.append({
                            "type": "cargo_name_ambiguous",
                            "cargo_element_id": cargo["element_id"],
                            "cargo_label": primary_label,
                            "cargo_name": cargo["name"],
                            "candidates": [p["element_id"] for p in hits],
                        })

        if matched_person is None:
            stripped = cargo["name_stripped"]
            if stripped and stripped != cargo["name_normalized"]:
                # Matching cruzado: stripped(cargo) vs stripped(person).
                hits = [
                    p for p in persons_by_name_stripped.get(stripped, [])
                    if p["element_id"] not in claimed
                ]
                if len(hits) == 1:
                    matched_person, method, confidence = hits[0], "name_stripped", 0.85
                elif len(hits) > 1:
                    self._audit_entries.append({
                        "type": "cargo_stripped_ambiguous",
                        "cargo_element_id": cargo["element_id"],
                        "cargo_label": primary_label,
                        "cargo_name": cargo["name"],
                        "cargo_stripped": stripped,
                        "candidates": [p["element_id"] for p in hits],
                    })

        if matched_person is None:
            # Cargo sem Person pareável — cluster fica com 1 só source
            # (ainda útil: enfileira foto, partido atual, etc.). Loga
            # pra auditoria saber que não achamos histórico TSE.
            self._audit_entries.append({
                "type": "cargo_without_person",
                "cargo_element_id": cargo["element_id"],
                "cargo_label": primary_label,
                "cargo_name": cargo["name"],
            })
            return

        assert method is not None and confidence is not None  # noqa: S101
        self._attach_source(
            canonical_id=canonical_id,
            node=matched_person,
            method=method,
            confidence=confidence,
        )
        claimed.add(matched_person["element_id"])

    def _attach_cpf_suffix_matches(
        self,
        persons_go: list[dict[str, Any]],
        claimed: set[str],
    ) -> None:
        """Anexa Person GO ao cluster do cargo via CPF-suffix + tokens de nome.

        Itera clusters cujo cargo âncora tem CPF mascarado e procura
        ``:Person`` GO com CPF pleno terminando nos mesmos 4 dígitos
        **e** cujo nome contém todos os tokens contentfuls do nome do
        cargo. Match único → attach (conf 0.92); múltiplos → audit.
        Persons já atribuídos a outro cluster (``claimed``) ou ao mesmo
        (via outras regras) são ignorados.
        """
        persons_by_suffix: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for person in persons_go:
            cpf_raw = person.get("cpf")
            if _is_sq_sentinel_cpf(cpf_raw) or _is_masked_cpf(cpf_raw):
                continue
            cpf_digits = _digits_only(cpf_raw)
            if len(cpf_digits) != 11 or cpf_digits == "00000000000":
                continue
            persons_by_suffix[cpf_digits[-4:]].append(person)

        for cluster in self._clusters.values():
            cargo = cluster.get("cargo")
            if not cargo:
                continue
            cargo_cpf = cargo.get("cpf")
            if not _is_masked_cpf(cargo_cpf):
                continue
            suffix = _visible_cpf_suffix(cargo_cpf)
            if not suffix:
                continue
            cargo_name_norm = cargo.get("name_normalized") or ""
            canonical_id = cluster["canonical"]["canonical_id"]
            already_in_cluster = {e["target_element_id"] for e in cluster["edges"]}

            candidates = [
                p for p in persons_by_suffix.get(suffix, [])
                if p["element_id"] not in claimed
                and p["element_id"] not in already_in_cluster
                and _cargo_tokens_subset_of_person(
                    cargo_name_norm,
                    p["name_normalized"] or "",
                )
            ]
            if len(candidates) == 1:
                self._attach_source(
                    canonical_id=canonical_id,
                    node=candidates[0],
                    method="cpf_suffix_name",
                    confidence=0.92,
                )
                claimed.add(candidates[0]["element_id"])
            elif len(candidates) > 1:
                self._audit_entries.append({
                    "type": "cpf_suffix_ambiguous",
                    "cargo_element_id": cargo["element_id"],
                    "cargo_label": cargo["primary_label"],
                    "cargo_name": cargo["name"],
                    "cpf_suffix": suffix,
                    "candidates": [p["element_id"] for p in candidates],
                })

    def _attach_cpf_suffix_token_overlap_matches(
        self,
        persons_go: list[dict[str, Any]],
        claimed: set[str],
    ) -> None:
        """Anexa Person GO via CPF-suffix + ≥1 token comum + cargo_tse.

        Fase intermediária entre fase 3 (``cpf_suffix_name``, requer
        todos os tokens do cargo no Person) e fase 4 (``cpf_suffix_cargo``,
        sem nome). Casa quando os 4 últimos dígitos do CPF batem, o
        ``cargo_tse_{YYYY}`` do Person está no mesmo nível do label do
        cargo (Senador/Deputado Federal/Estadual) e cargo+Person
        compartilham ≥1 token contentful.

        Cobre casos onde o nome de campanha foi reescrito (e.g.
        "ADRIANO DO BALDY" ↔ "ADRIANO ANTONIO AVELAR", "DANIEL AGROBOM"
        ↔ "DANIEL VIEIRA RAMOS", "DR. ZACHARIAS CALIL" ↔ "ZACARIAS
        CALIL HAMU") onde o sufixo CPF sozinho casa com 2-4 candidatos
        do mesmo cargo TSE, mas só um deles compartilha um token com o
        parlamentar. Match único → attach (conf 0.88); múltiplos →
        audit ``cpf_suffix_token_overlap_ambiguous`` + skip.
        """
        persons_by_suffix: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for person in persons_go:
            cpf_raw = person.get("cpf")
            if _is_sq_sentinel_cpf(cpf_raw) or _is_masked_cpf(cpf_raw):
                continue
            cpf_digits = _digits_only(cpf_raw)
            if len(cpf_digits) != 11 or cpf_digits == "00000000000":
                continue
            if not person.get("cargo_tse_set"):
                continue
            persons_by_suffix[cpf_digits[-4:]].append(person)

        for cluster in self._clusters.values():
            cargo = cluster.get("cargo")
            if not cargo:
                continue
            expected_cargo_tse = _CARGO_LABEL_TO_TSE.get(cargo["primary_label"])
            if not expected_cargo_tse:
                continue
            cargo_cpf = cargo.get("cpf")
            if not _is_masked_cpf(cargo_cpf):
                continue
            suffix = _visible_cpf_suffix(cargo_cpf)
            if not suffix:
                continue
            cargo_name_norm = cargo.get("name_normalized") or ""
            if not _contentful_tokens(cargo_name_norm):
                continue
            canonical_id = cluster["canonical"]["canonical_id"]
            already_in_cluster = {e["target_element_id"] for e in cluster["edges"]}

            candidates = [
                p for p in persons_by_suffix.get(suffix, [])
                if p["element_id"] not in claimed
                and p["element_id"] not in already_in_cluster
                and expected_cargo_tse in p["cargo_tse_set"]
                and _cargo_person_share_token(
                    cargo_name_norm,
                    p["name_normalized"] or "",
                )
            ]
            if len(candidates) == 1:
                self._attach_source(
                    canonical_id=canonical_id,
                    node=candidates[0],
                    method="cpf_suffix_token_overlap",
                    confidence=0.88,
                )
                claimed.add(candidates[0]["element_id"])
            elif len(candidates) > 1:
                self._audit_entries.append({
                    "type": "cpf_suffix_token_overlap_ambiguous",
                    "cargo_element_id": cargo["element_id"],
                    "cargo_label": cargo["primary_label"],
                    "cargo_name": cargo["name"],
                    "cpf_suffix": suffix,
                    "expected_cargo_tse": expected_cargo_tse,
                    "candidates": [
                        {"element_id": p["element_id"], "name": p["name"]}
                        for p in candidates
                    ],
                })

    def _attach_cpf_suffix_cargo_matches(
        self,
        persons_go: list[dict[str, Any]],
        claimed: set[str],
    ) -> None:
        """Anexa Person GO ao cluster via CPF-suffix + cargo_tse (sem nome).

        Fallback mais fraco do que ``_attach_cpf_suffix_matches``: em vez
        de exigir que todos os tokens do nome do cargo apareçam no
        Person, exige só que o cargo_tse_* do Person bata com o nível
        do cargo âncora (``Deputado Federal`` ↔ ``:FederalLegislator``,
        etc.). Cobre nomes de campanha com honoríficos não-padrão
        ("PROFESSOR") ou grafia divergente (ZH ↔ Z). Match único →
        attach (conf 0.85); múltiplos → audit.

        Salvaguarda crítica: só roda em clusters que **ainda não têm
        Person com CPF pleno** anexado pelas regras 1-3 ou fase 3. Se
        já existe Person CPF-bearing no cluster (via ``cpf_exact`` ou
        ``cpf_suffix_name``), a pessoa real já foi identificada via
        evidência forte e qualquer candidato extra com o mesmo sufixo
        de CPF é provavelmente homônimo que só colide nos 4 dígitos.
        Regressão real observada 2026-04-23: RUBENS OTONI (suffix 7149)
        já pegou RUBENS OTONI GOMIDE via ``cpf_suffix_name``; sem esta
        trava, a fase 4 também anexava GLEICY MARIA BARBOSA DOS SANTOS
        GUERRA (mesmo suffix, outro nome) como "único candidato
        não-claimed". Clusters que só têm Person sem CPF (via
        ``name_exact`` de shadow legítimo, ex.: "PROFESSOR ALCIDES"
        sem CPF batendo com o nome do cargo) continuam elegíveis — é
        justamente ali que fase 4 agrega valor.
        """
        persons_with_full_cpf: set[str] = set()
        persons_by_suffix: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for person in persons_go:
            cpf_raw = person.get("cpf")
            if _is_sq_sentinel_cpf(cpf_raw) or _is_masked_cpf(cpf_raw):
                continue
            cpf_digits = _digits_only(cpf_raw)
            if len(cpf_digits) != 11 or cpf_digits == "00000000000":
                continue
            persons_with_full_cpf.add(person["element_id"])
            if person.get("cargo_tse_set"):
                persons_by_suffix[cpf_digits[-4:]].append(person)

        for cluster in self._clusters.values():
            cargo = cluster.get("cargo")
            if not cargo:
                continue
            expected_cargo_tse = _CARGO_LABEL_TO_TSE.get(cargo["primary_label"])
            if not expected_cargo_tse:
                continue
            cargo_cpf = cargo.get("cpf")
            if not _is_masked_cpf(cargo_cpf):
                continue
            suffix = _visible_cpf_suffix(cargo_cpf)
            if not suffix:
                continue
            # Trava anti-falso-positivo: se o cluster já tem Person com
            # CPF pleno anexado (via cpf_exact, cpf_suffix_name ou
            # name_exact casado com Person que tinha CPF), a pessoa
            # real já foi identificada — fase 4 não agrega e só
            # arrisca colisão de sufixo.
            has_cpf_person_attached = any(
                e.get("target_element_id") in persons_with_full_cpf
                for e in cluster["edges"]
            )
            if has_cpf_person_attached:
                continue
            canonical_id = cluster["canonical"]["canonical_id"]
            already_in_cluster = {e["target_element_id"] for e in cluster["edges"]}

            candidates = [
                p for p in persons_by_suffix.get(suffix, [])
                if p["element_id"] not in claimed
                and p["element_id"] not in already_in_cluster
                and expected_cargo_tse in p["cargo_tse_set"]
            ]
            if len(candidates) == 1:
                self._attach_source(
                    canonical_id=canonical_id,
                    node=candidates[0],
                    method="cpf_suffix_cargo",
                    confidence=0.85,
                )
                claimed.add(candidates[0]["element_id"])
            elif len(candidates) > 1:
                self._audit_entries.append({
                    "type": "cpf_suffix_cargo_ambiguous",
                    "cargo_element_id": cargo["element_id"],
                    "cargo_label": cargo["primary_label"],
                    "cargo_name": cargo["name"],
                    "cpf_suffix": suffix,
                    "expected_cargo_tse": expected_cargo_tse,
                    "candidates": [
                        {"element_id": p["element_id"], "name": p["name"]}
                        for p in candidates
                    ],
                })

    def _attach_name_partido_matches(
        self,
        persons_go: list[dict[str, Any]],
        claimed: set[str],
    ) -> None:
        """Anexa Person GO ao cluster via (name + partido + uf) — cargo sem CPF.

        Cenário-alvo: cargos sem CPF publicado (Senadores, porque o
        pipeline ``senado_senadores_foto`` só traz id_senado + partido;
        StateLegislator quando a fonte ALEGO não tem partido) onde
        ``name_exact`` retorna múltiplos Persons e
        ``_disambiguate_by_partido`` devolve None (todos com mesmo
        partido ou partido do cargo ausente).

        Regra: se o cargo tem ``partido`` e existe ≥1 Person GO com
        (name_normalized, partido, uf) idênticos que ainda não está em
        nenhum cluster, anexa TODOS. Mesma pessoa real, múltiplas
        inscrições TSE ao longo dos anos (receitas, bens, candidato)
        geram N Person nodes no grafo com o mesmo conjunto de atributos.

        Salvaguarda: só roda em clusters cujo cargo tem CPF ausente
        (None, '', ou sentinel ``sq:``) — se o cargo tem CPF, as
        regras cpf_* já deveriam ter convergido.

        Confidence 0.78 — intencionalmente mais baixa que name_exact
        (0.95) porque homonimia completa em mesma UF + mesmo partido é
        rara mas teoricamente possível; ``confidence_min`` do cluster
        decresce e o PWA pode sinalizar "match permissivo" no futuro.
        """
        # Indexa Persons por (name_normalized, partido_upper, uf).
        persons_by_name_partido: dict[
            tuple[str, str, str], list[dict[str, Any]],
        ] = defaultdict(list)
        for person in persons_go:
            name_norm = person.get("name_normalized")
            partido_raw = person.get("partido")
            uf_raw = person.get("uf")
            if not name_norm or not partido_raw or not uf_raw:
                continue
            partido_upper = str(partido_raw).strip().upper()
            uf_upper = str(uf_raw).strip().upper()
            if not partido_upper or not uf_upper:
                continue
            persons_by_name_partido[
                (name_norm, partido_upper, uf_upper)
            ].append(person)

        for cluster in self._clusters.values():
            cargo = cluster.get("cargo")
            if not cargo:
                continue
            cargo_cpf = cargo.get("cpf")
            # Pula cargo com CPF pleno ou mascarado — as regras
            # cpf_exact/cpf_suffix_* já tiveram chance. Prossegue só
            # quando CPF é literalmente ausente/vazio ou sentinel sq.
            if cargo_cpf and not _is_sq_sentinel_cpf(cargo_cpf):
                # Inclui mascarado ``***.***.*NN-NN`` no skip porque fase 3/4
                # já cobrem esse formato.
                cpf_digits = _digits_only(cargo_cpf)
                if len(cpf_digits) >= 4 or _is_masked_cpf(cargo_cpf):
                    continue
            cargo_name = cargo.get("name_normalized") or ""
            cargo_partido = str(cargo.get("partido") or "").strip().upper()
            cargo_uf = str(cargo.get("uf") or "").strip().upper()
            if not cargo_name or not cargo_partido or not cargo_uf:
                continue
            canonical_id = cluster["canonical"]["canonical_id"]
            already_in_cluster = {
                e["target_element_id"] for e in cluster["edges"]
            }

            candidates = [
                p for p in persons_by_name_partido.get(
                    (cargo_name, cargo_partido, cargo_uf), [],
                )
                if p["element_id"] not in claimed
                and p["element_id"] not in already_in_cluster
            ]
            if not candidates:
                continue
            for person in candidates:
                self._attach_source(
                    canonical_id=canonical_id,
                    node=person,
                    method="name_partido_multi",
                    confidence=0.78,
                )
                claimed.add(person["element_id"])

    def _attach_municipal_name_matches(
        self,
        persons_go: list[dict[str, Any]],
        claimed: set[str],
    ) -> None:
        """Anexa duplicatas de vereador GO via (name + municipio + uf=GO).

        Cenário-alvo: vereadores GO duplicados — o TSE 2024 não publica
        CPF (memória 2026-04-23), e ``tse_bens`` 2024 cria ``:Person``
        sem CPF. Quando a mesma pessoa já existia com CPF (criada por
        TSE pré-2024 ou por outras pipelines), surge N :Person com mesmo
        ``name_normalized`` e mesma CamaraMunicipal — todos com
        ``MEMBRO_DE :CamaraMunicipal {uf:'GO', municipio:M}``. Caso
        canônico real medido em 2026-04-27: "ROMARIO BARBOSA POLICARPO"
        GOIANIA — id 8071 com CPF (TSE 2020+2022 + enriquecimento 2024)
        e id 501376 sem CPF (tse_bens 2024).

        Algoritmo:

        1. Indexa Persons GO por ``(name_normalized, municipio)`` separando
           por CPF pleno (LHS, âncora) vs sem-CPF/mascarado/sentinel
           (RHS, shadow vereador).
        2. Pra cada chave com ≥1 RHS:
           - Se LHS tem 1 elemento: cria/reusa cluster ancorado no LHS
             (``canon_cpf_{digits}``), anexa cada RHS via REPRESENTS conf
             0.90 (method ``name_municipio_vereador``). Se LHS já estava
             num cluster (Federal/Estadual via ``cpf_exact``, ex.: vereador
             que também concorreu a deputado federal), reusa esse cluster.
           - Se LHS tem >1 elemento (homônimos com CPF na mesma Câmara):
             audit ``municipal_lhs_ambiguous`` + skip — pai+filho com
             mesmo nome sentando juntos é raro mas existe.
           - Se LHS=0 (RHS órfão sem âncora com CPF): pula silenciosamente
             (já é coberto pelos audits agregados ``shadow_*`` ou fica
             como duplicata visível — fora do escopo desta fase).
        3. RHS já em ``claimed`` é skipped (idempotência inter-fase).

        Único path do pipeline que cria cluster ancorado em :Person fora
        de cargos Senator/Fed/State, porque vereador não tem label de
        cargo no grafo (sem ``:Vereador``; só ``:MEMBRO_DE :CamaraMunicipal``).
        Conf 0.90 — entre cpf_suffix_name (0.92) e cpf_suffix_token_overlap
        (0.88). Justificativa: nome exato + município é evidência forte
        (município é discriminador local), mas RHS sem CPF impede
        confirmação por documento.
        """
        # Indexa por (name_normalized, municipio) — restrito a Persons
        # com pelo menos 1 CamaraMunicipal GO. Persons GO sem MEMBRO_DE
        # CamaraMunicipal não entram (não são vereadores).
        lhs_index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        rhs_index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for person in persons_go:
            name_norm = person.get("name_normalized")
            munis = person.get("camara_municipios") or ()
            if not name_norm or not munis:
                continue
            cpf_raw = person.get("cpf")
            # Sentinel ``sq:{...}`` e mascarado ``***.***.*NN-NN`` não
            # contam como CPF pleno pra ancorar identidade — vão pro RHS.
            if _is_sq_sentinel_cpf(cpf_raw) or _is_masked_cpf(cpf_raw):
                cpf_digits = ""
            else:
                cpf_digits = _digits_only(cpf_raw)
            has_full_cpf = (
                len(cpf_digits) == 11 and cpf_digits != "00000000000"
            )
            for muni in munis:
                key = (name_norm, muni)
                if has_full_cpf:
                    lhs_index[key].append(person)
                else:
                    rhs_index[key].append(person)

        for key, rhs_list in rhs_index.items():
            name_norm, muni = key
            lhs_list = lhs_index.get(key, [])
            if not lhs_list:
                # RHS órfão sem âncora — fora do escopo: nem todos os
                # vereadores 2024 têm contraparte com CPF. Sem audit
                # próprio (ruidoso e a contagem agregada já indica via
                # rows_in vs num_sources).
                continue
            if len(lhs_list) > 1:
                self._audit_entries.append({
                    "type": "municipal_lhs_ambiguous",
                    "name_normalized": name_norm,
                    "municipio": muni,
                    "lhs_candidates": [p["element_id"] for p in lhs_list],
                    "rhs_count": len(rhs_list),
                })
                continue
            lhs = lhs_list[0]
            # Se LHS já está em algum cluster, reusa. Caso contrário,
            # cria cluster ancorado no LHS via canon_cpf_{digits}.
            existing_cid = self._find_canonical_for_element(lhs["element_id"])
            if existing_cid is not None:
                canonical_id = existing_cid
            else:
                try:
                    canonical_id = _canonical_id_for(
                        lhs["primary_label"], lhs,
                    )
                except ValueError:
                    # Sem CPF pleno → não dá pra derivar canon_cpf_*.
                    # Defensivo: a checagem ``has_full_cpf`` acima já
                    # filtra esse caso.
                    continue
                if canonical_id not in self._clusters:
                    canonical = self._build_canonical_row(canonical_id, lhs)
                    self._clusters[canonical_id] = {
                        "canonical": canonical,
                        "edges": [],
                        # Sem cargo âncora — fases cpf_suffix_* iteram
                        # ``cluster["cargo"]`` e devem pular este cluster.
                        "cargo": None,
                    }
                    self._attach_source(
                        canonical_id=canonical_id,
                        node=lhs,
                        method="cargo_root",
                        confidence=1.00,
                    )
                    claimed.add(lhs["element_id"])

            already_in_cluster = {
                e["target_element_id"]
                for e in self._clusters[canonical_id]["edges"]
            }
            for rhs in rhs_list:
                if rhs["element_id"] in claimed:
                    continue
                if rhs["element_id"] in already_in_cluster:
                    continue
                self._attach_source(
                    canonical_id=canonical_id,
                    node=rhs,
                    method="name_municipio_vereador",
                    confidence=0.90,
                )
                claimed.add(rhs["element_id"])

    def _find_canonical_for_element(self, element_id: str) -> str | None:
        """Retorna ``canonical_id`` que contém ``element_id`` ou None.

        Lookup linear nos clusters — usado pela fase
        ``name_municipio_vereador`` pra reusar cluster federal/estadual
        existente quando o LHS-Person-com-CPF já foi anexado por
        ``cpf_exact``. Custo O(N_clusters * avg_edges); N_clusters em GO
        é dezenas/centenas, ok pro single-shot do pipeline.
        """
        for cid, cluster in self._clusters.items():
            if any(
                e.get("target_element_id") == element_id
                for e in cluster["edges"]
            ):
                return cid
        return None

    def _disambiguate_by_partido(
        self,
        cargo: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Reduz candidates por partido do cargo; retorna único vencedor ou None."""
        cargo_partido = str(cargo.get("partido") or "").strip().upper()
        if not cargo_partido:
            return None
        matching = [
            p for p in candidates
            if str(p.get("partido") or "").strip().upper() == cargo_partido
        ]
        if len(matching) == 1:
            return matching[0]
        return None

    def _build_canonical_row(
        self,
        canonical_id: str,
        cargo: dict[str, Any],
    ) -> dict[str, Any]:
        """Monta o row do ``:CanonicalPerson`` a partir do cargo âncora."""
        primary_label = cargo["primary_label"]
        cargo_ativo = _CARGO_ATIVO_LABEL.get(primary_label)
        record_url = self._get_primary_url()
        return self.attach_provenance(
            {
                "canonical_id": canonical_id,
                "display_name": str(cargo.get("name") or ""),
                "uf": _TARGET_UF,
                "partido": (
                    str(cargo.get("partido")) if cargo.get("partido") else None
                ),
                "cargo_ativo": cargo_ativo,
                "num_sources": 0,  # atualizado em _attach_source
                "confidence_min": 1.0,
            },
            record_id=canonical_id,
            record_url=record_url,
        )

    def _find_node_by_kind(
        self,
        target_kind: str,
        target_key: str,
    ) -> dict[str, Any] | None:
        """Procura nó-fonte por chave estável (sq_candidato, id_camara, etc.).

        ``cpf`` é normalizado pra dígitos (aceita formato pleno
        ``"547.795.371-34"`` ou cru ``"54779537134"``); os outros são
        comparados como string crua. Retorna o primeiro match — assume
        que chaves estáveis são únicas no escopo do pipeline. Se houver
        ambiguidade, o caller fica com o primeiro e o operador deve
        usar uma chave mais específica.
        """
        target_key_normalized = target_key.strip()
        if target_kind == "cpf":
            target_key_normalized = _digits_only(target_key)
            if not target_key_normalized:
                return None
        for nodes in self._nodes_by_label.values():
            for node in nodes:
                value = node.get(target_kind)
                if value is None:
                    continue
                value_str = str(value)
                if target_kind == "cpf":
                    value_str = _digits_only(value_str)
                if value_str == target_key_normalized:
                    return node
        return None

    def _apply_manual_overrides(self, *, claimed: set[str]) -> None:
        """Aplica overrides do CSV humano em ``docs/entity_resolution_overrides.csv``.

        CSV ausente → no-op. Cada linha vira um attach manual_override
        ou um audit (motivo: no_cluster, no_target, conflict_other_cluster,
        invalid_target_kind, invalid_confidence). Idempotente quando o
        target já está no cluster apontado (skip silencioso).
        """
        path = _resolve_overrides_path()
        try:
            overrides = _load_overrides_csv(path)
        except OSError as exc:
            self._audit_entries.append({
                "type": "override_load_failed",
                "path": str(path),
                "error": str(exc),
            })
            return
        if not overrides:
            return

        for row in overrides:
            canonical_id = row["canonical_id"]
            target_kind = row["target_kind"]
            target_key = row["target_key"]

            if target_kind not in _OVERRIDE_TARGET_KINDS:
                self._audit_entries.append({
                    "type": "override_skipped",
                    "reason": "invalid_target_kind",
                    "canonical_id": canonical_id,
                    "target_kind": target_kind,
                    "target_key": target_key,
                })
                continue

            cluster = self._clusters.get(canonical_id)
            if cluster is None:
                self._audit_entries.append({
                    "type": "override_skipped",
                    "reason": "no_cluster",
                    "canonical_id": canonical_id,
                    "target_kind": target_kind,
                    "target_key": target_key,
                })
                continue

            target_node = self._find_node_by_kind(target_kind, target_key)
            if target_node is None:
                self._audit_entries.append({
                    "type": "override_skipped",
                    "reason": "no_target",
                    "canonical_id": canonical_id,
                    "target_kind": target_kind,
                    "target_key": target_key,
                })
                continue

            target_eid = target_node["element_id"]
            already_in_cluster = any(
                e.get("target_element_id") == target_eid
                for e in cluster["edges"]
            )
            if already_in_cluster:
                # Idempotente — afirmação humana confirma o que regra
                # automática já fez. Nada pra fazer.
                continue

            if target_eid in claimed:
                # Em outro cluster — conflito requer review humana.
                other_canonical = next(
                    (
                        cid for cid, cl in self._clusters.items()
                        if any(
                            e.get("target_element_id") == target_eid
                            for e in cl["edges"]
                        )
                    ),
                    None,
                )
                self._audit_entries.append({
                    "type": "override_skipped",
                    "reason": "conflict_other_cluster",
                    "canonical_id": canonical_id,
                    "other_canonical_id": other_canonical,
                    "target_kind": target_kind,
                    "target_key": target_key,
                })
                continue

            confidence_raw = row.get("confidence") or "1.0"
            try:
                confidence = float(confidence_raw)
            except ValueError:
                self._audit_entries.append({
                    "type": "override_skipped",
                    "reason": "invalid_confidence",
                    "canonical_id": canonical_id,
                    "target_kind": target_kind,
                    "target_key": target_key,
                    "confidence": confidence_raw,
                })
                continue

            self._attach_source(
                canonical_id=canonical_id,
                node=target_node,
                method="manual_override",
                confidence=confidence,
            )
            claimed.add(target_eid)
            self._audit_entries.append({
                "type": "override_applied",
                "canonical_id": canonical_id,
                "target_kind": target_kind,
                "target_key": target_key,
                "target_element_id": target_eid,
                "confidence": confidence,
                "added_by": row.get("added_by") or "",
                "added_at": row.get("added_at") or "",
            })

    def _attach_source(
        self,
        *,
        canonical_id: str,
        node: dict[str, Any],
        method: str,
        confidence: float,
    ) -> None:
        """Anexa um nó-fonte ao cluster via REPRESENTS + atualiza canonical."""
        cluster = self._clusters.get(canonical_id)
        if cluster is None:
            raise KeyError(f"cluster {canonical_id} não existe — chamar _resolve_cargo antes")

        element_id = node["element_id"]
        target_label = node["primary_label"]
        source_name_norm = node["name_normalized"]
        record_url = self._get_primary_url()

        record_id = f"{canonical_id}|{element_id}"
        edge = self.attach_provenance(
            {
                "source_key": canonical_id,  # canonical lado A
                # target_key só preservado pro enforce_provenance do
                # loader; o Cypher custom usa target_element_id.
                "target_key": element_id,
                "target_label": target_label,
                "target_element_id": element_id,
                "method": method,
                "confidence": float(confidence),
                "_source_name_norm": source_name_norm,  # só p/ fase 2
            },
            record_id=record_id,
            record_url=record_url,
        )
        cluster["edges"].append(edge)

        # Atualiza props agregadas do canonical.
        canonical = cluster["canonical"]
        canonical["num_sources"] = len(cluster["edges"])
        # min() ignora o campo inicial 1.0 do cargo root.
        canonical["confidence_min"] = min(
            canonical.get("confidence_min", 1.0), float(confidence),
        )
        # Display name: escolhe o da label mais oficial que entrou.
        if _CARGO_RANK.get(target_label, 99) < _CARGO_RANK.get(
            _display_source_label(canonical), 99,
        ):
            canonical["display_name"] = str(node.get("name") or canonical.get("display_name"))
            if node.get("partido"):
                canonical["partido"] = str(node["partido"])

    # ------------------------------------------------------------------
    # load — persiste no grafo + grava audit log
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.canonical_rows:
            logger.warning("[%s] nothing to load", self.name)
            self._write_audit_log()
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes(
            "CanonicalPerson",
            self.canonical_rows,
            key_field="canonical_id",
        )
        if self.represents_rels:
            # Target é dinâmico (Senator/Fed/State/Person) e
            # ``:Person`` não tem chave de propriedade estável universal
            # — usar elementId é o único caminho uniforme. O loader
            # genérico só aceita ``{prop: v}`` no MATCH, então montamos
            # o Cypher aqui direto.
            loader.run_query_with_retry(
                _REPRESENTS_MERGE_QUERY,
                self.represents_rels,
            )
        self._write_audit_log()

    def _write_audit_log(self) -> None:
        """Grava ``data/entity_resolution_politicos_go/audit_{run_id}.jsonl``."""
        audit_dir = Path(self.data_dir) / _SOURCE_ID
        audit_dir.mkdir(parents=True, exist_ok=True)
        path = audit_dir / f"audit_{self.run_id}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for entry in self._audit_entries:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info(
            "[%s] wrote %d audit entries to %s",
            self.name, len(self._audit_entries), path,
        )


# Cypher que cria/atualiza ``(:CanonicalPerson)-[:REPRESENTS]->(source)``
# em lote. Match do source é por ``elementId`` porque é a única chave
# uniformemente presente (:Person não tem senator_id nem legislator_id
# e CPF pode estar ausente/mascarado). O loader genérico
# ``load_relationships`` só suporta ``{prop: v}`` no MATCH, então este
# query fica inline.
_REPRESENTS_MERGE_QUERY = """
UNWIND $rows AS row
MATCH (cp:CanonicalPerson {canonical_id: row.source_key})
MATCH (src) WHERE elementId(src) = row.target_element_id
MERGE (cp)-[r:REPRESENTS]->(src)
SET r.method = row.method,
    r.confidence = row.confidence,
    r.source_id = row.source_id,
    r.source_record_id = row.source_record_id,
    r.source_url = row.source_url,
    r.ingested_at = row.ingested_at,
    r.run_id = row.run_id,
    r.target_label = row.target_label
"""
