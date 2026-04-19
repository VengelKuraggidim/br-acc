# Entity resolution — layer `:CanonicalPerson`

Documentação da estratégia de consolidação de nós "mesma pessoa real" no
grafo Neo4j do Fiscal Cidadão.

Pipeline: [`etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py`](../etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py)

Escopo: **políticos de Goiás** apenas (senadores, deputados federais GO,
deputados estaduais GO). Alinha com o produto Fiscal Cidadão.

---

## 1. Problema que resolve

O grafo acumulava múltiplos nós pra mesma pessoa física sem mecanismo
de dedup cross-label. Caso real medido em 2026-04-18:

| Label | Origem | Props | Tem foto |
|---|---|---|---|
| `:Person` | `tse` | `name="JORGE KAJURU REIS DA COSTA NASSER"`, `cpf=218...`, `partido="PRP"` (eleição antiga), `sq_candidato=...` | não |
| `:Person` | derivado | `name="JORGE KAJURU"` (shadow, só name, sem CPF, sem UF) | não |
| `:Senator` | `senado_senadores_foto` | `name="JORGE KAJURU REIS DA COSTA NASSER"`, `partido="PSB"`, `id_senado=5895`, `foto_url=...` | **sim** |

Sintoma UX: PWA busca "Kajuru" → `/buscar-tudo` retorna o `:Person` TSE →
perfil abre sem foto, partido PRP de 2014 em vez do PSB atual.

Medição completa:

| Fonte | Contagem | Observação |
|---|---|---|
| `:Senator` | 3 | Sem `cpf`. Todos GO. |
| `:FederalLegislator` | 17 | CPF mascarado (`***.***.*31-53`) — não dá pra joinar com TSE por CPF. |
| `:StateLegislator` | 0 (espera `alego` rodar) | CPF pleno, dá pra joinar. |
| `:Person` (UF=GO) | 4249 | 150251 no total; dedup por CPF já garantido. |
| `:Person` shadow (`uf=NULL`, `cpf=NULL`, só `name`) | 1569 | Criadas como referência por outros pipelines (autores de inquéritos, DOU, etc.). |
| Grupos de colisão de nome entre Persons GO | 2265 grupos (5708 nós) | Matching por nome exige discriminador extra. |

---

## 2. Por que `:CanonicalPerson` (estratégia C) e não `:SAME_AS`

Consideradas 3 alternativas (discussão completa nas memórias da sessão
2026-04-18):

- **A. Hard MERGE via `apoc.refactor.mergeNodes`** — destrói proveniência
  por pipeline (cada origem tem ``provenance`` próprio). CLAUDE.md §1
  manda preservar. **Descartada.**
- **B. Relação `:SAME_AS` entre nodes equivalentes** — mantém nós
  originais, adiciona `(a)-[:SAME_AS {confidence, method}]-(b)`. Queries
  precisam fazer transversal.
- **C. Camada canônica `:CanonicalPerson`** (escolhida) — label novo com
  `:REPRESENTS` apontando pros nós-fonte. Queries de UI pivotam pela
  camada canônica.

Escolhemos **C** porque o grafo-target é pequeno (20 nós de cargo
inicialmente) e adaptar as queries do `pwa_parity` é viável. C também
dá uma superfície natural pra agregar props cross-source (ex.: foto do
Senator + partido do Senator + histórico TSE do Person) sem ter que
walk em toda query.

---

## 3. Regras de matching (ordem decrescente de confiança)

Só a primeira regra que resolve **sem ambiguidade** vence. Ambiguidade
= >1 candidato → skip + audit-log.

### 3.1. `cpf_exact` (conf = 1.00)

`:Person.cpf` == `cargo.cpf`, ambos normalizados pra dígitos. Aplicável
apenas a `:StateLegislator` (pipeline `alego` grava CPF pleno).
Descartado pra `:FederalLegislator` (Câmara entrega CPF mascarado) e
`:Senator` (Senado não expõe CPF).

### 3.2. `name_exact` (conf = 0.95)

`_normalize_name(cargo.name)` == `_normalize_name(Person.name)` dentro
do escopo `Person.uf == 'GO'`. Normalização: upper + sem acento + sem
pontuação + whitespace colapsado. Match global único.

### 3.3. `name_exact_partido` (conf = 0.90)

Aplicado quando `name_exact` tem múltiplos candidatos Persons. Filtra
por `Person.partido == cargo.partido` (case-insensitive). Se restar
exatamente 1, é o match.

### 3.4. `name_stripped` (conf = 0.85)

Após tirar prefixos honoríficos (`DR.`, `DRA.`, `PROF.`, `CEL.`, `DEP.`,
`SEN.`, `VER.`, `DELEGADO`, `DELEGADA`, `PASTOR`, `PADRE`, etc.) das
primeiras palavras e sufixos patronímicos (`JUNIOR`, `JR`, `FILHO`,
`NETO`, `SOBRINHO`, `SEGUNDO`) das últimas. Resolve o caso "DR. ISMAEL
ALEXANDRINO" ↔ TSE "ISMAEL ALEXANDRINO JUNIOR" — ambos colapsam em
"ISMAEL ALEXANDRINO".

### 3.5. `shadow_name_exact` (conf = 0.80)

Pra `:Person` bare (`uf=NULL`, `cpf=NULL`, só `name`): se o nome
normalizado bate exatamente com o nome de **1 e só 1** source já
presente num cluster canônico, anexa. Conservador — evita "JOSE SILVA"
virar substring-match contra 20 políticos com esse nome.

---

## 4. Saída no grafo

### Nó `:CanonicalPerson`

`canonical_id` estável derivado da âncora do cluster. Prioridade:

1. `canon_senado_{id_senado}` — Senator âncora.
2. `canon_camara_{id_camara}` — FederalLegislator âncora.
3. `canon_alego_{dígitos_legislator_id}` — StateLegislator âncora.
4. `canon_cpf_{dígitos_cpf}` — Person com CPF pleno, sem cargo.

Props (além de proveniência):

- `display_name` — nome do cargo mais oficial (Senator > Fed > State > Person).
- `cargo_ativo` — `"senador"` / `"deputado_federal"` / `"deputado_estadual"` / `NULL`.
- `uf` — sempre `"GO"` (escopo).
- `partido` — do cargo ativo mais recente.
- `num_sources` — tamanho do cluster.
- `confidence_min` — menor confidence entre os REPRESENTS do cluster (útil pro frontend sinalizar match com dúvida).

### Aresta `:REPRESENTS`

`(:CanonicalPerson)-[:REPRESENTS]->(sourceNode)` — 1 por nó-fonte.

Props:

- `method` — `cargo_root` / `cpf_exact` / `name_exact` / `name_exact_partido` / `name_stripped` / `shadow_name_exact`.
- `confidence` — float em [0, 1].
- `target_label` — label do nó-fonte (redundante com `labels(target)`, otimiza query).
- Proveniência completa (`source_id="entity_resolution_politicos_go"`, `run_id`, etc.).

Query de lookup típica:

```cypher
MATCH (cp:CanonicalPerson {canonical_id: $id})-[:REPRESENTS]->(src)
RETURN cp, collect({label: head(labels(src)), node: src, method: type_rel}) AS sources
```

---

## 5. Idempotência

`MERGE (cp:CanonicalPerson {canonical_id: ...})` + `MERGE (cp)-[r:REPRESENTS]->(src)`.
Re-runs atualizam props (ex.: `partido` quando muda), não duplicam.

Matching do source no MERGE do REPRESENTS usa `elementId(src)` porque é
a única chave uniformemente presente (`:Person` não tem
`senator_id`/`legislator_id` e `cpf` pode estar ausente/mascarado). Isso
torna o `canonical_id` estável por cargo âncora mas o `REPRESENTS` se
renova a cada run — comportamento desejado pra reagir a novos pipelines-
fonte.

---

## 6. Audit log

Todas as ambiguidades (cargos com >1 Person candidato, shadows com >1
cluster candidato, cargos sem Person match, shadows sem match) são
escritas em:

```
data/entity_resolution_politicos_go/audit_{run_id}.jsonl
```

Um entry por linha, JSON com `type` + campos específicos por tipo:

- `cargo_cpf_ambiguous` — CPF bate com >1 Person GO. Indica TSE inconsistente.
- `cargo_name_ambiguous` — Nome exato bate com >1 Person GO, sem desempate por partido.
- `cargo_stripped_ambiguous` — Nome normalizado (honoríficos fora) bate com >1 Person.
- `cargo_without_person` — Nenhuma regra resolveu o cargo. Pode virar `shadow_name_exact` no Phase 2.
- `shadow_ambiguous` — Shadow Person bate com >1 cluster.
- `shadow_no_match` — Shadow sem match em nenhum cluster (comum pra autores bare).
- `cargo_no_stable_key` — Cargo sem `id_senado`/`id_camara`/`legislator_id` (carga parcial upstream).

Revisão humana: operador inspeciona `cargo_cpf_ambiguous` / `cargo_name_ambiguous` / `cargo_stripped_ambiguous` / `shadow_ambiguous` pra decidir manualmente. Os demais são informativos.

---

## 7. Limitações conhecidas / false positives

### GLAUSTIN DA FOKUS (deputado federal GO)

Apelido de campanha sem honorífico reconhecido; o nome legal no TSE
é diferente. Atualmente fica com cluster `canon_camara_204419` de
`num_sources=1` (só o cargo_root). **Remediação**: adicionar ao conjunto
`_HONORIFIC_PREFIXES` se o padrão "DA FOKUS" for prefix válido, ou
mapear manualmente via lookup table quando o TSE expuser campo `nome_social`.

### Shadow Persons sem cluster

1553 shadows permanecem sem match na run atual. Cada shadow é uma
referência bare (ex.: autor de inquérito parlamentar) que teria que ser
match por substring contra 4000+ Persons GO. Não fazemos porque
substring-match explode com colisões de nome (2265 grupos). O shadow
permanece no grafo — apenas sem associação canônica.

### False positives potenciais

- **Homônimos sem partido**: `name_exact` com 1 match único (no escopo
  GO) é tomado como verdadeiro, mesmo que seja coincidência. Mitigação:
  o `confidence=0.95` permite que o frontend destaque esse caso. Em
  backtest contra dados reais GO, não encontramos homônimo acidental
  nos 20 clusters atuais.
- **Nicknames esculhambados**: honoríficos regionais que não estão na
  lista (ex.: "TIO FULANO", "IRMÃO BELTRANO"). Solução manual:
  adicionar ao `_HONORIFIC_PREFIXES` ou mapear por `senator_id`
  lookup table.

---

## 8. Como auditar um cluster específico

```cypher
// Ver todas as sources de um político canônico.
MATCH (cp:CanonicalPerson {canonical_id: 'canon_senado_5895'})
      -[r:REPRESENTS]->(src)
RETURN labels(src), src.name, src.cpf, src.partido, r.method, r.confidence
ORDER BY r.confidence DESC
```

```cypher
// Ver políticos canônicos com match "duvidoso" (confidence < 0.9).
MATCH (cp:CanonicalPerson)-[r:REPRESENTS]->(src)
WHERE r.confidence < 0.9 AND r.method <> 'cargo_root'
RETURN cp.canonical_id, cp.display_name, r.method, r.confidence, labels(src), src.name
ORDER BY r.confidence ASC
```

```cypher
// Políticos canônicos sem Person match (só cargo_root) — candidatos
// a remediação manual.
MATCH (cp:CanonicalPerson)
WHERE cp.num_sources = 1
RETURN cp.canonical_id, cp.display_name, cp.cargo_ativo
```

---

## 9. Cadência

**Diária.** Re-rodar todo dia é barato (~1-2 segundos em grafo atual) e
captura:

- Novos senadores/deputados ingeridos nos pipelines de cargo.
- Novos Persons TSE em anos de eleição.
- Novos shadows criados por inquéritos/DOU.

Orquestração fica fora do pipeline — o operador chama via `cron` ou o
orquestrador externo depois dos pipelines-fonte rodarem
(`tse`, `camara_politicos_go`, `senado_senadores_foto`, `alego`).

---

## 10. Relação com outros pipelines derivados

O projeto tem dois pipelines "pure graph-internal" (source_format=`derived`):

- **`entity_resolution_politicos_go`** (este) — cria `:CanonicalPerson`.
- **`propagacao_fotos_person`** — copia `foto_url` entre labels de
  cargo ↔ `:Person` homônimo, necessário porque o fulltext
  `entity_search` da PWA só indexa `:Person`.

Ordem recomendada (ambos idempotentes): `propagacao_fotos_person` →
`entity_resolution_politicos_go`. A propagação faz o grafo ficar
mais rico antes da resolução; a resolução depois consolida os clusters.
Rodar na ordem inversa também funciona, mas um operador teria que
rerun pra ver o efeito combinado.
