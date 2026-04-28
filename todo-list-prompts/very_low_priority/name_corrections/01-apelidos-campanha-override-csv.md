# Override manual para apelidos de campanha sem honorífico — ✅ INFRA ENTREGUE 2026-04-27

> Commit `737f07e` adiciona fase 6 `manual_override` em
> `entity_resolution_politicos_go.py`. CSV versionado em
> `docs/entity_resolution_overrides.csv` (path configurável via env
> `BRACC_ER_OVERRIDES_PATH`):
>
> ```csv
> canonical_id,target_kind,target_key,confidence,notes,added_by,added_at
> ```
>
> `target_kind` aceito: `sq_candidato` | `id_senado` | `id_camara` |
> `legislator_id` | `cpf` (CPF normaliza pra dígitos antes de
> comparar). Conf default 1.0.
>
> Comportamentos cobertos: CSV ausente = no-op; target em outro
> cluster = audit `conflict_other_cluster`; target no mesmo cluster
> = idempotente; canonical inexistente = audit `no_cluster`. 8
> testes novos.
>
> Caso canônico que motivou a TODO (GLAUSTIN DA FOKUS) já caía pela
> fase 4 desde 2026-04-23, então `docs/entity_resolution_overrides.csv`
> NÃO foi criado nesta entrega — fica como reserva pra apelidos
> futuros. Quando aparecer um cluster órfão que nenhuma regra
> automática (1-5.5) cobrir, basta adicionar a linha CSV +
> commitar + re-rodar o ER pra anexar.

> Audit 2026-04-24 no Neo4j local: canon_camara_204419 (GLAUSTIN DA FOKUS)
> **já está matched** via fase 4 `cpf_suffix_cargo` (Person "GLAUSKSTON
> BATISTA RIOS", CPF `607.512.661-91`, conf 0.85). A regra 4 foi adicionada
> depois que esta TODO foi redigida (2026-04-18) e resolveu o caso original
> sem infra manual. Auditoria confirma zero clusters GO atualmente órfãos
> por apelido de campanha.
>
> Rebaixado pra `very_low_priority/`: infra de override CSV vira reserva
> pra eventual caso futuro onde nenhuma regra automática (1-4) aplique.
> Se usuária vir um cluster órfão que não case com CPF-suffix, só aí o
> débito volta pra ativa.

## Original

> Complemento do pipeline `entity_resolution_politicos_go` pra cobrir o
> "longo tail" de apelidos que o `name_stripped` não pega. Caso canônico:
> **GLAUSTIN DA FOKUS** (deputado federal GO) — apelido de YouTube usado
> como nome eleitoral; o nome legal no TSE é diferente e nenhum dos
> prefixos honoríficos (`DR.`, `DRA.`, `CEL.`, etc.) nem sufixos
> patronímicos (`JR`, `FILHO`, `NETO`) aparece. Regra 4 do pipeline
> (`name_stripped`) não resolve.

## Contexto
Dry-run em 2026-04-18 (`docs/entity_resolution.md`): 20 clusters
canônicos criados pra GO (3 senadores + 17 deputados federais). Desses:

- 19 matched com :Person TSE via `cpf_exact` / `name_exact` /
  `name_exact_partido` / `name_stripped` / `shadow_name_exact`.
- **1 cluster isolado** (`num_sources=1`, só cargo_root): canon_camara_204419
  = "GLAUSTIN DA FOKUS" (deputado federal GO, partido PODE).

Impacto: o PWA mostra Glaustin sem histórico TSE (candidaturas anteriores,
bens declarados, receitas de campanha). O cluster isolado não quebra UX —
só é mais pobre que os outros 19 — mas cresce com cada novo apelido que
entrar (candidatos "TIO FULANO", "IRMÃO BELTRANO", "YOUTUBER X", nomes de
guerra policial, etc.).

Alternativas consideradas e por que foram descartadas:

- **Adicionar `DA FOKUS` ao `_HONORIFIC_SUFFIXES`**: não é honorífico, é
  nome de canal; quebra semântica e pode criar falsos positivos.
- **Fuzzy name match (Jaro-Winkler, Levenshtein)**: threshold que pega
  Glaustin pega também homônimos coincidentes — risco de merge errado,
  viola CLAUDE.md §3 (nunca chutar/acusar).
- **Splink ML resolver** (já scaffoldado em
  `etl/src/bracc_etl/entity_resolution/`): overkill pra 1-2 casos/legislatura.
- **Wikidata Q-id + propriedade P1272 (TSE id)**: funciona quando o político
  tem entrada no Wikidata com CPF ou sq_candidato mapeado. Dependência
  externa + rate limit SPARQL. Ver follow-up 02.

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py` — pipeline
  atual; `_resolve_cargo()` é onde a override entraria (Phase 3, depois de
  `cpf_exact` / `name_*` / `shadow_*`).
- `docs/entity_resolution.md` §7 "Limitações conhecidas" — documenta
  Glaustin como caso sem match.
- `etl/tests/test_entity_resolution_politicos_go.py` — tests novos precisam
  cobrir "override carrega", "override aplica após regras automáticas",
  "override com target inválido loga warning e pula".

## Missão

1. **Criar `docs/entity_resolution_overrides.csv`** com schema:
   ```csv
   canonical_id,target_element_id_hint,target_label,method_override,confidence,notes,added_by,added_at
   ```
   - `target_element_id_hint` NÃO é o `elementId(x)` cru (instável entre
     re-ingestões do pipeline-fonte). Use chave estável: `sq_candidato`
     pra :Person TSE, `id_camara` pra :FederalLegislator, etc.
   - `method_override` = `"manual_override"` (novo método; conf default 1.0
     porque é afirmação humana com rastreabilidade).
   - `added_by` = login GitHub ou "fernando"; `added_at` = ISO date.
   - Exemplo pra Glaustin:
     ```csv
     canon_camara_204419,{sq_candidato_do_glaustin},Person,manual_override,1.0,"Apelido YouTube; nome legal TSE difere. Fonte: divulgacandcontas.tse.jus.br",fernando,2026-04-25
     ```

2. **Carregar override no pipeline** (Phase 3 em `transform()`):
   - Ler CSV em `_load_overrides_csv()` — path configurável via env
     `BRACC_ER_OVERRIDES_PATH`, default `docs/entity_resolution_overrides.csv`.
   - Pra cada linha: match do cluster pelo `canonical_id` + resolver o
     target via key estável (`sq_candidato`/`id_camara`/etc.) numa query
     Cypher dedicada que devolve elementId.
   - Chamar `_attach_source()` com `method="manual_override"`, conf 1.0.
   - Skippar (warning log) se o canonical_id não existir no grafo OU se
     o target key não resolver OU se o target já está anexado a OUTRO
     canonical (alerta de conflito). Nunca hard-error — re-runs devem
     seguir mesmo com CSV quebrado.

3. **Tests**:
   - `test_override_aplica_quando_cluster_existe` (happy path).
   - `test_override_skip_quando_canonical_nao_existe` (linha solta no CSV).
   - `test_override_skip_quando_target_ja_no_outro_cluster` (conflict).
   - `test_override_com_path_vazio_nao_quebra` (`BRACC_ER_OVERRIDES_PATH`
     apontando pra arquivo inexistente).
   - Fixture com CSV inline + driver mockado resolvendo targets.

4. **Audit log novo tipo**: `override_applied` (pra cada linha usada) e
   `override_skipped` (com razão: no_cluster/no_target/conflict).

5. **Doc**:
   - `docs/entity_resolution.md` §7 ganha subseção "Como adicionar override
     manual" com passo-a-passo do CSV + comando pra re-rodar o pipeline.
   - `CONTRIBUTING.md` menciona o CSV como o canal oficial pra corrigir
     matches errados/ausentes (NÃO editar o Python).

## Trade-offs explicitamente aceitos

- **Manutenção manual**: operador precisa revisar audit log e adicionar
  override quando novo apelido aparecer. Aceitável porque volume é
  ~1-2/legislatura (4 anos).
- **CSV versionado em git**: rastreabilidade total (quem adicionou,
  quando, por quê). Review via PR evita que agent sozinho force match
  sem revisão humana — reforça CLAUDE.md §3.
- **Conflito com Phase 1-2**: se Phase 1 matched Person X ao cluster e
  override aponta Person Y pro mesmo cluster, o pipeline deve seguir a
  override (afirmação humana vence regra automática). Logar o conflito
  no audit pra o operador remover a linha do CSV se for erro dela.

## Escopo não-objetivo desta TODO
- Resolver o caso Glaustin propriamente: isso é **uma linha no CSV**
  depois da infra estar pronta. A TODO é só criar a infra.
- Apelidos fora do escopo GO: `_TARGET_UF = "GO"` continua o mesmo.

## Cadência de revisão do audit log
Recomendação: operador checa `data/entity_resolution_politicos_go/audit_*.jsonl`
mensalmente (ou após cada re-ingestão de `camara_politicos_go` /
`senado_senadores_foto`). Cluster com `num_sources == 1` + `cargo_ativo IS
NOT NULL` é candidato a override.
