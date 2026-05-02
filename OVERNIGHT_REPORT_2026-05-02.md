# Overnight 2026-05-02 — relatório

3 commits novos, 1 design doc, 5 markdowns DONE removidos. Tudo no
`main` local, sem push (autorização não dada). bracc-api foi rebuildado
e está rodando na 8000 com o fix.

## Mudou

- **`4e7bf0a` fix(conexoes): dedup :Person legacy + :CampaignDonor**
  Pipeline legado `tse.py` agrega doações 2022 em `:Person {cpf=pleno}`
  sem `donation_id`. Pipeline novo `tse_prestacao_contas_go.py` cria
  `:CampaignDonor {doador_id=mascarado}` granular. A mesma doação
  caía 2× no grafo → `total_doacoes` dobrava.
  - `api/src/bracc/services/conexoes_service.py:280-371` — helpers
    `_last4_digits` + `_build_camp_donor_keys` e dedup no loop principal.
  - `api/tests/unit/test_conexoes_service.py:347-540` — 4 testes novos
    (TestDedupPersonLegacyVsCampaignDonor).
  - 71/71 testes do conexões passam; ruff clean; mypy clean (1 erro
    pré-existente em `as_str(target_props, "cpf")` linha 636 — commit
    `61196e7f`, não tocado).
  - **Validação Amilton (eid `...:7089`)**: API direta `localhost:8000`
    → `total_doacoes=R$ 429.852,92` vs TSE declara `R$ 421.500,72`,
    divergência **1,98%** → status `"ok"`. Antes do fix: `R$ 843k`.
- **`21de7f5` chore(todos): remove 4 prompts DONE.** Fechei
  `variados/10` (servidor estadual), `variados/11` (TCE-GO Phase 3),
  `debitos/tse-doou-campaigndonor-stubs-orfaos` (3 bugs antigos
  fechados; bug novo virou `4e7bf0a`), `debitos/tce-go-qlik-scraper`
  (Phase 1+2 DONE 2026-04-27), e a meta-task `limpar-todos-resolvidos`.
- **`d077b33` chore(todos): name_corrections cleanup + Fase 4 design.**
  Remove `name_corrections/02` e `03` (DONE), mantém `01` como reserva.
  Adiciona `medium_priority/dedup-busca-fase4-er-upstream.md` com
  proposta `name_first_last_match` (KARLOS CABRAL ↔ KARLOS X Y CABRAL).

## Validações de outras tasks (sem mudança de código)

### Validar shape API "Histórico de irregularidades"

Curl direto em `localhost:8000/politico/{eid}` pros 4 perfis do
followup-09 — todos batem com o esperado:

| Perfil   | Campo                    | Esperado | Got |
|----------|--------------------------|----------|-----|
| Mauro    | `sancoes_detalhe`        | 1        | 1 ✓ |
| Alcides  | `embargos_ambientais`    | 2        | 2 ✓ |
| Adelina  | `tce_go_irregulares`     | 2        | 2 ✓ |
| Adailton | `tcm_go_impedidos`       | 1        | 1 ✓ |

Code review estático em `pwa/index.html:2795-2918` confirma que as
4 keys batem entre PWA e backend. Truncate (280 char), `fmtData`
(ISO → DD/MM/YYYY), link TCE pdf e disclaimer TCM ("match por nome
— verificar fonte") tudo OK. **Validação visual no browser ainda
pendente do seu lado** — arrasta o PWA, abre cada um dos 4
elementIds e confere render.

### TCE-GO Phase 3 Tier 2 (name) opt-in

Rodei `EntityResolutionTceGoPipeline(enable_name_tier=True)` no
Neo4j local (script `/tmp/run_tier2_tce.py`, dry — sem load).
Resultado: `stubs_total=64, matched_cpf=0, matched_name=0,
unmatched=64`. **Confirma a memória `project_replay_local_2026_04_23`:
intersecção entre "servidor TCE-GO irregular" e "político GO no
grafo" continua vazia mesmo com fuzzy name.** Não é bug. Conclusão:
deixar Tier 2 OFF por default; abrir por demanda quando ER do TSE
histórico crescer e novos clusters surgirem.

### Auditoria CNAE/CNPJ no Neo4j local

| Métrica                              | Valor      | % do total |
|--------------------------------------|------------|------------|
| Total `:Company`                     | 347.956    | —          |
| com `cnae_principal`                 | 14.063     | 4,0 %      |
| com `situacao_cadastral`             | 12.978     | 3,7 %      |
| `cnae_principal` formatado (`-/`)    | 1.175      | —          |
| `cnae_principal` só dígitos          | 12.888     | —          |
| `tipo_entidade='comite_campanha'`    | 1.180      | —          |
| pares dígito-vs-formatado coexistindo| 340        | —          |

Cobertura de CNAE/situação ainda é ~4%. Fase 2 (RFB dump) não
rodou — fora de escopo overnight (multi-GB, semanas). 340 pares
são `:Company` legados criados antes do fix `format_cnpj` em
`tse_prestacao_contas_go.py:525` (memória
`project_cnpj_format_canon_grafo`). Backfill cypher de
normalização é candidato a próximo sprint enxuto.

## Aberturas (sem ação tomada — precisam decisão sua)

1. **Push dos 3 commits** — não fui autorizada. `git push origin main`
   quando você acordar (ou usa `/ultrareview` antes pra revisar).
2. **Backfill Cypher das duplicatas no grafo** — meu fix está só no
   service (defesa em profundidade). Limpeza definitiva exige
   `DETACH DELETE` das ~497k rels :Person 2022 sem `donation_id`
   onde há :CampaignDonor matching. Reversível só com re-rodar o
   pipeline `tse.py`. Risco baixo, ganho de não depender da camada
   service. Aguarda OK.
3. **Backfill formato CNPJ** — 340 `:Company` em formato dígito-puro
   coexistindo com formatado. Cypher de merge MERGE simples mas
   destrutivo. Aguarda OK.
4. **Fase 4 dedup busca PWA** — design em
   `todo-list-prompts/medium_priority/dedup-busca-fase4-er-upstream.md`.
   Não implementei — exige spot-check humano em ~20 matches antes de
   promover (alto risco de falso-positivo em homônimos).
5. **fiscal-backend (legacy, porta 8001)** — duplica a lógica do
   `bracc-api` em `backend/app.py:608+` (re-implementa
   `classificar`). Qualquer fix no service do bracc-api **não
   propaga** pra esse caminho. PWA usa `localhost:8000` direto, então
   está OK pra você. Mas `localhost:8001` retorna dado errado.
   Sugestão: ou deletar o wrapper (substituído pelo bracc-api), ou
   transformá-lo em proxy puro.

## Não toquei (precisaria sua permissão)

- Pipelines pesados (`pncp` nacional, `comprasnet`, `pgfn` full,
  RFB dump CNPJ).
- LAI / Fala.BR / e-mails para órgãos.
- Aura / Cloud Run (congelado por decisão).
- `chown` em `etl/archival/` (sudo).
- Backfill BrasilAPI overnight (rate-limit; você pede sob demanda).
- Push pro origin.

## Containers em execução

```
fiscal-bracc-api  Up      (8000)  — rebuildado com o fix
fiscal-neo4j      Up      (7474, 7687) — 4.42M nós, healthy
fiscal-backend    Up      (8001) — wrapper legado, NÃO tem o fix
```

`docker compose build bracc-api` foi necessário porque é built image
(memória `project_ceap_federal_ingerido`). Container ficou na rede
`br-acc_default` + `fiscal-cidadao_default` pra alcançar Neo4j.

---

# Apêndice 2 — Segunda leva (até 02h+)

Você perguntou se tinha mais. Sim. Mais 9 tarefas (#9-#17), 6
concluídas com mudança real, 2 dispensadas após investigação, 1 é
este apêndice.

## Mudou (commits novos + backfills no grafo local)

- **`259d53a` feat(er): fase 5.6 shadow_first_last_match (opt-in,
  audit-only)**. Atende KARLOS CABRAL ↔ KARLOS X Y CABRAL e similares.
  Default OFF + audit-only (`enable_first_last_match=False`,
  `first_last_audit_only=True`). 6 testes novos, 90/90 ER tests
  passam, ruff clean. Pra ativar: rodar com flag ON em audit-only,
  spot-check audit jsonl, depois ligar audit_only=False.
- **Backfill CNAE (Cypher, ações no Neo4j local)** — 13.270 :Company
  normalizadas de `9492800` → `9492-8/00`. Pós: 14.445 com cnae,
  100% formato canônico. Memória `project_cnae_dois_formatos_grafo`
  destrancada — agora há um único formato. (Sem código no repo;
  helper `_eh_partido_ou_comite` continua tolerante via dígitos
  como defesa.)
- **Backfill formato CNPJ (Cypher)** — 340 :Company com versão
  dígito-puro mergidos no nó canônico formatado via
  `apoc.refactor.mergeNodes`. 7 batches, 0 falhas. 9.011 rels
  migradas. 0 duplicatas remanescentes. Memória
  `project_cnpj_format_canon_grafo` agora reflete realidade.
- **Backfill dedup :Person 2022 (Cypher destrutivo)** — **4.780 rels
  :Person legacy 2022 deletadas** (R$ 37,8 mi de valor agregado),
  matching com :CampaignDonor por last4(cpf)+ano. Restantes Person
  2022 sem donation_id: 492.508 (de 497.288 originais — só duplicatas
  removidas, outros candidatos preservados). Validação Amilton
  manteve `total_doacoes=R$ 429,8k`, status "ok".
  Re-rodar tse.py 2022 recoloca caso necessário (reversível em
  horas).
- **fiscal-backend zumbi removido**. Service foi tirado do
  `docker-compose.yml` em 2026-04-18 (linha 72 do compose explica
  Fase 04.G); container ficou rodando como leftover. `docker stop
  fiscal-backend && docker rm`. PWA continua OK em `localhost:8000`.
  Memória `reference_dois_backends_paralelos` precisa atualizar
  (PWA escolhe API: localhost:8000 = local; dominio = Aura/Cloud Run).
- **Memórias atualizadas**:
  `project_validacao_tse_excesso_ingestao` (RESOLVIDO + causa real
  documentada) e `project_dedup_busca_pwa` (Fase 5.6 mencionada).
- **TODOs limpos**: `tightening-filtro-ano-doou`,
  `meta-stats-legislative-expense-count`,
  `revisar-alego-rate-limit-sibling`,
  `name_corrections/02-shadow-token-prefix-match`,
  `name_corrections/03-cargo-prefix-match-full-person` — todos
  confirmados DONE em commits anteriores. Mais 5 arquivos a menos.

## Validações

- **Órfãos sem cluster canônico (#16)**: 18.142 :Person com
  `cargo_tse_2024` sem `:CanonicalPerson`. **Todos sem CPF
  publicado** (LGPD TSE 2024). Distribuição: 17.509 vereadores GO +
  633 prefeitos GO. **Não é bug** — ER atual cobre só cargo
  federal/estadual; vereador municipal sem CPF é fora de escopo.
- **Cobertura CNAE 2024 GO (#15)**: 19.228 CNPJs únicos de prestador
  de contas no CSV TSE 2024 GO; 571 já no grafo, **18.657 faltando**.
  ROI alto pra carimbar tipo_entidade=comite_campanha + cnae=9492-8/00
  sem precisar do dump RFB (multi-GB). Caminho: rodar o pipeline
  `tse_prestacao_contas_go.py` pra 2024 (não rodei — pipeline
  pesado, fora de escopo overnight).

## Aberturas (segunda leva)

1. **Push de TODOS os commits novos** — agora são 5: `4e7bf0a`,
   `21de7f5`, `d077b33`, `259d53a` e o initial. `git push origin
   main`. Considerar `/ultrareview` antes.
2. **Rodar pipeline `tse_prestacao_contas_go` pra 2024** — captura
   18.657 CNPJs novos com proveniência completa. Tempo estimado
   30-60min; dependência: schema 2024 do TSE
   (memória `project_tse_2024_enriquecimento` diz "schema drift pra
   2024"; pode precisar patch). Dry-run com `--limit 100` primeiro.
3. **Ativar fase 5.6 ER em audit-only** — rodar
   `entity_resolution_politicos_go` com
   `enable_first_last_match=True, first_last_audit_only=True`,
   spot-check entries `shadow_first_last_match_audit` no jsonl,
   depois decidir se promove (`audit_only=False`).
4. **Aura/prod sync** — todos os backfills da noite (CNAE, CNPJ,
   dedup) foram só no Neo4j local (`bolt://localhost:7687`).
   Memória `project_aura_adiado_sem_grana` confirma: Aura
   congelado. Quando reabrir, replicar.

## Dispensados (após investigação confirmou serem fora de escopo)

- **Refactor fiscal-backend → proxy** — virou "remover container
  zumbi" porque o service nem está no compose desde 2026-04-18.
  Trabalho real seria garbage; resolução: stop+rm.
- **Investigar dedup busca Fase 4** — virou implementação direta
  da fase 5.6.

## Containers em execução agora

```
fiscal-bracc-api  Up      (8000)  — com o fix dedup, rebuildado
fiscal-neo4j      Up      (7474, 7687) — pós-backfills
buildx_buildkit   Up      — build cache
```

`fiscal-backend` foi removido. PWA usa `localhost:8000` direto.

