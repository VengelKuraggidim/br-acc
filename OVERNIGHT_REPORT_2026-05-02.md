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
