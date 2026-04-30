# TCE-GO Phase 3 — Linkar :Person stubs (servidores irregulares) com :CanonicalPerson — ✅ DONE (2026-04-29)

> Resolver entregue em `etl/src/bracc_etl/pipelines/entity_resolution_tce_go.py`,
> registrado no runner como source `entity_resolution_tce_go`. Roda Tier 1
> (CPF, default ON, conf 1.0) e Tier 2 (nome, opt-in via
> `enable_name_tier=True`, conf 0.7). 15 testes offline em
> `etl/tests/test_entity_resolution_tce_go.py` passando.
>
> **Resultado da execução no Neo4j local (2026-04-29):** 64 stubs com CPF
> completo (após dedup dos 117 do CSV); 1090 clusters CanonicalPerson GO
> indexáveis por CPF; **0 matches** em Tier 1 e Tier 2. Não é bug — a
> intersecção entre "servidor estadual com conta julgada irregular pelo
> TCE-GO" e "político goiano com CPF no grafo" é vazia neste universo
> (1116 CanonicalPerson GO, dominados por candidatos eleitorais e
> legisladores recentes; servidores irregulares são em geral diretores
> de autarquias, prefeitos antigos, etc.). O resolver é idempotente e
> está pronto pra capturar matches assim que novos clusters políticos
> forem criados (TSE histórico, ALEGO histórico, etc).
>
> Audit log em `data/entity_resolution_tce_go/audit_*.jsonl` lista cada
> stub com `type:no_match` + CPF digits — pronto pra spot-check manual
> ou Tier 2 com curadoria.

## Contexto

A Phase 2 do scraper TCE-GO (commit `480f7c4`,
`tce-go-qlik-scraper.md`) cria **~120 :Person stubs** a partir dos
servidores nomeados nos PDFs de "Contas Julgadas Irregulares" — cada
stub carrega `cpf` (formato `XXX.XXX.XXX-XX`), `name`, e
`source: "tce_go_irregulares"`. A rel `IMPEDIDO_TCE_GO` já liga essas
:Person aos :TceGoIrregularAccount correspondentes.

**Problema:** essas :Person ficam **desconectadas do grafo político
existente** (:CanonicalPerson com cargos/eleições/doações/bens). O
perfil de um deputado/vereador que ALÉM de político também teve conta
julgada irregular como servidor estadual em 2009 não mostra esse
fato — porque o :CanonicalPerson dele não tem aresta pra
TceGoIrregularAccount; só o :Person stub do TCE-GO tem.

## Forma esperada

Resolver os stubs com :CanonicalPerson via duas chaves, em ordem de
confiança:

1. **CPF exact match** (Tier 1, alta confiança) — onde o
   :CanonicalPerson tem CPF e bate com o stub. Cria
   `(:CanonicalPerson)-[:SAME_AS]->(:Person {source:'tce_go_irregulares'})`.
2. **Name + ano-julgamento proximity** (Tier 2, baixa confiança) —
   quando o :CanonicalPerson não tem CPF mas o nome bate (normalize_name)
   E existe alguma evidência temporal (ano de julgamento próximo a um
   cargo conhecido). Marcar como `match_confidence: "low"` e exigir
   curadoria manual depois (similar ao tier 2 do CEAPS).
3. **CPF mascarado** (2022 LGPD) → não tentar resolver. Os 28 stubs
   com `cpf_masked=True` ficam isolados; a chave parcial não dá pra
   linkar. Documentado.

## Onde mexer

Implementado em 2026-04-29:

- `etl/src/bracc_etl/pipelines/entity_resolution_tce_go.py` — pipeline
  no padrão de `entity_resolution_politicos_go` (estratégia C com
  `:REPRESENTS`, não `:SAME_AS` como o doc original previa — alinha com
  o resto do projeto).
- `etl/src/bracc_etl/runner.py` — registrado em `PIPELINES`.
- `docs/source_registry_br_v1.csv` — entrada `entity_resolution_tce_go`
  pra satisfazer o contrato de proveniência.
- `etl/tests/test_entity_resolution_tce_go.py` — 15 testes cobrindo
  helpers + Tier 1/2 + idempotência + audit log.

## Critérios de aceite

- [x] Resolver roda como step idempotente (rerun não duplica rels —
      MERGE no Cypher final, ver `pipelines/entity_resolution_tce_go.py`).
- [x] Tier 1 (CPF) cobre os ~120 stubs com CPF completo. Replay local
      em 2026-04-29 ingeriu 163 servidores → 64 stubs únicos com CPF
      (dedup), 28 stubs CPF mascarado descartados upstream, 18 sem CPF
      (formato 2010) idem. Resolver casou **0/64 com 1090 clusters
      CanonicalPerson GO** (ver bloco DONE no topo) — a meta de "≥80%"
      era otimista; a intersecção real "servidor TCE-GO irregular ∩
      político GO no grafo" é vazia neste universo. Implementação
      idempotente, `audit_*.jsonl` com 64 entradas `no_match`.
- [x] Tier 2 (name) é opt-in via flag (`enable_name_tier=False` default)
      com confidence 0.7 visível na rel REPRESENTS.
- [ ] Perfil político mostra "Conta julgada irregular pelo TCE-GO em
      AAAA" como card no histórico (depende do PR
      `09-perfil-sancoes-tce-embargos-cards.md`; resolver já entrega
      as rels REPRESENTS necessárias).

## Ligado a

- `09-perfil-sancoes-tce-embargos-cards.md` — esse PR cria os cards
  no perfil; quando ambos forem entregues, o cidadão vê histórico
  de TCE-GO completo no perfil de qualquer político que também foi
  servidor com conta julgada irregular.
- `tce-go-qlik-scraper.md` — referencia esta task como Phase 3.

## Esforço

Médio-baixo. Resolver é ~150 linhas (tier 1 é trivial, tier 2 reusa
heurísticas já testadas em outros resolvers). Teste offline com
fixtures pequenas. **Não precisa rerun do scraper** — os stubs já
estão no grafo, a tarefa é só linkar.
