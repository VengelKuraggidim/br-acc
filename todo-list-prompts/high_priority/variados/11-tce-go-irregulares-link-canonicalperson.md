# TCE-GO Phase 3 — Linkar :Person stubs (servidores irregulares) com :CanonicalPerson

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

- `etl/src/bracc_etl/entity_resolution/` — criar
  `tce_go_irregulares_resolver.py` no padrão dos resolvers existentes
  (ver `ceaps_senado_resolver.py` ou similar).
- `etl/src/bracc_etl/runner.py` — registrar o novo resolver no fluxo
  de pós-processamento (ou chamar dentro do `TceGoPipeline.load()`).
- Tests: fixture com 3 :Person stubs (1 com CPF que bate, 1 com nome
  que bate sem CPF, 1 órfão sem match) + verificação das rels SAME_AS.

## Critérios de aceite

- [ ] Resolver roda como step idempotente (rerun não duplica rels).
- [ ] Tier 1 (CPF) cobre os ~120 stubs com CPF completo — meta: ≥80%
      de match com :CanonicalPerson existentes.
- [ ] Tier 2 (name) é opt-in via flag (default OFF) com flag de
      confidence visível na rel.
- [ ] Perfil político mostra "Conta julgada irregular pelo TCE-GO em
      AAAA" como card no histórico (independe do PR — usa as rels já
      criadas).

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
