# ER fase 5.6 `shadow_first_last_match` — fechar gap de UF gating

> Débito criado 2026-05-02. Decorre do spot-check da fase 5.6 feito no
> mesmo dia (ver `project_dedup_busca_pwa` na auto-memory).

## Contexto

A fase 5.6 `shadow_first_last_match` foi implementada (commit `259d53a`)
pra cobrir o caso "X CABRAL ↔ X MARCIO Y CABRAL" — shadow de 2 tokens
batendo com cluster cujo source tem ≥3 tokens com primeiro+último
iguais. Default OFF.

Spot-check 2026-05-02 expôs uma **divergência** entre o design doc e o
código:

* O design (`dedup-busca-fase4-er-upstream.md`, gating #4) previa
  rejeitar match quando UF do shadow ≠ UF do cluster ou shadow sem UF.
* O código (`entity_resolution_politicos_go.py`, linhas 952–1013) **não
  enforce** esse gate. Como o pipeline só clusterer GO, todos os
  clusters são GO por construção; mas shadow Persons vêm com `uf=None`
  por causa do filtro do discovery (`MATCH (n:Person) WHERE n.uf IS
  NULL AND n.cpf IS NULL`). Resultado: o gate "shadow.uf == cluster.uf"
  nunca pode ser avaliado positivamente (shadow.uf é sempre `None`).

Os 11 candidatos do spot-check confirmaram: todos com shadow `uf=None`.
Risco real: um shadow "MAURO PEREIRA" originário da Bahia (que entrou
no grafo via doação/sanção sem UF) cola num cluster GO `MAURO JOSE
BATISTA PEREIRA`.

## O que fazer

Opção A — **inferir UF do shadow** via vizinhança:

1. Antes do shadow_first_last_match, derivar `inferred_uf` do shadow
   olhando arestas (`(:Person)-[:DOOU]->(:Candidatura {uf: ...})` ou
   `(:Person)-[:SANCIONADO_POR]->(:Sancao {uf: ...})` quando existem).
2. Se `inferred_uf` exists e ≠ cluster.uf → skip + audit
   `shadow_first_last_uf_mismatch`.
3. Se `inferred_uf is None` → manter o uniqueness gate como única
   defesa (status quo) mas marcar audit como `shadow_first_last_no_uf_evidence`
   pra observabilidade.

Opção B — **exigir partido também batendo** (gate adicional, como
fase 3 `name_partido_multi`):

1. Shadow precisa ter `partido` populado e cluster precisa ter o mesmo.
2. Mais conservador, mas reduz cobertura — muitos shadows de doadores
   não têm partido.

Opção C — **manter status quo + manual override**: scripts
`audit_first_last_match.py` + `promote_first_last_6.py` permitem
spot-check seletivo. Não fecha o gap, só formaliza o workflow manual.

## Recomendação

**A** preserva o ROI dos casos com sinal (commons como JOAO RODRIGUES)
e fecha o gap real. **B** é mais simples mas mata cobertura. **C** é
acceptable curto-prazo mas não escala.

## Quando

Não é bloqueante. A fase 5.6 segue default-OFF; quando a usuária pedir
mais dedups em massa (não os 6 raros já feitos manualmente), reabrir.

## Como atestar

1. Rodar `etl/scripts/audit_first_last_match.py` antes/depois.
2. Spot-check 20 candidatos aleatórios; meta ≥18/20.
3. Promover via `enable_first_last_match=True, first_last_audit_only=False`.
4. Smoke `/buscar-tudo` nos casos canônicos (KARLOS CABRAL etc).

## Não-objetivo

* Fuzzy/edit-distance — `dedup-busca-fase4-er-upstream.md` já marca
  fora de escopo.
