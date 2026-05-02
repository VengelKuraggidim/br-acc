# Dedup busca PWA — Fase 4: ER upstream para nomes diferentes

> Proposta criada 2026-05-02. **Implementada** como fase 5.6
> `shadow_first_last_match` no mesmo dia (commit `259d53a` — testes; a
> implementação caiu no commit `bff0ee1`). Default OFF.
> **Spot-check 2026-05-02:** 11 candidatos, 0 ambíguos; 6 promovidos
> manualmente via Cypher (`etl/scripts/promote_first_last_6.py`),
> 5 rejeitados por risco de homonímia cross-UF.
> **Gap remanescente:** UF gating não foi codificado — ver
> `debitos/er-fase56-uf-gating.md`.

## Contexto

Fase 3 do dedup do `/buscar-tudo` (memo `project_dedup_busca_pwa`) colapsa
linhas mesmo nome+UF; cargo mais recente ganha. Caso aberto:
nome **diferente** mas pessoa real igual — exemplo canônico
**KARLOS CABRAL** ↔ **KARLOS MARCIO VIEIRA CABRAL**.

A fase 3 não enxerga: nomes batem só no primeiro e último token, mas o
meio diverge. Hoje aparecem como dois resultados separados na busca.

Resolver upstream ideal: anexar ambos no mesmo `:CanonicalPerson` —
aí a fase 3 colapsa naturalmente via cluster.

## Estado do resolver atual

`etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py` tem
seis fases:

| Fase | Nome | Critério | Confidence |
|---|---|---|---|
| 1 | cpf_exact | CPF dígitos iguais | 1.0 |
| 2 | cpf_suffix_cargo | últimos 8 dígitos + cargo bate | 0.95 |
| 3 | name_partido_multi | nome igual + partido bate (≥2 hits) | 0.85 |
| 4 | name_municipio_vereador | nome + município (vereadores) | 0.80 |
| 5 | shadow_name_exact | nome **exato** num cluster | 0.80 |
| 5.5 | shadow_prefix_match | prefix de tokens (`JORGE KAJURU` ⊂ `JORGE KAJURU REIS DA COSTA NASSER`) | 0.70 |
| 6 | manual_override | CSV humano | 1.0 |

**Lacuna**: KARLOS CABRAL não é prefix de KARLOS MARCIO VIEIRA CABRAL
— `CABRAL` é o último token, não é continuação.

## Proposta — Fase 7: `name_first_last_match`

Casa shadow de **2 tokens** (apelido + sobrenome) com source de **≥3
tokens** cujo primeiro **e** último tokens batem com os do shadow.

```
shadow:  ["KARLOS", "CABRAL"]                       (2 tokens)
source:  ["KARLOS", "MARCIO", "VIEIRA", "CABRAL"]   (4 tokens, [0]==shadow[0], [-1]==shadow[-1])
→ match com confidence 0.65
```

**Gating** (anti falso-positivo):

1. Shadow precisa ter **exatamente 2 tokens contentful** (após
   `_strip_honorifics`). 1 token é genérico demais; 3+ tokens já é
   coberto por prefix_match.
2. Source precisa ter **≥3 tokens**. Permite "KARLOS X CABRAL" e
   "KARLOS X Y CABRAL" mas não shadow contra outro shadow.
3. Match precisa ser **único** — exatamente 1 cluster com (first, last)
   batendo. Ambíguo = audit + skip (mesmo padrão do prefix_match).
4. **UF precisa bater** entre shadow e cluster. Sem UF no shadow ou no
   cluster → skip (homonímia cruzando estados é alta).
5. Confidence 0.65: mais baixa que prefix_match (0.70) porque a
   evidência é mais fraca — tokens do meio podem coincidir por acaso
   menos que prefix-bater perfeitamente.

## Falsos-positivos esperados

* "MARIA SILVA" como shadow + "MARIA APARECIDA SILVA" cluster —
  combinação **ambígua no Brasil**. Gating #3 (uniqueness) deveria
  cortar; se NÃO cortar, é porque só 1 cluster GO casa, e provavelmente
  é certo. Mas vale rodar com `_audit_entries` ligado em primeira
  passagem antes de promover.
* "JOÃO PAULO" + "JOÃO X PAULO" (sobrenome composto) — mesma
  defesa de uniqueness.

## Recomendação operacional

1. Implementar a fase atrás de flag default-OFF (`enable_first_last_match=False`),
   igual `enable_name_tier` do `entity_resolution_tce_go`.
2. Rodar primeira vez com flag ligada num modo dry-run (só audit, sem
   gravar `:REPRESENTS`). Spot-check humano em 20 matches aleatórios
   antes de promover.
3. Promover quando spot-check ≥18/20 acertar. Caso contrário, ajustar
   gating (e.g. exigir partido também batendo).

## Esforço

* Pipeline: ~80 linhas (fase 4.5 e 5.5 são modelo direto).
* Testes: 8-10 fixtures (match único, ambíguo, UF divergente, ≥3 tokens
  source, 1 token shadow, etc).
* Re-run ER + smoke do `/buscar-tudo` no PWA.

Total: 1 dia incluindo spot-check.

## Onde mexer

* `etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py` —
  novo método `_attach_name_first_last_matches` espelhando
  `_attach_shadow_prefix_match` (que já está no arquivo). Index novo
  por `(first_token, last_token)` em paralelo aos índices existentes
  (`cluster_names`, prefix index).
* `etl/tests/test_entity_resolution_politicos_go.py` — fixtures novas.
* Audit log: tipo `name_first_last_match` ou `name_first_last_ambiguous`.

## Não-objetivo

* Stemming, fuzzy edit-distance, Levenshtein. Fase 7 mantém o princípio
  de "apenas matches de ALTA confiança upstream"; resto é trabalho do
  usuário via `manual_override` CSV.
