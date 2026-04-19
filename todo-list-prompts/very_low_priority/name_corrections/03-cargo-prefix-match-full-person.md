# Cargo-prefix match: nome parlamentar curto → :Person legal completo — ⏳ PENDENTE (2026-04-18)

> Estende o `entity_resolution_politicos_go` pra anexar ``:Person``
> completo (com CPF + sq_candidato + UF) quando o nó-fonte de cargo
> (`:FederalLegislator`, `:StateLegislator`, `:Senator`) tem **nome
> curto/parlamentar** que é prefix exato do **nome legal completo** do
> `:Person`. Caso canônico: `:FederalLegislator` `"GUSTAVO GAYER"` vs
> `:Person` `"GUSTAVO GAYER MACHADO DE ARAUJO"` (CPF `617.502.811-34`
> no Person; CPF `***.***.*61-34` mascarado no FL — match por CPF
> exato falha).

## Contexto

Descoberto em 2026-04-18 durante deploy do `propagacao_fotos_person`
(TODO prévio já mergeada): dos 17 `:FederalLegislator` GO com foto
oficial, **só 15** casaram com `:Person` homônimo via `name=` exato.
Os 2 restantes são casos clássicos de "nome parlamentar ≠ nome legal":

| FL.name          | Person.name (correspondente real)      | Problema       |
|------------------|----------------------------------------|----------------|
| GUSTAVO GAYER    | GUSTAVO GAYER MACHADO DE ARAUJO        | prefix         |
| DR. ISMAEL ALEXANDRINO | (candidato não-foi-encontrado) | honorífico+sobrenome cortado |

Impacto imediato observável: **foto do deputado federal em exercício
não aparece na busca da PWA** quando o nome parlamentar é curto. A
"propagação cross-label" que costura `foto_url` `:FL→:Person` depende
de match determinístico — se o match falha, o usuário vê o card sem
foto (ver screenshot Fernando 2026-04-18 20:53:33 consultando "marconi
perillo").

Este é o caso simétrico do TODO 02 (`shadow-token-prefix-match`):

| TODO       | Anchor do cluster | Alvo            | Forma do nome    |
|------------|-------------------|-----------------|------------------|
| 02 (existente) | nome longo (source) | shadow Person curto | shadow=prefix(source) |
| 03 (este)      | nome curto (source) | Person legal longo | source=prefix(Person) |

Sem o inverso, a Phase 2 fica assimétrica — pega shadows mas perde
nós "full Person com CPF+UF+sq_candidato". E pior: esses nós full são
os que o PWA prefere mostrar (têm mais dados).

## Por que `propagacao_fotos_person` não resolve isso

O pipeline atual (`etl/src/bracc_etl/pipelines/propagacao_fotos_person.py`)
faz match direto `p.name = src.name` porque nós assumimos que ambos
estão normalizados pro mesmo formato. Tecnicamente estão (upper +
sem acento), mas **semanticamente não**: `:FederalLegislator` guarda
nome parlamentar (curto), `:Person` guarda nome legal completo.

Complicar o Cypher do `propagacao_fotos_person` com prefix-match é
tentador mas **erra o lugar do fix**: propagação de foto é
cost-efetiva só se o casamento de identidade já foi feito upstream.
A solução certa é ensinar o `entity_resolution_politicos_go` a
reconhecer esse padrão — aí o `:CanonicalPerson` cluster liga os 2
nós, e o `propagacao_fotos_person` passa a seguir `:REPRESENTS`
(refactor futuro do pipeline, fora do escopo desta TODO).

## Por que não substring-anywhere

Mesmo argumento do TODO 02 §"Por que não substring-anywhere":
substring `"SILVA"` casaria com "JOÃO DA SILVA", "MARIA SILVA COSTA",
etc. — explode. Prefix exato é conservador porque exige que o nome
parlamentar seja uma **contração do início** do nome legal —
fenômeno linguístico real ("GUSTAVO GAYER" é prefix de "GUSTAVO
GAYER MACHADO DE ARAUJO" porque o primeiro nome + primeiro
sobrenome são sempre os 2 primeiros tokens no padrão brasileiro
de nomenclatura).

## Sinal secundário: últimos 2 dígitos do CPF

Pra reduzir risco de false positive quando o prefix curto casa com
múltiplos `:Person`, usar **últimos 2 dígitos do CPF (dígitos
verificadores)** como sinal secundário quando disponível:

- `FL.cpf` = `"***.***.*61-34"` → extrai `"34"` (últimos 2 depois do hífen)
- `Person.cpf` = `"617.502.811-34"` → extrai `"34"` (últimos 2 depois do hífen)
- Match se **prefix bate E últimos 2 dígitos do CPF batem**.

Probabilidade de colisão: 1/100 dentro de um grupo de homônimos. Com
prefix curto + CPF check-digits, confiança alta sem depender de CPF
completo (que está mascarado no `:FederalLegislator` por política
LGPD — ver CLAUDE.md §3).

Quando só o `:FederalLegislator` tem CPF (e o prefix bate Person
único), permitir match sem dígito verificador — é a política atual
das outras regras do pipeline. Quando ambos têm CPF e bate prefix
múltiplo, **exigir** dígitos verificadores pra desambiguar.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py`
  - `transform()` Phase 1 (cargo ↔ Person) é onde a regra entra, logo
    depois de `name_exact` / `name_exact_partido` e antes de
    `name_stripped` / `shadow_*`. Método novo:
    `_attach_source(method="cargo_prefix_of_person", confidence=0.75)`.
  - `_normalize_cpf_last2(cpf: str | None) -> str | None` helper novo
    que extrai `"34"` de qualquer formato conhecido (`"***.***.*61-34"`,
    `"617.502.811-34"`, `"61750281134"`). Documentar formatos aceitos.
- `docs/entity_resolution.md` §3 (regras de resolução) ganha subseção
  `cargo_prefix_of_person` descrevendo prefix + CPF check-digits.
- `etl/tests/test_entity_resolution_politicos_go.py`:
  - `TestCargoPrefixOfPerson` nova classe com happy path + 4 casos de
    skip/ambiguidade (ver §Tests).

## Missão

1. **Helper `_normalize_cpf_last2`** (puro, testável sozinho):
   - Input: `str | None`.
   - Output: `str | None` (2 dígitos se der pra extrair, senão None).
   - Aceita `"***.***.*61-34"`, `"617.502.811-34"`, `"61750281134"`,
     `"123-45"`, etc. Retorna None pra strings sem hífen + 2 dígitos
     no fim.

2. **Novo índice no `transform()`**: além de `cargo_source_by_name`,
   construir `cargo_source_by_prefix` mapeando
   `" ".join(source.name_tokens[:k])` → lista de sources, pra todo
   `k in [2, len(tokens))` (k=len seria o próprio `name_exact`).
   Dedupe por element_id.

3. **Nova Phase 1.5** depois de `name_stripped`, antes de shadow_*:
   ```python
   for person in persons_full:  # :Person com CPF + UF='GO' + sq_candidato
       if person["element_id"] in attached_persons:
           continue  # já consumido por Phase 1 mais forte
       tokens = person["name_normalized"].split()
       if len(tokens) < 3:
           continue  # nome curto = Phase 1 já resolveu ou skippa
       # Testa prefixes de tamanho crescente (2..len(tokens)-1):
       # fica com o match mais longo que der único + CPF-check OK.
       best_match = None
       for k in range(len(tokens) - 1, 1, -1):  # desce de len-1 até 2
           prefix = " ".join(tokens[:k])
           candidates = cargo_source_by_prefix.get(prefix, [])
           if not candidates:
               continue
           # Filtra por CPF last-2 quando disponível dos dois lados
           p_last2 = _normalize_cpf_last2(person["cpf"])
           filtered = [
               c for c in candidates
               if p_last2 is None
                  or _normalize_cpf_last2(c["cpf"]) is None
                  or _normalize_cpf_last2(c["cpf"]) == p_last2
           ]
           if len(filtered) == 1:
               best_match = filtered[0]
               break
           if len(filtered) > 1:
               audit: type="cargo_prefix_ambiguous",
                     person=person["element_id"],
                     candidates=[c["element_id"] for c in filtered],
                     prefix=prefix, k=k
               break  # não desce — ambiguidade em k maior é ambiguidade
       if best_match:
           _attach_source(
               canonical_id=best_match["canonical_id"],
               node=person,
               method="cargo_prefix_of_person",
               confidence=0.75,
           )
           attached_persons.add(person["element_id"])
   ```

4. **Gating explícito**:
   - `len(tokens) >= 3` pro Person alvo. Nome com 2 tokens (caso
     raro: ex-político sem sobrenome paterno registrado) cai em
     `name_exact` ou `shadow_*`.
   - Prefix mínimo de 2 tokens — `k in [2, len(tokens)-1]`.
   - CPF last-2 é **requisito** quando ambos lados têm CPF e o prefix
     bate >1 candidato. É **opcional** quando o prefix já é único
     (conservador: minimiza falso negativo).

5. **Audit log ganha 2 tipos novos**:
   - `cargo_prefix_of_person_match` (positivo, conf 0.75).
   - `cargo_prefix_ambiguous` (skip, com candidatos + CPF last-2
     de cada pra review humana decidir se vira override CSV via TODO 01).

6. **Tests** (em `TestCargoPrefixOfPerson`):
   - `test_prefix_match_unico_com_cpf_coincidente`:
     FL "GUSTAVO GAYER" cpf `***.***.*61-34` + Person "GUSTAVO GAYER
     MACHADO DE ARAUJO" cpf `617.502.811-34` → attach conf=0.75.
   - `test_prefix_match_unico_sem_cpf_no_fl`: FL "FLAVIA MORAIS"
     sem cpf + Person "FLAVIA DE SOUZA MORAIS" cpf qualquer → attach
     conf=0.75 (CPF last-2 é opcional quando único).
   - `test_prefix_match_ambiguo_cpf_desempata`:
     FL "JOSE SILVA" cpf `***.***.*00-34` + 3 Persons "JOSE SILVA
     PEREIRA"/"JOSE SILVA SANTOS"/"JOSE SILVA ALMEIDA" com CPFs
     diferentes; só o PEREIRA tem last-2 `34` → attach PEREIRA, audit
     descarta os outros 2 como `cargo_prefix_ambiguous_resolved_by_cpf`
     (ou o simples `cargo_prefix_ambiguous` se preferir flat).
   - `test_prefix_match_ambiguo_mesmos_check_digits_skip`:
     3 candidatos todos com last-2 `34` → `cargo_prefix_ambiguous`
     audit + skip (ninguém attach). Conservador.
   - `test_prefix_nao_desbanca_name_exact`:
     Person "GUSTAVO GAYER" (2 tokens, igual ao FL) → Phase 1 pega
     com `name_exact` conf=0.90; Phase 1.5 não tenta porque já foi
     attachado.
   - `test_len_tokens_menor_que_3_nunca_attach`:
     Person "JORGE KAJURU" (2 tokens) — Phase 1.5 skippa porque tokens
     <3; cai pra shadow ou outra regra.

7. **Stats no log final de `transform()`**:
   ```
   [entity_resolution_politicos_go] transformed: N clusters, M edges
   (cpf_exact=x, name_exact=y, name_stripped=z, cargo_prefix_of_person=w,
   shadow_exact=a, shadow_prefix=b), K audit entries
   ```

## Estimativa de impacto (a medir)

Dry-run atual (pre-implementação): 17 `:FederalLegislator` GO com foto,
15 casados via `name_exact`, 2 não casaram (Gayer + Alexandrino).
Expectativa qualitativa:

- **Otimista** (+15 matches): pega todos os deputados federais com
  nome parlamentar curto + completa coverage das próximas legislaturas.
- **Realista** (+5-10 matches): a maior parte já casa por name_exact
  ou cpf_exact; prefix resolve só o longo tail.
- **Risco de falso positive**: 0 se os tests §6 passarem. Conservadorismo
  do `len(tokens) >= 3` + CPF check-digits já segura.

Depois de merger, re-rodar:
```
uv run bracc-etl run --source entity_resolution_politicos_go
python scripts/refresh_photos.py --only propagacao_fotos_person
```

Re-contar na Neo4j:
```cypher
MATCH (p:Person) WHERE p.uf='GO' AND p.foto_url IS NOT NULL RETURN count(p)
```

## Trade-offs explicitamente aceitos

- **Confidence 0.75 < 0.90 de name_exact**: frontend pode opcionalmente
  marcar matches <0.85 com badge "match automático inferido" — mas não
  é bloqueio desta TODO. Prompt 01 (override CSV) cobre operadores que
  quiserem promover match pra 1.0.
- **Complexidade no transform()**: O(persons × max_prefix_k × clusters).
  persons_full ~4k, max_prefix_k ~5, clusters ~20 = 400k ops. Trivial.
- **Ordem de preferência**: Phase 1.5 roda depois de name_stripped; se
  `name_stripped` resolveu, não tenta. Isso evita conflito de
  confidências (stripped=0.85 > prefix=0.75). Documentar em
  `docs/entity_resolution.md`.
- **CPF last-2 como sinal é fraco estatisticamente**: 1/100 colisão.
  Com prefix + UF + `sq_candidato` combinados, o conjunto de Persons
  candidatos é pequeno (<20 por prefix plausível); 1/100 colisão no
  subset dá ~0.2% risco. Aceitável.
- **Este fix precede o `propagacao_fotos_person` rumo a CanonicalPerson**:
  quando o CanonicalPerson existir pra todos, o `propagacao_fotos_person`
  pode ser refactored pra seguir `:REPRESENTS` em vez de match por
  `name`. Essa refactor é um TODO separado — esta TODO só fecha o gap
  de match no entity resolution.

## Escopo não-objetivo

- Não implementar Jaro-Winkler / Levenshtein / fuzzy. Esses ficam pra
  casos que nem prefix nem CPF check-digits resolvem — cai em TODO 01
  (override CSV) via review humana do audit log.
- Não mudar `propagacao_fotos_person` — continua como está; quando o
  CanonicalPerson layer ganhar cobertura completa, refactor separado.
- Não mudar escopo GO (`_TARGET_UF = "GO"` permanece).
- Não resolver os honoríficos cortados ("DR. ISMAEL ALEXANDRINO" →
  Person correspondente pode nem existir no TSE). Isso é caso do
  TODO 01 (override manual) ou do próprio `name_stripped` existente.

## Cadência de revisão do audit log

Mensal + após cada re-ingestão de `camara_politicos_go`. Queries úteis:

```cypher
// Novos matches prefix (sanity check)
MATCH (cp:CanonicalPerson)-[r:REPRESENTS {method: 'cargo_prefix_of_person'}]->(p:Person)
RETURN cp.canonical_id, cp.display_name, p.name, p.cpf, r.confidence
ORDER BY r.added_at DESC LIMIT 20

// Ambiguidades que merecem override via TODO 01
// (ler data/entity_resolution_politicos_go/audit_*.jsonl com type=cargo_prefix_ambiguous)
```
