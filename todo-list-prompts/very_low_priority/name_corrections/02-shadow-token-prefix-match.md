# Shadow Person attach via token-prefix conservador — ⏳ PENDENTE (2026-04-18)

> Estende a Phase 2 do `entity_resolution_politicos_go` pra anexar
> shadow Persons cujo nome é prefix exato de um nome de cluster já
> resolvido. Caso canônico: shadow `"JORGE KAJURU"` (sem CPF, sem UF,
> só `name`) que deveria juntar no `canon_senado_5895` (Senator full
> name: `"JORGE KAJURU REIS DA COSTA NASSER"`) mas não anexa com a
> regra atual `shadow_name_exact` (exige igualdade total).

## Contexto
Dry-run em 2026-04-18 (`docs/entity_resolution.md` §3.5): dos 1569
shadow Persons (uf=NULL, cpf=NULL, só `name`), só alguns poucos bateram
por nome exato. 1553 ficaram em `shadow_no_match` no audit log.

Sub-conjunto interessante: shadows cujo `name_normalized` é **prefix
proper (<) do name_normalized** de um nó-fonte já no cluster. Exemplo
real:

- Cluster `canon_senado_5895` tem source :Senator `"JORGE KAJURU REIS DA
  COSTA NASSER"` (normalizado).
- Shadow `"JORGE KAJURU"` (normalizado: `"JORGE KAJURU"`, 2 tokens).
- `"JORGE KAJURU".split()` é prefix de `"JORGE KAJURU REIS DA COSTA
  NASSER".split()` → 2 tokens dos 6 batem do início.

Conservador por definição: cai fora se >1 cluster tem source cujo
prefix bate o shadow. Ex.: shadow `"JOSE SILVA"` seria prefix de muitos
políticos → skip + audit. Isso garante que a regra nunca mescla nós de
pessoas diferentes.

## Por que não substring-anywhere

Substring match (`shadow IN fullname` em qualquer posição) explode com
as 2265 colisões de nome no grafo. Exemplo do risco:

- Shadow `"SILVA"` apareceria em "JOÃO DA SILVA", "MARIA SILVA", "PEDRO
  SILVA COSTA" — cada um um Person diferente.
- Mesmo com filtro "exatamente 1 cluster bate", o shadow `"SILVA"`
  provavelmente não resolveria (>1 cluster) → skip, aceitável. Mas
  shadow `"DA COSTA"` (se existir) poderia bater coincidentemente 1
  cluster e merger errado.

Prefix é mais seguro porque exige que o shadow seja uma **abreviação do
início** do nome completo — fenômeno linguístico real (DOU cita "JORGE
KAJURU" como forma curta, não "KAJURU NASSER").

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py`
  - `transform()` linhas de Phase 2 (atualmente só `shadow_name_exact`
    em `cluster_names[name_norm]`).
  - `_attach_source(method="shadow_prefix_match", confidence=0.70)`
    ganha novo método. Conf 0.70 porque tem mais ruído potencial que
    `shadow_name_exact` (0.80).
- `etl/tests/test_entity_resolution_politicos_go.py`:
  - `TestShadowPrefixMatch` nova classe com happy path + 3 casos
    de skip (prefix bate múltiplos clusters; shadow tem só 1 token;
    shadow já foi consumido por regra mais forte).

## Missão

1. **Novo índice no `transform()`**: além de `cluster_names[name_norm]`,
   construir `cluster_prefixes: dict[str, list[str]]` mapeando
   `" ".join(name_tokens[:k])` → `[canonical_id...]` para todo k em
   `[2, len(tokens)-1]` (k=2 pega "JORGE KAJURU"; k=len(tokens) seria o
   próprio name_exact, já coberto). Dedupe por canonical_id dentro da
   lista.

2. **Nova Phase 2.5** após `shadow_name_exact`:
   ```python
   for shadow in persons_shadow:
       if shadow["element_id"] in attached_shadows:
           continue  # já consumido por shadow_name_exact
       tokens = shadow["name_normalized"].split()
       if len(tokens) < 2:
           continue  # 1 token é muito genérico (sobrenome sozinho)
       prefix = " ".join(tokens)
       candidate_ids = sorted(set(cluster_prefixes.get(prefix, [])))
       if len(candidate_ids) == 1:
           _attach_source(canonical_id=candidate_ids[0],
                          node=shadow, method="shadow_prefix_match",
                          confidence=0.70)
           attached_shadows.add(shadow["element_id"])
       elif len(candidate_ids) > 1:
           audit: type="shadow_prefix_ambiguous", candidates=candidate_ids
       # else: cai pro shadow_no_match já existente
   ```

3. **Gating explícito**: só ativa se `tokens >= 2`. Shadow
   `"SILVA"` (1 token) nunca entra nesta regra — evita catástrofe
   semântica. Tokens com <3 caracteres (DA, DE, DO, DOS, DAS, E) são
   contados normalmente porque `_normalize_name()` já preserva eles e
   a prefix-match respeita ordem.

4. **Audit log ganha 2 tipos novos**:
   - `shadow_prefix_match` (informativo, fora de erro).
   - `shadow_prefix_ambiguous` (pulo — review humana decide se vira
     override manual via TODO 01).

5. **Tests** (em `TestShadowPrefixMatch`):
   - `test_shadow_curto_matcha_cluster_unico` — shadow "JORGE KAJURU" +
     Senator "JORGE KAJURU REIS ..." → attach conf=0.70.
   - `test_shadow_prefix_multiplos_clusters_skip` — shadow "JOAO SILVA"
     + 3 clusters "JOAO SILVA PEREIRA"/"JOAO SILVA SANTOS"/... → skip
     + audit `shadow_prefix_ambiguous`.
   - `test_shadow_1_token_nunca_attach` — shadow "SILVA" (só sobrenome)
     → nunca vira match mesmo que só 1 cluster bate.
   - `test_shadow_prefix_nao_desbanca_name_exact` — shadow "JORGE
     KAJURU" co-existindo com shadow "JORGE KAJURU REIS DA COSTA
     NASSER" exato; o exact consome primeiro, prefix vai pro outro.
   - `test_shadow_prefix_respeita_ordem_tokens` — shadow "KAJURU
     JORGE" (invertido) NÃO bate prefix de "JORGE KAJURU ...".

6. **Stats no log final de transform()**:
   ```
   [entity_resolution_politicos_go] transformed: N clusters, M edges
   (cpf_exact=x, name_*=y, name_stripped=z, shadow_exact=a,
   shadow_prefix=b), K audit entries
   ```
   Separar contadores por método dá visibilidade de quanto cada regra
   contribui.

## Estimativa de impacto (a medir)

Dry-run atual: 1553 shadows em `shadow_no_match`. Expectativa qualitativa
(a confirmar rodando a nova regra):

- **Otimista** (~10% resolvem): nomes curtos de políticos conhecidos que
  aparecem em DOU/inquéritos como forma abreviada (Kajuru, Flávia
  Morais, Iris Rezende, etc.).
- **Realista** (~2-5%): maior parte dos shadows é de pessoas não-políticas
  (autores de inquéritos, servidores mencionados em DOU) sem cluster
  canônico pra bater.
- **Zero**: se as colisões forem muito fortes, quase tudo cai em
  `shadow_prefix_ambiguous`. Aceitável — pior caso é zero regressão.

## Trade-offs explicitamente aceitos

- **Falsos negativos > falsos positivos**: prefix-only perde shadows
  como `"KAJURU"` (1 token) ou `"REIS NASSER"` (sufixo). Manda pro
  audit pra review. CLAUDE.md §3.
- **Complexidade no transform()**: O(shadows × clusters × tokens_max)
  no pior caso, mas cluster count é pequeno (~20), shadows ~1500,
  tokens_max ~6 — < 200k ops, trivial.
- **Confidence 0.70 < 0.80 de shadow_name_exact**: frontend pode
  opcionalmente destacar matches abaixo de 0.80 com um ícone de
  "dúvida" — mas não é bloqueio desta TODO.

## Escopo não-objetivo
- Não implementar Jaro-Winkler / fuzzy match. Se prefix não resolver,
  cai pra TODO 01 (override manual).
- Não mudar o escopo GO.
- Não refatorar Phase 1 (cargo ↔ Person) — essa continua como está.
