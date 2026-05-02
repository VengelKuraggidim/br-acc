# ALEGO — 3 StateLegislator sem pareamento ER

> Aberto em 2026-05-02 após batch ER ALEGO (memo
> `project_er_alego_batch_2026_05_02.md`). 16/19 pareados; sobram 3.

## Contexto

ALEGO não publica CPF dos deputados. O pareamento automático
StateLeg→Person TSE é por nome (token-match). Três StateLeg GO ficaram
sem candidato único após passe 1 (todos os tokens) e passe 2 (fuzzy +
apelido com prefixo profissional):

| StateLeg ALEGO | Problema |
|---|---|
| `BIA DE LIMA` | Zero candidatos no TSE GO Dep.Estadual com tokens BIA+LIMA |
| `DELEGADA FERNANDA` | 9 candidatos com FERNANDA/FERNANDO; ambíguo |
| `MAJOR ARAUJO` | 15+ candidatos com ARAUJO; ambíguo |

Sem o REPRESENTS, abrir o perfil pelo nome eleitoral cai no `:Person`
TSE → `is_estadual_go=False` → 0 verba indenizatória ALEGO no perfil.

## Ações

1. **Lookup manual**: confirmar nome civil de cada um (TSE/Diário Oficial
   ALEGO/imprensa) e localizar o `:Person` correspondente no Neo4j local.
   - `BIA DE LIMA` (PT-GO) → confirmar se é BEATRIZ ou similar; pode estar
     ingerido como vereadora/2018 sem cargo_tse_2022 = Dep.Estadual.
   - `DELEGADA FERNANDA` (REPUBLICANOS-GO) → restringir por partido +
     foto.
   - `MAJOR ARAUJO` (PL-GO) → restringir por partido + foto.

2. **Aplicar REPRESENTS** no padrão do batch:
   ```cypher
   MATCH (c:CanonicalPerson) WHERE elementId(c) = $canon
   MATCH (p:Person) WHERE elementId(p) = $pid
   MERGE (c)-[r:REPRESENTS]->(p)
     ON CREATE SET r.source = 'er_alego_reconcile_2026_05_02',
                   r.method = 'manual_lookup',
                   r.ingested_at = datetime()
   ```

3. **Validação**: abrir o `:Person` no `/politico/{id}` e confirmar
   `total_despesas_gabinete_fmt > 0` com aviso ALEGO.

## Notas

- Reverter o batch inteiro: `MATCH ()-[r:REPRESENTS {source:'er_alego_reconcile_2026_05_02'}]->() DELETE r`
- Filtro do passe automático: `:Person {uf:'GO'}` com `cargo_tse_*`
  contendo "DEPUTADO ESTADUAL" e CPF preenchido. Se um dos três só
  apareceu como vereador/suplente em 2022, o filtro descartou — relaxar
  pra qualquer cargo TSE 2022 GO pode trazer candidato.
