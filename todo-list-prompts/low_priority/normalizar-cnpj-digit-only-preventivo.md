# Normalização preventiva — 375 :Company com CNPJ em dígito-puro

> Audit em 2026-05-02 confirmou que **0 desses 375 são duplicatas
> hoje** (não há contraparte formatada no grafo). Mas se algum
> pipeline futuro ingerir essas mesmas entidades com CNPJ formatado
> (`XX.XXX.XXX/XXXX-XX`), vão virar :Company órfãos paralelos.

## Estado em 2026-05-02

```
Format     Count
formatted  347.241   ← convenção do grafo
digits     375       ← lixo legado (municípios, fundos públicos)
```

Sample dos 375:
- `00005959000110` — MUNICIPIO DE INDIARA
- `00027722000130` — MUNICIPIO DE SANTA ISABEL
- `00079830000156` — MUNICIPIO DE PANAMA
- `00463568000149` — FUNDO MUNICIPAL DE SAUDE
- `00544963000156` — FUNDO ESTADUAL DE SAUDE
- `01038829000146` — FUNDO ESTADUAL DE ASSISTENCIA SOCIAL
- ...

Provavelmente vieram de pipelines `comprasnet`/`pncp_go`/`pgfn` antes
do fix `format_cnpj`.

## Cypher de normalização (preventivo, MERGE-safe)

```cypher
MATCH (c1:Company)
WHERE NOT (c1.cnpj CONTAINS '.' OR c1.cnpj CONTAINS '/' OR c1.cnpj CONTAINS '-')
  AND size(c1.cnpj) = 14
WITH c1,
     substring(c1.cnpj,0,2)+'.'+substring(c1.cnpj,2,3)+'.'+substring(c1.cnpj,5,3)
       +'/'+substring(c1.cnpj,8,4)+'-'+substring(c1.cnpj,12,2) AS formatted
SET c1.cnpj = formatted
RETURN count(c1) AS normalizadas;
```

## Pré-checks antes de rodar

1. Re-confirmar que ainda há **0 pares duplicados** (caso novo pipeline
   tenha ingerido as mesmas entidades com formato pontuado entre
   2026-05-02 e a data do TODO):
   ```cypher
   MATCH (c1:Company)
   WHERE NOT (c1.cnpj CONTAINS '.')
   WITH c1, substring(c1.cnpj,0,2)+'.'+substring(c1.cnpj,2,3)
            +'.'+substring(c1.cnpj,5,3)+'/'+substring(c1.cnpj,8,4)
            +'-'+substring(c1.cnpj,12,2) AS formatted
   OPTIONAL MATCH (c2:Company {cnpj: formatted}) WHERE c2 <> c1
   RETURN count(c2) AS pares_duplicados;
   ```
   Se `pares_duplicados > 0`, **NÃO rodar SET direto** — precisa MERGE+
   transferência de rels antes do delete (mais complexo).
2. Confirmar nenhum dos 375 tem rels que apontam pra ele com chave em
   formato dígito (rels que usam `cnpj` como FK também viram órfãs):
   ```cypher
   MATCH (c:Company)<-[r]-()
   WHERE NOT (c.cnpj CONTAINS '.') AND r.cnpj IS NOT NULL
   RETURN count(r);
   ```

## Por que está em low_priority

- Não há duplicatas hoje (audit 2026-05-02)
- A heurística de classificação no PWA (`tipo_entidade='comite_campanha'`)
  já não depende do formato do CNPJ
- Os 375 são entidades públicas (municípios), que raramente são
  re-ingeridas com formato diferente

Sobe pra medium se algum re-import de comprasnet/pncp/RFB criar pares
duplicados (basta re-rodar a query do pré-check 1).
