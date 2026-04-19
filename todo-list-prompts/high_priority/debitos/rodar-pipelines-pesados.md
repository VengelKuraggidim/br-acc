# Rodar pipelines pesados — `pgfn` + `comprasnet`

## Contexto

Prompt `05-run-downloaded-pipelines.md` pedia re-executar 6 pipelines
abandonadas mid-run pra popular `rows_in` / `rows_loaded` no
IngestionRun (bug conhecido dos prompts 01-03; fix trivial no padrão
dos commits `ee3e973`, `8456d3f`, `bf06b37`, `4ed081f`, `3345535`,
`4721b53`, `55490f7`).

Escopo ajustado: 4 pipelines leves (<400 MB) rodaram nesta sessão
(`camara`, `tesouro_emendas`, `senado`, `siop`) e carimbaram IngestionRun
com contadores corretos. Os **2 pesados** ficam como débito nesta nota.

## Fontes afetadas

| Source | Tamanho | Razão do diferimento |
|---|---|---|
| `pgfn` | ~1.2 GB (SIDA CSVs, multi-arquivo) | `transform()` usa `iterrows()` em ~10M rows; já confirmado em sessão anterior que leva >1h e o Agent foi morto mid-run. Contadores nunca chegaram a serem gravados. |
| `comprasnet` | ~4 GB (JSONs PNCP) | Volume bruto grande; carga histórica cobre anos de procurement federal. Não roda em janela de cron 10 min. |

## Fix aplicado (não bloqueante)

Mesmo sem rodar, o fix de `rows_in` / `rows_loaded` **foi aplicado**
aos dois pipelines nesta sessão (commit `fix(etl): 6 pipelines —
reporta rows_in/loaded no IngestionRun`). Próxima vez que alguém rodar,
contadores já aparecem no IngestionRun sem precisar de outro patch.

Pontos de fix:
- `etl/src/bracc_etl/pipelines/pgfn.py::transform()` — acumula
  `total_rows_scanned` nos chunks e seta `self.rows_in` no final.
- `etl/src/bracc_etl/pipelines/pgfn.py::load()` — `self.rows_loaded +=`
  retorno de `loader.load_nodes("Finance", ...)`.
- `etl/src/bracc_etl/pipelines/comprasnet.py::extract()` —
  `self.rows_in = len(all_records)` após carregar JSONs.
- `etl/src/bracc_etl/pipelines/comprasnet.py::load()` —
  `self.rows_loaded +=` retorno de `loader.load_nodes("Contract", ...)`.

## Como rodar quando houver janela

```bash
# Descobrir senha do Neo4j local
NEO4J_PW="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)"

# pgfn — esperar >1h, NÃO rodar em cron curto
cd etl
uv run python -m bracc_etl.runner run --source pgfn \
  --neo4j-password "$NEO4J_PW" \
  --data-dir /home/alladrian/PycharmProjects/br-acc/data 2>&1 | tee /tmp/pgfn.log

# comprasnet — volume grande, reservar sessão dedicada
uv run python -m bracc_etl.runner run --source comprasnet \
  --neo4j-password "$NEO4J_PW" \
  --data-dir /home/alladrian/PycharmProjects/br-acc/data 2>&1 | tee /tmp/comprasnet.log
```

Depois, confirmar contadores:

```bash
docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
  "MATCH (r:IngestionRun {source_id: 'pgfn'}) RETURN r.run_id, r.status, r.rows_in, r.rows_loaded ORDER BY r.started_at DESC LIMIT 1"
```

## Melhoria futura (opcional)

`pgfn.transform()` usa `filtered.iterrows()` dentro do loop de chunks
— principal gargalo de performance. Refatorar pra operações
vetorizadas pandas (montar os dicts via `.to_dict(orient='records')`
após filtrar) provavelmente derruba de >1h pra poucos minutos.
Não é blocker: pipeline funciona, só é lento. Deixa pra quando alguém
tocar em pgfn por outro motivo.
