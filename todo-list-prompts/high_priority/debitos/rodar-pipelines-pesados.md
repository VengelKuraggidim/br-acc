# Rodar pipelines pesados — `pgfn` + `comprasnet`

## Estado 2026-04-24 06:30 — download comprasnet em progresso, pgfn pronto

Sessão overnight 2026-04-24 disparou o download de comprasnet em
background (BG `bnelxyias`, log em `/tmp/comprasnet_dl.log`):

- 2019/2020: `[]` (pre-PNCP, esperado).
- 2021: 9 MB ✅
- 2022: 69 MB ✅
- 2023: 406 MB ✅
- **2024: ~27% (página ~590/2175 às 06:30, ETA mais ~80 min só pra fechar 2024).**
- 2025/2026: pendentes; total ETA da run ~3-4h mais.

`data/pgfn/`: **54 CSVs GO-scoped já em disco** (mar/2024 → mar/2026,
SIDA_1 a SIDA_6, ~2 GB). Pronto pra ingestão.

### Quando a usuária acordar

1. Confirmar download terminou: `tail /tmp/comprasnet_dl.log` deve
   mostrar "Done" ou ausência de novas requests por ~10min. Se ainda
   estiver rodando 2025/2026, esperar.
2. Rodar **comprasnet** (fix OOM streaming já aplicado em commit
   `0d407d5`):
   ```bash
   cd /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/etl
   uv run python -m bracc_etl.runner run --source comprasnet \
     --neo4j-password changeme \
     --data-dir /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data \
     2>&1 | tee /tmp/comprasnet_ingest.log
   ```
   Peak RSS previsto ~3-5 GB; ETA ~40min.
3. Validar IngestionRun no Neo4j local:
   ```bash
   docker exec fiscal-neo4j cypher-shell -u neo4j -p changeme \
     "MATCH (r:IngestionRun {source_id:'comprasnet'}) RETURN r.run_id, r.status, r.rows_in, r.rows_loaded ORDER BY r.started_at DESC LIMIT 1"
   ```
4. Rodar **pgfn** (independente, dados já em disco):
   ```bash
   cd /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/etl
   uv run python -m bracc_etl.runner run --source pgfn \
     --neo4j-password changeme \
     --data-dir /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data \
     2>&1 | tee /tmp/pgfn_ingest.log
   ```
   `transform()` usa `iterrows()` (memo TODO) — ETA >1h. Dataset
   GO-only pode ser menor que o memo previu (1.2GB filtrado upstream).
5. Validar nodes/rels no grafo:
   ```cypher
   MATCH (c:Contract) WHERE c.source_id='comprasnet' RETURN count(c);
   MATCH (f:Finance) WHERE f.source_id='pgfn' RETURN count(f);
   ```

### Original


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
| `comprasnet` | ~6,4 GB (JSONs PNCP 2019-2026) | Dados crus já em `data/comprasnet/` (download completo até 2026-04-19). **OOM fix aplicado em commit `0d407d5`** — `extract()` agora enumera arquivos por ano, `run()` streama um ano de cada vez (peak ~3-5 GB RSS, não mais 17 GB). Seguro rodar numa janela dedicada. |

## Fixes aplicados (não bloqueantes)

### Counters `rows_in` / `rows_loaded` (commit anterior)

Mesmo sem rodar, o fix de `rows_in` / `rows_loaded` **foi aplicado**
aos dois pipelines (commit `fix(etl): 6 pipelines — reporta
rows_in/loaded no IngestionRun`). Próxima vez que alguém rodar,
contadores já aparecem no IngestionRun sem precisar de outro patch.

Pontos de fix:
- `etl/src/bracc_etl/pipelines/pgfn.py::transform()` — acumula
  `total_rows_scanned` nos chunks e seta `self.rows_in` no final.
- `etl/src/bracc_etl/pipelines/pgfn.py::load()` — `self.rows_loaded +=`
  retorno de `loader.load_nodes("Finance", ...)`.

### Comprasnet OOM + provenance retrofit (commit `0d407d5`)

Após tentar rodar `comprasnet` em 2026-04-19, `bracc-etl` foi
OOM-killed a 17 GB RSS (`json.loads()` de 6,4 GB cru inflando em dicts
Python). Fix em duas frentes:

- **Provenance retrofit**: `transform()` agora stampa Contract/Company
  nodes + VENCEU/REFERENTE_A rels via `self.attach_provenance()`
  (alinhando com padrão dos 10 GO retrofitados em 2026-04-18).
- **Streaming per-year**: `_stream_json_array()` (stdlib
  `json.JSONDecoder.raw_decode` com buffer deslizante) substitui
  `json.loads(read_text)`. `run()` override processa cada
  `{year}_contratos.json` em ciclo independente extract→transform→load,
  flushando Neo4j e limpando working sets entre anos. Peak RSS
  previsto: ~3-5 GB (limitado pelo maior ano, 2025 = 3,2 GB cru).
  `IngestionRun` continua sendo 1 run lógico cobrindo todos os anos.

29 testes comprasnet passam (21 originais + 5 provenance + 3
streaming/per-year).

## Pré-condição: download dos dados crus (2026-04-22)

**Atualização 2026-04-22:** `data/comprasnet/` e `data/pgfn/` estão
**vazios** no dev local de vengel-kuraggidim-sitagi. O doc original foi
redigido em ambiente `/home/alladrian/` com dados já baixados — não
é o estado atual. Antes de rodar, precisa disparar o download (6,4 GB
comprasnet + ~1,2 GB pgfn). O download em si é o maior gargalo agora,
não a ingestão — não é mais quick win.

Caminho correto (ajustado pra user vengel): substituir `/home/alladrian/`
por `/home/vengel-kuraggidim-sitagi/` em todos os comandos abaixo.

## Como rodar quando houver janela

### Pré-checks

```bash
# Neo4j local de pé?
docker ps --filter name=fiscal-neo4j --format '{{.Status}}'

# Dados crus em disco? (2019-2026 esperados)
ls -lh /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data/comprasnet/

# Senha do Neo4j local
NEO4J_PW="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)"

# RAM/swap livres (o fix de streaming não precisa muito, mas checar)
free -h
```

### Run — comprasnet (pós-fix OOM)

```bash
cd /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/etl

# Opcional: smoke test rápido com limit antes do run completo
uv run python -m bracc_etl.runner run --source comprasnet \
  --neo4j-password "$NEO4J_PW" \
  --data-dir /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data \
  --limit 100 2>&1 | tee /tmp/comprasnet_smoke.log

# Run completo — 6 ciclos curtos (2019/2020 no-op, 2021-2026 com dados).
# Monitorar RSS: peak previsto ~3-5 GB. Se passar de 10 GB, algo errado.
uv run python -m bracc_etl.runner run --source comprasnet \
  --neo4j-password "$NEO4J_PW" \
  --data-dir /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data 2>&1 | tee /tmp/comprasnet.log &

# Monitoramento em outra aba
watch -n 5 "ps aux | grep bracc-etl | grep -v grep | awk '{print \$6/1024/1024\" GB \"\$11}'"
```

### Run — pgfn (sem fix OOM ainda — esperar >1h, um chunk de cada vez)

```bash
cd /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/etl
uv run python -m bracc_etl.runner run --source pgfn \
  --neo4j-password "$NEO4J_PW" \
  --data-dir /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data 2>&1 | tee /tmp/pgfn.log
```

### Verificação pós-run

```bash
# IngestionRun (1 linha por run completo; comprasnet agrega todos os anos)
docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
  "MATCH (r:IngestionRun {source_id: 'comprasnet'}) RETURN r.run_id, r.status, r.rows_in, r.rows_loaded ORDER BY r.started_at DESC LIMIT 1"

# Counts de grafo
docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
  "MATCH (n:Contract) WHERE n.source_id = 'comprasnet' RETURN count(n) AS contracts"
docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
  "MATCH ()-[r:VENCEU]->() WHERE r.source_id = 'comprasnet' RETURN count(r) AS venceu_rels"
docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
  "MATCH ()-[r:REFERENTE_A]->() WHERE r.source_id = 'comprasnet' RETURN count(r) AS referente_a_rels"

# Sample stamped provenance (validar contrato)
docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
  "MATCH (c:Contract {source_id: 'comprasnet'}) RETURN c.contract_id, c.source_id, c.source_record_id, c.source_url, c.run_id LIMIT 3"
```

### Se crashar de novo

Se `bracc-etl` ainda OOM apesar do fix per-year, a última linha de defesa é chunked-load
dentro de cada ano (quebrar `contract_nodes`/`company_nodes` em
batches de N=50k, flushar, descartar). Isso quebra o contrato
3-fase do `Pipeline` base — foi descartado na sessão inicial como
desnecessário. Se revisitar, ver opção 3 na proposta (commit
anterior, conversa 2026-04-19).

## Melhoria futura (opcional)

`pgfn.transform()` usa `filtered.iterrows()` dentro do loop de chunks
— principal gargalo de performance. Refatorar pra operações
vetorizadas pandas (montar os dicts via `.to_dict(orient='records')`
após filtrar) provavelmente derruba de >1h pra poucos minutos.
Não é blocker: pipeline funciona, só é lento. Deixa pra quando alguém
tocar em pgfn por outro motivo.
