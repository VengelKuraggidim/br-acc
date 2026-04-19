# Executar pipelines baixados mas nunca ingeridos

## Contexto

Ver `README.md` desta pasta + `CLAUDE.md`. Seis pipelines têm dados
baixados em `data/` mas nunca chamaram `bracc-etl run` — nenhum
`IngestionRun` foi criado no grafo. Diferente dos prompts 01-04 (que
têm runs com zero rows, indicando bug no extract), aqui o pipeline
simplesmente nunca foi executado.

## Fontes neste prompt

| Source | Tamanho em disco | Valor esperado |
|---|---|---|
| `comprasnet` | 4.0 GB | Contratos federais (ComprasNet) — muito volume |
| `pgfn` | 1.9 GB | Dívida ativa federal — devedores CPF/CNPJ |
| `siop` | 379 MB | Orçamento federal — emendas por autor |
| `senado` | 97 MB | CEAPS (cota senatorial) — gastos de senadores |
| `tesouro_emendas` | 64 MB | Pagamento de emendas parlamentares |
| `camara` | 20 MB | CEAP (cota parlamentar) — gastos de deputados federais |

## Missão

**Ordem sugerida (menor → maior)**:

1. `camara` → 20 MB, rápido
2. `tesouro_emendas` → 64 MB
3. `senado` → 97 MB
4. `siop` → 379 MB
5. `pgfn` → 1.9 GB
6. `comprasnet` → 4.0 GB (por último — pode levar horas)

Pra cada um, nesta ordem:

1. **Confirmar que dados em `data/<source>/` existem** e têm formato
   esperado pelo pipeline (ver `etl/src/bracc_etl/pipelines/<source>.py`):
   ```bash
   ls -la /home/alladrian/PycharmProjects/br-acc/data/<source>/ | head
   ```

2. **Rodar a pipeline**:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc/etl
   NEO4J_PASSWORD="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
   uv run python -m bracc_etl.runner run --source <source> --data-dir ../data 2>&1 | tee /tmp/<source>.log
   ```

3. **Verificar no grafo**:
   ```
   MATCH (r:IngestionRun {source_id: '<source>'})
   RETURN r.status, r.rows_in, r.rows_loaded, r.started_at
   ORDER BY r.started_at DESC LIMIT 3
   ```
   Esperado: `status=loaded`, `rows_loaded > 0`.

4. **Se falhar com zero rows ou erro** → mesma investigação dos prompts
   01-04 (path, schema, filtro). Abrir arquivo de log e diagnosticar.
   Se não conseguir fixar, documentar em
   `docs/pipeline_status.md` e continuar pro próximo.

5. **Se sucesso** → próximo pipeline. Não precisa commit por pipeline —
   esta tarefa é **execução**, não código. Só commitar se descobrir e
   corrigir bug no caminho.

## Cuidados

- **Disco**: `comprasnet` carrega 4 GB. Neo4j pode inchar. Olhar espaço:
  ```bash
  docker exec fiscal-neo4j du -sh /data
  df -h /
  ```

- **Tempo**: pipelines grandes podem rodar horas. Se `comprasnet` não
  der pra terminar nesta sessão, parar no ponto, não matar processo
  violentamente — deixar terminar ou Ctrl+C limpo.

- **Paralelização**: NÃO rodar mais de 1 pipeline ETL em paralelo contra
  o mesmo Neo4j — write locks + memória. Rodar sequencial.

## Critério de "pronto"

- Cada uma das 6 fontes com `IngestionRun.rows_loaded > 0` e badge
  `com_dados` na aba Fontes
- OU documentada como débito em `docs/pipeline_status.md` se descobrir
  que está quebrada

## Se travar

Reportar estado parcial ao humano: quais rodaram, quais falharam, o
que foi descoberto. Não forçar conclusão completa — documentação honesta
vale mais.
