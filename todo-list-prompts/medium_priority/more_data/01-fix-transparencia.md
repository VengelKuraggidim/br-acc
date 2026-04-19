# Investigar e corrigir pipeline `transparencia` (Portal da Transparência federal)

## Contexto do projeto

Fiscal Cidadão é fork do `brunoclz/br-acc` focado em Goiás, mas o grafo
ingere dados **nacionais** de propósito (decisão de 2026-04-18 — ver
memória `project_go_scope_policy`). Políticos GO têm ligação com
contratos federais, empresas nacionais, etc; filtrar cedo perde as
conexões.

## Evidência do problema

No Neo4j local (`docker exec fiscal-neo4j cypher-shell -u neo4j -p ...`):

```
MATCH (r:IngestionRun) WHERE r.source_id IN ['transparencia', 'portal_transparencia']
RETURN r.source_id, r.status, r.rows_in, r.rows_loaded, r.started_at
```

Resultado em 2026-04-19:
- `portal_transparencia`, status=`loaded`, `rows_in=0`, `rows_loaded=0`, started 2026-04-18T00:06:13Z

No disco:
```
du -sh /home/alladrian/PycharmProjects/br-acc/data/transparencia/
# 1,9G
```

Pipeline `etl/src/bracc_etl/pipelines/transparencia.py` rodou, entrou no
`load()`, mas nem extraiu linha da fonte — `rows_in=0`. Ao mesmo tempo
tem 1.9 GB baixado em `data/transparencia/`. Algo está desconectado
entre o download e o que o extract procura.

## Duas hipóteses principais

1. **Path divergente**: pipeline procura arquivos em path diferente de
   `data/transparencia/*`. Conferir a constante de path no código.
2. **Schema mudou**: CSV da Receita/CGU teve colunas renomeadas, parser
   descarta tudo silenciosamente.
3. **Filtro restritivo**: pipeline filtra por UF/data e mapeamento está
   errado (ex: esperar `UF_ORGAO='GO'` quando na fonte é `SG_UF='GO'`).
4. **Source_id mismatch**: pipeline escreve `IngestionRun.source_id =
   'portal_transparencia'` mas a entrada no registry é `transparencia`.
   Isso é confirmado. Ver prompt `06-fix-source-id-alias.md` — coordenar.

## Missão

1. **Ler `etl/src/bracc_etl/pipelines/transparencia.py`** e entender:
   - Que diretório o `extract()` olha
   - Que schema CSV ele espera
   - Se tem filtro UF/data e onde

2. **Listar o que tem em `data/transparencia/`**:
   ```bash
   ls -la /home/alladrian/PycharmProjects/br-acc/data/transparencia/ | head -30
   ```

3. **Rodar localmente com verbose** pra ver onde zero:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc/etl
   NEO4J_PASSWORD="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
   uv run python -m bracc_etl.runner run --source transparencia --data-dir ../data 2>&1 | tee /tmp/transparencia.log
   ```
   Olhar logs: onde o pipeline desiste, ou se lê N linhas mas filtra tudo.

4. **Fixar o que estiver quebrado**. Possíveis caminhos:
   - Ajustar path de leitura
   - Atualizar schema (adicionar/remover colunas conforme Portal Transparencia atual)
   - Relaxar filtro UF se estiver restritivo demais (lembrando: escopo é
     nacional pro grafo, não só GO — ver guardrail no README desta pasta)
   - Corrigir `source_id` pra bater com registry (`transparencia`, não
     `portal_transparencia`)

5. **Rerodar** e verificar `IngestionRun.rows_loaded > 0` + nodes novos
   no grafo (ex: `:Contract`, `:Person`, `:Payment` — depende do que a
   pipeline cria).

6. **Test**: rodar `make test-etl` — se tests do pipeline existirem em
   `etl/tests/test_transparencia.py`, garantir que passam. Se mudou
   schema, atualizar fixtures.

7. **Pre-commit** + commit atômico. Mensagem estilo:
   `fix(etl): transparencia — extract agora lê <path X> / schema <Y>`

## Critério de "pronto"

- `IngestionRun` mais recente com `source_id='transparencia'`,
  `rows_loaded > 0`
- `/stats` endpoint mostra `contract_count` crescendo (ou o campo
  relevante — atualmente tá em 32037, deve aumentar)
- Badge da fonte muda de `parcial` pra `com_dados` na aba Fontes da PWA
  (pode precisar esperar 5min de cache ou reiniciar container API)

## Se travar

- Se o schema mudou e dados em `data/` estão em formato velho → pode ser
  necessário rodar `scripts/download_transparencia.py` pra redownload.
- Se `data/transparencia/` só tem um dataset parcial (só contratos, não
  servidores/emendas) → anotar o que está faltando e carregar o que dá.
  Não expandir escopo nesta sessão.
- Se bug é fora de escopo (ex: bug em `bracc_etl.base`) → parar e
  reportar ao humano.
