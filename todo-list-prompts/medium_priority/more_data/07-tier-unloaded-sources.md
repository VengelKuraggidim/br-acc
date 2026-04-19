# Tierar as ~100 fontes nunca baixadas nem rodadas

## Contexto

Ver `README.md` desta pasta + `CLAUDE.md`. Das 124 fontes catalogadas
em `docs/source_registry_br_v1.csv`, cerca de **100 nunca foram
baixadas nem rodadas**. Estão no registry porque escopo v1 do upstream
(`brunoclz/br-acc`) as previa, mas o fork Fiscal Cidadão nunca
materializou.

Esta tarefa é **planejamento, não código**. Produz um documento de
priorização pra nortear próximos sprints de ETL.

## Missão

1. **Listar fontes não-carregadas**. Critério:
   - `in_universe_v1 = true` no CSV
   - Sem `IngestionRun` no grafo
   - Sem diretório em `data/`

   Script:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc
   # Todas as in_universe
   awk -F',' 'NR>1 && $9=="true" {print $1}' docs/source_registry_br_v1.csv | sort > /tmp/universe.txt
   # Com IngestionRun
   docker exec fiscal-neo4j cypher-shell -u neo4j \
     -p "$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
     "MATCH (r:IngestionRun) RETURN DISTINCT r.source_id" \
     | tail -n +2 | tr -d '"' | sort > /tmp/ran.txt
   # Com dados em disco
   ls /home/alladrian/PycharmProjects/br-acc/data/ > /tmp/downloaded.txt
   # Fontes não-carregadas
   comm -23 /tmp/universe.txt <(cat /tmp/ran.txt /tmp/downloaded.txt | sort -u)
   ```

2. **Para cada fonte não-carregada, avaliar**:
   - **Impacto pra fiscalização GO**: alto (`sanctions`, `pep_cgu`,
     `ceaf`, `dou`, `ibama`, `datajud`) vs baixo (TCEs de outros
     estados, portais estaduais não-GO).
   - **Custo de carga**: pequeno (API JSON) vs médio (CSV GB) vs grande
     (40M+ registros, dataset completo).
   - **Blocker conhecido**: alguns têm `notes` no registry indicando
     form-wall, CAPTCHA, arquivos `.7z` sem lib no Python, etc.
   - **Pipeline existe?**: `ls etl/src/bracc_etl/pipelines/<source>.py`.
     Se não, é trabalho maior (implementar + testar).

3. **Produzir matriz de priorização** em
   `/home/alladrian/PycharmProjects/br-acc/todo-list-prompts/medium_priority/more_data/07-priorizacao-tier.md`:

   Formato sugerido:

   ```markdown
   # Priorização das fontes não-carregadas

   ## Tier P1 — alto impacto, baixo custo (fazer primeiro)
   | Source | Por quê | Custo estimado |
   |---|---|---|
   | sanctions | Cruzar empresas sancionadas com contratos GO | ~30min, CSV pequeno |
   | ... | ... | ... |

   ## Tier P2 — alto impacto, alto custo
   ...

   ## Tier P3 — baixo impacto (adiar)
   ...

   ## Tier PX — blocker conhecido (débito documentado)
   ...
   ```

4. **Não criar prompts de execução** pra cada uma — objetivo é só o
   documento de tier. Sprints futuros criam prompts específicos usando
   esse tier como guia.

## Critério de "pronto"

Arquivo `07-priorizacao-tier.md` existe, cobre as ~100 fontes, cada uma
atribuída a um tier com razão. Commit único:
`docs: tier das fontes GO nao carregadas`.

## Cuidados

- **Não inventar impacto**. Se você não sabe o que uma fonte entrega
  (ex: `carf_tax_appeals`), marcar como "pesquisar" e seguir — não chutar.
- **Respeitar débito existente**: algumas fontes já têm prompt em
  `todo-list-prompts/high_priority/` (ex: `07-backfill-cnae-cnpj.md`,
  `10-teto-gastos-campanha.md`, `caged`, `rais`). Cross-referenciar,
  não duplicar.
- **Tom editorial neutro** (ver `make neutrality`): descrever fontes
  pelo que entregam, não pelo que "poderiam expor".

## Se travar

Se descobrir que a estrutura de tiering precisa de input humano
(decisão editorial sobre prioridade), parar e listar as ambiguidades
num bloco "Decisões pendentes" no arquivo. Humano resolve, tu continua.
