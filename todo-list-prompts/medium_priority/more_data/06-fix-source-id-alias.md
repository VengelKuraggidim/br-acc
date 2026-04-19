# Normalizar `source_id` entre pipelines e registry

## Contexto

Ver `README.md` desta pasta + `CLAUDE.md`. Alguns pipelines escrevem
`IngestionRun.source_id` com nome divergente do `source_id` canônico
no registry (`docs/source_registry_br_v1.csv`). Isso quebra
rastreabilidade e força aliases ad-hoc na camada API.

## Evidência do problema

Comparando o que o grafo reporta com o registry:

| Graph (`IngestionRun.source_id`) | Registry (`source_id`) |
|---|---|
| `portal_transparencia` | `transparencia` |
| `tribunal_superior_eleitoral` | `tse` |

A camada API já tem workaround em
`api/src/bracc/services/sources_public_service.py::_GRAPH_TO_REGISTRY_ALIAS`:
```python
_GRAPH_TO_REGISTRY_ALIAS = {
    "portal_transparencia": "transparencia",
    "tribunal_superior_eleitoral": "tse",
}
```

Isso é remendo — o correto é o pipeline escrever `source_id` canônico
desde a origem. Aliases são difícil de manter (esquecer de adicionar
quando nova pipeline divergir).

## Missão

1. **Ler pipelines divergentes**:
   - `etl/src/bracc_etl/pipelines/transparencia.py`
   - `etl/src/bracc_etl/pipelines/tse.py`

   Procurar onde `source_id` (ou `self.name`) é definido. Deve ser
   atributo da classe ou parâmetro do `super().__init__(...)`.

2. **Corrigir pra bater com registry**:
   - `transparencia.py`: `source_id = "transparencia"` (não `portal_transparencia`)
   - `tse.py`: `source_id = "tse"` (não `tribunal_superior_eleitoral`)

3. **Varredura**: rodar para achar outras divergências:
   ```bash
   docker exec fiscal-neo4j cypher-shell -u neo4j \
     -p "$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
     "MATCH (r:IngestionRun) RETURN DISTINCT r.source_id ORDER BY r.source_id"
   ```
   Comparar com registry:
   ```bash
   awk -F',' 'NR>1 && $9=="true" {print $1}' \
     /home/alladrian/PycharmProjects/br-acc/docs/source_registry_br_v1.csv | sort
   ```
   Identificar outros pares divergentes. Corrigir todos.

4. **Migração retrospectiva** dos `IngestionRun` antigos:
   ```cypher
   MATCH (r:IngestionRun {source_id: 'portal_transparencia'})
   SET r.source_id = 'transparencia'

   MATCH (r:IngestionRun {source_id: 'tribunal_superior_eleitoral'})
   SET r.source_id = 'tse'
   ```
   (Adicionar pares descobertos no step 3.)

5. **Remover alias na API** em
   `api/src/bracc/services/sources_public_service.py`:
   - Apagar `_GRAPH_TO_REGISTRY_ALIAS` ou deixar vazio com comentário
     explicando que agora pipelines escrevem canônico.

6. **Tests**: garantir que
   `api/tests/unit/test_sources_public_service.py` continua verde.
   Rodar `make test-api`. Se algum test depende dos aliases, adaptar.

7. **Pre-commit** + commit **atômico**. Mensagem:
   `fix(etl+api): source_id canonico em pipelines e remove alias remendo`

## Critério de "pronto"

- Todo `IngestionRun` no grafo tem `source_id` que bate 1:1 com registry
- `_GRAPH_TO_REGISTRY_ALIAS` removido (ou vazio) do service API
- Badge live na aba Fontes continua funcionando
- `make pre-commit` verde

## Cuidados

- **Não alterar o schema do IngestionRun**. Só o valor de `source_id`.
- **Migração cypher é destrutiva** — rodar em ordem contra o mesmo dado
  pode criar conflito se houver `{run_id}` duplicado. Testar em query
  `MATCH ... RETURN` antes de `SET`.
- Se houver pipeline em execução **agora** (status=running), esperar
  terminar antes da migração.

## Se travar

Se descobrir que mudar `source_id` quebra lógica de merge em `:SourceDocument`
ou constraints, parar e reportar. Pode ser que o alias na API seja o
menor dos males — documentar e deixar como está.
