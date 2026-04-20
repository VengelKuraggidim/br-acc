# Repopular CEAP federal no Aura (cota gabinete/gasolina/telefone)

## Contexto

No PWA, abrir perfil de deputado federal mostra seção **"Cota Parlamentar
(CEAP)"** vazia pra todo mundo. Stats endpoint de prod confirma:

```bash
curl -s https://fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app/api/v1/meta/stats \
  | jq '.expense_count'
# → 0
```

Zero nós `:Expense` no Aura. A migração do Flask live-call pro FastAPI
read-from-graph (commit `341e334 feat(api): DespesasService lê CEAP do
grafo`) tirou a dependência da API da Câmara em tempo real — mas o
pipeline `camara_deputados_ceap` precisa ter rodado pra popular os nós,
e não rodou contra o Aura de produção (ou rodou e não persistiu).

**Sintoma ao usuário**: UX regressão — antes (Flask) aparecia CEAP
automaticamente porque vinha de live-call; agora sai vazio e dispara o
texto `aviso_despesas` ("Dados de gastos parlamentares nao disponiveis
...") pra qualquer deputado federal.

**Fix UX complementar já aplicado nesta sessão** (commit sobre
`perfil_politico_connections.cypher`): busca `/buscar-tudo` devolvendo
`:Person` TSE agora resolve pro `:FederalLegislator` irmão via cluster
`:CanonicalPerson`, então quando CEAP for populado o deputado federal
correto vai aparecer automaticamente.

## Pipelines afetados

- **`camara_deputados_ceap`** — fonte primária da cota parlamentar
  (CEAP) federal. Popula `:Expense` + `(:FederalLegislator)-
  [:GASTOU_CEAP]->(:Expense)`. Já fixado no padrão de rows_in/loaded
  (commit `42e8228`).
- (Opcional) **`camara` (bulk CEAP)** — alternativa/backup pro endpoint
  por deputado, se o volume por-deputado ficar lento.

## Como rodar

Credencial do Aura vem do Secret Manager (mesmo fluxo do wrapper
`deploy_all.sh --auto`). Não dá pra rodar ETL pesado direto do Cloud
Run — precisa rodar local apontando pro Aura:

```bash
# 1. Pegar a senha do Aura do Secret Manager
export NEO4J_URI="$(gcloud secrets versions access latest --secret=neo4j-uri)"
export NEO4J_USER="$(gcloud secrets versions access latest --secret=neo4j-user)"
export NEO4J_PASSWORD="$(gcloud secrets versions access latest --secret=neo4j-password)"

# 2. Rodar o pipeline contra o Aura
cd etl
uv run python -m bracc_etl.runner run --source camara_deputados_ceap \
  --neo4j-uri "$NEO4J_URI" \
  --neo4j-user "$NEO4J_USER" \
  --neo4j-password "$NEO4J_PASSWORD" \
  --data-dir /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/data \
  2>&1 | tee /tmp/camara_deputados_ceap_aura.log
```

**Volume esperado**: ~70 deputados federais GO × ~15 tipos de despesa
× ~36 meses de legislatura ≈ 37k linhas CEAP. Deve rodar em poucos
minutos (API da Câmara tem rate limit, mas volume é moderado).

## Validação pós-run

```bash
# 1. Stats: expense_count > 0 (esperado ≳ 30k)
curl -s https://fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app/api/v1/meta/stats \
  | jq '.expense_count'

# 2. Perfil de deputado federal GO deve trazer despesas_gabinete
#    populado. Exemplo (id_camara de um deputado GO ativo):
curl -s "https://fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app/politico/<ID_CAMARA>" \
  | jq '.despesas_gabinete | length'
# → esperado > 0
```

## Riscos / cuidados

- **Aura Free tier**: tem limite de 200k nós / 400k rels. Stats hoje
  marca ~150k nós e ~153k rels. 37k Expense + 37k rels = ~74k novos
  objetos → pode estourar. **Checar headroom antes de rodar**; se
  estourar, migrar pra Aura Professional ou filtrar CEAP por anos
  recentes (só 2024/2025).
- **IngestionRun não está sendo carimbado no Aura** (`ingestion_run_count: 0`
  em stats). Separado deste débito, mas relacionado — vale investigar em
  paralelo por que os IngestionRuns locais não chegam no Aura (bug de
  replicação ou pipeline não está gravando no Neo4j apontado).

## Origem

Diagnóstico em sessão de 2026-04-19 em conversa com o usuário sobre
"emendas não aparecem mais". Dois problemas distintos identificados:

1. **Busca devolve Person TSE sem cluster canônico** → resolvido nesta
   sessão no Cypher (`perfil_politico_connections.cypher`).
2. **CEAP completamente vazio no Aura** → este débito.
