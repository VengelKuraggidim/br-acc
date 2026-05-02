# Aura Free atingiu 200.000 nodes — qualquer ingestão nova em prod quebra

## Contexto

Em 2026-04-22 tentei rodar o pipeline `custo_mandato_municipal_go`
apontando pro Aura prod. Inserção falhou com:

```
ClientError: You have exceeded the logical size limit of 200000 nodes
in your database (attempt to add 2 nodes would reach 200002 nodes).
Please consider upgrading to the next tier.
```

Snapshot do grafo no momento (`apoc.meta.stats`):

- nodeCount: **200.000** (100% do cap Aura Free)
- relCount: **181.611** (45% do cap 400k — sobra grande)

## Top labels no Aura prod

| Label | Count |
|---|---|
| CampaignExpense | 36.782 |
| DeclaredAsset | 27.680 |
| Sanction | 24.037 |
| Finance | 22.366 |
| Contract | 19.533 |
| Company | 18.645 |
| Person | 16.959 |
| LegislativeExpense | 12.080 |
| CampaignDonation | 11.550 |
| GoGazetteAct | 4.051 |

Dominado por entidades TSE (CampaignExpense + CampaignDonation =
~48k) + declarações de bens + sanções. Nenhum bloco obviamente
descartável sem perder sinal analítico.

## Impacto

**Qualquer ingestão nova em prod está bloqueada.** Isso é um regressão
silenciosa — pipelines commitados (ex.: `custo_mandato_municipal_go`
entregue em 2026-04-22) não rodam em prod enquanto o grafo estiver
neste tamanho.

Pipelines afetados (com mais probabilidade de próximo run):

- `custo_mandato_municipal_go` — bloqueado (2 cargos + componentes,
  total ~6 nós).
- `custo_mandato_br` com atualizações — qualquer mudança em `_COMPONENTS`
  precisa add de nó.
- `camara_politicos_go` rerun — se a Câmara adicionar deputado novo GO,
  insere nó.
- Novas rodadas TSE — cada eleição adiciona `CampaignExpense`/`Donation`.

## Opções

### 1. Upgrade pra Aura Professional (recomendado)

Aura Free → Professional:

- **Custo**: aproximadamente US$ 65/mês (8 GB) ou US$ 195/mês (16 GB)
  no tier mais baixo do Professional — valores do portal em 2026-04.
- **Quota**: sem limite fixo de nodes/rels dentro do storage cota.
- **Execução**: fernandoeq@live.com (owner do projeto GCP + acesso ao
  portal Aura) precisa fazer o upgrade. Billing GCP não é o mesmo que
  billing Aura.

### 2. Cleanup pra liberar espaço no Free

Candidatos a remoção sem perda analítica crítica:

- **CampaignExpense por ano**: 36.782 CampaignExpense representam várias
  eleições. Cortar anos < 2018 pode liberar ~10-15k nós sem afetar
  queries do PWA (que focam em 2022/2024).
- **Sanction cleanup**: 24k Sanction, alguns duplicados por source_id.
  Auditoria pode encontrar dupes.
- **DeclaredAsset**: 27.680, possivelmente inclui declarações históricas
  que não estão sendo consumidas pelo PWA hoje.

Operação requer audit Cypher + backup + `apoc.periodic.iterate` pra
delete em batch. Risco: remover dado que volte a ser pedido depois.

### 3. Migração pra self-hosted

Subir Neo4j Community em Oracle VM (script em
`~/oci_create_instance.sh` — memo `reference_oracle_vm_retry_script`).
Sem quota fixa. Custo: só VM (possivelmente Always Free). Requer
expose porta + cert + restore dump. Alto esforço operacional.

## Recomendação

Curto prazo: **cleanup (#2)** — identificar 20k+ nós descartáveis e
limpar. Dá ar pra próximas ingestões sem fazer upgrade.

Médio prazo: **upgrade (#1)** — se o projeto virar regular e aceitar
custo de infra, migrar pra Professional.

## Verificação antes de qualquer ingestão futura

```bash
NEO4J_PASSWORD="$(gcloud secrets versions access latest --secret=fiscal-cidadao-neo4j-password --project=fiscal-cidadao-493716)" \
  uv run python -c "
import os
from neo4j import GraphDatabase
with GraphDatabase.driver('neo4j+s://5cb9f76f.databases.neo4j.io', auth=('5cb9f76f', os.environ['NEO4J_PASSWORD'])) as drv:
    with drv.session(database='5cb9f76f') as s:
        r = s.run('CALL apoc.meta.stats() YIELD nodeCount, relCount RETURN nodeCount, relCount').single()
        print('nodes:', r['nodeCount'], '/ 200000')
        print('rels:', r['relCount'], '/ 400000')
        headroom = 200000 - r['nodeCount']
        print('headroom de nodes:', headroom)
"
```

Se `headroom < N_nodes_esperados_do_pipeline`, NÃO rodar — triggar
este débito primeiro.

## Cruzamento com débitos existentes

- `repopular-ceap-aura.md` (já resolvido) alertou sobre o risco em
  2026-04-21 ("pode estourar; checar headroom antes"). A previsão
  materializou-se.
- Pipelines futuros do Tier P1 (`dou`, `sanctions`, `stf`, `bndes`,
  etc. — 20 fontes) TODOS vão bater neste bloqueador.
