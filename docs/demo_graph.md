# Grafo de demonstração — cópia reduzida

Fork reduzido do grafo principal (2.4M nodes / 1.17M rels) pra uma
versão que cabe em tiers grátis/baratos (Aura Free: 200k/400k;
Hetzner CX22: qualquer coisa). Alvo default: **80% do Aura Free**
(160k nodes / 320k rels).

## Quando usar

- Mockup pra apresentar o projeto sem custo de infra
- Hospedar em Aura Free ou VPS pequena
- Ambiente de staging/CI com tempo de carga <1 min

**Não** substitui o grafo principal pra análise real — é uma fatia
intencionalmente incompleta.

## Critério de corte

**Invariantes (sempre incluídos, atemporais):**
- Políticos GO: `CanonicalPerson {uf='GO'}` + `Senator|FederalLegislator|StateLegislator {uf='GO'}`
- Nodes ligados via `REPRESENTS` (entity resolution)
- `GoMunicipality` (246 municípios)

**Eventos diretos do seed (sem filtro temporal — dates esparsos no grafo):**
- `Amendment` (via `PROPOS` ou `AUTOR_EMENDA`)
- `DeclaredAsset` (via `DECLAROU_BEM`)
- `Election` candidatadas (via `CANDIDATO_EM`)
- `PartyMembership` (via `FILIADO_A`)
- Doações enviadas/recebidas (via `DOOU`)

**Eventos datados (janela temporal deslizante):**
- `LegislativeExpense.ano >= cutoff` (CEAP + ALEGO)
- `CampaignExpense.ano >= cutoff` (gastos eleitorais GO — label já 100% GO)
- `CampaignDonation.ano >= cutoff` (doações eleitorais GO)
- `GoGazetteAct.date/published_at >= cutoff` (atos do diário oficial)
- `GoProcurement.date/published_at >= cutoff` (licitações municipais)
- `Contract.date >= cutoff` (empresas doadoras)
- `Sanction.date_start >= cutoff`
- `Finance.date >= cutoff` (dívidas fiscais de empresas admitidas)

**Entidades ligadas (2-hop, atemporais):**
- `Company` que doou aos políticos (via `DOOU`)
- `Company` que venceu licitação GO (via `CONTRATOU_GO`)
- `Person` sócio dessas empresas (via `SOCIO_DE_SNAPSHOT` ou `SOCIO_DE`)
- `CampaignDonor` ligado aos políticos
- `DeclaredAsset` de todos Person GO
- `MunicipalExpenditure` / `MunicipalRevenue` de municípios GO
- `Election` (volume baixo — 54 nodes)

**Algoritmo:** o script testa `cutoff = start_year` e recua ano-a-ano
até estourar o orçamento. Último ano que coube vence.

## Setup

### 1. Senha do destino

```bash
# Adiciona NEO4J_DEMO_PASSWORD ao .env
echo "NEO4J_DEMO_PASSWORD=demo-$(openssl rand -hex 16)" >> .env
source .env  # ou exportar manualmente
```

### 2. Subir container destino

```bash
docker compose -f docker-compose.demo.yml up -d
# Aguarda healthcheck (~30s)
docker compose -f docker-compose.demo.yml ps
```

O container `fiscal-neo4j-demo` sobe em **:7688 (bolt)** e **:7475 (browser)**,
volume `fiscal-neo4j-demo-data` separado. O `fiscal-neo4j` principal (7687/7474)
fica intocado.

### 3. Dry-run (recomendado)

```bash
cd etl
uv run python ../scripts/build_demo_graph.py \
  --source-password "$NEO4J_PASSWORD" \
  --dry-run-only
```

Imprime tabela tipo (números reais do grafo atual):

```
cutoff   nodes        rels         nodes %    rels %     status
2025     46,992       65,357       29.4%      20.4%      fits
2024     55,426       73,148       34.6%      22.9%      fits
2023     63,644       80,817       39.8%      25.3%      fits
2022     81,213       93,765       50.8%      29.3%      fits  ← inclui Campaign 2022
2018    124,167      127,379       77.6%      39.8%      fits
→ Melhor cutoff: 2018 (mais antigo que ainda coube)
```

O "salto" em 2022 é do volume de CampaignExpense/CampaignDonation da
eleição 2022 (filtrado por valor ≥ R$ 1.5k por default — ver
`--campaign-min-value`). Baixar pra R$ 1k sobe pra 93%, subir pra
R$ 10k reduz pra ~54%.

### 4. Build completo

```bash
cd etl
uv run python ../scripts/build_demo_graph.py \
  --source-password "$NEO4J_PASSWORD" \
  --target-password "$NEO4J_DEMO_PASSWORD" \
  --wipe-target
```

Tempo típico: ~3-8 min (depende de volume). Progresso por log.

### 5. Validar no browser

```
http://localhost:7475
user: neo4j
pass: $NEO4J_DEMO_PASSWORD
```

Queries de sanity:

```cypher
// Contagens
MATCH (n) RETURN count(n);
MATCH ()-[r]->() RETURN count(r);

// Políticos GO incluídos
MATCH (p:CanonicalPerson {uf:'GO'})
WHERE p.cargo_ativo IS NOT NULL
RETURN p.display_name, p.cargo_ativo LIMIT 50;

// Emendas por político
MATCH (p)-[:PROPOS]->(a:Amendment)
RETURN p.display_name, count(a) AS emendas
ORDER BY emendas DESC LIMIT 10;
```

## Flags avançadas

| Flag | Default | Descrição |
|---|---|---|
| `--node-budget` | 160000 | Limite de nodes (80% Aura Free) |
| `--rel-budget` | 320000 | Limite de rels (80% Aura Free) |
| `--start-year` | 2025 | Janela começa aqui (só último ano) |
| `--min-year` | 2018 | Não recua além disso |
| `--campaign-min-value` | 1500.0 | R$ mínimo pra incluir CampaignExpense/Donation |
| `--source-uri` | `bolt://localhost:7687` | Grafo fonte |
| `--target-uri` | `bolt://localhost:7688` | Grafo destino |
| `--dry-run-only` | — | Só mede, não copia |
| `--wipe-target` | — | Apaga destino antes de copiar |

## Exportar pra Aura Free

Depois do build validado no container demo:

```bash
# Dump dentro do container
docker exec fiscal-neo4j-demo neo4j-admin database dump neo4j \
  --to-path=/data/dumps

# Copia pro host
docker cp fiscal-neo4j-demo:/data/dumps/neo4j.dump /tmp/demo.dump

# Upload pro Aura Free via UI (painel de import) ou cypher-shell remoto
```

Aura Free não aceita `neo4j-admin load` direto — precisa importar via
Aura Import tool (UI) ou `cypher-shell` + `CREATE` incremental.

## Troubleshooting

- **"Seed vazio"** — grafo fonte não tem políticos GO. Conferir com
  `MATCH (n:CanonicalPerson {uf:'GO'}) RETURN count(n)`.
- **"EXCEEDS já em 2025"** — dataset é denso demais pro start-year.
  Aumentar budget, ou restringir: rodar manualmente com `--start-year 2026`.
- **Healthcheck demo falha** — senha não bateu. Ver
  `docker logs fiscal-neo4j-demo`. Variável `NEO4J_DEMO_PASSWORD` no
  `.env` precisa estar presente **antes** de subir o container.
- **"Senha do source não informada"** — exportar `NEO4J_PASSWORD`
  (ou passar `--source-password` direto).

## Limpar destino

```bash
docker compose -f docker-compose.demo.yml down -v
```

Apaga container + volume. O grafo fonte (`fiscal-neo4j`) fica intocado.
