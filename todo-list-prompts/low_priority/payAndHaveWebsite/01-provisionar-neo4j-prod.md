---
# Provisionar Neo4j prod + migrar grafo local — ⏳ PENDENTE

> **Bloqueador do deploy.** Aura Free (criado em 2026-04-18, instance
> `5cb9f76f`) não cabe o grafo local: 2.4M nodes / 1.17M rels vs limite
> 200k/400k. Esse prompt decide caminho final e importa o grafo.

## Contexto

Sessão de 2026-04-18 preparou toda infraestrutura do Cloud Run mas
pausou ao descobrir o gap de tamanho. Fernando quer seguir com plano
pago agora.

Duas opções viáveis:

### Opção A — GCE VM + Neo4j Community (recomendação da sessão anterior)
- Custo: ~$25-30/mês (`e2-medium`, 2 vCPU, 4GB RAM, 30GB SSD)
- Pros: dataset ilimitado, latência <5ms ao Cloud Run (mesma região),
  controle total
- Contras: ops (backup manual, patches), setup inicial ~1h

### Opção B — Aura Professional
- Custo: começa em ~$65/mês (AuraDB Professional Standard tier baixo)
- Pros: zero ops, backups automáticos, TLS nativo
- Contras: 3x mais caro; latência us-east1 (~120ms de SA); sizing
  exato pro dataset de 2.4M nodes pode exigir tier maior (~$200+/mês)

**Pergunte ao Fernando qual escolher antes de executar.** A decisão
muda toda a sequência depois.

## Arquivos relevantes

- `api/src/bracc/secrets.py` — carrega `neo4j-password` do Secret Manager
- `api/src/bracc/config.py` — reads `NEO4J_URI`, `NEO4J_USER`,
  `NEO4J_DATABASE` do env
- `scripts/deploy/deploy_api.sh` — já aceita override via env vars
- `docs/deploy.md` — doc atual com setup Aura Free (atualizar pós-decisão)
- Neo4j local: container `fiscal-neo4j` com volume `br-acc_neo4j-data`;
  senha em `.env` ou via `docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2`

## Missão

### Se Opção A (GCE VM):

1. Criar VM `e2-medium` em `southamerica-east1`:
   ```bash
   gcloud compute instances create fiscal-cidadao-neo4j \
     --project=fiscal-cidadao-493716 \
     --zone=southamerica-east1-a \
     --machine-type=e2-medium \
     --image-family=debian-12 \
     --image-project=debian-cloud \
     --boot-disk-size=30GB \
     --boot-disk-type=pd-ssd \
     --tags=neo4j-server \
     --metadata=enable-oslogin=TRUE
   ```
2. Firewall só pra Serverless VPC Connector (sem expor 7687 público):
   ```bash
   gcloud compute networks vpc-access connectors create fiscal-connector \
     --project=fiscal-cidadao-493716 \
     --region=southamerica-east1 \
     --subnet=default \
     --subnet-project=fiscal-cidadao-493716
   ```
3. SSH na VM, instalar Docker + subir Neo4j 5 Community em container
   com volume persistente (`/var/lib/docker/volumes/neo4j-data`).
4. Configurar senha forte (não usar a local), gerar nova versão do
   secret `fiscal-cidadao-neo4j-password`:
   ```bash
   printf '%s' 'SENHA_FORTE' | gcloud secrets versions add \
     fiscal-cidadao-neo4j-password \
     --project=fiscal-cidadao-493716 --data-file=-
   ```
5. Migrar grafo local → VM:
   - Dump local: `docker exec fiscal-neo4j neo4j-admin database dump
     neo4j --to-path=/var/lib/neo4j/dumps`
   - Copiar dump via `gcloud compute scp` ou bucket intermediário
   - Load na VM: `docker exec neo4j-prod neo4j-admin database load
     neo4j --from-path=/imports`
6. Validar contagens batem: 2.447.964 nodes / 1.174.867 rels.
7. Documentar URI interna: `bolt://<IP_INTERNO_VM>:7687` (usar IP
   interno, não público).

### Se Opção B (Aura Professional):

1. No painel Aura, upgrade da instância Free → Professional (pode
   exigir criar nova instância se Aura não permitir upgrade in-place).
2. Anotar nova URI; atualizar secret com nova senha.
3. Export grafo local:
   ```bash
   docker exec fiscal-neo4j cypher-shell -u neo4j -p "$NEO4J_PW" \
     "CALL apoc.export.cypher.all('/data/export.cypher', {format:'plain'})"
   docker cp fiscal-neo4j:/data/export.cypher /tmp/export.cypher
   ```
4. Importar no Aura via `cypher-shell` remoto ou driver Python. Aura
   Pro não aceita `neo4j-admin load` direto — só importa via Cypher.
5. Validar contagens.

### Comum às duas opções:

- Atualizar `docs/deploy.md` §2 com o setup escolhido.
- Deletar instância Aura Free antiga (`5cb9f76f`) se for Opção A — evita
  ocupar slot free.
- Exportar `NEO4J_URI`, `NEO4J_USER`, `NEO4J_DATABASE` corretos (os
  valores do Aura Free não servem mais).

## Critérios de aceite

- Neo4j prod provisionado e acessível (via Cloud Run posteriormente —
  não precisa expor publicamente).
- Secret `fiscal-cidadao-neo4j-password` atualizado com senha nova.
- Grafo importado: `MATCH (n) RETURN count(n)` == 2.447.964 (±10%
  aceitável — alguns nodes podem ter sido recriados com mesmas chaves).
- `MATCH ()-[r]->() RETURN count(r)` == 1.174.867 (±10%).
- `docs/deploy.md` atualizado refletindo a escolha.
- Custo confirmado com Fernando antes de provisionar.

## Guardrails

- **Confirmar escolha A vs B com Fernando antes de provisionar.** Erro
  aqui custa $$$.
- **Não expor Neo4j publicamente** — Opção A usa Serverless VPC
  Connector; Opção B usa TLS do Aura (já built-in).
- Dump + import de grafo grande demora (~30-60 min). Não interromper.
- Commits atômicos: prefix `feat(deploy):` ou `chore(deploy):`.
- `make pre-commit` verde.
