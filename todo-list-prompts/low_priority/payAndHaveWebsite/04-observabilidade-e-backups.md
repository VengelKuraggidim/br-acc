---
# Observabilidade + backups automáticos — ⏳ PENDENTE

> Depende de [03-dominio-e-cdn.md](03-dominio-e-cdn.md).
> Site no ar, domínio configurado. Agora garantir que falhas não
> passam despercebidas + que dados não se perdem.

## Contexto

Depois do prompt 03 o site funciona mas não há monitoramento. Quatro
coisas que podem dar errado sem ninguém saber:

1. **API cair** — Cloud Run não pagina por padrão.
2. **Neo4j encher disco / crashar** (se Opção A/GCE VM do prompt 01).
3. **Custo escalar** — bug pode disparar instâncias, atacante pode
   DDoS, tráfego viral pode estourar budget.
4. **Dados se corromperem** — sem backup do Neo4j prod não dá pra
   recuperar.

Também: pipelines ETL precisam rodar periodicamente pra manter dados
fresh. Hoje isso é manual.

## Arquivos relevantes

- `scripts/deploy/create_service_account.sh` — talvez precisar SA
  adicional pra Cloud Scheduler
- `config/bootstrap_all_contract.yml` — referência dos 62 pipelines
- `Makefile` targets `bootstrap-go`, `bootstrap-all` — rodar em
  Cloud Run Jobs ou Cloud Build

## Missão

### 1. Uptime check + alerta no Cloud Monitoring

```bash
gcloud monitoring uptime create fiscal-cidadao-api-health \
  --resource-type=uptime-url \
  --resource-labels=host=api.<dominio>,project_id=fiscal-cidadao-493716 \
  --path=/health \
  --period=5m \
  --timeout=10s
```

Criar alerting policy: se 3 checks consecutivos falham → email pro
`fernandoeq@live.com` (ou canal que Fernando indicar).

### 2. Budget alert

Evitar surpresa no fim do mês:

```bash
gcloud billing budgets create \
  --billing-account=<BILLING_ACCOUNT_ID> \
  --display-name="Fiscal Cidadao monthly" \
  --budget-amount=100USD \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.9 \
  --threshold-rule=percent=1.0 \
  --filter-projects=fiscal-cidadao-493716
```

Ajustar `budget-amount` conforme o setup escolhido (Opção A: ~$50;
Opção B: ~$100).

### 3. Backup Neo4j prod

**Se Opção A (GCE VM)**:
- Cron job diário na VM: `neo4j-admin database dump neo4j
  --to-path=/backups`
- Upload do dump pro `gs://fiscal-cidadao-archival/neo4j-backups/YYYY-MM-DD.dump`
- Retenção 30 dias via lifecycle rule do bucket
- Testar restore em VM temporária mensalmente (documentar no README)

**Se Opção B (Aura Professional)**:
- Aura faz backup automático diário (já incluso). Documentar em
  `docs/deploy.md` qual é a janela de retenção.
- Setup export adicional pro `gs://fiscal-cidadao-archival/` via Cypher
  export semanal (redundância — não confiar só no Aura).

### 4. ETL recorrente via Cloud Scheduler + Cloud Run Jobs

Pipelines precisam re-rodar periodicamente pra manter dados frescos.
Pra cada pipeline (ou grupo), criar Cloud Run Job:

```bash
gcloud run jobs create bracc-etl-go \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1 \
  --image=gcr.io/fiscal-cidadao-493716/bracc-etl:latest \
  --command="make" \
  --args="bootstrap-go-noninteractive" \
  --service-account=fiscal-cidadao-etl@...
```

Schedule:
```bash
gcloud scheduler jobs create http bracc-etl-go-weekly \
  --project=fiscal-cidadao-493716 \
  --location=southamerica-east1 \
  --schedule="0 3 * * 0" \
  --uri="https://<region>-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/fiscal-cidadao-493716/jobs/bracc-etl-go:run" \
  --http-method=POST \
  --oauth-service-account-email=fiscal-cidadao-scheduler@...
```

Cadência sugerida:
- `folha_go`, `pncp_go`, `tcm_go` — semanal (dados mudam pouco)
- `querido_diario_go`, `camara_goiania` — diário (publicações frequentes)
- TSE, TCE-GO — mensal

Precisa construir imagem ETL (`etl/Dockerfile`) se ainda não existe.

### 5. Log-based alerts críticos

- Erro em pipeline: log `level=ERROR` em `bracc_etl` → alert
- Neo4j connection errors na API: > 10/min → alert
- CORS block: grande volume indica PWA apontando pra URL errada

## Critérios de aceite

- Uptime check ativo, alerta configurado com destino email confirmado.
- Budget alert ativo (50%, 90%, 100% do limite).
- Backup Neo4j: mecanismo ativo (GCE cron ou Aura built-in + export
  adicional), dump mais recente < 25h de idade.
- ≥ 2 Cloud Run Jobs ETL ativos com scheduler (pelo menos os GO de
  maior frequência).
- Log-based alerts pra erros críticos ativos.
- `docs/deploy.md` §observabilidade documentando tudo.

## Guardrails

- Confirmar budget amount com Fernando (50/100/200 USD).
- Confirmar destino dos alerts (email, Slack, SMS) — default email
  se não especificado.
- Não criar mais de 2-3 scheduled jobs no primeiro commit; validar
  custo de cada antes de expandir pra todos os 62 pipelines.
- Custo Cloud Scheduler: $0.10/job/mês — negligível.
- Commits: `feat(ops):` pra monitoring/alerts, `feat(etl):` pra Cloud
  Run Jobs.
