# Subir Fiscal Cidadão no ar (plano pago) — sequência de prompts

Sessão de 2026-04-18 deixou todo o trabalho de Cloud Run pronto, mas
o deploy pausou porque o grafo local (2.4M nodes / 1.17M rels) **não
cabe** no Aura Free (limite 200k nodes / 400k rels). Retomar quando
Fernando decidir seguir com plano pago ou auto-hospedar.

## Pré-req já feito (não refazer)

- Dockerfile Cloud Run-ready (`api/Dockerfile`)
- Scripts: `scripts/deploy/create_service_account.sh`,
  `create_archival_bucket.sh`, `upload_pwa.sh`, `deploy_api.sh`
- Secrets no Secret Manager: `fiscal-cidadao-neo4j-password`,
  `fiscal-cidadao-jwt-secret`, `fiscal-cidadao-transparencia-key`
- Service account `fiscal-cidadao-api@fiscal-cidadao-493716.iam.gserviceaccount.com`
- Buckets `gs://fiscal-cidadao-archival` e `gs://fiscal-cidadao-pwa`
- Aura Free instance `5cb9f76f` (vai ser substituída ou deletada)
- Doc: `docs/deploy.md`

## Ordem dos prompts

Cada prompt depende do anterior. Rodar em ordem.

1. **[01-provisionar-neo4j-prod.md](01-provisionar-neo4j-prod.md)** —
   decidir Aura Professional vs GCE VM Neo4j Community; provisionar
   instância; exportar grafo local + importar no destino; validar
   contagens.
2. **[02-deploy-api-cloud-run.md](02-deploy-api-cloud-run.md)** —
   build + deploy da imagem; atualizar secret/URI com os dados novos;
   smoke tests `/health`, `/status`, `/politico/{id}`.
3. **[03-dominio-e-cdn.md](03-dominio-e-cdn.md)** — registrar/configurar
   domínio; Load Balancer + Cloud CDN pra PWA; Cloud Run domain
   mapping pra API; managed SSL; CORS restrito.
4. **[04-observabilidade-e-backups.md](04-observabilidade-e-backups.md)** —
   uptime checks; alertas de erro; budget alert; snapshot automático
   do Neo4j (se GCE VM); scheduler pra ETL recorrente.
5. **[05-cicd-github-actions.md](05-cicd-github-actions.md)** —
   (opcional) migrar deploy manual pra GitHub Actions com Workload
   Identity Federation.

## Guardrails globais desses prompts

- **No new branches** — trabalhar em `main`.
- **No auto-push** — commits locais apenas.
- **Stop on ambiguidade** — pergunte ao Fernando antes de inventar.
- **Secrets** — nunca printar conteúdo; usar `--data-file=` em
  comandos gcloud.
- **Orçamento** — confirmar com Fernando antes de criar recurso com
  custo recorrente ≥ $10/mês.
