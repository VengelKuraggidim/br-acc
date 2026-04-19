---
# CI/CD via GitHub Actions — ⏳ PENDENTE (opcional)

> Depende de [04-observabilidade-e-backups.md](04-observabilidade-e-backups.md).
> **Opcional** — só vale a pena depois que o fluxo manual estabilizou
> (sem retrabalho frequente). Se estiver no ar e estável, automatizar.

## Contexto

Hoje deploy é 100% manual: push pra `main` não faz nada automático.
Pra cada release, Fernando precisa:

1. `gcloud builds submit api/ --tag ...`
2. `bash scripts/deploy/deploy_api.sh`

Funciona mas tem cost em cognitive load + tempo. Automatizar quando
o repo estiver em cadência de múltiplos deploys/semana.

**Padrão recomendado**: Workload Identity Federation (WIF) — GitHub
Actions autentica no GCP via OIDC, **sem** service account keys em
GitHub secrets. Mais seguro que key JSON.

## Arquivos relevantes

- `.github/workflows/` — criar `deploy-api.yml` aqui (diretório pode
  não existir ainda)
- `scripts/deploy/deploy_api.sh` — workflow vai reutilizar
- `api/Dockerfile` — build
- `pwa/` — se quiser também auto-upload da PWA no push

## Missão

### 1. Criar Workload Identity Pool + Provider

```bash
gcloud iam workload-identity-pools create github-actions \
  --project=fiscal-cidadao-493716 \
  --location=global \
  --display-name="GitHub Actions"

gcloud iam workload-identity-pools providers create-oidc github \
  --project=fiscal-cidadao-493716 \
  --location=global \
  --workload-identity-pool=github-actions \
  --display-name="GitHub OIDC" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='VengelKuraggidim/fiscal-cidadao'" \
  --issuer-uri="https://token.actions.githubusercontent.com"
```

### 2. SA pro GitHub Actions

```bash
gcloud iam service-accounts create fiscal-cidadao-ci \
  --project=fiscal-cidadao-493716 \
  --display-name="Fiscal Cidadão CI/CD"

# Permitir o GitHub impersonar:
gcloud iam service-accounts add-iam-policy-binding \
  fiscal-cidadao-ci@fiscal-cidadao-493716.iam.gserviceaccount.com \
  --project=fiscal-cidadao-493716 \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github-actions/attribute.repository/VengelKuraggidim/fiscal-cidadao"

# Dar roles pro CI deploy:
for role in roles/run.admin roles/storage.admin roles/cloudbuild.builds.editor \
  roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding fiscal-cidadao-493716 \
    --member="serviceAccount:fiscal-cidadao-ci@fiscal-cidadao-493716.iam.gserviceaccount.com" \
    --role="$role"
done
```

### 3. Workflow de deploy

Criar `.github/workflows/deploy-api.yml`:

```yaml
name: Deploy API

on:
  push:
    branches: [main]
    paths:
      - 'api/**'
      - 'scripts/deploy/**'
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      PROJECT_ID: fiscal-cidadao-493716
    steps:
      - uses: actions/checkout@v4

      - name: Auth to GCP
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github-actions/providers/github
          service_account: fiscal-cidadao-ci@fiscal-cidadao-493716.iam.gserviceaccount.com

      - name: Setup gcloud
        uses: google-github-actions/setup-gcloud@v2

      - name: Build image
        run: |
          DEPLOY_TAG="v${{ github.sha }}"
          gcloud builds submit api/ \
            --tag "gcr.io/${PROJECT_ID}/fiscal-cidadao-api:${DEPLOY_TAG}" \
            --tag "gcr.io/${PROJECT_ID}/fiscal-cidadao-api:latest"

      - name: Deploy
        env:
          NEO4J_URI: ${{ vars.NEO4J_URI }}
          NEO4J_USER: ${{ vars.NEO4J_USER }}
          NEO4J_DATABASE: ${{ vars.NEO4J_DATABASE }}
          DEPLOY_TAG: v${{ github.sha }}
        run: bash scripts/deploy/deploy_api.sh

      - name: Smoke test
        run: |
          URL=$(gcloud run services describe fiscal-cidadao-api \
            --region=southamerica-east1 --format='value(status.url)')
          curl -fsS "$URL/health"
```

Setar `vars.NEO4J_URI`, `vars.NEO4J_USER`, `vars.NEO4J_DATABASE` no
GitHub (Settings → Variables and secrets → Actions → Variables; **não
Secrets** — URI não é secret).

### 4. Workflow pra PWA (opcional)

`.github/workflows/deploy-pwa.yml` rodando `scripts/deploy/upload_pwa.sh`
em push que mexa em `pwa/**`.

### 5. Atualizar `docs/deploy.md`

Adicionar seção "CI/CD" descrevendo que push pra `main` deploya
automaticamente; manter o fluxo manual como fallback.

## Critérios de aceite

- WIF pool + provider criados; constraint repository válido
  (previne outros repos do Fernando usarem a mesma trust).
- SA `fiscal-cidadao-ci` com roles mínimos pra deploy.
- Workflow `deploy-api.yml` verde em push de teste pra `main`.
- Smoke test passa no fim do workflow.
- Documentação em `docs/deploy.md`.
- Rollback documentado: se auto-deploy falhar, `gcloud run services
  update-traffic --to-revisions=REVISION_ANTERIOR=100`.

## Guardrails

- **Princípio do menor privilégio** — SA de CI não precisa de
  `roles/owner`. Lista acima é mínima.
- **attribute-condition** no WIF provider é crítico — sem isso,
  qualquer repo do GitHub pode se autenticar como esse SA. Não pular.
- **Secrets do repo**: `NEO4J_URI` pode ser Variable (não Secret) —
  não é dado sensível. Password fica só no Secret Manager, workflow
  não precisa acessar.
- Commits: `feat(ci):` pros workflows, `docs(deploy):` pra docs.
- Testar em workflow_dispatch antes de mergear o workflow pra evitar
  loop de deploy em commits de fixup.
