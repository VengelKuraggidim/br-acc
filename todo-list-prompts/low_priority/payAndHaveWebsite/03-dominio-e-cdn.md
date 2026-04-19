---
# Domínio unificado + PWA/API no mesmo host — ⏳ PENDENTE

> Depende de [02-deploy-api-cloud-run.md](02-deploy-api-cloud-run.md)
> (Cloud Run ativo + PWA no bucket). Estado atual: PWA em
> `storage.googleapis.com/fiscal-cidadao-pwa/...` aponta direto pra
> API `fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app` via CORS `*`. Funciona
> mas URL é feia e CORS aberto é guardrail frouxo.

## Objetivo

PWA e API no **mesmo host**, com:
- Domínio próprio (`fiscalcidadao.org.br` ou `fiscal-cidadao.web.app`)
- HTTPS automático
- `/api/*` → Cloud Run; `/*` → bucket PWA
- CORS restrito ao próprio domínio

## Caminho A — Firebase Hosting (recomendado, $0/mês)

**Por que:** grátis (10GB storage + 360MB/dia egress), mesmo projeto GCP,
integra nativo com Cloud Run via rewrites, SSL automático, CDN global.

### Setup

1. Instalar Firebase CLI (`npm i -g firebase-tools`) ou usar container.
2. `firebase login` com conta GCP.
3. `firebase init hosting --project=fiscal-cidadao-493716`:
   - Public directory: `pwa`
   - Single-page app: **No** (PWA já tem rotas próprias)
   - GitHub action deploy: opcional
4. Criar/editar `firebase.json`:

```json
{
  "hosting": {
    "public": "pwa",
    "ignore": ["firebase.json", "**/.*", "**/node_modules/**"],
    "headers": [
      {
        "source": "/index.html",
        "headers": [
          { "key": "Cache-Control", "value": "no-cache, max-age=0" }
        ]
      },
      {
        "source": "/sw.js",
        "headers": [
          { "key": "Cache-Control", "value": "no-cache, max-age=0" }
        ]
      }
    ],
    "rewrites": [
      {
        "source": "/api/**",
        "run": {
          "serviceId": "fiscal-cidadao-api",
          "region": "southamerica-east1"
        }
      }
    ]
  }
}
```

5. Reverter `pwa/index.html` pra `/api` relativo:
   ```js
   // ANTES:
   : "https://fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app";
   // DEPOIS:
   : "/api";
   ```
6. Deploy: `firebase deploy --only hosting`.
7. URL default: `https://fiscal-cidadao-493716.web.app`.
8. **Restringir CORS** — em `scripts/deploy/deploy_api.sh` trocar
   `CORS_ORIGINS=*` por `CORS_ORIGINS=https://fiscal-cidadao-493716.web.app`
   (+ domínio custom quando tiver). Redeploy.
9. Testar: abrir `https://fiscal-cidadao-493716.web.app` — busca
   deve funcionar com `/api/status`, `/api/buscar-tudo`, etc.

### Custom domain (opcional)

1. Comprar domínio (~$10-15/ano):
   - Cloud Domains: `.com.br` ~R$40/ano, `.org.br` ~R$40/ano
   - Registro.br direto: mais barato mas sem integração automática
   - Namecheap `.com` ~$10/ano
2. No console Firebase → Hosting → Add custom domain.
3. Firebase dá TXT record pra verificação + A/AAAA pra apontar DNS.
4. Adicionar no painel do registrador. SSL provisiona em ~1h.

## Caminho B — Load Balancer + Cloud CDN (~$18/mês)

**Por que:** controle fino, integra com Cloud Armor (DDoS/WAF),
path matching mais flexível, backend buckets nativos. Só vale se já
for pagar algo mais ou precisar de WAF.

### Setup resumido

1. Reservar IP estático global:
   ```bash
   gcloud compute addresses create fiscal-cidadao-ip --global
   ```
2. Criar backend bucket pro PWA:
   ```bash
   gcloud compute backend-buckets create fiscal-cidadao-pwa-backend \
     --gcs-bucket-name=fiscal-cidadao-pwa --enable-cdn
   ```
3. Criar Serverless NEG pro Cloud Run:
   ```bash
   gcloud compute network-endpoint-groups create fiscal-cidadao-api-neg \
     --region=southamerica-east1 \
     --network-endpoint-type=serverless \
     --cloud-run-service=fiscal-cidadao-api
   ```
4. Criar backend service apontando pro NEG:
   ```bash
   gcloud compute backend-services create fiscal-cidadao-api-backend \
     --global --load-balancing-scheme=EXTERNAL_MANAGED
   gcloud compute backend-services add-backend fiscal-cidadao-api-backend \
     --global --network-endpoint-group=fiscal-cidadao-api-neg \
     --network-endpoint-group-region=southamerica-east1
   ```
5. Criar URL map com path matcher:
   - `/api/*` → `fiscal-cidadao-api-backend`
   - Default → `fiscal-cidadao-pwa-backend`
6. Managed SSL cert + HTTPS target proxy + forwarding rule:
   ```bash
   gcloud compute ssl-certificates create fiscal-cidadao-cert \
     --domains=fiscalcidadao.org.br --global
   gcloud compute target-https-proxies create fiscal-cidadao-https-proxy \
     --url-map=fiscal-cidadao-url-map \
     --ssl-certificates=fiscal-cidadao-cert
   gcloud compute forwarding-rules create fiscal-cidadao-https-fr \
     --global --address=fiscal-cidadao-ip \
     --target-https-proxy=fiscal-cidadao-https-proxy \
     --ports=443
   ```
7. Apontar domínio pro IP estático no registrador.

### Custo real

- Forwarding rule: $0.025/h = ~$18/mês
- Data processing: $0.008/GB (cobra só o tráfego)
- Cloud CDN cache: $0.02-0.08/GB egress
- SSL cert: grátis (managed)

Pra tráfego de demo (<100GB/mês): **~$18-20/mês** total.

## Critérios de aceite

- [ ] `https://<domínio>/` carrega PWA
- [ ] `https://<domínio>/api/status` retorna JSON da API
- [ ] Busca no PWA funciona (chamando `/api/buscar-tudo?q=...`)
- [ ] HTTPS válido (sem warning no browser)
- [ ] `CORS_ORIGINS` restringido ao próprio domínio (não mais `*`)
- [ ] `pwa/index.html` voltou pra `/api` relativo
- [ ] docs/deploy.md atualizado com setup escolhido

## Guardrails

- **Confirmar com Fernando qual caminho (A ou B) antes de provisionar
  recursos pagos.** B custa $18+/mês, A é $0.
- **Não deixar `CORS_ORIGINS=*` em produção** depois que o domínio
  estiver unificado — é fail-open por default.
- Se custom domain: avisar que DNS propagation pode levar até 48h
  (geralmente 5-30min).
- Após migrar pra `/api` relativo, testar que PWA serve pelo novo
  domínio ANTES de apagar a versão do bucket antigo.
