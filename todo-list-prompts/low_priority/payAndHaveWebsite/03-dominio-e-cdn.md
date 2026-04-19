---
# Domínio custom + Cloud CDN pra PWA + HTTPS — ⏳ PENDENTE

> Depende de [02-deploy-api-cloud-run.md](02-deploy-api-cloud-run.md).
> API funcional no Cloud Run, PWA no GCS público. Agora apontar um
> domínio real + CDN pra PWA + restringir CORS.

## Contexto

Depois do prompt 02, o site está "no ar" mas com URLs feias:
- API: `https://fiscal-cidadao-api-xxx-rj.a.run.app`
- PWA: `https://storage.googleapis.com/fiscal-cidadao-pwa/index.html`

Pra compartilhar com público (leigos), precisa:
1. Domínio registrado (ex: `fiscalcidadao.org.br`)
2. Cloud Load Balancer + Cloud CDN pra servir PWA com cache global
3. Cloud Run domain mapping pra API em `api.<dominio>`
4. Certificados SSL managed (grátis, renovação automática)
5. CORS restrito ao domínio final (não mais `*`)

Custo adicional: **Load Balancer ~$18/mês** (mínimo, independe de
tráfego) + tráfego saída do GCS (~$0 pra volume baixo).

## Arquivos relevantes

- `pwa/index.html` — hardcoded URL da API (procurar `.a.run.app` ou
  `localhost`); precisa atualizar pra `https://api.<dominio>`
- `pwa/sw.js` — service worker pode ter URLs cacheadas
- `scripts/deploy/deploy_api.sh` — ajustar `CORS_ORIGINS=https://<dominio>`
- `scripts/deploy/upload_pwa.sh` — script pode ganhar `--cache-control`
  mais agressivo quando CDN estiver na frente
- `docs/deploy.md` §7 "Decisões futuras" — atualizar quando feito

## Missão

1. **Confirmar domínio com Fernando** — qual domínio? Já registrado?
   Onde (Registro.br, Cloudflare, Google Domains)?
2. **Verificar domínio no Google Cloud**:
   ```bash
   gcloud domains verify <dominio>
   ```
   Seguir o processo de adicionar TXT record no DNS.

3. **Cloud Run domain mapping pra API**:
   ```bash
   gcloud run domain-mappings create \
     --service=fiscal-cidadao-api \
     --domain=api.<dominio> \
     --region=southamerica-east1 \
     --project=fiscal-cidadao-493716
   ```
   Adicionar o CNAME/A record que o gcloud retornar.

4. **PWA com Load Balancer + CDN**:
   - Criar backend bucket: `gcloud compute backend-buckets create
     fiscal-cidadao-pwa-backend --gcs-bucket-name=fiscal-cidadao-pwa
     --enable-cdn`
   - URL map apontando `/` pro backend bucket
   - Target HTTPS proxy com managed cert: `gcloud compute
     ssl-certificates create fiscal-cidadao-cert
     --domains=<dominio>,www.<dominio> --global`
   - Global forwarding rule na porta 443
   - Adicionar registro A apontando `<dominio>` e `www.<dominio>` pro
     IP do forwarding rule

5. **Restringir CORS**:
   Editar `scripts/deploy/deploy_api.sh`:
   ```bash
   ENV_VARS="${ENV_VARS},CORS_ORIGINS=https://<dominio>,https://www.<dominio>"
   ```
   Rebuild + redeploy (prompt 02 de novo com a env var nova).

6. **Atualizar PWA pra apontar pra `https://api.<dominio>`**:
   - Editar `pwa/index.html` substituindo URL antiga
   - Rerun `bash scripts/deploy/upload_pwa.sh`
   - Force-refresh no navegador pra invalidar SW

7. **Smoke test final**:
   ```bash
   curl -fsS https://<dominio>/ | head -20           # PWA HTML via CDN
   curl -fsS https://api.<dominio>/health             # API via domain mapping
   ```
   Abrir `https://<dominio>` em navegador, confirmar PWA carrega e faz
   chamadas com sucesso pra `https://api.<dominio>`.

## Critérios de aceite

- Domínio registrado e verificado no GCP.
- `https://<dominio>` serve a PWA via Cloud CDN (response header
  `via: 1.1 google` ou `age: N` indica cache).
- `https://api.<dominio>/health` responde `{"status":"ok"}`.
- Certificado SSL status `ACTIVE` em ambos domínios (pode levar 15-60
  min pro managed cert provisionar).
- `CORS_ORIGINS` restrito ao domínio final; request de outra origem
  bloqueada pelo browser.
- `pwa/index.html` commitado apontando pra URL final.
- `docs/deploy.md` §7 atualizado documentando setup final.

## Guardrails

- Confirmar domínio + custo do Load Balancer ($18/mês) com Fernando
  antes de criar recursos.
- Managed cert demora até 1h pra sair de `PROVISIONING` → `ACTIVE`.
  Não recriar repetidamente.
- Se CDN cachear arquivo errado: `gcloud compute url-maps
  invalidate-cdn-cache` com path.
- Commits: `feat(deploy):` pro setup, `docs:` pra atualizações.
