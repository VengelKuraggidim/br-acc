# IAM: dar `secretAccessor` pra `vengelkuraggidim@gmail.com` no GCP — ⏳ PENDENTE (2026-04-22)

> Bloqueia rodar a stack local apontando pro Aura prod / GCP Secret Manager
> com a conta da usuária. Setup local→Aura prod ficou parado nesta etapa.

## Contexto

Em 2026-04-22 a usuária configurou o `.env` pra rodar local apontando pro
Aura prod (5cb9f76f) usando GCP Secret Manager pros segredos. Setup
seguiu o que o esposo (owner do projeto) sugeriu:

```
NEO4J_URI=neo4j+s://5cb9f76f.databases.neo4j.io
NEO4J_USER=5cb9f76f
NEO4J_DATABASE=5cb9f76f
GCP_PROJECT_ID=fiscal-cidadao-493716
```

Rodou `gcloud auth application-default login` com `vengelkuraggidim@gmail.com`.
ADC ficou OK (token gerado), conexão GCP API funcionando.

**Mas o smoke test falhou** ao puxar `NEO4J_PASSWORD` do GSM:

```
google.api_core.exceptions.PermissionDenied: 403
Permission 'secretmanager.versions.access' denied on resource
```

## Por que falhou

IAM do projeto `fiscal-cidadao-493716` (verificado via `gcloud projects
get-iam-policy`):

- `roles/owner` → `fernandoeq@live.com` (esposo)
- `roles/editor` → `vengelkuraggidim@gmail.com` (usuária)

**Pegadinha do GCP**: roles `owner` e `editor` legacy **não incluem
acesso a Secret Manager** — secrets foram excluídos dos basic roles de
propósito. Precisa adicionar `roles/secretmanager.secretAccessor`
explicitamente, mesmo pra owner.

## Missão

**Esposo precisa rodar (uma vez, ele tem owner):**

```
gcloud projects add-iam-policy-binding fiscal-cidadao-493716 \
  --member=user:vengelkuraggidim@gmail.com \
  --role=roles/secretmanager.secretAccessor
```

Isso dá acesso a todos os secrets do projeto. Se preferir granular, roda
por secret:

```
gcloud secrets add-iam-policy-binding fiscal-cidadao-neo4j-password \
  --member=user:vengelkuraggidim@gmail.com \
  --role=roles/secretmanager.secretAccessor
# idem pra fiscal-cidadao-transparencia-key e fiscal-cidadao-jwt-secret
```

## Validar depois

Smoke test pronto em `/tmp/smoke_aura.py` (não commitado — script
descartável):

```
api/.venv/bin/python /tmp/smoke_aura.py
```

Esperado:
- Puxa senha do GSM (sem erro)
- Conecta no Aura
- Conta de FedLeg uf=GO retorna >0 (esperado: 17 conforme `project_ceap_federal_ingerido.md`)

Se o `/tmp/smoke_aura.py` não estiver mais lá quando voltarmos, dá pra
recriar facilmente — é ~50 linhas de Python.

## Arquivos relevantes

- `api/src/bracc/secrets.py` — `load_secret()` busca no GSM via ADC.
- `.env` — NEO4J_URI/USER/DATABASE/GCP_PROJECT_ID já configurados.
- Memos relacionados:
  - `project_credenciais_externas.md` — papéis IAM no GCP
  - `reference_local_aponta_aura_prod.md` — setup completo

## Critérios de aceite

- [ ] Esposo rodou `add-iam-policy-binding` (owner ou granular)
- [ ] `gcloud projects get-iam-policy fiscal-cidadao-493716` mostra
  `vengelkuraggidim@gmail.com` em `roles/secretmanager.secretAccessor`
- [ ] Smoke test `/tmp/smoke_aura.py` passa (senha puxada + Aura
  respondendo + count de FedLeg GO > 0)

## Prioridade

**Alta.** Bloqueia qualquer trabalho local que precise bater no Aura
prod ou ler secrets do GSM. Fix é um único comando do esposo —
custo baixo, valor alto.
