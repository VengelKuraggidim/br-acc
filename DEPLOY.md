# Deploy - Fiscal Cidadao

Guia para colocar o app online usando **Oracle Cloud Free Tier** (gratis pra sempre).

---

## Passo 1: Criar conta na Oracle Cloud

1. Acesse [cloud.oracle.com](https://cloud.oracle.com) e crie uma conta
2. Vai pedir cartao de credito para verificacao, mas **nao cobra nada** no free tier
3. Escolha a regiao mais proxima (ex: Brazil East - Sao Paulo)

## Passo 2: Criar a VM (servidor)

1. No painel Oracle Cloud, va em **Compute > Instances > Create Instance**
2. Configure:
   - **Nome:** fiscal-cidadao
   - **Image:** Ubuntu 22.04 (ou 24.04)
   - **Shape:** Ampere A1 (ARM) - escolha **4 OCPUs e 24GB RAM** (tudo gratis)
   - **Boot volume:** 100GB (gratis ate 200GB)
3. Em **Add SSH keys**, baixe a chave ou cole sua chave publica (`~/.ssh/id_rsa.pub`)
4. Clique em **Create**
5. Anote o **IP publico** que aparece (ex: `132.145.xxx.xxx`)

## Passo 3: Liberar portas na Oracle Cloud

**IMPORTANTE** - Sem isso o site nao abre!

1. No painel, va em **Networking > Virtual Cloud Networks**
2. Clique na VCN da sua instancia
3. Clique na **subnet** > **Security List**
4. Adicione 2 regras de **Ingress**:
   - Porta **80** (HTTP): Source `0.0.0.0/0`, TCP, Destination Port `80`
   - Porta **443** (HTTPS): Source `0.0.0.0/0`, TCP, Destination Port `443`

## Passo 4: Acessar o servidor

```bash
ssh -i sua-chave.key ubuntu@SEU_IP_PUBLICO
```

## Passo 5: Setup do servidor

```bash
# Clonar o projeto (ou copiar via scp)
git clone SEU_REPOSITORIO ~/fiscal-cidadao
cd ~/fiscal-cidadao

# Rodar setup (instala Docker, abre firewall)
sudo bash deploy/setup-server.sh
```

## Passo 6: Configurar variaveis de ambiente

```bash
# Copiar o exemplo e editar
cp deploy/.env.prod.example .env
nano .env
```

Edite pelo menos:
- `NEO4J_PASSWORD` - coloque uma senha forte
- `JWT_SECRET_KEY` - rode `openssl rand -hex 32` e cole o resultado
- `DOMAIN` - seu dominio (se tiver) ou o IP publico

## Passo 7: Subir o app

```bash
# Subir tudo
docker compose -f docker-compose.prod.yml up -d

# Ver os logs
docker compose -f docker-compose.prod.yml logs -f

# Verificar se tudo esta rodando
docker compose -f docker-compose.prod.yml ps
```

Pronto! Acesse `http://SEU_IP_PUBLICO` no navegador.

## Passo 8 (opcional): Dominio + HTTPS

Se voce tiver um dominio (pode pegar gratis no [Freenom](https://freenom.com) ou barato no [Registro.br](https://registro.br)):

1. No painel DNS do seu dominio, crie um registro **A** apontando para o IP da VM
2. Rode o script de SSL:

```bash
sudo bash deploy/setup-ssl.sh seudominio.com
```

Isso configura HTTPS automatico com Let's Encrypt (gratis).

---

## Carregar os dados (ETL)

Os dados do Neo4j nao sobem automaticamente. Para carregar:

```bash
# Se voce ja tem o dump do Neo4j local:
# Copie pra VM via scp e restaure

# OU rode o ETL para baixar dados novos:
docker compose -f docker-compose.prod.yml --profile etl run etl python -m bracc.etl.run
```

## Comandos uteis

```bash
# Parar tudo
docker compose -f docker-compose.prod.yml down

# Reiniciar
docker compose -f docker-compose.prod.yml restart

# Ver logs de um servico
docker compose -f docker-compose.prod.yml logs -f bracc-api

# Atualizar (apos git pull)
docker compose -f docker-compose.prod.yml up -d --build
```

## Quanto custa

| Recurso | Oracle Free Tier |
|---------|-----------------|
| VM ARM 4 OCPU + 24GB RAM | Gratis |
| 100GB disco | Gratis |
| 10TB trafego/mes | Gratis |
| IP publico | Gratis |
| **Total** | **R$ 0,00/mes** |
