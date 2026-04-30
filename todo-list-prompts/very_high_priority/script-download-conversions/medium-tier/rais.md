# Converter `rais` de `file_manifest` pra `script_download`

> **DONE — 2026-04-29.** Convertido via Rota C (system `7z` binary
> subprocess sobre `RAIS_ESTAB_PUB.7z` ~120 MB), não BigQuery. Evita as
> creds GCP que estão pendentes de provisionamento (memória
> `credenciais_externas`) e fica no mesmo padrão do `qlik` (system bin
> não-pip). Pré-requisito: `apt install 7zip`. Pipeline emite só
> `:LaborStats` agregado por (CNAE subclasse × UF), com a coluna `year`
> agora dinâmica — `_from_aggregated` lê do CSV e do filename. CLI em
> `scripts/download_rais.py`. Arquivo mantido pelo histórico; deletar
> quando limpar `medium-tier/`.

## Contexto

Pipeline `rais` (Relação Anual de Informações Sociais — microdados
MTE/PDET) é um dos últimos 2 pipelines ainda em
`acquisition_mode: file_manifest` (o outro é `caged`). Todos os ~32
outros já usam `script_download`.

Bloqueado desde 2026-04-18 por combinação de: (a) dumps anuais multi-GB,
(b) PDET atrás de form-wall, (c) mirror em `basedosdados.org` exige
credenciais GCP que ainda não estão provisionadas no ambiente da
usuária.

Registro atual no contrato:

```json
{
  "pipeline_id": "rais",
  "acquisition_mode": "file_manifest",
  "required_inputs": ["data/rais/*"],
  "blocking_reason_if_any": "Multi-GB annual dumps em PDET form-wall; basedosdados.org exige credencial GCP/BigQuery (deps pyarrow + google-cloud-bigquery nao estao no base). Reconsiderar quando creds GCP estiverem disponiveis ou quando PDET expor mirror publico.",
  "core": false,
  "credential_env": ["GOOGLE_APPLICATION_CREDENTIALS"]
}
```

Pipeline é agregado (RAIS pública é de-identificada — sem CPF/CNPJ).
Gera só nós `:LaborStats` por `CNAE subclasse + UF`, pra usar como
sector reference data em queries (match por prefixo de CNAE), não
como relação direta.

## Status atual do upstream

- **PDET**: `https://pdet.mte.gov.br/rais` — form wall pra microdados; não dá pra programar sem quebrar ToS.
- **FTP arquivo**: `ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/` — intermitente; alguns anos em `.7z`, outros em `.zip`, vintages muito antigas em `.txt` fixed-width.
- **`basedosdados.org` (BigQuery público)**: tabela `basedosdados-public.br_me_rais.microdados_estabelecimentos` — exige conta GCP + `pyarrow` + `google-cloud-bigquery`.
- **Extract do pipeline** (`etl/src/bracc_etl/pipelines/rais.py:53-75`) tem 2 caminhos:
  1. **Caminho rápido** (pré-agregado): procura `data/rais/rais_2022_aggregated.csv`. Se existir, lê como `dtype=str`, espera colunas `cnae_subclass`, `uf`, `establishment_count`, `total_employees`, `total_clt`, `total_statutory`, `avg_employees`.
  2. **Fallback** (agrega in-process): procura `data/rais/RAIS_ESTAB_PUB*.txt*`, lê com `sep=";" encoding="latin-1"`, colunas "CNAE 2.0 Subclasse", "Qtd Vínculos Ativos", "Qtd Vínculos CLT", "Qtd Vínculos Estatutários", "UF" (código IBGE → mapeia pra sigla).

Se o `fetch_to_disk` entregar o CSV pré-agregado, o pipeline usa
caminho rápido e nunca precisa processar o `.txt` cru. Isso muda
fundamentalmente o ROI da conversão.

## Bloqueios

1. **Creds GCP ausentes**: usuária (`vengelkuraggidim@gmail.com`) é editor no projeto, mas precisa `secretAccessor` separado para GSM. Owner GCP é o esposo (`fernandoeq@live.com`) — provisionamento manual, não automatizável hoje. (Ver memória `credenciais_externas`.)
2. **Deps ausentes**: `pyarrow` + `google-cloud-bigquery` não estão em `pyproject.toml`.
3. **Escala**: RAIS Estabelecimentos ~2–4 GB/ano cru; RAIS Vínculos ~50+ GB/ano. Full-history ingest sobe fácil de 100 GB em disco. `--year` tem que ser obrigatório na CLI, sem default pra histórico completo.
4. **PDET form-wall**: não automatizável.

## Como investigar antes de mexer

1. **Procurar CSV agregado aberto**: alguém (IPEA, FGV, DataLake gov, CKAN municipal) às vezes publica RAIS agregada por ano em URL estável. Se achar, é quick-win — `fetch_to_disk` vira 20 linhas de `httpx`.
2. **Confirmar com dona do projeto** se basedosdados via BigQuery é direção desejada. Implica:
   - Adicionar `pyarrow` + `google-cloud-bigquery` a `pyproject.toml`.
   - Manter `credential_env: [GOOGLE_APPLICATION_CREDENTIALS]` no contrato.
   - Aceitar que pipeline só roda em ambientes com creds provisionadas (local dela hoje não é um deles).

### Investigação 2026-04-23

FTP listing `RAIS/2022/` retorna apenas `.7z` (RAIS_ESTAB_PUB.7z 91 MB
+ RAIS_VINC_PUB_*.7z um por região, total ~3 GB). Sem `.csv.gz`,
`.zip` ou `.parquet` no espelho oficial. **Rota A via PDET fica
indisponível** — ainda depende de outro espelho aberto (IPEA, FGV,
DataLake) ou da Rota B (BigQuery + creds GCP). Próxima ação requer
decisão da dona sobre dep + creds.

## Fix proposto (quando desbloquear)

### Rota A — CSV agregado em URL aberta (preferida)

1. `fetch_to_disk(output_dir, *, year, limit=None)` em `etl/src/bracc_etl/pipelines/rais.py`:
   - `httpx` GET → salva como `rais_<year>_aggregated.csv` (nome casa com caminho rápido do `extract()`).
2. `scripts/download_rais.py` (argparse):
   - `--year` **obrigatório**, repetível (aceitar múltiplos `--year 2022 --year 2023`).
   - `--output-dir`, `--limit`.
   - Guarda: se range total > 3 anos, exigir flag `--force-full` (evitar baixar histórico por acidente).
3. Flip contract entry: `acquisition_mode: script_download`, limpar `blocking_reason_if_any`, **remover** `credential_env` (não precisa mais), adicionar `download_commands`.

### Rota B — BigQuery via basedosdados.org

Só se Rota A não render e/ou equipe priorizar dados mais recentes que o
agregado público disponível.

1. Adicionar `pyarrow` + `google-cloud-bigquery` a `pyproject.toml`.
2. `fetch_to_disk`:
   - Autentica via `GOOGLE_APPLICATION_CREDENTIALS`.
   - Query SQL em `basedosdados-public.br_me_rais.microdados_estabelecimentos` filtrada por ano; `GROUP BY cnae_2_subclasse, sigla_uf` (replicar o que o fallback `_aggregate_raw` faz in-process).
   - `.to_dataframe()` → salva `rais_<year>_aggregated.csv` no layout do caminho rápido.
3. CLI + flags iguais à Rota A.
4. Flip contract: `acquisition_mode: script_download`, **manter** `credential_env`, adicionar `download_commands`.

## Prioridade

**Baixa.** `:LaborStats` RAIS é sector reference data e não está no
caminho quente da ingesta política GO. Deixar na fila até:

- Equipe priorizar enriquecimento setorial por CNAE; OU
- Creds GCP forem provisionadas (ver memória `credenciais_externas`); OU
- Achar CSV agregado RAIS aberto em URL estável (Rota A vira quick-win).

## Arquivos envolvidos

- `etl/src/bracc_etl/pipelines/rais.py` — adicionar `fetch_to_disk()` no módulo.
- `scripts/download_rais.py` — criar (CLI argparse).
- `config/bootstrap_all_contract.yml` — flip `acquisition_mode` + limpar `blocking_reason_if_any` + adicionar `download_commands` + (Rota A) remover `credential_env`.
- `pyproject.toml` — (condicional, só Rota B) adicionar `pyarrow` + `google-cloud-bigquery`.

## Referências

- Padrão canônico: `todo-list-prompts/very_high_priority/script-download-conversions/PATTERN.md`.
- Exemplo minimalista: `etl/src/bracc_etl/pipelines/tcu.py` + `scripts/download_tcu.py`.
- CSV Portal Transparência: `etl/src/bracc_etl/pipelines/tesouro_emendas.py`.
- ZIP-consolidado-split-por-ano (se PDET publicar ZIP com múltiplos anos): `etl/src/bracc_etl/pipelines/siop.py::fetch_to_disk`.
