# Converter `caged` de `file_manifest` pra `script_download`

## Contexto

Pipeline `caged` (Novo CAGED — emprego formal PDET/MTE) é um dos
últimos 2 pipelines ainda em `acquisition_mode: file_manifest` no
`config/bootstrap_all_contract.yml` (o outro é `rais`). Todos os ~32
outros já migraram pra `script_download` com padrão `fetch_to_disk()`
no módulo + CLI em `scripts/download_<name>.py`.

Bloqueado desde 2026-04-18 pela dependência ausente `py7zr` — upstream
PDET serve microdados em `.7z` e stdlib Python não lê esse formato.

Registro atual no contrato:

```json
{
  "pipeline_id": "caged",
  "acquisition_mode": "file_manifest",
  "required_inputs": ["data/caged/*"],
  "blocking_reason_if_any": "Upstream PDET serve microdados em .7z; py7zr nao esta nos deps. Pipeline aggregate-only/stale — conversao tem ROI baixo. Re-avaliar quando py7zr for aprovado ou quando PDET expor mirror .csv.gz.",
  "core": false
}
```

Pipeline é **aggregate-only / stale**: só computa totais de admissão e
desligamento por `ano+mês+UF+município+CNAE subclasse+CBO+tipo` e grava
como nó `:LaborStats`. Não faz linkage a `:Person`/`:Company`. ROI de
converter é baixo comparado ao esforço de destravar o `.7z`.

## Status atual do upstream

- **PDET/MTE FTP**: `ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/<YYYY>/<YYYYMM>/CAGEDMOV<YYYYMM>.7z` — cada mês ~150–400 MB descompactado.
- **Portal HTTP**: `https://pdet.mte.gov.br/microdados-novo-caged` — login wall; não programático.
- **Extract do pipeline** (`etl/src/bracc_etl/pipelines/caged.py:96-100`): espera `caged_*.csv` em `data/caged/`. Lê como `dtype=str`, `keep_default_na=False`, separador default (vírgula). Colunas esperadas: `ano`, `mes`, `sigla_uf`, `id_municipio`, `cnae_2_subclasse`, `cbo_2002`, `tipo_movimentacao`, `salario_mensal`.

Ou seja, `fetch_to_disk` teria que: baixar `.7z` mensal → descompactar
→ renomear/remapear colunas (PDET usa `UF`, `Município`, `Seção`,
`Subclasse 2.0 CNAE`, etc., em maiúsculas e com acento) → salvar como
`caged_<YYYYMM>.csv` no layout que o `extract()` já consome.

## Bloqueios

1. **`py7zr` não está em `pyproject.toml`.** Alternativas:
   - `libarchive` → binding C, depende de lib de sistema (`libarchive-dev`); ruim em Docker slim.
   - `7z` CLI via `subprocess` → precisa do binário instalado no host; frágil.
   - Pura-Python sem dep → inviável para 7z.
2. **Aggregate-only / stale**: conversão não desbloqueia enriquecimento de grafo — `:LaborStats` CAGED fica como sector reference data, e nem é consumido em query política GO hoje.

## Como investigar antes de mexer

Antes de defender `py7zr`, checar se PDET começou a publicar mirror em
formato que stdlib lê:

```bash
# listing do diretório do mês
curl -s 'ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/2024/202401/'
# procurar extensões alternativas: .csv.gz, .zip, .parquet
```

Se houver `.csv.gz` ou `.zip`, vira quick-win: `fetch_to_disk` usa só
`httpx` + stdlib (`gzip` / `zipfile`), sem dep nova.

## Fix proposto (quando desbloquear)

### Rota A — PDET publicar mirror `.csv.gz`/`.zip` (preferida)

1. `fetch_to_disk(output_dir, *, year, month, limit=None)` em `etl/src/bracc_etl/pipelines/caged.py`:
   - `httpx` stream download → descomprime com `gzip`/`zipfile` → remapeia colunas pra layout lower-case esperado pelo `extract()` → salva `caged_<YYYYMM>.csv`.
   - Respeita `limit` cortando linhas pré-gravação.
2. Criar `scripts/download_caged.py` (argparse):
   - `--year` obrigatório, `--month` repetível (default = todos os 12), `--output-dir`, `--limit`.
3. Smoke test:
   ```python
   from unittest.mock import MagicMock
   from bracc_etl.pipelines.caged import CagedPipeline
   p = CagedPipeline(driver=MagicMock(), data_dir="./data")
   p.extract(); p.transform(); p.load()  # load escreve LaborStats
   ```
4. Flip contract entry: `acquisition_mode: script_download`, limpar `blocking_reason_if_any`, adicionar `download_commands`.

### Rota B — adicionar `py7zr` ao projeto

Só se Rota A não render e algum débito concreto pedir LaborStats CAGED.
Confirmar com dona do projeto antes de mexer em `pyproject.toml`.
Passos iguais à Rota A, mas com descompactação via `py7zr.SevenZipFile`.

## Prioridade

**Baixa.** Pipeline é aggregate-only e não está no caminho quente da
ingesta política GO. Deixar na fila até:

- Alguém pedir `:LaborStats` CAGED em análise; OU
- PDET publicar mirror `.csv.gz`/`.zip` (então vira quick-win, fica alta); OU
- Projeto decidir adicionar `py7zr` por outro motivo.

## Arquivos envolvidos

- `etl/src/bracc_etl/pipelines/caged.py` — adicionar `fetch_to_disk()` no módulo.
- `scripts/download_caged.py` — criar (CLI argparse).
- `config/bootstrap_all_contract.yml` — flip `acquisition_mode` + limpar `blocking_reason_if_any` + adicionar `download_commands`.
- `pyproject.toml` — (condicional, só Rota B) adicionar `py7zr`.

## Referências

- Padrão canônico: `todo-list-prompts/very_high_priority/script-download-conversions/PATTERN.md`.
- Exemplo minimalista: `etl/src/bracc_etl/pipelines/tcu.py` + `scripts/download_tcu.py`.
- CSV Portal Transparência: `etl/src/bracc_etl/pipelines/tesouro_emendas.py` + `scripts/download_tesouro_emendas.py`.
- ZIP em memória (pattern se upstream publicar `.zip`): `etl/src/bracc_etl/pipelines/camara.py::_download_ceap_csv`.
