# Bug — `tse_prestacao_contas_go` dropa 100% das despesas no transform

## Contexto

Pipeline `tse_prestacao_contas_go` foi executado contra dados reais TSE 2022
em **2026-04-18** (primeira vez end-to-end sem fixture). Resultado do log:

```
[tse_prestacao_contas_go] uf=GO year=2022 receitas=12015 despesas=113071 bens=0
[tse_prestacao_contas_go] transformed persons=1171 donations=12015 expenses=0 rels=12015
[tse_prestacao_contas_go] loaded persons=1171 donations=12015 expenses=0
```

**113.071 linhas de despesa foram extraídas do CSV, 0 sobreviveram ao
transform.** Receitas (mesma estrutura de CPF/valor) passou 12.015 → 12.015,
então o parser CSV em si funciona — o problema está nos nomes de coluna que
o transform espera.

Consequência: property `total_despesas_tse_2022` está em **0.0 pra todos
os 1.171 candidatos GO**, o que quebra o cross-check com teto legal em
`api/src/bracc/services/teto_service.py::calcular_teto` (fica reportando
"0% do teto utilizado" pra todo mundo). É um gap silencioso — pipeline
completou com exit 0, log avisou mas sem raise.

Bens = 0 é **independente** e esperado: CSV de bens vive no ZIP irmão
`bem_candidato_2022.zip`, ingerido pelo pipeline `tse_bens`. Não é parte
deste bug.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py`
  - linha 510: `valor_raw = row.get("VR_PAGTO_DESPESA") or row.get("VR_DESPESA_CONTRATADA") or "0"`
  - linha 498-514: loop de despesas com filtros `len(cpf_digits) != 11` e `valor <= 0`
  - linha 499-502: CPF via `NR_CPF_CANDIDATO` ou `CPF_CANDIDATO`
  - linha 527-530: fornecedor via `NR_CPF_CNPJ_FORNECEDOR` / `CPF_CNPJ_FORNECEDOR`
- `etl/archival/tse_prestacao_contas/2026-04/954b8a10119c.bin` — ZIP
  arquivado (433 MB, content-addressed). **Re-executável sem re-download**
  — basta `archive_fetch` bater cache no segundo run.
- `etl/tests/test_tse_prestacao_contas_go.py` — tem fixtures minimais; o
  fixture usa colunas `VR_PAGTO_DESPESA` / `NR_CPF_CANDIDATO` e passa, o
  que mascarou o bug. **Tests precisam ser regenerados a partir do CSV real.**

## Hipóteses (pra investigar antes de editar)

1. **Nome da coluna de valor mudou** — TSE pode ter migrado pra
   `VR_DESPESA`, `VR_PAGO`, `VALOR_DESPESA`, ou separado em
   `VR_DESPESA_CONTRATADA` (que **está** no `or` mas ficou em 0 — então
   talvez a coluna real seja outra).
2. **Nome da coluna de CPF mudou no arquivo de despesas** — receitas tem
   `NR_CPF_CANDIDATO` mas despesas pode usar `CPF_CANDIDATO` sem o `NR_` ou
   variação tipo `NR_CPF_CANDIDATO_FINANCEIRO`. O filtro
   `len(cpf_digits) != 11` dropa tudo silenciosamente se a coluna não bate.
3. **Valor chega como string vazia** — `parse_numeric_comma("")` → 0.0,
   então `valor <= 0` pula. Se a coluna existe mas vem vazia (campo opcional
   da TSE), todas as 113k rows pulam.
4. **Campo de UF no despesas não é `SG_UF` nem `UF_ELEICAO`** — se o
   filtro UF no extract (linha 332) passou (113.071 linhas), esta não é a
   causa. Só mencionado pra descartar.

## Missão

1. **Inspecionar o CSV real sem editar código ainda**:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc
   python -c "
   import zipfile, io, csv
   z = zipfile.ZipFile('etl/archival/tse_prestacao_contas/2026-04/954b8a10119c.bin')
   for name in z.namelist():
       if 'despesas_pagas_candidatos_2022_BRASIL.csv' in name.lower() or \
          'despesas_pagas_candidatos_2022_brasil.csv' in name.lower():
           with z.open(name) as fh:
               text = io.TextIOWrapper(fh, encoding='latin-1', newline='')
               reader = csv.DictReader(text, delimiter=';')
               print('FIELDS:', reader.fieldnames)
               for i, row in enumerate(reader):
                   if (row.get('SG_UF') or row.get('UF_ELEICAO') or '').upper() == 'GO':
                       print('SAMPLE GO ROW:', row)
                       break
           break
   "
   ```
   Output esperado: lista exata de `fieldnames` + 1 row GO pra ver quais
   colunas têm o CPF do candidato e o valor pago.

2. **Ajustar o `or`-chain** em `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py`:
   - Adicionar os nomes de coluna reais descobertos na etapa 1 ao
     `row.get(...) or row.get(...) or "0"` da linha 510 (valor) e 499-502
     (CPF).
   - **Não remover** os nomes antigos — alguns ZIPs TSE mais velhos
     (2018/2014) podem usar os nomes de código atual; manter fallback.

3. **Regenerar fixtures** em `etl/tests/fixtures/` (ou onde estiverem):
   extrair ~20 linhas reais GO do CSV descoberto em (1), anonimizar o
   CPF candidato, salvar como `despesas_pagas_candidatos_2022_BRASIL.csv`
   dentro de um ZIP fixture minimal. **Sem esse passo, o test-suite passa
   mas o bug volta.**

4. **Adicionar guard no transform**: se `len(self._despesas_raw) > 0` e
   `len(self.expenses) == 0` ao final do loop de despesas, emitir log
   `ERROR` (não warning). Falha silenciosa é pior que pipeline quebrado
   aqui — teto legal fica errado e ninguém nota. Considerar `raise
   RuntimeError` gated por env var (ex: `BRACC_STRICT_TRANSFORM=1`) pra
   produção Cloud Run abortar.

5. **Re-rodar `uv run bracc-etl run --source tse_prestacao_contas_go`**
   local (archival bate cache, é ~2min). Validar via cypher-shell:
   ```cypher
   MATCH (p:Person) WHERE p.total_despesas_tse_2022 > 0
   RETURN count(p) AS candidatos_com_despesas, avg(p.total_despesas_tse_2022) AS media;
   ```
   Esperado: count > 1.000 (próximo dos 1.171 com receitas), média na
   casa de dezenas/centenas de milhares de reais.

6. **Checar se `tse_bens` tem o mesmo padrão de bug**: pode ser que o
   pipeline irmão `etl/src/bracc_etl/pipelines/tse_bens.py` também use
   nomes de coluna desatualizados. Run + validate via cypher (`patrimonio_declarado`
   count esperado ≥ 1.000). Os 1.176 candidatos com patrimônio no grafo
   hoje são de execução prévia — confirmar que rodada nova não regride.

## Critérios de aceite

- `total_despesas_tse_2022 > 0` em ≥ 95% dos 1.171 candidatos GO com
  `total_tse_2022 > 0`.
- Fixture de test carrega CSV com colunas reais TSE 2022 (não sintéticas)
  e valida ≥ 1 expense gerada.
- `make test-etl` passa.
- Guard de falha silenciosa emite ERROR log quando `extract > 0, transform == 0`.
- Archival não regenera (mesmo SHA `954b8a10119c` continua sendo o snapshot).

## Guardrails

- **Não mexer no archival** — re-execução usa cache content-addressed.
- **Não commitar fixture com CPF real** — mascarar pra `***.***.***-XX`
  antes de commitar, ou usar CPF válido sintético (gerador dev).
- Provenance: rows novas continuam recebendo `source_id`,
  `source_record_id`, `source_url`, `source_snapshot_uri`. Não quebrar
  `attach_provenance` call em linha 542-557.
- `make pre-commit` verde (neutrality + contracts).

## Prioridade

**very_high** — bug silencioso ativo em prod quando o pipeline rodar no
Cloud Run; `teto_service.calcular_teto` reporta 0% pra todo candidato
2022, o que é um claim factualmente errado exibido pro usuário. Consertar
antes do primeiro deploy público.

## Timestamps da execução que pegou o bug

- Início: 2026-04-18T17:35:27Z
- Fim:    2026-04-18T17:37:23Z
- ZIP:    433 MB (`etl/archival/tse_prestacao_contas/2026-04/954b8a10119c.bin`)
- Run ID: `tse_prestacao_contas_20260418143534` (aproximado, ver log do
  `Neo4jBatchLoader`)
