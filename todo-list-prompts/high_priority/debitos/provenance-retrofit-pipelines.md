# Retrofit de provenance em pipelines recém-fixados — ✅ RESOLVIDO no codigo (2026-04-22)

> Commit `49657f6` aplica o padrao `_stamp()` + `attach_provenance()`
> nos 6 pipelines listados abaixo (camara, tesouro_emendas, siop, pgfn,
> senado; comprasnet ja tinha sido feito previamente em `0d407d5`).
>
> Cada pipeline ganha um helper `_stamp(row, record_id=...)` e cada
> node/rel lista passa por ele antes do `loader.load_*`. Rels recebem
> SET com os 5 campos de provenance (source_id, source_record_id,
> source_url, ingested_at, run_id).
>
> Pendente: reingestao dos dados ja carregados (backfill retroativo) —
> debito separado em `rodar-pipelines-pesados.md` pra comprasnet/pgfn
> pesados.


## Contexto

Na sessão de 2026-04-19 (commits `42e8228` + reruns dos prompts 01–05 de
`medium_priority/more_data/`), 6 pipelines receberam fix trivial de
`rows_in`/`rows_loaded` no `IngestionRun`. Isso resolveu o badge "com dados"
na PWA, mas não tocou no **segundo** débito que os mesmos pipelines
arrastam: nodes e relationships carregados **sem** carimbo de proveniência
(`source_id` / `source_url` / `ingested_at` / `run_id`).

O loader emite warning `[provenance:nodes:<Label>] N/N rows violate
contract (missing ['source_id', 'source_url', 'ingested_at', 'run_id'])`
em cada batch. Contrato documentado em `docs/provenance.md`. Nenhum
pipeline quebra, mas a rastreabilidade exigida pela missão do projeto
não está carimbada.

## Pipelines afetados

| Source | Node labels sem provenance | Rel types sem provenance |
|---|---|---|
| `camara` | Deputy, FederalExpense (e derivados) | GASTOU, REGISTROU (ou equivalentes) |
| `tesouro_emendas` | AmendmentPayment (ou equivalente) | PAGOU |
| `senado` | Senator, SenatorExpense | GASTOU |
| `siop` | BudgetItem, FederalAmendment | ALOCOU, AUTOR |
| `pgfn` | Finance | DEVE |
| `comprasnet` | Contract, Company | VENCEU, REFERENTE_A |

Confirmado nesta sessão:
- `pgfn` rerun 05:12: warnings de Finance (1.007.085 rows) e DEVE.
- `comprasnet` rerun 05:20: warnings de Contract (1.312.074), Company
  (189.813), VENCEU (1.312.074), REFERENTE_A (1.312.074).

Pipelines que **já** carimbam provenance (modelos de referência):
- `etl/src/bracc_etl/pipelines/transparencia.py` (commit `4ed081f`)
- `etl/src/bracc_etl/pipelines/tse.py` / `tse_bens.py` (commits `55490f7` / `3345535`)
- `etl/src/bracc_etl/pipelines/cvm.py` / `cvm_funds.py` (commits `8456d3f` / `bf06b37`)

## Padrão de fix (idêntico ao dos commits de referência)

1. Em `transform()` (ou onde rows são montadas):
   ```python
   row = self.attach_provenance(
       row,
       record_id=<id único do registro>,
       record_url=<URL canônica da fonte>,
   )
   ```
   Use a constante `_SOURCE_URL` do próprio módulo (ex: `_CVM_SOURCE_URL`)
   quando o `source_id` não bater com o registry — evita `ValueError`
   no lookup. Caso o `source_id` já seja canônico (post commit `d23baee`),
   omita `record_url` e deixa o helper resolver.

2. Em `load()`, para **relationships**, propague os campos provenance
   no `SET` da query Cypher:
   ```cypher
   MATCH (a:Node1 {id: row.a_id}), (b:Node2 {id: row.b_id})
   MERGE (a)-[r:REL_TYPE {...}]->(b)
   SET r.source_id = row.source_id,
       r.source_record_id = row.source_record_id,
       r.source_url = row.source_url,
       r.ingested_at = row.ingested_at,
       r.run_id = row.run_id
   ```
   O dict passado como `rows` no `loader.load_relationships` precisa ter
   esses campos — geralmente vêm de `attach_provenance` no dict raiz
   do relationship antes do load.

3. Teste rodando o pipeline local e conferindo que não aparece mais
   warning `[provenance:...] N/N rows violate contract`:
   ```bash
   NEO4J_PW="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)"
   cd etl && uv run python -m bracc_etl.runner run --source <src> \
     --neo4j-password "$NEO4J_PW" \
     --data-dir /home/alladrian/PycharmProjects/br-acc/data 2>&1 | grep -i provenance
   # Saída esperada: vazia ou só mensagens INFO do enforcer, sem WARNING de missing.
   ```

4. Validar no grafo que todos os nodes/rels daquela label têm provenance:
   ```cypher
   MATCH (n:<Label>) WHERE n.run_id = '<run_id_novo>'
   RETURN count(CASE WHEN n.source_id IS NULL THEN 1 END) AS sem_source_id,
          count(CASE WHEN n.source_url IS NULL THEN 1 END) AS sem_source_url,
          count(n) AS total
   ```

## Critério de "pronto"

- Os 6 pipelines acima rodados localmente sem emitir warning
  `[provenance:...] violate contract`.
- `make pre-commit` verde (inclui `check-provenance-contract`).
- Commits atômicos por pipeline ou 1 commit combinado com diff claro.
  Mensagem estilo: `feat(etl): <src> — carimba provenance em nodes/rels`.

## Escopo explícito do que NÃO fazer aqui

- **Não** reingerir dados históricos. O backfill retroativo dos nodes
  existentes é outro débito (decisão editorial: vale limpar a base?).
  Este débito trata só dos pipelines pra runs **futuras** gerarem
  nodes/rels com provenance.
- **Não** refatorar estrutura do pipeline (batch size, transform
  signature, etc.) — mantém o padrão atual.

## Referência rápida

- Helper: `bracc_etl.base.Pipeline.attach_provenance()`
- Enforcer: `bracc_etl.loader.<...>.enforce_provenance()` (emite os warnings)
- Contrato: `docs/provenance.md`
- Governance check: `make check-provenance-contract`
