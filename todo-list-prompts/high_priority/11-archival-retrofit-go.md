# Retrofit archival nos 10 pipelines GO legados

## Contexto

A camada de archival (`bracc_etl.archival.archive_fetch`) foi introduzida
pra gravar snapshots content-addressed do payload bruto das fontes no
momento da ingestão. A motivação: proveniência com só URL + timestamp
não sobrevive se o portal muda de endereço ou tira a página do ar.

O campo `source_snapshot_uri` está no contrato de proveniência, é
**opt-in** (nullable), e já é exposto na API como
`ProvenanceBlock.snapshot_url`. Pipelines legados continuam funcionando
sem ele — mas linha do tempo do Fiscal Cidadão exige cópia imutável da
fonte pra satisfazer o requisito de proveniência rastreável.

Ver [`docs/archival.md`](../../docs/archival.md) para a arquitetura.

## Escopo

Retrofitar os 10 pipelines GO atuais para:

1. Capturar `response.content` (bytes crus) no ponto de cada fetch HTTP.
2. Chamar `archive_fetch(url, content, content_type, run_id, source_id)`
   e guardar a URI retornada (geralmente em memória, chaveada por
   resource id / URL, pra reusar ao montar rows).
3. Passar `snapshot_uri=` em cada `attach_provenance(...)` que deriva
   daquela fetch.

## Pipelines e ordem sugerida (por valor)

| Ordem | Pipeline | Fonte | Complexidade |
| ---: | --- | --- | --- |
| 1 | `folha_go` | CSVs mensais dadosabertos.go.gov.br | Baixa |
| 2 | `pncp_go` | JSON PNCP por resource_id | Baixa |
| 3 | `alego` | HTML + PDF atos parlamentares | Média (PDF) |
| 4 | `ssp_go` | PDFs mensais de estatísticas de segurança | Média |
| 5 | `tce_go` | HTML portal TCE (dashboards) | Alta |
| 6 | `tcm_go` | HTML portal TCM | Alta |
| 7 | `tcmgo_sancoes` | HTML/PDF sanções | Média |
| 8 | `state_portal_go` | JSON/HTML portal do estado | Média |
| 9 | `querido_diario_go` | PDFs diários oficiais | Baixa (1 fetch por dia) |
| 10 | `camara_goiania` | JSON/HTML câmara municipal | Média |

Um commit atômico por pipeline. Cada commit deve:
- Manter os testes existentes verdes.
- Adicionar 1-2 testes cobrindo o novo comportamento (snapshot gravado,
  URI propagada pra `attach_provenance`).
- NÃO mudar o contrato de proveniência nem o módulo `archival` — isso
  já foi feito na fase de infra.

## Como testar cada pipeline

Em `etl/tests/test_{pipeline}_pipeline.py` adicione um caso:

```python
def test_carimba_source_snapshot_uri_em_rows(
    self, archival_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(archival_root))
    # ... rodar pipeline com mock de HTTP devolvendo bytes conhecidos ...
    # ... verificar que rows têm source_snapshot_uri plausível ...
    # ... verificar que arquivo existe em archival_root ...
```

Use a fixture `archival_root` existente em
`etl/tests/test_archival.py` como referência.

## Critério de conclusão

- 10 commits, 1 por pipeline, todos verdes em `make test-etl`.
- `grep -l "archive_fetch" etl/src/bracc_etl/pipelines/*.py` lista os
  10 pipelines GO.
- `docs/data-sources.md` atualizado citando que archival está ativo
  pra cada pipeline.

## Fora de escopo

- Pipelines federais (CNPJ, TSE, etc.) — Fiscal Cidadão restringe a GO.
- Backend GCS para archival — tracked separadamente.
- Endpoint HTTP pra servir snapshots ao cliente — depende de GCS.
