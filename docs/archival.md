# Camada de archival de fontes externas

## Motivação

O contrato de proveniência (ver [`provenance.md`](provenance.md)) grava
`source_url` + `ingested_at` em cada nó e relacionamento — isso aponta
onde o dado estava e quando foi lido. Não basta.

Governos brasileiros mudam URLs, reestruturam portais e tiram páginas do
ar com frequência. Um usuário que volta pra verificar um fato semanas
depois pode encontrar 404, redirect quebrado ou conteúdo completamente
diferente. Fiscal Cidadão precisa poder mostrar **prova** de que o dado
existia na forma reportada no momento em que foi ingerido.

A solução é gravar uma **cópia bruta content-addressed** do payload HTTP
da fonte no momento da ingestão e carimbar a URI dessa cópia no bloco
de proveniência de cada row derivado daquela fetch.

## API

O módulo `bracc_etl.archival` expõe duas funções:

```python
from bracc_etl.archival import archive_fetch, restore_snapshot

# Dentro de Pipeline.extract() ou .transform():
content = response.content           # bytes crus do HTTP
uri = archive_fetch(
    url=response.url,
    content=content,
    content_type=response.headers["content-type"],
    run_id=self.run_id,
    source_id=self.source_id,
)
# uri é uma string relativa: "folha_go/2026-04/abc123def456.csv"

# Depois, ao montar cada row:
row = self.attach_provenance(
    {"cpf": "...", "nome": "..."},
    record_id="...",
    record_url="https://...",
    snapshot_uri=uri,          # ← carimba a cópia imutável
)

# Em testes/debug, pra re-ler o snapshot:
raw = restore_snapshot(uri)
```

### Contrato

- **Content-addressed**: caminho = `{source_id}/{YYYY-MM}/{sha256[:12]}.{ext}`.
  Mesmo `content` sempre cai no mesmo caminho → idempotente. Chamar
  `archive_fetch` N vezes com o mesmo payload grava 1 vez só; as demais
  retornam a URI existente sem reescrever.
- **Bucket mensal** (`YYYY-MM`) deriva do `run_id` (formato canônico
  `{source_id}_YYYYMMDDHHMMSS`). Evita diretórios gigantes ao longo de
  meses.
- **Extensão** deriva de `content_type` (`text/html` → `.html`,
  `application/json` → `.json`, PDF/PNG/XML/CSV/texto suportados).
  Desconhecido → `.bin` (payload segue preservado).
- **Escrita atômica**: `.tmp` + `rename`, protege contra leitores que
  pegam o arquivo a meio caminho.

## Storage

Raiz configurável via variável de ambiente:

```bash
BRACC_ARCHIVAL_ROOT=/var/lib/bracc/archival  # default: ./archival/
```

Relativo é resolvido a partir do `cwd` do pipeline (os alvos do `Makefile`
dão `cd etl/` antes de rodar, então o default acaba sendo
`etl/archival/`). O diretório `archival/` está em `.gitignore` — snapshots
são reproduzíveis via re-run do pipeline, nunca entram no repo.

### Hook pra GCS / S3 / IPFS

Fernando roda o Fiscal Cidadão em GCP (Asgard Studio). O próximo passo
natural é plugar um adapter que envia o mesmo blob content-addressed
pra um bucket GCS, mantendo o formato de URI idêntico em dev e prod.

O hook esperado: subclasse ou adapter que reescreve `_write_bytes` /
`_read_bytes` pra apontar pro bucket, preservando `"mesmo content →
mesma URI → idempotente"`. **Não reinventar o formato da URI** — dados
já carimbados no grafo com URIs no padrão atual precisam continuar
resolvíveis depois da migração.

Implementação futura sugerida:

```python
# bracc_etl/archival_gcs.py (não existe ainda)
class GcsArchival:
    def __init__(self, bucket: str) -> None: ...
    def archive_fetch(self, ...) -> str:
        # mesmo content-addressed layout, grava no bucket
        ...
```

Ativação via flag de ambiente (ex.: `BRACC_ARCHIVAL_BACKEND=gcs`).

## Contrato de proveniência

O campo `source_snapshot_uri` foi adicionado ao
`bracc_etl.schemas.provenance.PROVENANCE_COLUMNS`, **nullable** e
**opt-in** (não entra em `_REQUIRED_PROVENANCE_FIELDS`). Consequências:

- Pipelines novos **devem** popular via `attach_provenance(snapshot_uri=…)`.
- Pipelines legados (os 10 GO atuais) continuam funcionando sem mudança —
  `enforce_provenance` não exige o campo, e `attach_provenance` omite a
  chave quando `snapshot_uri is None` (não escreve string vazia no Neo4j).
- `Neo4jBatchLoader` auto-propaga o campo em `load_nodes` e
  `load_relationships` via iteração de `PROVENANCE_FIELDS` — nenhum
  código adicional por pipeline.
- Na camada API, `ProvenanceBlock.snapshot_url` expõe o valor ao cliente
  final (nullable também).

## Retrofit nos 10 pipelines GO legados

Fase separada. Ordem sugerida por valor:

1. `folha_go` — CSVs estáticos, archival barato e alto valor.
2. `pncp_go` — JSON por resource_id, já tem URL deep-link.
3. `alego` — HTML + PDF de atos parlamentares.
4. `ssp_go`, `tce_go`, `tcm_go`, `tcmgo_sancoes`, `state_portal_go`,
   `querido_diario_go`, `camara_goiania`.

Plano por pipeline: capturar `response.content` no ponto de extração,
chamar `archive_fetch`, propagar a URI via `attach_provenance(snapshot_uri=…)`.
Ver [`../todo-list-prompts/high_priority/11-archival-retrofit-go.md`](../todo-list-prompts/high_priority/11-archival-retrofit-go.md)
para o plano detalhado.

## Quando NÃO usar archival

- Pipelines que baixam arquivos de GB (CNPJ completo): `archive_fetch`
  assume payload in-memory. Esses usam um mecanismo paralelo de
  `script_download` que já grava o arquivo bruto em disco — a URI desse
  arquivo pode ser carimbada diretamente sem passar pelo módulo
  `archival`.
- Dados derivados de joins locais ou recomputações: o snapshot é da
  **fonte primária**, não de cálculos intermediários.
