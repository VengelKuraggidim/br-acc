# Re-rodar `camara_goiania` mensalmente — captar suplentes, troca de partido, atualizações de bio

## Contexto

Fase 2a do `camara_goiania` foi entregue + ingerida em **2026-05-02**
(commit `9d9a8d0`): 41 `:GoVereador` da 20ª Legislatura no Neo4j local,
100% com `party`, `photo_url`, `gabinete`, provenance e snapshot HTML.

Os perfis no portal CMG (`https://www.goiania.go.leg.br/institucional/parlamentares/<slug>`)
mudam ao longo do mandato:

- **Suplentes assumindo** → novo slug aparece na listagem (ou um slug
  desaparece quando titular volta).
- **Troca de partido** (filiação migra durante o mandato; visto em
  vereadores GO 2024 já 4 trocas formais até maio).
- **Atualização de bio / foto** — vereadores costumam reescrever a
  biografia após eleição/posse de comissões.
- **Mudança de gabinete / telefones / e-mail**.

Como não há feed de mudanças, a única forma de capturar é **re-rodar
o scraper** periodicamente.

## Rodada recomendada

Cadência: **mensal** (1ª segunda-feira). Custo: ~14s download (41 GETs
× 0.5s pause + listagem) + ~5s ETL. `archive_fetch` é
content-addressed → re-runs sem mudança não regravam HTML; só inflate
é o `vereadores.json` consolidado (também idempotente).

```bash
uv run --project etl python scripts/download_camara_goiania.py \
  --output-dir data/camara_goiania
cd etl && uv run bracc-etl run --source camara_goiania \
  --neo4j-uri "bolt://localhost:7687" \
  --neo4j-password "changeme" \
  --neo4j-database "neo4j" \
  --data-dir ../data
```

## O que conferir no log

- `[camara_goiania] N parlamentares to fetch` — se N mudar versus a
  rodada anterior, suplente entrou/saiu. Comparar slugs em
  `data/camara_goiania/vereadores.json` antes/depois.
- `archive_fetch` "wrote camara_goiania/<bucket>/<hash>.html" só sai
  quando o conteúdo HTML mudou — sinaliza atualização real do perfil.
- `:GoVereador` no Neo4j: `MATCH (v:GoVereador) RETURN count(v)`. Se
  cair sem motivo conhecido, investigar se `_extract_profile_slugs`
  perdeu padrão (ex.: portal redesenhou o `<base href>`).

## Trigger pra investigação manual

- N de vereadores muda de mais de 2 entre runs consecutivos.
- Algum vereador perde `party` ou `gabinete` (string vazia) numa rodada
  em que tinha antes — pode ser drift do parser regex (Plone reformatou
  bloco de campos).
- `bio_summary` fica vazio em mais de 5 perfis simultaneamente — mesmo
  sintoma.

## Reabrir Fase 2b quando

Despesas/folha/diárias seguem em
`camaragoiania.nucleogov.com.br` (SPA RequireJS). Plano completo em
`todo-list-prompts/medium_priority/debitos/camara-goiania-scraping.md`
seção "Fase 2b". Reabrir quando:

- Stakeholder pedir R$ no perfil de cada vereador no PWA.
- TCE-GO Qlik scraper (`tce_go_qlik`) ficar estável e a infra
  Selenium puder ser reaproveitada sem novo bootstrap.
