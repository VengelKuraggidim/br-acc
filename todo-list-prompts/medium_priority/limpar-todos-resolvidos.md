# Limpeza de TODOs já marcados como DONE/resolvido

Esses arquivos já têm marcadores `✅ DONE` ou `Opção A aplicada` no
topo, indicando que o trabalho foi feito numa sessão anterior. O
conteúdo já está parcial ou totalmente refletido em
`medium_priority/more_data/07-priorizacao-tier.md`. Confirmar que não há
nada pendente e remover.

## Candidatos a remoção

- `high_priority/variados/10-perfil-servidor-estadual-ou-tirar-do-clicavel.md`
  — "Opção A aplicada — 2026-04-29" (cursor pointer só em quem tem
  perfil). Conferir se a UX final está OK e remover.
- `high_priority/variados/11-tce-go-irregulares-link-canonicalperson.md`
  — "✅ DONE (2026-04-29)" (entity_resolution_tce_go.py rodou, 0 matches
  esperados). Já consolidado em `07-priorizacao-tier.md`. Pode remover.
- `high_priority/debitos/tse-doou-campaigndonor-stubs-orfaos.md`
  — Bug 1, Bug 2 e Bug 3 todos resolvidos (Bug 3 fechado em 2026-04-30
  por mim, ver entrada no `07-priorizacao-tier.md`). Conteúdo grande;
  pode migrar pro tier ou remover direto.
- `high_priority/debitos/tce-go-qlik-scraper.md`
  — "✅ DONE (2026-04-27)" (Selenium + Firefox headless, Phase 1+2
  concluídas). Conteúdo extenso; vale migrar lessons-learned pro tier
  antes de remover.

## Como fazer

Para cada arquivo:
1. Ler o conteúdo, conferir se há follow-up não-óbvio (ex.: TODO
   embedded no meio do texto que foi esquecido).
2. Se há follow-up: extrair pra novo arquivo separado em
   `high_priority/variados/`.
3. Se há lessons-learned não capturados em outro lugar: adicionar 1
   parágrafo em `medium_priority/more_data/07-priorizacao-tier.md`.
4. `rm` no arquivo.

## Esforço

Pequeno (10-20min). Pode rodar em batch numa sessão dedicada.
