# [USUÁRIA] Pedido LAI à SSP-GO — 14 naturezas em nível município

Pendência manual da usuária. Sem isso, as 14 naturezas além de
homicídio doloso continuam só em nível estadual (cobertura via
`ssp_go` PDFs anuais; municipal via `mjsp_municipios` resolveu apenas
homicídio doloso).

## Como abrir o pedido

Dois caminhos válidos — escolher um:

1. **e-SIC Goiás (recomendado)** — `https://goias.gov.br/transparencia/sic/`
   ou diretamente `https://sic.goias.gov.br/`. Login via gov.br ou
   cadastro local. Pedido vira protocolo numerado, prazo legal de 20
   dias úteis (LAI, art. 11 §1º + §2º), prorrogável por mais 10. Fica
   gravado e rastreável; recurso administrativo se negado.

2. **E-mail direto ao Observatório de Segurança Pública** —
   `observatorio.ssp@goias.gov.br` (endereço do rodapé do portal
   `goias.gov.br/seguranca/estatisticas/`). Mais informal, sem
   protocolo. Útil pra esclarecer escopo antes de protocolar via
   e-SIC, mas o caminho oficial é o e-SIC.

**Sugestão**: mandar primeiro um e-mail ao observatório (rascunho
curto abaixo) pra validar formato e disponibilidade. Se eles
responderem positivo, formalizar via e-SIC depois (com protocolo)
referenciando a conversa. Se não responderem em 5 dias úteis, ir
direto pro e-SIC.

## Rascunho — e-mail informal ao Observatório

> **Para:** observatorio.ssp@goias.gov.br
> **Assunto:** Solicitação de dados municipais de ocorrências criminais (UF GO, base RAI/ODISSEU)
>
> Prezados,
>
> Sou pesquisadora cívica trabalhando no projeto Fiscal Cidadão, que
> integra dados públicos do estado de Goiás em um grafo de
> transparência (`fiscal-cidadao`). Atualmente carregamos os
> boletins anuais publicados em
> `https://goias.gov.br/seguranca/estatisticas/`, que trazem os
> totais estaduais das 15 naturezas (homicídio doloso, feminicídio,
> estupro, latrocínio, lesão seguida de morte, roubos e furtos por
> categoria).
>
> Para análise comparativa por município, gostaria de solicitar um
> recorte adicional dessas mesmas naturezas com **granularidade
> município × naturaza × mês**, idealmente em CSV ou XLSX, cobrindo
> os **últimos 2 anos completos** (linha de base; depois solicitamos
> a série histórica completa que vocês mantiverem). A série
> consolidada do MJSP/SINESP em `dados.mj.gov.br` cobre apenas
> homicídio doloso em nível municipal — daí a importância de obter
> as outras 14 naturezas direto da SSP-GO.
>
> Naturezas de interesse (mesmas dos boletins anuais publicados):
>
> 1. Homicídio doloso (já temos via SINESP — pode ser opcional)
> 2. Feminicídio
> 3. Estupro
> 4. Latrocínio
> 5. Lesão corporal seguida de morte
> 6. Roubo a transeunte
> 7. Roubo de veículos
> 8. Roubo em comércio
> 9. Roubo em residência
> 10. Roubo de carga
> 11. Roubo a instituição financeira
> 12. Furto de veículos
> 13. Furto em comércio
> 14. Furto em residência
> 15. Furto a transeunte
>
> Caso a base RAI/ODISSEU já gere esse recorte para uso interno,
> qualquer formato exportável serve — adaptamos o pipeline ao
> schema que vocês usarem.
>
> Os dados serão republicados de forma agregada e devidamente
> creditados à SSP-GO no portal `fiscalcidadao.com.br`. Se houver
> qualquer restrição de uso, por favor me avisem.
>
> Agradeço antecipadamente,
>
> [Nome completo]
> [Instituição/cidade]
> Projeto Fiscal Cidadão — `https://github.com/[seu-usuário]/fiscal-cidadao`

## Rascunho — pedido formal via e-SIC

Usar o mesmo conteúdo do e-mail acima, com ajustes:

- **Órgão destinatário:** Secretaria de Estado da Segurança Pública (SSP-GO)
- **Tipo de pedido:** Acesso à Informação (LAI Lei 12.527/2011 + Lei
  Estadual 18.025/2013)
- **Forma de recebimento:** download/e-mail
- **Justificativa:** "Pesquisa cívica para projeto de transparência
  pública. A LAI dispensa justificativa, mas registro o uso para
  contextualização."
- **Pedido:** colar a lista de naturezas + período (últimos 2 anos
  completos, mensal, agregado por município).

## Quando o CSV chegar

1. Renomear pra `ocorrencias.csv` e colocar em
   `data/ssp_go/ocorrencias.csv` (no host local — o pipeline
   `ssp_go` já tem path de override implementado;
   ver `etl/src/bracc_etl/pipelines/ssp_go.py:570`).
2. Schema esperado pelo pipeline atual:
   `municipio | cod_ibge | natureza | periodo | quantidade`
   (separador `;` ou `,`). Se o CSV vier com nomes diferentes, o
   `row_pick` no `transform` aceita aliases (`nome_municipio`,
   `cidade`, `tipo_ocorrencia`, `crime`, `mes_ano`, `data`,
   `total`, `count`, `ocorrencias`).
3. Rodar `make run pipeline=ssp_go` (ou
   `bracc-etl run --pipeline ssp_go`) — o operator-CSV tem
   precedência sobre o scrape de PDFs.
4. Validar 3 municípios de teste (Goiânia, Anápolis, Aparecida de
   Goiânia) contra a fonte. Atualizar
   `todo-list-prompts/high_priority/debitos/ssp-go-granularidade-municipio.md`
   marcando como totalmente resolvido.

## Prazos e follow-up

- **e-SIC**: 20 dias úteis (prorrogáveis por mais 10).
- **E-mail informal**: sem prazo legal — se não responderem em 5
  dias úteis, escalar pro e-SIC.
- **Negativa**: cabem 2 níveis de recurso administrativo (autoridade
  superior + CGU/Ouvidoria estadual). Se chegar a esse ponto,
  registrar o número do protocolo aqui e revisar a estratégia.
