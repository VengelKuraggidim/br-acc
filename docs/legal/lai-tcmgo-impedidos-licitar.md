# Pedido LAI — TCM-GO — Lista de Impedidos de Licitar ou Contratar

**Status**: rascunho pronto para envio (2026-04-29)
**Canal primário (informal, 5 dias úteis)**: e-mail direto pra
`ouvidoria@tcmgo.tc.br` (Ouvidoria do Tribunal de Contas dos Municípios
do Estado de Goiás).
**Canal de escalonamento (oficial, prazo legal 20 dias úteis + 10
prorrogáveis)**: Fala.BR — https://falabr.cgu.gov.br/ — esfera "Estadual",
estado "GO", órgão "TCM-GO" (login via gov.br).
**NÃO usar**: e-SIC estadual `sic.goias.gov.br` está fora do ar
(verificado 2026-04-29).
**Fundamento**: Lei nº 12.527/2011 (LAI), art. 10 e art. 11.

---

## Assunto

Solicitação de acesso à informação — exportação em CSV da lista de pessoas
físicas e jurídicas impedidas de licitar ou contratar com a administração
pública municipal goiana, mantida pelo TCM-GO.

## Corpo do pedido

Prezada Ouvidoria do Tribunal de Contas dos Municípios do Estado de Goiás,

Com base na Lei nº 12.527/2011 (Lei de Acesso à Informação), solicito o
fornecimento, em formato aberto e estruturado (preferencialmente CSV ou
planilha eletrônica), da relação completa e atualizada de pessoas físicas
e jurídicas impedidas de licitar ou contratar com a administração pública
municipal do Estado de Goiás, conforme publicada pelo TCM-GO no widget
em `https://tcmgo.tc.br/portalwidgets/xhtml/impedimento/impedimento.jsf`
(embutido em
`https://www.tcmgo.tc.br/site/tcm-em-acao/impedidos-de-licitar-ou-contratar/`).

Para os fins desta solicitação, peço que o arquivo contenha, no mínimo,
os campos atualmente exibidos no widget:

1. Nome (pessoa física ou razão social)
2. CPF ou CNPJ
3. Data de início do impedimento
4. Data de término do impedimento (quando houver)
5. Órgão / ente municipal responsável pela penalidade
6. Número do processo administrativo
7. Situação atual do impedimento (vigente, suspenso, encerrado, etc.)

A finalidade do pedido é o uso jornalístico e de pesquisa em projeto cívico
de transparência pública (https://github.com/VengelKuraggidim/fiscal-cidadao),
que cruza dados oficiais para auxiliar cidadãos no acompanhamento de
contratos e sanções administrativas. Toda informação requerida já é
publicada pelo próprio TCM-GO em portal aberto; o pedido se restringe a
viabilizar o acesso em formato processável por máquina, conforme o
art. 8º, §3º, II e III da LAI.

Caso a base seja muito extensa para envio por e-mail, solicito link de
download ou orientação sobre o melhor canal para retirada do arquivo.

Esclareço que o portal `tcmgo.tc.br` adota `robots.txt` com diretiva
`Disallow: /`, o que inviabiliza a coleta automatizada legítima da
informação já pública, motivando este pedido formal pela via da LAI.

Agradeço a atenção e fico à disposição para esclarecimentos adicionais.

Atenciosamente,

Anastácia Almeida Campos — vengelkuraggidim@gmail.com
Projeto Fiscal Cidadão (uso jornalístico / pesquisa cívica)

---

## Itens a preencher antes de enviar

- [ ] Confirmar nome completo + CPF (necessário pro Fala.BR; opcional no
      e-mail informal, mas ajuda na fé pública do pedido).
- [ ] Disparar primeiro pelo e-mail (`ouvidoria@tcmgo.tc.br`); aguardar
      5 dias úteis. Se não responderem, escalar pro Fala.BR com cópia
      deste mesmo texto.
- [ ] Salvar número de protocolo da resposta (Fala.BR) ou ID do tíquete
      da ouvidoria para anexar ao TODO `tcmgo-impedidos-jsf-scraper.md`
      quando o CSV chegar.

## Próximo passo após resposta

1. Receber CSV (ou outro formato).
2. Normalizar cabeçalho para
   `nome;cpf_cnpj;data_inicio;data_fim;orgao;processo;situacao` (separador `;`).
3. Salvar em `data/tcmgo_sancoes/impedidos_licitar.csv`.
4. Rodar `python scripts/download_tcmgo_sancoes.py` (caminho de ingestão
   já cobre o arquivo — pipeline carrega como `:TcmGoImpedido` +
   `IMPEDIDO_TCMGO` com `list_kind='impedidos_licitar'`).
