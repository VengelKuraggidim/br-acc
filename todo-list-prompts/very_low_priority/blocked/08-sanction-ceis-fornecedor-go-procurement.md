# Selo CEIS em fornecedor estadual GO — bloqueado: precisa expandir loader PNCP

## Contexto

Auditoria 2026-05-02: 24.037 `:Sanction` (CEIS/CNEP) e 1.933 `:GoProcurement` (PNCP-GO). Cruzar fornecedor sancionado com licitação estadual GO geraria selo "**fornecedora sancionada CEIS contratada por GO**" — caso clássico de irregularidade.

**Bloqueio**: o loader `pncp_go` atualmente só armazena `cnpj_agency` (CNPJ da prefeitura/órgão contratante), não captura `cnpj_fornecedor` (vencedor da licitação). Sem o CNPJ do vencedor não há como cruzar com `Sanction.cnpj`.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/pncp_go.py`
- API PNCP: https://pncp.gov.br/api/consulta — endpoints de "resultados" trazem fornecedor

## Missão (depois de desbloquear PNCP)

1. **Pré-requisito**: expandir `pncp_go.py` para capturar dados do vencedor de cada licitação (CNPJ, razão social, valor homologado).
2. Criar `:GoProcurementResult` ou estender `:GoProcurement` com `cnpj_fornecedor`, `valor_homologado`, `data_homologacao`.
3. Criar rel `(c:Company)-[:VENCEU_LICITACAO_GO]->(gp:GoProcurement)`.
4. Cruzar `Sanction.cnpj == c.cnpj` para companies que venceram licitação GO; criar selo no card do procurement.
5. Adicionar aba "Fornecedoras sancionadas" no perfil do município.

## Critérios de aceite

- Loader PNCP traz vencedor de cada licitação resolvida.
- Pelo menos 1 cruzamento Sanction × fornecedor GO comprovado.
- PWA mostra alerta visual.
- `make pre-commit` verde.

## Guardrails

- API PNCP tem rate limit; respeitar.
- CEIS pode ter sanção expirada (`date_end` no passado) — diferenciar visualmente "ativa" vs "histórica".

## Dependência

**Bloqueado por**: expansão do loader PNCP (item separado).

## Status

Sem ETA — só destravar quando alguém pegar o trabalho do PNCP completo.
