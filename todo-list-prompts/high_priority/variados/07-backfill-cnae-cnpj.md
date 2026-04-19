# Backfillar CNAE + Situação Cadastral nos 52k+ CNPJs do grafo — ⏳ PENDENTE (2026-04-18)

> Trabalho de multi-semanas (RFB bulk dump). Situação cadastral parcial já
> coberta on-demand por `brasilapi_cnpj_status` (prompt 08 ✅), mas backfill
> em massa via dump RFB não foi implementado.

## Contexto
Hoje os nós `:Company` no Neo4j só têm `razao_social` e `cnpj` — sem CNAE, sem situação cadastral, sem porte, sem endereço, sem capital social. Isso prejudica:

- **Classificação correta**: o PWA usa regex no nome ("DIRECAO...", "ELEICAO...") pra adivinhar se é partido/comitê/fundo/outro. CNPJs que não seguem o padrão caem em "outros" incorretamente (ex: MAGDA MOFATTO e HUMBERTO TEOFILO são comitês mas não começam com "ELEICAO"). Com CNAE correto (comitês de campanha = **CNAE 9492-8/00**), a classificação vira determinística.
- **Identificar empresas ativas**: hoje não dá pra saber se uma empresa doadora (ou sócia do político) foi baixada/suspensa — dado que é sinal vermelho pra investigação.
- **Outras análises**: porte, data de abertura, município da sede — tudo zerado hoje.

A pipeline `etl/src/bracc_etl/pipelines/cnpj.py` existe mas parece ter rodado só pra criar nós `:Company` básicos com CNPJ+razão social; os campos ricos não foram carregados. Investigar se é bug da pipeline ou falta de ingestão completa.

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/cnpj.py` (pipeline existente)
- `etl/tests/test_cnpj_pipeline.py`
- `docs/data-sources.md` (linha referenciando CNPJ Receita Federal)
- Schema Neo4j: `api/src/bracc/queries/` — queries que leem `Company.cnae`, `Company.situacao_cadastral`, etc
- PWA: `pwa/index.html` — função `classificarCNPJ` no card "Quem financiou a campanha" (usa regex hoje; trocar por lookup de CNAE quando disponível)

## Fonte dos dados
- **Receita Federal / CNPJ público**: download bulk em `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/` (zipados mensalmente). Dezenas de GB total — baixar só o que precisar (tabelas `Empresas`, `Estabelecimentos`, `Simples`).
- **Alternativa mais enxuta**: `https://dadosabertos.rfb.gov.br/CNPJ/` ou mirror no S3 público. Scripts da ZanyMonk/dados-publicos-cnpj no GitHub mostram como processar.
- **Para comitês de campanha especificamente (CNAE 9492-8/00)**: TSE publica a lista de comitês com CNPJ em `prestacao-de-contas-eleitorais` — subset pequeno, pode ser ingerido separado e antecipar essa classificação sem precisar do dump inteiro da Receita.

## Missão (em ordem de impacto/esforço)
1. **Fase rápida (~45min) — marca comitês de campanha sem precisar do dump Receita**:
   - Baixar "prestação de contas" do TSE (mesmo dataset da TODO 07/01, se já implementada — reaproveitar).
   - Extrair lista de CNPJs de comitês (têm `ds_cargo_candidatura` e `nm_ue` — geralmente identificam).
   - `MERGE (c:Company {cnpj: $cnpj}) SET c.tipo_entidade = 'comite_campanha', c.cnae_principal = '9492-8/00', c.cargo_candidatura = $cargo, c.ano_eleicao = $ano`.
   - Ajustar PWA pra usar `c.tipo_entidade` quando existir, fallback pro regex atual.
2. **Fase completa (~2-4h) — backfill Receita Federal**:
   - Baixar dumps da Receita (só tabelas Empresa + Estabelecimento).
   - Processar com pandas/duckdb (CSV gigante — streaming essencial).
   - Fazer MATCH/UPDATE no Neo4j: `c.cnae_principal`, `c.cnae_fiscal`, `c.situacao_cadastral` (ATIVA/BAIXADA/SUSPENSA/INAPTA), `c.data_situacao`, `c.porte`, `c.municipio`, `c.uf`, `c.data_abertura`, `c.capital_social`.
   - Rodar em batches de 50k via APOC pra não travar o Neo4j.
3. **Trocar classificação no PWA**: substituir a função `classificarCNPJ` (hoje regex no nome) por lookup do campo `target_props.cnae_principal`/`tipo_entidade`. Manter o fallback regex pra CNPJs que ainda não foram backfilled.

## Critérios de aceite
- Pelo menos 90% dos CNPJs do grafo com `situacao_cadastral` preenchido.
- Comitês de campanha (CNAE 9492-8/00) identificados com 100% de precisão.
- PWA classifica CNPJs por CNAE quando disponível, regex como fallback.
- CNPJs baixados/suspensos aparecem com flag visual no PWA.

## Guardrails
- Dataset Receita tem milhões de registros. Não ingerir tudo — filtrar pelos 52k CNPJs que já estão no grafo usando JOIN antes de gravar no Neo4j.
- LGPD: campos públicos da Receita podem ser ingeridos livremente; mas cruzar com CPF de sócio exige cuidado.
- `make pre-commit` verde.

## Dependência
- Pode ser feita independente. Fase rápida (comitês via TSE) pode reusar o ETL da TODO 07/01 se já existir.
