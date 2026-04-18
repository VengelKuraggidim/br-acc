# Comparar gastos de campanha vs teto legal do cargo (TSE)

## Contexto
O TSE define um **teto de gastos de campanha** por cargo e por eleição (federal) e por município (prefeito/vereador). Ex.: em 2022, deputado federal tinha teto de ~R$ 2,1 milhões; governador de GO tinha teto de ~R$ 21 milhões; vereador de Goiânia em 2024 teve teto próprio por município.

Hoje o app só mostra quanto o político RECEBEU e quanto GASTOU, mas **não compara com o teto legal**. Pra o usuário leigo, "usou 87% do teto" é muito mais útil que "gastou R$ 1,8 mi" sem referência.

## Desafio de dados
Pesquisei em 2026-04-17 e os tetos são publicados:
- **Federal**: Resolução TSE por eleição (número varia por ano). Para 2022: Resolução TSE nº 23.607/2019 com atualizações. Valores em tabelas no site do TSE, **não em dataset estruturado**.
- **Municipal**: cada município tem teto baseado no número de eleitores, publicado pelo TRE respectivo. Também **sem CSV bulk fácil**.
- **Base dos Dados** (`basedosdados.org`): pode ter tabela consolidada — checar primeiro.

## Opções de implementação

### Opção A — Hardcode (MVP rápido, ~30min)
Criar `backend/tetos_campanha.py` com dict estático:
```python
TETOS_2022 = {
    "Deputado Federal": 2_100_000,
    "Deputado Estadual": 1_050_000,  # valores aproximados - ajustar
    "Senador": 5_000_000,
    "Governador": { "GO": 21_000_000, "SP": 70_000_000, ... },
    "Prefeito": None,  # varia por município
}
```
Cobertura parcial (cargos federais/estaduais), mas suficiente pro primeiro corte.

### Opção B — Ingerir Base dos Dados (~1-2h)
Checar se `basedosdados.org` tem tabela `br_tse_eleicoes.limites_gastos_campanha` ou similar. Se sim, ingerir via BigQuery client + gravar como propriedades no nó `Person` (por ano + cargo).

### Opção C — Scraping das resoluções TSE (~3-4h)
Baixar cada Resolução TSE que define tetos, parsear tabelas (PDF), estruturar. Trabalho grande, baixa manutenibilidade.

## Arquivos relevantes
- `backend/apis_externas.py` ou novo módulo `backend/tetos_campanha.py` com os valores.
- `backend/app.py` `PerfilPolitico` — adicionar `teto_gastos: TetoGastos | None = None` com campos `valor_limite`, `valor_gasto`, `pct_usado`, `cargo`, `ano_eleicao`, `ano_referencia`.
- `backend/app.py` no `perfil_politico` — ler o gasto total (precisa vir do pipeline despesas TSE; pode não estar no grafo hoje — investigar).
- `pwa/index.html` — barra de progresso: "Usou X% do teto legal" verde (<70%), amarelo (70-90%), vermelho (>90% ou ultrapassou).

## Missão (em ordem)
1. **Checar Base dos Dados primeiro** (15 min) — se tiver CSV pronto, cai pra Opção B automaticamente.
2. **Se não: Opção A** — hardcode tetos 2022 (federal/estadual/governador/senador GO + principais UFs) com fonte legal no comentário.
3. **Ingestão de despesas de campanha TSE**: já tem o ZIP baixado (`despesas_pagas_candidatos_2022_GO.csv` em `data/tse_prestacao/`, 38MB). Somar por `SQ_CANDIDATO` como fizemos para receitas, gravar como `p.total_despesas_tse_2022` no Neo4j.
4. **Backend**: calcular `pct_usado = total_despesas / teto_cargo`.
5. **PWA**: barra de progresso colorida + número + texto leigo ("Gastou R$ 1,8 mi dos R$ 2,1 mi permitidos — usou 87% do teto").

## Critérios de aceite
- Deputado federal de GO mostra "Gastou R$ X de R$ 2,1 mi permitidos — XX% do teto".
- Candidatos acima do teto aparecem com alerta vermelho (infração grave).
- Município sem teto cadastrado: omitir seção em vez de mostrar dado errado.
- Fonte legal citada (Resolução TSE nº / LC nº) abaixo do card.

## Guardrails
- **Atenção**: tetos variam por ano eleitoral. Hardcode precisa comentar a fonte e o ano.
- Se a UF do político não tem o cargo mapeado, degradação silenciosa (não mostra seção).
- `make pre-commit` verde.
- Não usar valores "chutados" — sempre linkar à Resolução TSE no comentário do código.

## Prioridade
Média-alta — não é sinal de irregularidade tão forte quanto contas desaprovadas, mas é métrica super relevante e acessível pra leigo. "Usou 95% do teto" é impactante.

## Dependência
- Depende de ter `total_despesas_tse_YYYY` no grafo — requer processar o CSV de despesas do ZIP TSE (já baixado em `data/tse_prestacao/pc2022.zip`).
