# Governador GO — capturar valor do subsídio (Lei estadual)

## Contexto
A aba **Governador GO** do bloco "Quanto custa um cargo político?" no PWA (home) renderiza empty-state hoje porque o pipeline `custo_mandato_br` registra o cargo apenas como **stub**, sem valor numérico:

- `etl/src/bracc_etl/pipelines/custo_mandato_br.py:218-230` — único componente é `governador_go:subsidio` com `valor_mensal=None`.
- Observação atual: *"teto constitucional: subsídio do Ministro do STF (CF Art. 37 XI). Valor exato fixado por Lei estadual GO; consulte Casa Civil/DOE-GO."*
- `fonte_url` aponta pra `https://www.casacivil.go.gov.br/` — placeholder genérico, não a Lei.

Endpoint `/custo-mandato/governador_go` responde 200, mas com `custo_mensal_individual=0` e o frontend cai no empty-state em `pwa/index.html` (`semDados = componentes.every(c => c.valor_mensal === null)`).

## O que precisa ser feito

1. **Localizar a Lei estadual GO** que fixa o subsídio do governador (e provavelmente do vice). Pistas:
   - Buscar no portal da ALEGO (https://al.go.leg.br/) por "subsídio governador" ou "fixa subsídio Chefe Poder Executivo".
   - DOE-GO tem busca textual em https://www.diariooficial.go.gov.br/ — filtrar por Casa Civil/Governadoria.
   - Costuma haver Lei nova a cada início de mandato (2023 pra mandato 2023-2026); às vezes um Decreto Legislativo da ALEGO.
2. **Capturar o valor** (subsídio mensal bruto, R$). Verificar se há também:
   - Verba de representação / verba de gabinete / pessoal de gabinete.
   - Auxílio-moradia (residência oficial geralmente exclui).
   - Veículo oficial / segurança (em geral não monetizado, OK ignorar).
3. **Adicionar componentes ao pipeline** em `_CARGO_COMPONENTES["governador_go"]`:
   - `subsidio` com `valor_mensal=<R$>`, `fonte_legal="Lei nº X/AAAA (GO)"`, `fonte_url=<link DOE-GO>`.
   - Se houver, `gabinete` / `representacao` como componentes adicionais (mesmo padrão de `dep_estadual_go`).
4. **Re-rodar o pipeline**: `bracc-etl run custo_mandato_br` (faz `archive_fetch` da Lei e materializa no grafo).
5. **Validar** no PWA local: `curl http://localhost:8000/custo-mandato/governador_go` deve retornar `custo_mensal_individual > 0`; aba mostra tabela ao invés do empty-state.

## Onde mexer
- `etl/src/bracc_etl/pipelines/custo_mandato_br.py` — `_CARGO_COMPONENTES["governador_go"]` (linhas 218-230).
- `etl/archival/custo_mandato_br/` — novo snapshot da Lei capturado pelo pipeline.

## Bonus
- `vice_governador_go` não existe no pipeline ainda. Geralmente o subsídio é fixado na mesma Lei (~95% do valor do governador). Fácil de adicionar junto.
- Replicar pra outros estados depois (cada UF tem sua própria Lei). MVP é só GO.

## Esforço
30min-1h se a Lei estiver rapidamente localizável no DOE-GO; até 2h se precisar caçar emendas/atualizações.
