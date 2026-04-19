# Separar "Doadores de campanha" de "Empresas ligadas" no perfil do político — ✅ CONCLUÍDO (2026-04-18)

> Implementado via `ConexoesService` (fase 04.B). Categorias separadas: doadores PF,
> doadores PJ, sócios, familiares, órgãos públicos.

## Contexto
Hoje o perfil do político (`pwa/index.html` seção conexões + `backend/app.py` `perfil_politico`) joga TUDO na mesma lista chamada `empresas`: pessoas físicas doadoras, empresas doadoras, sócios, familiares, órgãos estaduais onde o político está lotado, etc. Resultado: usuário leigo vê "27 empresas conectadas" mas metade é CPF de pessoa física (doador individual), e a relação é genérica ("Doou para campanha" misturado com "Sócio(a) de" etc.).

Depois do fix 04 (inbound connections), o volume explodiu — fica ainda mais confuso sem separar categorias.

## Problema concreto
Ex.: Cairo Salim Marcelino Lopes (GO/PSD). Payload atual retorna 27 itens na lista `empresas`, sendo:
- 20 pessoas físicas com CPF mascarado como "doador"
- 7 empresas com CNPJ como "doador"
- 0 sócios, 0 familiares, 0 contratos

Tudo cai no mesmo balde. Usuário leigo olha e não entende.

## Arquivos relevantes
- `backend/app.py`:
  - `EmpresaConectada` (model dataclass ~linha 100)
  - `PerfilPolitico` (model ~linha 105)
  - loop de classificação de conexões (`perfil_politico`, ~linha 558-626)
  - `traduzir_relacao` (~linha 202)
- `pwa/index.html`:
  - seção "Empresas e pessoas conectadas" (~linha 1376-1400)
  - CSS `.conexao-icon` / `.conexao-*` (~linha 600-620)

## Missão
1. **Criar grupos separados no payload**, em vez de uma lista única:
   - `doadores_empresa: list[DoadorEmpresa]` — CNPJs que doaram (nome, cnpj, valor total doado, nº de doações)
   - `doadores_pessoa: list[DoadorPessoa]` — CPFs que doaram (nome, valor total, nº de doações) — **agregar** por CPF pra reduzir ruído
   - `socios: list[SocioConectado]` — sócios de empresas do político (se houver)
   - `familia: list[FamiliarConectado]` — CONJUGE_DE / PARENTE_DE
   - `lotacao: list[LotacaoConectada]` — órgão público onde o político é servidor (ex.: state_agency, public_office)
   - Manter `empresas` só para empresas com relação direta não-doação (sócio, contratante, etc.)
2. **Backend agrega valores** quando a rel tem `valor`:
   - Somar `valor_doado` quando rel_type = `DOOU` e rel.properties tem `valor`/`amount` (TSE traz).
   - Expor `total_doacoes_recebidas_fmt` no `PerfilPolitico` (BRL).
3. **PWA renderiza cada categoria como card separado**, com título-explicativo-leigo:
   - "Quem deu dinheiro pra campanha (empresas)" — mostra top 5, total, "ver todos"
   - "Quem deu dinheiro pra campanha (pessoas físicas)" — idem
   - "Empresas onde é sócio(a)" — só se tiver
   - "Familiares na política" — só se tiver
   - Cada card com 1 linha explicando em português coloquial o que aquela categoria significa.
4. **Manter compat retroativa**: pode deixar `empresas` legado vazio ou com deprecation comment; mas garantir que perfis antigos não quebrem.
5. **Testes**:
   - `backend/tests/test_app.py`: fixture com mix de DOOU (pessoa+empresa), SOCIO_DE, CONJUGE_DE, state_agency — assert que cada vai pro grupo correto.
   - PWA: abrir Caiado/Cairo manualmente e conferir que cada seção renderiza com copy leigo.

## Critérios de aceite
- Perfil do Cairo Salim mostra no PWA: "7 empresas doaram R$ X pra campanha" e "20 pessoas doaram R$ Y" em cards separados, com valor total e top 5 por valor.
- Perfil do Caiado mostra seção "Sócio de" (se tiver) separada de doadores.
- Nenhum card aparece vazio — categoria sem dados é omitida.
- Copy leigo: evitar "conexão", "entidade", "CNPJ" no título; usar "empresa", "pessoa", "sócio", "família".

## Guardrails
- Tocar só em `backend/app.py` + `pwa/index.html` (respeitar memória `project_dual_frontends.md`: `/frontend` está morto).
- CPFs de doadores já vêm mascarados do BRACC — não desmascarar; mostrar só nome + "CPF ***.XXX.XXX-**".
- `make pre-commit` verde.

## Dependência
- **Fix 04 (inbound connections no backend) precisa estar mergeado primeiro**, senão não há conexões inbound pra agrupar.
