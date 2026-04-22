# `etl/archival/` owned by root — bloqueia pipelines locais — ⏳ PENDENTE (2026-04-21)

> Já rastreado no memo `project_fotos_politicos_pendente.md` — documentado
> aqui como débito explícito pra não se perder (afeta mais pipelines que
> só os de fotos).

## Contexto

O diretório `etl/archival/` foi criado por um container Docker rodando
como `root`, então:

```
$ ls -la etl/ | grep archival
drwxr-xr-x  8 root  root  archival
```

Pipelines locais rodando como usuário (não-root) tentam criar
`etl/archival/<source_id>/` e falham com:

```
PermissionError: [Errno 13] Permission denied: 'archival/tse_prestacao_contas'
```

**Workaround atual**: setar `BRACC_ARCHIVAL_ROOT=/tmp/archival_xxx` no
ambiente da run. Funciona mas: (a) snapshots ficam fora do layout
canônico; (b) quebra o content-addressed cache reuse entre runs; (c)
viola o contrato de archival documentado em `docs/archival.md`.

## Pipelines já impactados

- `tse_prestacao_contas_go` — encontrado em 2026-04-21 ao fechar o todo
  07 Fase 1.
- Memo `project_fotos_politicos_pendente.md` menciona senado / alego /
  tse / wikidata / propagacao_fotos_person como esperando este chown.

Provavelmente afeta **todo pipeline que chama `archive_fetch`** a partir
de uma run local.

## Missão

1. `sudo chown -R $USER:$USER /home/vengel-kuraggidim-sitagi/PycharmProjects/fiscal-cidadao/etl/archival`.
2. Verificar se tem snapshots gravados lá que precisam ser preservados
   (content-addressed, então reruns ressincronizam sozinhos — mas
   melhor não apagar).
3. Investigar **por que** o dir foi criado como root — docker-compose
   provavelmente roda ETL como root. Opções:
   - Ajustar o `Dockerfile` / compose pra rodar o container de ETL como
     o UID do host (`user: "${UID}:${GID}"`).
   - Usar volume nomeado em vez de bind mount pra eliminar o clash de
     UID.
4. Rerun dos pipelines listados no memo pra reconciliar snapshots
   pendentes (fotos políticos).

## Arquivos relevantes

- `docker-compose.yml` — seção `bracc-etl` (verificar `user:` field).
- `etl/Dockerfile` — USER diretiva (se existir).
- `etl/src/bracc_etl/archival.py` — `_archival_root()` + `BRACC_ARCHIVAL_ROOT`
  fallback.

## Critérios de aceite

- [ ] `etl/archival` writable pelo usuário host sem sudo.
- [ ] Container docker-compose ETL não gera arquivos root no host.
- [ ] `BRACC_ARCHIVAL_ROOT` override não é mais necessário em runs locais.
- [ ] Pipelines listados no memo re-rodados.

## Prioridade

**Alta.** Qualquer trabalho de ETL local precisa contornar isso — fricção
recorrente em cada pipeline novo.
