"""Helpers primitivos compartilhados entre services (coerção + normalização).

Originalmente estes utilitários viviam em
``bracc.services.conexoes_service`` como funções privadas
(``_as_str``, ``_as_float``, ``_norm_type``). Conforme outros services
(``despesas_service``, ``perfil_service``, etc.) passaram a precisar do
mesmo tipo de tratamento defensivo sobre ``props`` e ``rel_props`` do
Neo4j, a duplicação virou convite pra drift silencioso.

Aqui ficam as versões públicas (sem underscore). Contrato estável:

* :func:`as_str` — ``props[key]`` se for string não-vazia; senão ``None``.
* :func:`as_float` — coerção best-effort pra ``float``; ``0.0`` se
  ``None`` ou parse falhar (nunca levanta).
* :func:`norm_type` — lowercase seguro pra labels Neo4j (que vêm em
  PascalCase); ``""`` se a entrada não for string.
* :func:`archival_url` — prefixa URIs relativas de snapshot archival com
  ``/archival/`` pra que o browser bata no endpoint correto em vez de
  cair no fallback do PWA.

Funções puras, stateless, zero IO — seguem o padrão dos demais helpers
da camada ``services/``.
"""

from __future__ import annotations

from typing import Any


def as_str(props: dict[str, Any], key: str) -> str | None:
    """Lê ``props[key]`` se for string não-vazia, senão ``None``.

    Evita repetir ``isinstance(..., str)`` em cada call-site e mantém
    o type-narrowing correto para mypy.

    Returns
    -------
    str | None
        String não-vazia ou ``None`` (nunca ``""``, nunca não-string).
    """
    value = props.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def as_float(value: Any) -> float:
    """Coerção best-effort pra float.

    Contrato:

    * ``None``                  → ``0.0``
    * ``int``/``float``         → ``float(value)``
    * string parseável (``"12.34"``) → ``float(...)``
    * qualquer outra coisa (``"abc"``, ``object()``) → ``0.0``

    Nunca levanta — comportamento projetado para agregações em que um
    valor inválido deve ser descartado silenciosamente sem derrubar a
    request inteira.
    """
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def norm_type(target_type: Any) -> str:
    """Normaliza ``target_type`` pra lowercase.

    Labels do Neo4j vêm em PascalCase (``Company``, ``Amendment``) mas
    a classificação interna trabalha em lowercase pra reduzir
    fragilidade. Retorna ``""`` se a entrada não for string (em vez de
    levantar ``AttributeError``), pra que o caller possa tratar o
    shape inesperado como "sem match" silenciosamente.
    """
    if not isinstance(target_type, str):
        return ""
    return target_type.lower()


def archival_url(snapshot_uri: str | None) -> str | None:
    """Reescreve URI relativa de snapshot pra caminho servido pelo nginx.

    O loader carimba ``source_snapshot_uri`` como caminho content-addressed
    relativo à raiz ``BRACC_ARCHIVAL_ROOT`` (ex.: ``tse_prestacao_contas/
    2026-04/954b8a10119c.bin``). Serializar esse caminho cru no JSON faz o
    browser resolver contra o origin da página — o link cai no fallback do
    PWA em vez do arquivo. O nginx serve o diretório archival no prefixo
    ``/archival/``; esta função garante que todo ``snapshot_url`` que sai da
    API já venha com o prefixo correto.

    Contrato:

    * ``None`` / string vazia → ``None``.
    * Já começa com ``http://``, ``https://`` ou ``/archival/`` → passa
      sem alteração (pipelines podem gravar URLs absolutas em casos
      excepcionais).
    * Qualquer outra coisa → ``/archival/{uri.lstrip('/')}``.
    """
    if not snapshot_uri:
        return None
    if snapshot_uri.startswith(("http://", "https://", "/archival/")):
        return snapshot_uri
    return f"/archival/{snapshot_uri.lstrip('/')}"
