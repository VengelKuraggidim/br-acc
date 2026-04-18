"""Response shapes for PWA parity endpoints.

These shapes are kept byte-compatible with the legacy Flask backend
(``backend/app.py``) so the PWA (``pwa/index.html``) keeps rendering
without any client-side change while the migration to FastAPI is in
flight. Do not refactor the shapes here without first updating the PWA
consumer; the renaming work is tracked in the post-migration cleanup.
"""

from pydantic import BaseModel


class StatusResponse(BaseModel):
    """Mirrors the Flask ``/status`` response.

    The ``vereadores_goiania`` count is scoped to the Goiania capital
    (see ``person_counts_by_uf.cypher``); the other politico counts are
    UF-wide. The booleans and totals drive the landing-page cards.
    """

    status: str
    bracc_conectado: bool
    total_nos: int
    total_relacionamentos: int
    deputados_federais: int
    deputados_estaduais: int
    senadores: int
    servidores_estaduais: int = 0
    cargos_comissionados: int = 0
    municipios_go: int = 0
    licitacoes_go: int = 0
    nomeacoes_go: int = 0
    vereadores_goiania: int = 0


class BuscarTudoItem(BaseModel):
    """One search result item rendered by the PWA result list.

    ``tipo`` matches the Neo4j label lower-cased and (historically)
    without underscores, matching what ``/api/v1/search`` already
    emits. ``icone`` is a UI hint the PWA maps to an avatar.
    """

    id: str
    tipo: str
    nome: str
    documento: str | None = None
    score: float = 0.0
    icone: str = "outro"
    detalhe: str = ""
    is_pep: bool | None = None
    is_comissionado: bool | None = None


class BuscarTudoResponse(BaseModel):
    """Envelope for ``/buscar-tudo``; matches the Flask payload keys."""

    resultados: list[BuscarTudoItem]
    total: int
    pagina: int
