"""PWA parity endpoints.

These routes reproduce the shapes emitted by the legacy Flask backend
(``backend/app.py``) so the existing PWA (``pwa/index.html``) can be
pointed at the FastAPI service with no client-side change.

Paths are intentionally mounted at the root (``/status``,
``/buscar-tudo``) because the PWA calls ``${API}/status`` — i.e. the
raw service root — whereas the rest of the FastAPI surface lives
under ``/api/v1``. Keeping the parity routes out of the ``/api/v1``
tree preserves a clean boundary: the ``/api/v1`` contract stays
graph-native, and this router is a thin PWA-facing facade that can be
removed once the PWA is updated to call ``/api/v1`` directly.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from neo4j import AsyncSession

from bracc.dependencies import get_session
from bracc.models.pwa_parity import StatusResponse
from bracc.services.neo4j_service import execute_query_single

router = APIRouter(tags=["pwa-parity"])

UF_FILTRO = "GO"


@router.get("/status", response_model=StatusResponse)
async def pwa_status(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> StatusResponse:
    """PWA landing-page counters (aggregated in one Cypher round-trip)."""
    try:
        record = await execute_query_single(
            session, "pwa_status", {"uf": UF_FILTRO}
        )
        bracc_conectado = True
    except Exception:  # noqa: BLE001 — mirror Flask: degrade on DB errors
        record = None
        bracc_conectado = False

    if record is None:
        return StatusResponse(
            status="online",
            bracc_conectado=bracc_conectado,
            total_nos=0,
            total_relacionamentos=0,
            deputados_federais=0,
            deputados_estaduais=0,
            senadores=0,
            servidores_estaduais=0,
            cargos_comissionados=0,
            municipios_go=0,
            licitacoes_go=0,
            nomeacoes_go=0,
            vereadores_goiania=0,
        )

    return StatusResponse(
        status="online",
        bracc_conectado=bracc_conectado,
        total_nos=int(record["total_nos"] or 0),
        total_relacionamentos=int(record["total_relacionamentos"] or 0),
        deputados_federais=int(record["deputados_federais"] or 0),
        deputados_estaduais=int(record["deputados_estaduais"] or 0),
        senadores=int(record["senadores"] or 0),
        vereadores_goiania=int(record["vereadores_goiania"] or 0),
        servidores_estaduais=int(record["servidores_estaduais"] or 0),
        cargos_comissionados=int(record["cargos_comissionados"] or 0),
        municipios_go=int(record["municipios_go"] or 0),
        licitacoes_go=int(record["licitacoes_go"] or 0),
        nomeacoes_go=int(record["nomeacoes_go"] or 0),
    )
