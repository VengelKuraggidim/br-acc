"""Router do endpoint ``GET /custo-mandato/{cargo}``.

Substitui o card hardcoded ``Quanto custa um deputado federal?`` que
vivia em ``pwa/index.html`` por dado lido do grafo (pipeline
``custo_mandato_br``) com proveniência clicável por componente.

Cargos suportados são restritos via path enum pra rejeitar valores fora
do MVP no FastAPI antes de tocar o banco.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path
from neo4j import AsyncSession  # noqa: TC002 — runtime via FastAPI Depends introspection

from bracc.dependencies import get_session
from bracc.models.custo_mandato import CustoMandato
from bracc.services import custo_mandato_service

router = APIRouter(tags=["custo-mandato"])


class CargoEnum(StrEnum):
    """Cargos do MVP. Manter alinhado com
    :data:`bracc.services.custo_mandato_service.CARGOS_SUPORTADOS`."""

    dep_federal = "dep_federal"
    senador = "senador"
    dep_estadual_go = "dep_estadual_go"
    governador_go = "governador_go"


@router.get("/custo-mandato/{cargo}", response_model=CustoMandato)
async def get_custo_mandato(
    session: Annotated[AsyncSession, Depends(get_session)],
    cargo: Annotated[CargoEnum, Path(description="Cargo eletivo (MVP)")],
) -> CustoMandato:
    """Custo mensal/anual do cargo eletivo + composição com proveniência.

    Erros:
        * 404 — cargo válido (MVP) mas pipeline ``custo_mandato_br`` não
          rodou ainda no ambiente (nó ``:CustoMandato`` ausente).
    """
    try:
        return await custo_mandato_service.obter_custo_mandato(
            session, cargo.value,
        )
    except custo_mandato_service.CargoNaoEncontradoError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
