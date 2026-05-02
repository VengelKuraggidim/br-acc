"""Router do endpoint ``GET /custo-mandato/{cargo}``.

Substitui o card hardcoded ``Quanto custa um deputado federal?`` que
vivia em ``pwa/index.html`` por dado lido do grafo (pipelines
``custo_mandato_br`` + ``custo_mandato_municipal_go``) com proveniência
clicável por componente.

Cargos suportados são restritos via path validation contra
:data:`bracc.services.custo_mandato_service.CARGOS_SUPORTADOS` —
rejeita valores fora do conjunto antes de tocar o banco. O conjunto
cresceu além do que cabe num ``StrEnum`` (24 cargos hoje: federal +
estadual + 10 municípios GO × 2), então a validação virou frozenset
direto + ``Path(pattern=...)``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path
from neo4j import AsyncSession  # noqa: TC002 — runtime via FastAPI Depends introspection

from bracc.dependencies import get_session
from bracc.models.custo_mandato import CustoMandato
from bracc.services import custo_mandato_service

router = APIRouter(tags=["custo-mandato"])

# Pattern conservador: lowercase + underscore + dígitos (slugs municipais
# usam só letras/underscores). Cargos fora desse formato são rejeitados
# pelo FastAPI antes do handler — ainda batemos contra
# ``CARGOS_SUPORTADOS`` no handler pra cobrir slugs bem-formados que não
# estão no MVP (ex.: ``prefeito_inexistente``).
_CARGO_PATTERN = r"^[a-z][a-z0-9_]*$"


@router.get("/custo-mandato/{cargo}", response_model=CustoMandato)
async def get_custo_mandato(
    session: Annotated[AsyncSession, Depends(get_session)],
    cargo: Annotated[
        str,
        Path(
            description="Cargo eletivo suportado",
            pattern=_CARGO_PATTERN,
            min_length=2,
            max_length=64,
        ),
    ],
) -> CustoMandato:
    """Custo mensal/anual do cargo eletivo + composição com proveniência.

    Erros:
        * 422 — cargo malformado (não bate o pattern de slug).
        * 404 — cargo malformado-bem mas (a) fora do conjunto suportado
          ou (b) suportado mas pipeline ainda não rodou no ambiente.
    """
    if cargo not in custo_mandato_service.CARGOS_SUPORTADOS:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Cargo '{cargo}' não está no conjunto suportado. "
                f"Veja CARGOS_SUPORTADOS."
            ),
        )
    try:
        return await custo_mandato_service.obter_custo_mandato(session, cargo)
    except custo_mandato_service.CargoNaoEncontradoError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
