from fastapi import APIRouter

from limnopulse_api.api.dependencies import PrincipalDep
from limnopulse_api.api.v1.schemas import MeResponse

router = APIRouter(tags=["me"])


@router.get("/me", response_model=MeResponse)
async def get_me(principal: PrincipalDep) -> MeResponse:
    return MeResponse.model_validate(principal.model_dump())
