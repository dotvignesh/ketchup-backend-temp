"""Group availability controllers (HTTP layer)."""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends

from api.dependencies import get_current_user_id
from services import availability_group_service

router = APIRouter(prefix="/api/groups", tags=["availability"])


@router.post("/{group_id}/availability")
async def compute_group_availability(
    group_id: UUID,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    user_id: UUID = Depends(get_current_user_id),
):
    return await availability_group_service.compute_group_availability(
        group_id=group_id,
        user_id=user_id,
        time_min=time_min,
        time_max=time_max,
    )

