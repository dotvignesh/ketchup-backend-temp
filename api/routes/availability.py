"""Availability controllers (HTTP layer)."""
from uuid import UUID

from fastapi import APIRouter, Depends

from api.dependencies import get_current_user_id
from models.schemas import AvailabilityBlocksUpdate
from services import availability_service

router = APIRouter(prefix="/api/users", tags=["availability"])


@router.get("/me/availability")
async def get_availability(user_id: UUID = Depends(get_current_user_id)):
    return await availability_service.get_user_availability(user_id=user_id)


@router.put("/me/availability")
async def update_availability(
    body: AvailabilityBlocksUpdate,
    user_id: UUID = Depends(get_current_user_id),
):
    blocks = [block.model_dump(exclude_none=False) for block in body.blocks]
    return await availability_service.replace_user_availability(
        user_id=user_id,
        blocks=blocks,
    )
