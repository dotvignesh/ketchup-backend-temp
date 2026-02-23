"""User routes."""

from uuid import UUID

from fastapi import APIRouter, Depends

from api.dependencies import get_current_user_id
from models.schemas import UserPreferencesUpdate
from services import user_service

router = APIRouter(prefix="/api/users", tags=["users"])


@router.get("/me", response_model=dict)
async def get_current_user(user_id: UUID = Depends(get_current_user_id)):
    """Get current user profile with groups and pending invites."""
    return await user_service.get_current_user(user_id=user_id)


@router.put("/me/preferences")
async def update_preferences(
    body: UserPreferencesUpdate,
    user_id: UUID = Depends(get_current_user_id),
):
    return await user_service.update_preferences(
        user_id=user_id,
        updates=body.model_dump(exclude_none=True),
    )
