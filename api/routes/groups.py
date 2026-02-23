"""Group controllers (HTTP layer)."""

from uuid import UUID

from fastapi import APIRouter, Depends

from api.dependencies import get_current_user_id
from models.schemas import (
    GroupCreate,
    GroupInviteRequest,
    GroupPreferencesUpdate,
    GroupUpdate,
)
from services import group_service

router = APIRouter(prefix="/api/groups", tags=["groups"])


@router.post("", status_code=201)
async def create_group(
    body: GroupCreate,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.create_group(name=body.name, user_id=user_id)


@router.get("")
async def list_groups(user_id: UUID = Depends(get_current_user_id)):
    return await group_service.list_groups(user_id=user_id)


@router.get("/{group_id}")
async def get_group(
    group_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.get_group(group_id=group_id, user_id=user_id)


@router.put("/{group_id}")
async def update_group(
    group_id: UUID,
    body: GroupUpdate,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.update_group(
        group_id=group_id,
        user_id=user_id,
        name=body.name,
    )


@router.post("/{group_id}/invite", status_code=201)
async def invite_members(
    group_id: UUID,
    body: GroupInviteRequest,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.invite_members(
        group_id=group_id,
        user_id=user_id,
        emails=body.emails,
    )


@router.post("/{group_id}/invite/accept")
async def accept_invite(
    group_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.accept_invite(group_id=group_id, user_id=user_id)


@router.post("/{group_id}/invite/reject")
async def reject_invite(
    group_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.reject_invite(group_id=group_id, user_id=user_id)


@router.put("/{group_id}/preferences")
async def update_group_preferences(
    group_id: UUID,
    body: GroupPreferencesUpdate,
    user_id: UUID = Depends(get_current_user_id),
):
    return await group_service.update_group_preferences(
        group_id=group_id,
        user_id=user_id,
        updates=body.model_dump(exclude_none=True),
    )

