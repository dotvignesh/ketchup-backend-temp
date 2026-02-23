"""Plan controllers (HTTP layer)."""

from uuid import UUID

from fastapi import APIRouter, Body, Depends

from api.dependencies import get_current_user_id
from models.schemas import RefinePlansRequest, VoteRequest
from services import plans_service

router = APIRouter(prefix="/api/groups", tags=["plans"])


@router.post("/{group_id}/generate-plans", status_code=201)
async def generate_plans(
    group_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await plans_service.generate_plans(group_id=group_id, user_id=user_id)


@router.get("/{group_id}/plans/{round_id}")
async def get_plans(
    group_id: UUID,
    round_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await plans_service.get_plans(
        group_id=group_id,
        round_id=round_id,
        user_id=user_id,
    )


@router.post("/{group_id}/plans/{round_id}/vote", status_code=201)
async def submit_vote(
    group_id: UUID,
    round_id: UUID,
    body: VoteRequest,
    user_id: UUID = Depends(get_current_user_id),
):
    return await plans_service.submit_vote(
        group_id=group_id,
        round_id=round_id,
        user_id=user_id,
        rankings=body.rankings,
        notes=body.notes,
    )


@router.get("/{group_id}/plans/{round_id}/results")
async def get_voting_results(
    group_id: UUID,
    round_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await plans_service.get_voting_results(
        group_id=group_id,
        round_id=round_id,
        user_id=user_id,
    )


@router.post("/{group_id}/plans/{round_id}/refine", status_code=201)
async def refine_plans(
    group_id: UUID,
    round_id: UUID,
    body: RefinePlansRequest | None = Body(default=None),
    user_id: UUID = Depends(get_current_user_id),
):
    return await plans_service.refine_plans(
        group_id=group_id,
        round_id=round_id,
        user_id=user_id,
        descriptors=(body.descriptors if body else None),
        lead_note=(body.lead_note if body else None),
    )


@router.post("/{group_id}/plans/{round_id}/finalize", status_code=201)
async def finalize_plan(
    group_id: UUID,
    round_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    return await plans_service.finalize_plan(
        group_id=group_id,
        round_id=round_id,
        user_id=user_id,
    )
