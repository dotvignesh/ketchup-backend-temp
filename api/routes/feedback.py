"""Post-event feedback routes."""

from uuid import UUID

from fastapi import APIRouter, Depends

from api.dependencies import get_current_user_id
from models.schemas import FeedbackCreate
from services import feedback_service

router = APIRouter(prefix="/api/groups", tags=["feedback"])


@router.post("/{group_id}/events/{event_id}/feedback", status_code=201)
async def submit_feedback(
    group_id: UUID,
    event_id: UUID,
    body: FeedbackCreate,
    user_id: UUID = Depends(get_current_user_id),
):
    """Submit post-event feedback (Loved/Liked/Disliked)."""
    return await feedback_service.submit_feedback(
        group_id=group_id,
        event_id=event_id,
        user_id=user_id,
        rating=body.rating,
        notes=body.notes,
        attended=body.attended,
    )


@router.get("/{group_id}/events/{event_id}/feedback")
async def get_feedback(
    group_id: UUID,
    event_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
):
    """Get all feedback for an event."""
    return await feedback_service.get_feedback(
        group_id=group_id,
        event_id=event_id,
        user_id=user_id,
    )
