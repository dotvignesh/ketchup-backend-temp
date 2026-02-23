"""Shared access-control helpers for group-scoped resources."""

from uuid import UUID

from database import db
from services.errors import ForbiddenError, NotFoundError


async def require_active_group_member(group_id: UUID, user_id: UUID) -> None:
    """Ensure a user is an active member of a group."""
    member = await db.fetchrow(
        "SELECT id FROM group_members WHERE group_id = $1 AND user_id = $2 AND status = 'active'",
        group_id,
        user_id,
    )
    if not member:
        raise ForbiddenError("Not a member of this group")


async def require_group_lead(group_id: UUID, user_id: UUID, detail: str) -> None:
    """Ensure a user is the lead of a group."""
    group = await db.fetchrow("SELECT lead_id FROM groups WHERE id = $1", group_id)
    if not group:
        raise NotFoundError("Group not found")
    if group["lead_id"] != user_id:
        raise ForbiddenError(detail)


async def get_user_email_or_404(user_id: UUID) -> str:
    """Fetch user email or raise not found."""
    user = await db.fetchrow("SELECT email FROM users WHERE id = $1", user_id)
    if not user:
        raise NotFoundError("User not found")
    return str(user["email"])


async def require_event_in_group(event_id: UUID, group_id: UUID) -> None:
    """Ensure an event belongs to the target group."""
    event = await db.fetchrow(
        "SELECT id FROM events WHERE id = $1 AND group_id = $2",
        event_id,
        group_id,
    )
    if not event:
        raise NotFoundError("Event not found")

