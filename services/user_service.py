"""User domain service."""

from __future__ import annotations

from uuid import UUID

from database import db
from services.errors import NotFoundError


async def get_current_user(user_id: UUID) -> dict[str, object]:
    user = await db.fetchrow(
        "SELECT id, email, name FROM users WHERE id = $1",
        user_id,
    )
    if not user:
        raise NotFoundError("User not found")

    groups = await db.fetch(
        """
        SELECT g.id, g.name, g.lead_id, g.status, gm.role
        FROM groups g
        JOIN group_members gm ON g.id = gm.group_id
        WHERE gm.user_id = $1 AND gm.status = 'active'
        ORDER BY g.name
        """,
        user_id,
    )

    invites = await db.fetch(
        """
        SELECT gi.id, gi.group_id, g.name as group_name, u.name as inviter_name
        FROM group_invites gi
        JOIN groups g ON gi.group_id = g.id
        JOIN users u ON gi.invited_by = u.id
        WHERE gi.email = $1 AND gi.status = 'pending'
        """,
        user["email"],
    )

    return {
        "user_id": str(user["id"]),
        "email": user["email"],
        "name": user["name"],
        "google_calendar_connected": False,
        "groups": [
            {
                "id": str(group["id"]),
                "name": group["name"],
                "lead_id": str(group["lead_id"]),
                "status": group["status"],
                "role": group["role"],
            }
            for group in groups
        ],
        "pending_invites": [
            {
                "id": str(invite["id"]),
                "group_id": str(invite["group_id"]),
                "group_name": invite["group_name"],
                "inviter_name": invite["inviter_name"],
            }
            for invite in invites
        ],
    }


async def update_preferences(
    user_id: UUID,
    updates: dict[str, object],
) -> dict[str, object]:
    return {"user_id": str(user_id), "preferences": updates}

