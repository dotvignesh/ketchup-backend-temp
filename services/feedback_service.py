"""Feedback domain service."""

from __future__ import annotations

from uuid import UUID

from database import db
from services.errors import BadRequestError
from services.group_access import require_active_group_member, require_event_in_group

VALID_RATINGS = {"loved", "liked", "disliked"}


async def submit_feedback(
    group_id: UUID,
    event_id: UUID,
    user_id: UUID,
    rating: str,
    notes: str | None,
    attended: bool,
) -> dict[str, str | None]:
    await require_active_group_member(group_id, user_id)
    await require_event_in_group(event_id, group_id)

    if rating not in VALID_RATINGS:
        raise BadRequestError("Rating must be loved, liked, or disliked")

    row = await db.fetchrow(
        """
        INSERT INTO feedback (event_id, user_id, rating, notes, attended)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, notes = EXCLUDED.notes, attended = EXCLUDED.attended
        RETURNING id, rating, notes
        """,
        event_id,
        user_id,
        rating,
        notes,
        attended,
    )
    return {
        "feedback_id": str(row["id"]),
        "rating": row["rating"],
        "notes": row["notes"],
    }


async def get_feedback(
    group_id: UUID,
    event_id: UUID,
    user_id: UUID,
) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)
    await require_event_in_group(event_id, group_id)

    rows = await db.fetch(
        """
        SELECT f.id, f.user_id, u.name, f.rating, f.notes, f.attended
        FROM feedback f
        JOIN users u ON f.user_id = u.id
        WHERE f.event_id = $1
        """,
        event_id,
    )

    loved = sum(1 for row in rows if row["rating"] == "loved")
    liked = sum(1 for row in rows if row["rating"] == "liked")
    disliked = sum(1 for row in rows if row["rating"] == "disliked")

    return {
        "feedbacks": [
            {
                "id": str(row["id"]),
                "user_id": str(row["user_id"]),
                "name": row["name"],
                "rating": row["rating"],
                "notes": row["notes"],
                "attended": row["attended"],
            }
            for row in rows
        ],
        "summary": {"loved": loved, "liked": liked, "disliked": disliked},
    }

