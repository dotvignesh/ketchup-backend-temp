"""Availability domain service."""

from __future__ import annotations

from datetime import time
from uuid import UUID

from database import db


def _parse_clock_time(raw_value: str, fallback: time) -> time:
    return time.fromisoformat(raw_value) if ":" in raw_value else fallback


async def get_user_availability(user_id: UUID) -> dict[str, list[dict[str, str | None]]]:
    rows = await db.fetch(
        """
        SELECT id, day_of_week, start_time, end_time, label, location
        FROM availability_blocks
        WHERE user_id = $1
        ORDER BY day_of_week, start_time
        """,
        user_id,
    )
    return {
        "blocks": [
            {
                "id": str(row["id"]),
                "day_of_week": row["day_of_week"],
                "start_time": str(row["start_time"]) if row["start_time"] else None,
                "end_time": str(row["end_time"]) if row["end_time"] else None,
                "label": row["label"],
                "location": row["location"],
            }
            for row in rows
        ]
    }


async def replace_user_availability(
    user_id: UUID,
    blocks: list[dict[str, object]],
) -> dict[str, list[dict[str, str | int | None]]]:
    await db.execute("DELETE FROM availability_blocks WHERE user_id = $1", user_id)

    persisted_blocks: list[dict[str, str | int | None]] = []
    for block in blocks:
        start = _parse_clock_time(str(block["start_time"]), fallback=time(9, 0))
        end = _parse_clock_time(str(block["end_time"]), fallback=time(17, 0))

        row = await db.fetchrow(
            """
            INSERT INTO availability_blocks
                (user_id, day_of_week, start_time, end_time, label, location)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, day_of_week, start_time, end_time, label, location
            """,
            user_id,
            block["day_of_week"],
            start,
            end,
            block.get("label"),
            block.get("location"),
        )
        persisted_blocks.append(
            {
                "id": str(row["id"]),
                "day_of_week": row["day_of_week"],
                "start_time": str(row["start_time"]),
                "end_time": str(row["end_time"]),
                "label": row["label"],
                "location": row["location"],
            }
        )

    return {"blocks": persisted_blocks}

