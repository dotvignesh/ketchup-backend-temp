"""Authentication domain service."""

from __future__ import annotations

from database import db


async def google_signin(
    email: str,
    name: str | None,
    google_id: str | None,
) -> dict[str, object]:
    normalized_email = email.strip().lower()

    row = await db.fetchrow(
        "SELECT id, email, name FROM users WHERE email = $1",
        normalized_email,
    )
    if row:
        return {
            "id": row["id"],
            "email": row["email"],
            "name": row["name"],
        }

    row = await db.fetchrow(
        """
        INSERT INTO users (email, name, google_id)
        VALUES ($1, $2, $3)
        RETURNING id, email, name
        """,
        normalized_email,
        name or email.split("@")[0],
        google_id,
    )
    return {
        "id": row["id"],
        "email": row["email"],
        "name": row["name"],
    }
