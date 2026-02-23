"""Shared API dependencies."""

import secrets
from uuid import UUID

from fastapi import Header, HTTPException

from config import get_settings


def get_current_user_id(
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    x_internal_auth: str | None = Header(None, alias="X-Internal-Auth"),
) -> UUID:
    """Read and validate the authenticated user ID from trusted proxy headers."""
    settings = get_settings()
    expected_key = settings.backend_internal_api_key.strip()
    if expected_key and (
        not x_internal_auth or not secrets.compare_digest(x_internal_auth, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Invalid internal auth header")

    if not x_user_id:
        raise HTTPException(status_code=401, detail="X-User-Id header required")
    try:
        return UUID(x_user_id)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid user ID") from exc
