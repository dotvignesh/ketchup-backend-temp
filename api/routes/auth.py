"""Authentication routes for Google OAuth."""

from fastapi import APIRouter

from models.schemas import GoogleSigninRequest, GoogleSigninResponse
from services import auth_service

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/google-signin", response_model=GoogleSigninResponse)
async def google_signin(body: GoogleSigninRequest):
    """
    Create user on first Google sign-in, or return existing user.

    Called by Auth.js v5 in the JWT callback after Google OAuth completes.
    This ensures every Google-authenticated user has a corresponding row
    in our users table with a stable UUID.
    """
    row = await auth_service.google_signin(
        email=body.email,
        name=body.name,
        google_id=body.google_id,
    )
    return GoogleSigninResponse(
        user_id=row["id"],
        email=row["email"],
        name=row["name"],
    )
