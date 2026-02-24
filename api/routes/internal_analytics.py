"""Internal analytics control endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from analytics.orchestrator import get_analytics_status, refresh_materialized_features
from api.dependencies import require_internal_api_key

router = APIRouter(prefix="/api/internal/analytics", tags=["internal-analytics"])


@router.get("/status")
async def analytics_status(
    job_name: str = Query(
        default="analytics_feature_materialization",
        min_length=1,
        max_length=120,
    ),
    _: None = Depends(require_internal_api_key),
):
    return await get_analytics_status(job_name=job_name)


@router.post("/rebuild", status_code=202)
async def analytics_rebuild(
    job_name: str = Query(
        default="analytics_feature_materialization",
        min_length=1,
        max_length=120,
    ),
    _: None = Depends(require_internal_api_key),
):
    try:
        return await refresh_materialized_features(job_name=job_name)
    except Exception as exc:  # pragma: no cover - defensive HTTP mapping
        raise HTTPException(
            status_code=500,
            detail=f"Analytics rebuild failed: {exc.__class__.__name__}",
        ) from exc
