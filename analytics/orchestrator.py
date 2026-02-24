"""Orchestrates analytics materialization jobs and run tracking."""

from __future__ import annotations

import os
from typing import Any

from analytics.bootstrap import ensure_analytics_schema
from analytics.jobs import (
    FEATURE_VERSION,
    build_group_feature_snapshots,
    build_plan_outcome_facts,
    build_venue_performance_priors,
)
from analytics.repositories import (
    begin_pipeline_run,
    finish_pipeline_run,
    get_latest_pipeline_run,
)


def _resolve_version_sha() -> str | None:
    for env_key in ("GIT_SHA", "COMMIT_SHA", "SOURCE_COMMIT"):
        value = os.getenv(env_key, "").strip()
        if value:
            return value[:80]
    return None


async def refresh_materialized_features(
    job_name: str = "analytics_feature_materialization",
) -> dict[str, Any]:
    """Run analytics jobs and persist a pipeline run record."""
    await ensure_analytics_schema()
    run_id = await begin_pipeline_run(job_name, version_sha=_resolve_version_sha())

    row_counts: dict[str, int] = {}
    try:
        row_counts["plan_outcome_fact"] = await build_plan_outcome_facts()
        row_counts["venue_performance_prior"] = await build_venue_performance_priors()
        row_counts["group_feature_snapshot"] = await build_group_feature_snapshots(
            feature_version=FEATURE_VERSION,
        )
        await finish_pipeline_run(
            run_id,
            status="success",
            row_counts=row_counts,
            error_summary=None,
        )
    except Exception as exc:
        await finish_pipeline_run(
            run_id,
            status="failed",
            row_counts=row_counts,
            error_summary=f"{exc.__class__.__name__}: {exc}",
        )
        raise

    latest = await get_latest_pipeline_run(job_name)
    return {
        "job_name": job_name,
        "feature_version": FEATURE_VERSION,
        "row_counts": row_counts,
        "latest_run": latest,
    }


async def get_analytics_status(
    job_name: str = "analytics_feature_materialization",
) -> dict[str, Any]:
    await ensure_analytics_schema()
    latest = await get_latest_pipeline_run(job_name)
    return {
        "job_name": job_name,
        "feature_version": FEATURE_VERSION,
        "latest_run": latest,
    }
