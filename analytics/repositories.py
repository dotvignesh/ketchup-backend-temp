"""Async repositories for analytics materialized tables."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from database import db


def _as_utc_iso(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_row_counts(value: Any) -> dict[str, int]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    if isinstance(value, dict):
        out: dict[str, int] = {}
        for key, raw in value.items():
            try:
                out[str(key)] = int(raw)
            except (TypeError, ValueError):
                continue
        return out
    return {}


async def begin_pipeline_run(job_name: str, version_sha: str | None = None) -> UUID:
    row = await db.fetchrow(
        """
        INSERT INTO analytics.pipeline_runs (job_name, status, version_sha)
        VALUES ($1, 'running', $2)
        RETURNING id
        """,
        job_name,
        (version_sha or None),
    )
    if not row:
        raise RuntimeError("Failed to create analytics pipeline run")
    return row["id"]


async def finish_pipeline_run(
    run_id: UUID,
    *,
    status: str,
    row_counts: dict[str, int] | None = None,
    error_summary: str | None = None,
) -> None:
    await db.execute(
        """
        UPDATE analytics.pipeline_runs
        SET finished_at = NOW(),
            status = $2,
            row_counts = $3::jsonb,
            error_summary = $4
        WHERE id = $1
        """,
        run_id,
        status,
        json.dumps(row_counts or {}),
        (error_summary[:1200] if error_summary else None),
    )


async def get_latest_pipeline_run(job_name: str | None = None) -> dict[str, Any] | None:
    if job_name:
        row = await db.fetchrow(
            """
            SELECT id, job_name, started_at, finished_at, status, row_counts, error_summary, version_sha
            FROM analytics.pipeline_runs
            WHERE job_name = $1
            ORDER BY started_at DESC
            LIMIT 1
            """,
            job_name,
        )
    else:
        row = await db.fetchrow(
            """
            SELECT id, job_name, started_at, finished_at, status, row_counts, error_summary, version_sha
            FROM analytics.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """,
        )

    if not row:
        return None

    return {
        "id": str(row["id"]),
        "job_name": row["job_name"],
        "started_at": _as_utc_iso(row["started_at"]),
        "finished_at": _as_utc_iso(row["finished_at"]),
        "status": row["status"],
        "row_counts": _parse_row_counts(row["row_counts"]),
        "error_summary": row["error_summary"],
        "version_sha": row["version_sha"],
    }


async def get_latest_group_feature_snapshot(group_id: UUID) -> dict[str, Any] | None:
    row = await db.fetchrow(
        """
        SELECT
            group_id,
            snapshot_at,
            feature_version,
            top_activity_tags,
            budget_mode,
            mobility_mode,
            historical_novelty_score,
            refine_descriptor_weights
        FROM analytics.group_feature_snapshot
        WHERE group_id = $1
          AND is_latest = TRUE
        ORDER BY snapshot_at DESC
        LIMIT 1
        """,
        group_id,
    )
    if not row:
        return None

    weights = row["refine_descriptor_weights"]
    if isinstance(weights, str):
        try:
            weights = json.loads(weights)
        except json.JSONDecodeError:
            weights = {}
    if not isinstance(weights, dict):
        weights = {}

    return {
        "group_id": str(row["group_id"]),
        "snapshot_at": _as_utc_iso(row["snapshot_at"]),
        "feature_version": row["feature_version"],
        "top_activity_tags": [str(x) for x in (row["top_activity_tags"] or []) if str(x).strip()],
        "budget_mode": row["budget_mode"] or "unspecified",
        "mobility_mode": row["mobility_mode"] or "unknown",
        "historical_novelty_score": (
            float(row["historical_novelty_score"])
            if row["historical_novelty_score"] is not None
            else None
        ),
        "refine_descriptor_weights": {
            str(key): float(value)
            for key, value in weights.items()
            if isinstance(key, str) and isinstance(value, (int, float))
        },
    }


async def get_group_venue_priors(group_id: UUID, limit: int = 40) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 200))
    rows = await db.fetch(
        """
        SELECT
            venue_key,
            win_rate,
            avg_rank,
            attendance_rate,
            feedback_score,
            sample_size,
            updated_at
        FROM analytics.venue_performance_prior
        WHERE group_id = $1
        ORDER BY sample_size DESC, win_rate DESC, feedback_score DESC NULLS LAST
        LIMIT $2
        """,
        group_id,
        safe_limit,
    )

    priors: list[dict[str, Any]] = []
    for row in rows:
        priors.append(
            {
                "venue_key": str(row["venue_key"]),
                "win_rate": float(row["win_rate"] or 0),
                "avg_rank": float(row["avg_rank"]) if row["avg_rank"] is not None else None,
                "attendance_rate": (
                    float(row["attendance_rate"])
                    if row["attendance_rate"] is not None
                    else None
                ),
                "feedback_score": (
                    float(row["feedback_score"]) if row["feedback_score"] is not None else None
                ),
                "sample_size": int(row["sample_size"] or 0),
                "updated_at": _as_utc_iso(row["updated_at"]),
            }
        )
    return priors
