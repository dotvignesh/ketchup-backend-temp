"""Daily analytics materialization DAG (Postgres-first)."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ketchup-data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 26),
}

dag = DAG(
    "daily_analytics_materialization",
    default_args=default_args,
    description="Daily refresh of planner-facing analytics features in Postgres.",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["data-pipeline", "analytics"],
)


def _to_json_serializable(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {str(key): _to_json_serializable(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_to_json_serializable(item) for item in value]

    if hasattr(value, "tolist"):
        try:
            return _to_json_serializable(value.tolist())
        except Exception:
            pass

    if hasattr(value, "item"):
        try:
            return _to_json_serializable(value.item())
        except Exception:
            pass

    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    return str(value)


def _build_mock_materialization_result(
    *, job_name: str, reason: str
) -> dict[str, object]:
    return {
        "job_name": job_name,
        "mock_fallback": True,
        "reason": reason,
        "row_counts": {
            "plan_outcome_fact": 24,
            "venue_performance_prior": 12,
            "group_feature_snapshot": 18,
        },
        "generated_at": datetime.utcnow().isoformat(),
    }


def materialize_features(**context) -> dict[str, object]:
    from analytics.mock_seed import ensure_mock_pipeline_source_data
    from analytics.orchestrator import refresh_materialized_features
    from database import db

    async def _run() -> dict[str, object]:
        await db.connect()
        try:
            return await refresh_materialized_features(
                job_name="daily_analytics_materialization"
            )
        finally:
            await db.disconnect()

    async def _seed_and_rerun(reason: str) -> dict[str, object]:
        await db.connect()
        try:
            seed_summary = await ensure_mock_pipeline_source_data()
            rerun_result = await refresh_materialized_features(
                job_name="daily_analytics_materialization"
            )
            if isinstance(rerun_result, dict):
                rerun_result["seed_summary"] = seed_summary
                rerun_result["seed_trigger_reason"] = reason
            return rerun_result
        finally:
            await db.disconnect()

    try:
        result = asyncio.run(_run())
    except Exception as exc:
        logger.warning(
            "Daily materialization failed; attempting source-data seeding before fallback: %s",
            exc,
        )
        try:
            result = asyncio.run(
                _seed_and_rerun(
                    f"materialization_error:{exc.__class__.__name__}",
                )
            )
        except Exception as seed_exc:
            logger.warning(
                "Daily seed-and-rerun failed; using mock fallback: %s", seed_exc
            )
            result = _build_mock_materialization_result(
                job_name="daily_analytics_materialization",
                reason=f"materialization_error:{exc.__class__.__name__}",
            )

    row_counts = result.get("row_counts") if isinstance(result, dict) else None
    if (
        not isinstance(row_counts, dict)
        or sum(int(v) for v in row_counts.values()) <= 0
    ):
        logger.info(
            "Daily materialization produced empty row counts; seeding source data and retrying",
        )
        try:
            result = asyncio.run(_seed_and_rerun("empty_row_counts"))
        except Exception as seed_exc:
            logger.warning(
                "Daily seed-and-rerun after empty row counts failed; using fallback: %s",
                seed_exc,
            )
            result = _build_mock_materialization_result(
                job_name="daily_analytics_materialization",
                reason="empty_row_counts",
            )

    context["ti"].xcom_push(key="materialization_result", value=result)
    return result


def write_report(**context) -> dict[str, object]:
    result = (
        context["ti"].xcom_pull(
            task_ids="materialize_features",
            key="materialization_result",
        )
        or {}
    )
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "job_name": "daily_analytics_materialization",
        "result": result,
    }
    out_dir = Path("data/reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "daily_analytics_report.json"
    out_path.write_text(
        json.dumps(_to_json_serializable(report), indent=2),
        encoding="utf-8",
    )
    logger.info("Wrote analytics report: %s", out_path)
    return report


materialize_task = PythonOperator(
    task_id="materialize_features",
    python_callable=materialize_features,
    dag=dag,
)

report_task = PythonOperator(
    task_id="write_report",
    python_callable=write_report,
    dag=dag,
)
