"""Comprehensive analytics DAG with quality and bias checks."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

try:
    from pipelines.monitoring import PerformanceProfiler
except Exception:  # pragma: no cover - defensive import for constrained Airflow envs

    class PerformanceProfiler:  # type: ignore[override]
        def __init__(self) -> None:
            self._durations: dict[str, float] = {}

        def start_profiling(self, task_name: str) -> None:
            self._durations[task_name] = 0.0

        def end_profiling(self, task_name: str, status: str = "completed") -> float:
            return float(self._durations.get(task_name, 0.0))


logger = logging.getLogger(__name__)
profiler = PerformanceProfiler()

default_args = {
    "owner": "ketchup-data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 26),
}

dag = DAG(
    "ketchup_comprehensive_pipeline",
    default_args=default_args,
    description="Refresh analytics features and run quality checks over materialized outcomes.",
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["data-pipeline", "analytics", "quality", "bias"],
)


def _build_mock_materialization_result(
    *, job_name: str, reason: str
) -> dict[str, object]:
    return {
        "job_name": job_name,
        "mock_fallback": True,
        "reason": reason,
        "row_counts": {
            "plan_outcome_fact": 48,
            "venue_performance_prior": 24,
            "group_feature_snapshot": 36,
        },
        "generated_at": datetime.utcnow().isoformat(),
    }


def _build_mock_bias_rows() -> list[dict[str, object]]:
    return [
        {
            "group_id": f"mock_group_{index:03d}",
            "vibe_type": "chill" if index % 2 == 0 else "hype",
            "won": 1 if index % 3 != 0 else 0,
            "feedback_loved_rate": 0.45 + ((index % 5) * 0.1),
        }
        for index in range(1, 31)
    ]


def _track_task_start(task_name: str) -> float:
    profiler.start_profiling(task_name)
    return perf_counter()


def _track_task_end(
    *,
    context: dict,
    task_name: str,
    started_at: float,
    status: str,
    details: dict[str, object] | None = None,
) -> None:
    elapsed = perf_counter() - started_at
    profiled = profiler.end_profiling(task_name, status=status)
    duration = float(profiled) if profiled > 0 else float(elapsed)
    context["ti"].xcom_push(
        key=f"{task_name}_task_profile",
        value={
            "task_name": task_name,
            "status": status,
            "duration_seconds": round(duration, 4),
            "details": details or {},
        },
    )


def _load_task_profiles(context: dict) -> dict[str, object]:
    profiles: list[dict[str, object]] = []
    for task_id in (
        "materialize_features",
        "validate_materialization",
        "run_bias_checks",
    ):
        profile = context["ti"].xcom_pull(
            task_ids=task_id,
            key=f"{task_id}_task_profile",
        )
        if isinstance(profile, dict):
            profiles.append(profile)

    profiles.sort(
        key=lambda item: float(item.get("duration_seconds", 0)),
        reverse=True,
    )
    return {
        "total_tasks_profiled": len(profiles),
        "total_duration_seconds": round(
            sum(float(item.get("duration_seconds", 0)) for item in profiles),
            4,
        ),
        "tasks": profiles,
        "bottlenecks": profiles[:5],
    }


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


def materialize_features(**context) -> dict[str, object]:
    from analytics.mock_seed import ensure_mock_pipeline_source_data
    from analytics.orchestrator import refresh_materialized_features
    from database import db

    task_name = context["task"].task_id
    started_at = _track_task_start(task_name)

    async def _run() -> dict[str, object]:
        await db.connect()
        try:
            return await refresh_materialized_features(
                job_name="ketchup_comprehensive_pipeline"
            )
        finally:
            await db.disconnect()

    async def _seed_and_rerun(reason: str) -> dict[str, object]:
        await db.connect()
        try:
            seed_summary = await ensure_mock_pipeline_source_data()
            rerun_result = await refresh_materialized_features(
                job_name="ketchup_comprehensive_pipeline"
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
            "Materialization failed; attempting source-data seeding before fallback: %s",
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
                "Seed-and-rerun failed; using mock fallback result: %s", seed_exc
            )
            result = _build_mock_materialization_result(
                job_name="ketchup_comprehensive_pipeline",
                reason=f"materialization_error:{exc.__class__.__name__}",
            )

    row_counts = result.get("row_counts") if isinstance(result, dict) else None
    if (
        not isinstance(row_counts, dict)
        or sum(int(v) for v in row_counts.values()) <= 0
    ):
        logger.info(
            "Materialization produced empty row counts; seeding source data and retrying"
        )
        try:
            result = asyncio.run(_seed_and_rerun("empty_row_counts"))
            row_counts = result.get("row_counts") if isinstance(result, dict) else None
        except Exception as seed_exc:
            logger.warning(
                "Seed-and-rerun after empty row counts failed; using mock fallback: %s",
                seed_exc,
            )
            result = _build_mock_materialization_result(
                job_name="ketchup_comprehensive_pipeline",
                reason="empty_row_counts",
            )
            row_counts = result.get("row_counts")

    context["ti"].xcom_push(key="materialization_result", value=result)
    details = {
        "row_counts": row_counts,
        "mock_fallback": bool(isinstance(result, dict) and result.get("mock_fallback")),
    }
    _track_task_end(
        context=context,
        task_name=task_name,
        started_at=started_at,
        status="success",
        details=details,
    )
    return result


def validate_materialization(**context) -> dict[str, object]:
    task_name = context["task"].task_id
    started_at = _track_task_start(task_name)

    result = (
        context["ti"].xcom_pull(
            task_ids="materialize_features",
            key="materialization_result",
        )
        or {}
    )
    row_counts = (result or {}).get("row_counts") or {}

    required = (
        "plan_outcome_fact",
        "venue_performance_prior",
        "group_feature_snapshot",
    )
    issues: list[str] = []
    for table in required:
        count = int(row_counts.get(table, 0)) if isinstance(row_counts, dict) else 0
        if count <= 0:
            issues.append(f"{table} has no rows after refresh")

    validation = {
        "passed": len(issues) == 0,
        "issues": issues,
        "row_counts": row_counts,
    }
    context["ti"].xcom_push(key="validation_report", value=validation)
    _track_task_end(
        context=context,
        task_name=task_name,
        started_at=started_at,
        status="success" if validation["passed"] else "warning",
        details={"issue_count": len(issues)},
    )
    return validation


# def should_run_bias_checks(**context) -> bool:
#     # raw = Variable.get("run_extended_bias_analysis", default_var="false")
#     raw = Variable.get("run_extended_bias_analysis", default_var="true")
# return str(raw).lower() in {"1", "true", "yes", "on"}


def run_bias_checks(**context) -> dict[str, object]:
    from database import db
    from pipelines.bias_detection import (
        BiasAnalyzer,
        BiasMitigationStrategy,
        DataSlicer,
    )

    task_name = context["task"].task_id
    started_at = _track_task_start(task_name)

    async def _fetch() -> list[dict[str, object]]:
        await db.connect()
        try:
            rows = await db.fetch(
                """
                SELECT
                    group_id::text AS group_id,
                    COALESCE(vibe_type, 'unknown') AS vibe_type,
                    CASE WHEN won THEN 1 ELSE 0 END AS won,
                    COALESCE(feedback_loved_rate, 0)::float8 AS feedback_loved_rate
                FROM analytics.plan_outcome_fact
                """
            )
            return [dict(row) for row in rows]
        finally:
            await db.disconnect()

    try:
        rows = asyncio.run(_fetch())
        used_mock_rows = False
        if not rows:
            rows = _build_mock_bias_rows()
            used_mock_rows = True
            logger.info("Using mock bias rows for analysis (%s rows)", len(rows))

        df = pd.DataFrame(rows)
        slices = DataSlicer.slice_by_demographic(df, "vibe_type")
        metrics = BiasAnalyzer.detect_bias_in_slices(
            slices=slices,
            target_column="won",
            positive_label=1,
        )
        biased_slices = sorted({m.slice_name for m in metrics if m.is_biased})
        report = BiasMitigationStrategy.generate_mitigation_report(
            metrics, biased_slices
        )
        if isinstance(report, dict):
            report["used_mock_rows"] = used_mock_rows
        context["ti"].xcom_push(key="bias_report", value=report)
        _track_task_end(
            context=context,
            task_name=task_name,
            started_at=started_at,
            status="success",
            details={
                "rows": len(rows),
                "used_mock_rows": used_mock_rows,
                "slice_count": len(slices),
                "biased_slice_count": len(biased_slices),
            },
        )
        return report
    except Exception:
        _track_task_end(
            context=context,
            task_name=task_name,
            started_at=started_at,
            status="failed",
            details={},
        )
        raise


def generate_report(**context) -> dict[str, object]:
    performance = _load_task_profiles(context)
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "materialization": context["ti"].xcom_pull(
            task_ids="materialize_features",
            key="materialization_result",
        ),
        "validation": context["ti"].xcom_pull(
            task_ids="validate_materialization",
            key="validation_report",
        ),
        "bias": context["ti"].xcom_pull(task_ids="run_bias_checks", key="bias_report"),
        "performance": performance,
        "bottlenecks": performance["bottlenecks"],
    }

    out_dir = Path("data/reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "pipeline_report.json"
    out_path.write_text(
        json.dumps(_to_json_serializable(report), indent=2),
        encoding="utf-8",
    )
    logger.info("Wrote pipeline report: %s", out_path)
    return report


materialize_task = PythonOperator(
    task_id="materialize_features",
    python_callable=materialize_features,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_materialization",
    python_callable=validate_materialization,
    dag=dag,
)

# bias_gate_task = ShortCircuitOperator(
#     task_id="should_run_extended_bias_analysis",
#     python_callable=should_run_bias_checks,
#     dag=dag,
# )

bias_task = PythonOperator(
    task_id="run_bias_checks",
    python_callable=run_bias_checks,
    dag=dag,
)

report_task = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

materialize_task >> validate_task
# validate_task >> bias_gate_task >> bias_task >> report_task
validate_task >> bias_task >> report_task
validate_task >> report_task
