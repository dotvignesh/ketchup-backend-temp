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


def materialize_features(**context) -> dict[str, object]:
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

    result = asyncio.run(_run())
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
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
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
