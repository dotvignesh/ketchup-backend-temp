"""Run Postgres analytics materialization jobs."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from analytics.orchestrator import refresh_materialized_features
from database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def _run() -> dict:
    await db.connect()
    try:
        return await refresh_materialized_features()
    finally:
        await db.disconnect()


def main() -> None:
    try:
        result = asyncio.run(_run())
        root = Path(__file__).resolve().parents[1]
        metrics_dir = root / "data" / "metrics"
        reports_dir = root / "data" / "reports"
        metrics_dir.mkdir(parents=True, exist_ok=True)
        reports_dir.mkdir(parents=True, exist_ok=True)

        metrics_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "job_name": result["job_name"],
            "feature_version": result["feature_version"],
            "row_counts": result["row_counts"],
        }
        (metrics_dir / "materialization_metrics.json").write_text(
            json.dumps(metrics_payload, indent=2),
            encoding="utf-8",
        )
        (reports_dir / "analytics_status.json").write_text(
            json.dumps(result, indent=2),
            encoding="utf-8",
        )
        logger.info("Analytics materialization completed: %s", result["row_counts"])
    except Exception:
        logger.exception("Analytics materialization failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
