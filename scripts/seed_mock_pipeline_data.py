"""Seed deterministic mock source data so Airflow DAGs can process non-empty inputs."""

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

from analytics.mock_seed import ensure_mock_pipeline_source_data
from analytics.orchestrator import refresh_materialized_features
from database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def _run() -> dict[str, object]:
    await db.connect()
    try:
        seed_summary = await ensure_mock_pipeline_source_data()
        materialization = await refresh_materialized_features(
            job_name="mock_seed_materialization"
        )
        return {
            "seed_summary": seed_summary,
            "materialization": materialization,
        }
    finally:
        await db.disconnect()


def main() -> None:
    try:
        result = asyncio.run(_run())
        reports_dir = ROOT_DIR / "data" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "message": "Mock source data ensured for Airflow analytics DAGs",
            **result,
        }
        out_path = reports_dir / "mock_seed_report.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        row_counts = (
            payload.get("materialization", {}).get("row_counts", {})
            if isinstance(payload.get("materialization"), dict)
            else {}
        )
        logger.info("Mock seed completed. Materialized row counts: %s", row_counts)
        logger.info("Wrote seed report to %s", out_path)
    except Exception:
        logger.exception("Mock pipeline data seeding failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
