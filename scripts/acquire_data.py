"""Acquire raw availability features from live Postgres tables."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/appdb"


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _build_mock_calendar_data() -> pd.DataFrame:
    reference_date = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "user_id": f"mock_user_{index:03d}",
            "availability_percentage": float(max(15, 95 - (index * 3) % 70)),
            "num_busy_intervals": int(2 + (index % 8)),
            "total_busy_hours": float(5 + (index * 2.25) % 42),
            "reference_date": reference_date,
        }
        for index in range(1, 26)
    ]
    return pd.DataFrame(rows)


async def _extract_calendar_data(database_url: str) -> pd.DataFrame:
    conn = await asyncpg.connect(database_url)
    try:
        rows = await conn.fetch(
            """
            SELECT
                u.id::text AS user_id,
                CASE
                    WHEN COUNT(ab.id) = 0 THEN 100.0
                    ELSE GREATEST(
                        5.0,
                        100.0 - LEAST(
                            95.0,
                            (
                                SUM(EXTRACT(EPOCH FROM (ab.end_time - ab.start_time))) / 3600.0
                            ) / 168.0 * 100.0
                        )
                    )
                END AS availability_percentage,
                COUNT(ab.id)::integer AS num_busy_intervals,
                COALESCE(
                    SUM(EXTRACT(EPOCH FROM (ab.end_time - ab.start_time))) / 3600.0,
                    0
                )::numeric(10,2) AS total_busy_hours,
                NOW()::timestamptz AS reference_date
            FROM users u
            LEFT JOIN availability_blocks ab ON ab.user_id = u.id
            GROUP BY u.id
            ORDER BY u.id
            """
        )
        if not rows:
            return pd.DataFrame(
                columns=[
                    "user_id",
                    "availability_percentage",
                    "num_busy_intervals",
                    "total_busy_hours",
                    "reference_date",
                ]
            )
        return pd.DataFrame([dict(row) for row in rows])
    finally:
        await conn.close()


def main() -> None:
    try:
        root = Path(__file__).resolve().parents[1]
        raw_dir = root / "data" / "raw"
        metrics_dir = root / "data" / "metrics"
        _ensure_dirs(raw_dir, metrics_dir)

        database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
        source = "postgres"
        used_mock_fallback = False

        try:
            calendar_df = asyncio.run(_extract_calendar_data(database_url))
        except Exception as exc:
            logger.warning(
                "Falling back to mock calendar data because extraction failed: %s",
                exc,
            )
            calendar_df = pd.DataFrame()

        if calendar_df.empty:
            calendar_df = _build_mock_calendar_data()
            source = "mock_fallback"
            used_mock_fallback = True
            logger.info("Using mock calendar data (%s rows)", len(calendar_df))

        calendar_path = raw_dir / "calendar_data.csv"
        calendar_df.to_csv(calendar_path, index=False)

        metrics = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "used_mock_fallback": used_mock_fallback,
            "calendar_records": int(len(calendar_df)),
            "calendar_missing_values": int(calendar_df.isnull().sum().sum()),
        }
        metrics_path = metrics_dir / "acquisition_metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

        logger.info("Saved calendar data to %s", calendar_path)
        logger.info("Saved metrics to %s", metrics_path)
    except Exception:
        logger.exception("Data acquisition stage failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
