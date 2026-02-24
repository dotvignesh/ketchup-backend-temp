"""Extract user feedback data from Postgres for pipeline stages."""

from __future__ import annotations

import asyncio
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


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _build_mock_feedback() -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "feedback_id": f"mock_feedback_{index:03d}",
            "user_id": f"mock_user_{(index % 6) + 1:03d}",
            "group_id": f"mock_group_{(index % 2) + 1:03d}",
            "rating": ["loved", "liked", "neutral"][index % 3],
            "attended": bool(index % 5 != 0),
            "comment": "Auto-generated feedback for local pipeline runs",
            "submitted_at": now,
        }
        for index in range(1, 25)
    ]
    return pd.DataFrame(rows)


async def _extract_feedback(database_url: str) -> pd.DataFrame:
    conn = await asyncpg.connect(database_url)
    try:
        rows = await conn.fetch(
            """
            SELECT
                f.id::text AS feedback_id,
                f.user_id::text AS user_id,
                e.group_id::text AS group_id,
                f.rating,
                f.attended,
                COALESCE(f.notes, '') AS comment,
                f.created_at AS submitted_at
            FROM feedback f
            JOIN events e ON e.id = f.event_id
            ORDER BY f.created_at DESC
            """
        )
        if not rows:
            return pd.DataFrame(
                columns=[
                    "feedback_id",
                    "user_id",
                    "group_id",
                    "rating",
                    "attended",
                    "comment",
                    "submitted_at",
                ]
            )
        return pd.DataFrame([dict(row) for row in rows])
    finally:
        await conn.close()


def main() -> None:
    try:
        root = Path(__file__).resolve().parents[1]
        raw_dir = root / "data" / "raw"
        _ensure_dir(raw_dir)

        database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
        try:
            feedback_df = asyncio.run(_extract_feedback(database_url))
        except Exception as exc:
            logger.warning(
                "Falling back to mock feedback data because extraction failed: %s",
                exc,
            )
            feedback_df = pd.DataFrame()

        if feedback_df.empty:
            feedback_df = _build_mock_feedback()
            logger.info("Using mock user feedback data (%s rows)", len(feedback_df))

        output_path = raw_dir / "user_feedback.csv"
        feedback_df.to_csv(output_path, index=False)

        logger.info("Saved user feedback data to %s", output_path)
    except Exception:
        logger.exception("User feedback acquisition stage failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
