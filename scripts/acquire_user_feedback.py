"""Extract user feedback data from Postgres for pipeline stages."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
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
        feedback_df = asyncio.run(_extract_feedback(database_url))
        output_path = raw_dir / "user_feedback.csv"
        feedback_df.to_csv(output_path, index=False)

        logger.info("Saved user feedback data to %s", output_path)
    except Exception:
        logger.exception("User feedback acquisition stage failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
