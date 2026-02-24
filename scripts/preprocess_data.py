"""Preprocess raw data into cleaned, feature-engineered outputs."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipelines.preprocessing import DataCleaner, FeatureEngineer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _preprocess_calendar(calendar_df: pd.DataFrame) -> pd.DataFrame:
    calendar_df = DataCleaner.remove_duplicates(calendar_df, subset=["user_id"])
    calendar_df = DataCleaner.handle_missing_values(
        calendar_df,
        strategy="fill",
        fill_value=0,
    )

    if "reference_date" in calendar_df.columns:
        calendar_df["reference_date"] = calendar_df["reference_date"].replace(0, None)
        calendar_df["reference_date"] = calendar_df["reference_date"].fillna(
            datetime.now(timezone.utc).isoformat(),
        )

    if "total_busy_hours" in calendar_df.columns:
        calendar_df = DataCleaner.remove_outliers(
            calendar_df,
            column="total_busy_hours",
            method="iqr",
            threshold=1.5,
        )

    calendar_df = FeatureEngineer.create_availability_features(calendar_df)

    return calendar_df


def main() -> None:
    try:
        root = Path(__file__).resolve().parents[1]
        raw_dir = root / "data" / "raw"
        processed_dir = root / "data" / "processed"
        metrics_dir = root / "data" / "metrics"
        _ensure_dirs(processed_dir, metrics_dir)

        calendar_path = raw_dir / "calendar_data.csv"
        if not calendar_path.exists():
            raise FileNotFoundError(
                f"Input data not found: {calendar_path}. Run acquire_data first.",
            )

        calendar_df = pd.read_csv(calendar_path)
        calendar_processed = _preprocess_calendar(calendar_df)

        calendar_out = processed_dir / "calendar_processed.csv"
        calendar_processed.to_csv(calendar_out, index=False)

        metrics = {
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "calendar_input_records": int(len(calendar_df)),
            "calendar_output_records": int(len(calendar_processed)),
        }

        metrics_path = metrics_dir / "preprocessing_metrics.json"
        with metrics_path.open("w", encoding="utf-8") as handle:
            json.dump(metrics, handle, indent=2)

        logger.info("Saved calendar processed data to %s", calendar_out)
        logger.info("Saved metrics to %s", metrics_path)
    except Exception:
        logger.exception("Data preprocessing stage failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
