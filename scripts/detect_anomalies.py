"""Detect anomalies in processed data and emit a report."""

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

from pipelines.validation import AnomalyDetector, ValidationResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _summarize_result(result: ValidationResult) -> dict:
    return {
        "passed": bool(result.passed),
        "issue_count": int(result.issue_count),
        "issues": result.issues,
        "quality_level": result.quality_level.value,
    }


def main() -> None:
    try:
        root = Path(__file__).resolve().parents[1]
        processed_dir = root / "data" / "processed"
        reports_dir = root / "data" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        input_path = processed_dir / "calendar_processed.csv"
        if not input_path.exists():
            raise FileNotFoundError(
                f"Processed dataset missing: {input_path}. Run preprocess_data first.",
            )

        calendar_df = pd.read_csv(input_path)

        calendar_missing = AnomalyDetector.detect_missing_values(calendar_df, 10.0)
        calendar_duplicates = AnomalyDetector.detect_duplicates(
            calendar_df,
            subset=["user_id"],
        )
        calendar_outliers = AnomalyDetector.detect_outliers(
            calendar_df,
            column="total_busy_hours",
            method="iqr",
        )

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "calendar": {
                "missing_values": _summarize_result(calendar_missing),
                "duplicates": _summarize_result(calendar_duplicates),
                "outliers": _summarize_result(calendar_outliers),
            },
        }

        report_path = reports_dir / "anomaly_report.json"
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)

        logger.info("Saved anomaly report to %s", report_path)
    except Exception:
        logger.exception("Anomaly detection stage failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
