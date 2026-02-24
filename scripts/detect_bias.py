"""Detect bias across demographic slices in processed data."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipelines.bias_detection import BiasAnalyzer, BiasMitigationStrategy, DataSlicer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


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

        if "availability_category" not in calendar_df.columns:
            calendar_df["availability_category"] = "unknown"

        if "selected" not in calendar_df.columns:
            calendar_df["selected"] = (
                calendar_df["availability_percentage"].fillna(0) >= 50
            ).astype(int)

        if "predicted_selected" not in calendar_df.columns:
            calendar_df["predicted_selected"] = calendar_df["selected"]

        slices = DataSlicer.slice_by_demographic(
            calendar_df,
            "availability_category",
        )

        bias_metrics = BiasAnalyzer.detect_bias_in_slices(
            slices,
            target_column="selected",
            prediction_column="predicted_selected",
            positive_label=1,
        )

        biased_slices = sorted({m.slice_name for m in bias_metrics if m.is_biased})
        mitigation_report = BiasMitigationStrategy.generate_mitigation_report(
            bias_metrics,
            biased_slices,
        )

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "bias_metrics": [
                {
                    "slice": m.slice_name,
                    "metric": m.metric_name,
                    "value": float(m.value),
                    "threshold": float(m.threshold),
                    "is_biased": bool(m.is_biased),
                }
                for m in bias_metrics
            ],
            "mitigation_report": json.loads(
                json.dumps(
                    mitigation_report,
                    default=lambda x: (
                        bool(x)
                        if isinstance(x, (bool, np.bool_))
                        else (
                            float(x) if isinstance(x, (float, np.floating)) else str(x)
                        )
                    ),
                ),
            ),
        }

        report_path = reports_dir / "bias_report.json"
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)

        logger.info("Saved bias report to %s", report_path)
    except Exception:
        logger.exception("Bias detection stage failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
