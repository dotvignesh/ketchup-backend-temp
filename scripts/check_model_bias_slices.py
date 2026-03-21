#!/usr/bin/env python3
"""Analyze model bias using slice tables over synthetic evaluation results."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def bootstrap_ci(
    values: pd.Series, n_boot: int = 5000, alpha: float = 0.05, seed: int = 0
) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    vals = np.array(values)
    boots = rng.choice(vals, size=(n_boot, len(vals)), replace=True).mean(axis=1)
    low = np.quantile(boots, alpha / 2)
    high = np.quantile(boots, 1 - alpha / 2)
    return float(low), float(high)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        default="data/reports/model_bias_results.csv",
        help="Path to results CSV.",
    )
    parser.add_argument(
        "--out",
        default="data/reports/model_bias_slicing_report.md",
        help="Markdown report output path.",
    )
    parser.add_argument(
        "--min-n", type=int, default=5, help="Minimum row count for worst-slice tables."
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    df["full_budget_ok"] = np.isclose(df["budget_compliance"], 1.0).astype(int)

    overall = df[
        [
            "json_valid",
            "options_count_ok",
            "budget_compliance",
            "distance_compliance",
            "logistics_feasible",
            "full_budget_ok",
        ]
    ].mean()
    overall_ci = bootstrap_ci(df["budget_compliance"])

    city_budget = (
        df.groupby(["city_tier", "budget_tier"])
        .agg(
            n=("cycle_id", "count"),
            budget_compliance=("budget_compliance", "mean"),
            full_budget_ok=("full_budget_ok", "mean"),
            json_valid=("json_valid", "mean"),
        )
        .reset_index()
        .sort_values(["budget_compliance", "n"])
    )

    worst = city_budget.iloc[0]
    worst_values = df[
        (df["city_tier"] == worst["city_tier"])
        & (df["budget_tier"] == worst["budget_tier"])
    ]["budget_compliance"]
    worst_ci = bootstrap_ci(worst_values)

    intersection = (
        df.groupby(["city_tier", "budget_tier", "distance_bucket", "car_ratio_bucket"])
        .agg(
            n=("cycle_id", "count"),
            budget_compliance=("budget_compliance", "mean"),
            full_budget_ok=("full_budget_ok", "mean"),
        )
        .reset_index()
    )
    worst_intersection = (
        intersection[intersection["n"] >= args.min_n]
        .sort_values("budget_compliance")
        .head(10)
    )

    print("\n=== Overall ===")
    print(overall)
    print(
        f"\nOverall budget_compliance 95% CI (bootstrap): [{overall_ci[0]:.3f}, {overall_ci[1]:.3f}]"
    )

    print("\n=== Slice: city_tier x budget_tier ===")
    print(
        city_budget[
            ["city_tier", "budget_tier", "n", "budget_compliance", "full_budget_ok"]
        ].to_string(index=False)
    )

    overall_table = pd.DataFrame(
        {"metric": overall.index, "value": overall.values}
    ).to_markdown(index=False)
    report = f"""# Bias Slicing Eval Report (Synthetic Baseline)

_Data source:_ `{args.csv}`
_Rows (planning cycles):_ {len(df)}

## Overall metrics

{overall_table}

Overall `budget_compliance` bootstrap 95% CI: **[{overall_ci[0]:.3f}, {overall_ci[1]:.3f}]**

## Slice: city_tier x budget_tier

{city_budget[["city_tier", "budget_tier", "n", "budget_compliance", "full_budget_ok"]].to_markdown(index=False)}

### Worst slice

- Slice: **{worst["city_tier"]} x {worst["budget_tier"]}** (n={int(worst["n"])})
- Mean `budget_compliance`: **{worst["budget_compliance"]:.3f}** (overall {overall["budget_compliance"]:.3f})
- Mean `full_budget_ok`: **{worst["full_budget_ok"]:.3f}** (overall {overall["full_budget_ok"]:.3f})
- Slice `budget_compliance` bootstrap 95% CI: **[{worst_ci[0]:.3f}, {worst_ci[1]:.3f}]**

## Worst intersection slices (min_n={args.min_n})

{(worst_intersection.to_markdown(index=False) if len(worst_intersection) else "(none; increase N or lower min_n)")}

## Interpretation + mitigation plan (no fine-tuning)

This baseline run suggests a **coverage/constraint disparity**: some slices, typically low-budget and sparse-coverage cases, violate budget constraints more often.

Planned mitigations:
1. **Prefilter** candidate venues by budget before generation.
2. **Validate then repair** when any generated option violates the requested budget.
3. **Fallback mode** when too few compliant venues exist.

Trade-offs:
- repair increases cost and latency on the failing subset,
- prefiltering may reduce option diversity in low-coverage slices.
"""

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(f"\n[INFO] Wrote report: {output_path}")


if __name__ == "__main__":
    main()
