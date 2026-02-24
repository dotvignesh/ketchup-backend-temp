"""Schema, range, anomaly, and statistics validation helpers for ETL output."""

import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityLevel(Enum):
    """Severity buckets for validation outcomes."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class ValidationResult:
    """Structured result returned by validators and anomaly detectors."""

    passed: bool
    issue_count: int
    issues: List[str]
    quality_level: DataQualityLevel


class SchemaValidator:
    """Column-level schema and required-field checks."""

    @staticmethod
    def _column_matches_expected_type(series: pd.Series, expected_type: type) -> bool:
        dtype = series.dtype

        # Accept both naive and timezone-aware datetime dtypes for datetime expectations.
        try:
            if np.issubdtype(expected_type, np.datetime64):
                return bool(pd.api.types.is_datetime64_any_dtype(dtype))
        except TypeError:
            pass

        try:
            if np.issubdtype(expected_type, np.timedelta64):
                return bool(pd.api.types.is_timedelta64_dtype(dtype))
        except TypeError:
            pass

        try:
            if np.issubdtype(expected_type, np.bool_):
                return bool(pd.api.types.is_bool_dtype(dtype))
        except TypeError:
            pass

        try:
            if np.issubdtype(expected_type, np.integer):
                return bool(pd.api.types.is_integer_dtype(dtype))
        except TypeError:
            pass

        try:
            if np.issubdtype(expected_type, np.floating):
                return bool(pd.api.types.is_float_dtype(dtype))
        except TypeError:
            pass

        try:
            if np.issubdtype(expected_type, np.number):
                return bool(pd.api.types.is_numeric_dtype(dtype))
        except TypeError:
            pass

        if expected_type in (object, np.object_, str, np.str_):
            return bool(
                pd.api.types.is_object_dtype(dtype)
                or pd.api.types.is_string_dtype(dtype)
                or pd.api.types.is_categorical_dtype(dtype)
            )

        try:
            return bool(np.issubdtype(dtype, expected_type))
        except TypeError:
            return False

    @staticmethod
    def validate_schema(df: pd.DataFrame, schema: Dict[str, type]) -> ValidationResult:
        issues = []

        for col, expected_type in schema.items():
            if col not in df.columns:
                issues.append(f"Missing required column: {col}")
                continue

            if not SchemaValidator._column_matches_expected_type(
                df[col], expected_type
            ):
                issues.append(
                    f"Column {col} has type {df[col].dtype}, "
                    f"expected {expected_type}",
                )

        for col in df.columns:
            if col not in schema:
                logger.warning(f"Unexpected column in DataFrame: {col}")

        passed = len(issues) == 0
        quality_level = (
            DataQualityLevel.HIGH
            if passed
            else DataQualityLevel.MEDIUM if len(issues) < 3 else DataQualityLevel.LOW
        )

        logger.info(
            f"Schema validation: {'PASSED' if passed else 'FAILED'} "
            f"({len(issues)} issues)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=len(issues),
            issues=issues,
            quality_level=quality_level,
        )

    @staticmethod
    def validate_required_fields(
        df: pd.DataFrame,
        required_fields: List[str],
    ) -> ValidationResult:
        issues = []

        for field in required_fields:
            if field not in df.columns:
                issues.append(f"Required field missing: {field}")
                continue

            null_count = df[field].isnull().sum()
            if null_count > 0:
                issues.append(
                    f"Field {field} has {null_count} null values "
                    f"({null_count / len(df) * 100:.2f}%)",
                )

        passed = len(issues) == 0
        quality_level = (
            DataQualityLevel.CRITICAL
            if len(issues) > 5
            else DataQualityLevel.HIGH if passed else DataQualityLevel.MEDIUM
        )

        logger.info(
            f"Required fields validation: {'PASSED' if passed else 'FAILED'} "
            f"({len(issues)} issues)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=len(issues),
            issues=issues,
            quality_level=quality_level,
        )


class RangeValidator:
    """Numeric range and categorical value checks."""

    @staticmethod
    def validate_numeric_range(
        df: pd.DataFrame,
        column: str,
        min_value: float = None,
        max_value: float = None,
    ) -> ValidationResult:
        issues = []

        if column not in df.columns:
            issues.append(f"Column not found: {column}")
        else:
            if min_value is not None:
                violations = (df[column] < min_value).sum()
                if violations > 0:
                    issues.append(
                        f"{violations} values in {column} below minimum {min_value}",
                    )

            if max_value is not None:
                violations = (df[column] > max_value).sum()
                if violations > 0:
                    issues.append(
                        f"{violations} values in {column} above maximum {max_value}",
                    )

        passed = len(issues) == 0
        quality_level = DataQualityLevel.HIGH if passed else DataQualityLevel.MEDIUM

        logger.info(
            f"Range validation for {column}: {'PASSED' if passed else 'FAILED'} "
            f"({len(issues)} issues)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=len(issues),
            issues=issues,
            quality_level=quality_level,
        )

    @staticmethod
    def validate_categorical_values(
        df: pd.DataFrame,
        column: str,
        allowed_values: List[str],
    ) -> ValidationResult:
        issues = []

        if column not in df.columns:
            issues.append(f"Column not found: {column}")
        else:
            invalid_values = df[~df[column].isin(allowed_values)]
            if len(invalid_values) > 0:
                unique_invalid = invalid_values[column].unique()
                issues.append(
                    f"{len(invalid_values)} invalid values in {column}: "
                    f"{unique_invalid}",
                )

        passed = len(issues) == 0
        quality_level = DataQualityLevel.HIGH if passed else DataQualityLevel.MEDIUM

        logger.info(
            f"Categorical validation for {column}: "
            f"{'PASSED' if passed else 'FAILED'} ({len(issues)} issues)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=len(issues),
            issues=issues,
            quality_level=quality_level,
        )


class AnomalyDetector:
    """Anomaly checks for missing data, duplicates, and outliers."""

    @staticmethod
    def detect_missing_values(
        df: pd.DataFrame,
        threshold_pct: float = 10.0,
    ) -> ValidationResult:
        issues = []

        for col in df.columns:
            missing_pct = (df[col].isnull().sum() / len(df)) * 100
            if missing_pct > threshold_pct:
                issues.append(
                    f"Column {col} has {missing_pct:.2f}% missing values",
                )
            elif missing_pct > 0:
                logger.warning(
                    f"Column {col} has {missing_pct:.2f}% missing values",
                )

        passed = len(issues) == 0
        quality_level = (
            DataQualityLevel.HIGH
            if passed
            else (
                DataQualityLevel.MEDIUM
                if len(issues) < 3
                else DataQualityLevel.CRITICAL
            )
        )

        logger.info(
            f"Missing values detection: {'PASSED' if passed else 'FAILED'} "
            f"({len(issues)} issues)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=len(issues),
            issues=issues,
            quality_level=quality_level,
        )

    @staticmethod
    def detect_duplicates(
        df: pd.DataFrame,
        subset: List[str] = None,
    ) -> ValidationResult:
        duplicates = df.duplicated(subset=subset).sum()
        issues = []

        if duplicates > 0:
            dup_pct = (duplicates / len(df)) * 100
            issues.append(f"Found {duplicates} duplicate rows ({dup_pct:.2f}%)")

        passed = duplicates == 0
        quality_level = DataQualityLevel.HIGH if passed else DataQualityLevel.MEDIUM

        logger.info(
            f"Duplicate detection: {'PASSED' if passed else 'FAILED'} "
            f"({duplicates} duplicates)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=duplicates,
            issues=issues,
            quality_level=quality_level,
        )

    @staticmethod
    def detect_outliers(
        df: pd.DataFrame,
        column: str,
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> ValidationResult:
        issues = []

        if column not in df.columns:
            issues.append(f"Column not found: {column}")
        else:
            if method == "iqr":
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
            elif method == "zscore":
                from scipy import stats

                series = df[column]
                non_null = series.dropna()
                z_scores = np.abs(stats.zscore(non_null))
                outlier_index = non_null.index[z_scores > threshold]
                outliers = df.loc[outlier_index]
            else:
                outliers = pd.DataFrame()

            if len(outliers) > 0:
                out_pct = (len(outliers) / len(df)) * 100
                issues.append(
                    f"Found {len(outliers)} outliers in {column} "
                    f"({out_pct:.2f}%) using {method} method",
                )

        passed = len(issues) == 0
        quality_level = DataQualityLevel.HIGH if passed else DataQualityLevel.MEDIUM

        logger.info(
            f"Outlier detection for {column}: "
            f"{'PASSED' if passed else 'ANOMALY DETECTED'} "
            f"({len(issues)} issues)",
        )

        return ValidationResult(
            passed=passed,
            issue_count=len(issues),
            issues=issues,
            quality_level=quality_level,
        )


class DataStatisticsGenerator:
    """Dataset profiling and statistics persistence helpers."""

    @staticmethod
    def generate_statistics(df: pd.DataFrame) -> Dict[str, Any]:
        stats = {
            "record_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
            "duplicates_count": df.duplicated().sum(),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2,
            "generated_at": pd.Timestamp.now().isoformat(),
        }

        for col in df.columns:
            col_stats = {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_percentage": float(
                    (df[col].isnull().sum() / len(df)) * 100,
                ),
                "unique_count": int(df[col].nunique()),
                "memory_usage_bytes": int(df[col].memory_usage(deep=True)),
            }

            if pd.api.types.is_numeric_dtype(df[col]):
                col_stats.update(
                    {
                        "mean": float(df[col].mean()),
                        "std": float(df[col].std()),
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "median": float(df[col].median()),
                        "q25": float(df[col].quantile(0.25)),
                        "q75": float(df[col].quantile(0.75)),
                    },
                )
            elif pd.api.types.is_object_dtype(df[col]):
                col_stats["mode"] = (
                    str(df[col].mode()[0]) if len(df[col].mode()) > 0 else None
                )

            stats["columns"][col] = col_stats

        logger.info(f"Generated statistics for {len(df)} records")
        return stats

    @staticmethod
    def save_statistics(stats: Dict[str, Any], filepath: str) -> None:
        parent_dir = os.path.dirname(filepath)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(
                stats,
                f,
                indent=2,
                default=lambda obj: obj.item() if hasattr(obj, "item") else str(obj),
            )

        logger.info(f"Saved statistics to {filepath}")
