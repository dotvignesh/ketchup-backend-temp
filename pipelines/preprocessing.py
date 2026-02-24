"""Data cleaning, aggregation, and feature helpers for ETL jobs."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataCleaner:
    """Basic cleaning transforms for tabular pipeline inputs."""

    @staticmethod
    def remove_duplicates(df: pd.DataFrame, subset: List[str] = None) -> pd.DataFrame:
        initial_rows = len(df)
        df_cleaned = df.drop_duplicates(subset=subset, keep="first")
        removed = initial_rows - len(df_cleaned)

        logger.info(f"Removed {removed} duplicate rows (kept {len(df_cleaned)})")
        return df_cleaned

    @staticmethod
    def handle_missing_values(
        df: pd.DataFrame,
        strategy: str = "drop",
        fill_value: Any = None,
    ) -> pd.DataFrame:
        missing_before = df.isnull().sum().sum()

        if strategy == "drop":
            df_cleaned = df.dropna()
        elif strategy == "forward_fill":
            df_cleaned = df.fillna(method="ffill")
        elif strategy == "backward_fill":
            df_cleaned = df.fillna(method="bfill")
        elif strategy == "fill":
            df_cleaned = df.fillna(fill_value)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

        missing_after = df_cleaned.isnull().sum().sum()
        logger.info(
            f"Handled missing values: {missing_before} -> {missing_after} "
            f"using strategy '{strategy}'",
        )
        return df_cleaned

    @staticmethod
    def remove_outliers(
        df: pd.DataFrame,
        column: str,
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> pd.DataFrame:
        initial_rows = len(df)

        if method == "iqr":
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            if pd.isna(IQR) or IQR == 0:
                logger.info("Skipping IQR outlier removal for %s: zero/NaN IQR", column)
                return df.copy()
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            df_cleaned = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

        elif method == "zscore":
            from scipy import stats

            series = df[column]
            non_null = series.dropna()
            z_scores = np.abs(stats.zscore(non_null))
            keep_index = non_null.index[z_scores < threshold]
            df_cleaned = df.loc[keep_index]
        else:
            raise ValueError(f"Unknown method: {method}")

        removed = initial_rows - len(df_cleaned)
        logger.info(f"Removed {removed} outliers using {method} method")
        return df_cleaned


class DataAggregator:
    """Build aggregated DataFrames from raw API and storage records."""

    @staticmethod
    def aggregate_calendar_data(calendar_records: List[Dict]) -> pd.DataFrame:
        aggregated = []

        for record in calendar_records:
            user_id = record.get("user_id")
            busy_intervals = record.get("busy_intervals", [])
            availability_pct = record.get("availability_percentage", 0)

            aggregated.append(
                {
                    "user_id": user_id,
                    "num_busy_intervals": len(busy_intervals),
                    "availability_percentage": availability_pct,
                    "total_busy_hours": sum(
                        (
                            datetime.fromisoformat(interval["end"])
                            - datetime.fromisoformat(interval["start"])
                        ).total_seconds()
                        / 3600
                        for interval in busy_intervals
                    ),
                    "last_updated": datetime.now().isoformat(),
                },
            )

        df_aggregated = pd.DataFrame(aggregated)
        logger.info(f"Aggregated calendar data for {len(aggregated)} users")
        return df_aggregated

    @staticmethod
    def aggregate_venue_data(venue_records: List[Dict]) -> pd.DataFrame:
        aggregated = []

        for venue in venue_records:
            aggregated.append(
                {
                    "venue_id": venue.get("venue_id"),
                    "name": venue.get("name"),
                    "category": venue.get("category"),
                    "rating": venue.get("rating", 0),
                    "price_level": venue.get("price_level", 0),
                    "latitude": venue.get("location", {}).get("latitude"),
                    "longitude": venue.get("location", {}).get("longitude"),
                    "review_count": venue.get("review_count", 0),
                    "last_updated": datetime.now().isoformat(),
                },
            )

        df_aggregated = pd.DataFrame(aggregated)
        logger.info(f"Aggregated venue data for {len(aggregated)} venues")
        return df_aggregated

    @staticmethod
    def aggregate_group_preferences(preference_records: List[Dict]) -> pd.DataFrame:
        aggregated = []

        for record in preference_records:
            group_id = record.get("group_id")
            members = record.get("members", [])
            preferences = record.get("preferences", {})

            aggregated.append(
                {
                    "group_id": group_id,
                    "num_members": len(members),
                    "avg_price_level": preferences.get("avg_price_level", 0),
                    "preferred_categories": json.dumps(
                        preferences.get("categories", []),
                    ),
                    "dietary_restrictions": json.dumps(preferences.get("dietary", [])),
                    "max_travel_distance_km": preferences.get("max_distance_km", 0),
                    "last_updated": datetime.now().isoformat(),
                },
            )

        df_aggregated = pd.DataFrame(aggregated)
        logger.info(f"Aggregated preferences for {len(aggregated)} groups")
        return df_aggregated


class FeatureEngineer:
    """Feature derivations used by downstream analytics and quality checks."""

    @staticmethod
    def create_venue_features(df: pd.DataFrame) -> pd.DataFrame:
        df_features = df.copy()

        df_features["rating_bucket"] = pd.cut(
            df_features["rating"],
            bins=[0, 2, 3, 4, 5],
            labels=["low", "medium", "high", "excellent"],
        )

        df_features["price_quality_ratio"] = df_features["price_level"] / (
            df_features["rating"] + 1
        )

        max_reviews = df_features["review_count"].max()
        if max_reviews and max_reviews > 0:
            df_features["popularity_score"] = df_features["review_count"] / max_reviews
        else:
            df_features["popularity_score"] = 0.0

        df_features["quality_score"] = (
            0.6 * (df_features["rating"] / 5)
            + 0.3 * df_features["popularity_score"]
            + 0.1 * (5 - df_features["price_level"]) / 4
        )

        logger.info("Created venue features")
        return df_features

    @staticmethod
    def create_availability_features(df: pd.DataFrame) -> pd.DataFrame:
        df_features = df.copy()

        df_features["availability_category"] = pd.cut(
            df_features["availability_percentage"],
            bins=[0, 25, 50, 75, 100],
            labels=["low", "medium", "high", "very_high"],
        )

        df_features["busy_intensity"] = df_features["num_busy_intervals"] / (
            df_features["total_busy_hours"] + 1
        )

        df_features["availability_score"] = 1 - (
            df_features["availability_percentage"] / 100
        )

        logger.info("Created availability features")
        return df_features
