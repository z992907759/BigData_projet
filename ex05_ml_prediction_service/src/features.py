from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


TARGET_COL = "total_amount"


def build_dataset(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, Dict[str, List[str]]]:
    """
    Prepare ML dataset (X, y) from cleaned NYC taxi data.

    Expected input columns (from Ex2 cleaned parquet):
    - tpep_pickup_datetime, tpep_dropoff_datetime
    - passenger_count, trip_distance, fare_amount, total_amount
    - VendorID, RatecodeID, payment_type, PULocationID, DOLocationID

    Cleaning (minimal, consistent with Ex2):
    - drop rows with missing required cols
    - enforce positive passenger_count/trip_distance/fare_amount/total_amount
    - enforce date pickup < date dropoff
    - build time features: pickup_hour, pickup_dayofweek, pickup_month

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe from Minio (already cleaned by Spark, but we re-check).

    Returns
    -------
    X : pd.DataFrame
        Feature matrix.
    y : pd.Series
        Target.
    spec : Dict[str, List[str]]
        Lists of numeric and categorical feature names.
    """
    required = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        TARGET_COL,
        "PULocationID",
        "DOLocationID",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    data = df.copy()

    # Parse datetimes robustly
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"], errors="coerce")
    data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"], errors="coerce")

    # Drop invalid core fields
    data = data.dropna(subset=required)

    # Apply consistent filters
    data = data[
        (data["passenger_count"] > 0)
        & (data["trip_distance"] > 0)
        & (data["fare_amount"] > 0)
        & (data[TARGET_COL] > 0)
        & (data["tpep_pickup_datetime"] < data["tpep_dropoff_datetime"])
    ]

    # Optional outlier trimming (keeps RMSE stable)
    data = data[(data["trip_distance"] < 200)]
    data = data[(data[TARGET_COL] < 500)]  # avoid extreme anomalies

    # Time features
    data["pickup_hour"] = data["tpep_pickup_datetime"].dt.hour.astype(np.int8)
    data["pickup_dayofweek"] = data["tpep_pickup_datetime"].dt.dayofweek.astype(np.int8)
    data["pickup_month"] = data["tpep_pickup_datetime"].dt.month.astype(np.int8)

    # Candidate features aligned with DW / Ex2
    numeric_candidates = [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "pickup_hour",
        "pickup_dayofweek",
        "pickup_month",
    ]
    categorical_candidates = [
        "VendorID",
        "RatecodeID",
        "payment_type",
        "PULocationID",
        "DOLocationID",
        "store_and_fwd_flag",
    ]

    numeric_cols = [c for c in numeric_candidates if c in data.columns]
    categorical_cols = [c for c in categorical_candidates if c in data.columns]

    # Fill missing numeric with median; categorical with "UNKNOWN"
    for c in numeric_cols:
        if data[c].isna().any():
            data[c] = data[c].fillna(data[c].median())

    for c in categorical_cols:
        data[c] = data[c].astype(str).fillna("UNKNOWN")

    feature_cols = numeric_cols + categorical_cols
    X = data[feature_cols].copy()
    y = data[TARGET_COL].astype(float).copy()

    spec = {"numeric_cols": numeric_cols, "categorical_cols": categorical_cols, "feature_cols": feature_cols}
    return X, y, spec
