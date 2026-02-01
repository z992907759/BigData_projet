# src/train.py
import json
import os
from pathlib import Path

import joblib
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

from src.data import load_parquets_from_minio
from src.features import build_dataset


ARTIFACTS_DIR = Path("artifacts")
MODEL_PATH = ARTIFACTS_DIR / "model.joblib"
SPEC_PATH = ARTIFACTS_DIR / "feature_spec.json"


def main() -> None:
    """
    Train a regression model to predict total_amount from cleaned NYC taxi data.

    Data source:
    - Minio bucket: MINIO_BUCKET_CLEAN
    - Prefix: year=YEAR/month=MONTH/

    Outputs:
    - artifacts/model.joblib
    - artifacts/feature_spec.json
    """
    bucket = os.environ["MINIO_BUCKET_CLEAN"]
    year = os.environ["YEAR"]
    month = os.environ["MONTH"]
    prefix = f"year={year}/month={month}/"

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_parquets_from_minio(bucket=bucket, prefix=prefix)
    X, y, spec = build_dataset(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    numeric_cols = spec["numeric_cols"]
    categorical_cols = spec["categorical_cols"]

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ],
        remainder="drop",
    )

    model = HistGradientBoostingRegressor(
        max_depth=8,
        learning_rate=0.08,
        max_iter=300,
        random_state=42,
    )

    pipe = Pipeline(steps=[("preprocess", preprocessor), ("model", model)])
    pipe.fit(X_train, y_train)

    preds = pipe.predict(X_test)
    rmse = mean_squared_error(y_test, preds, squared=False)

    print(f"[EX5] Train rows: {len(X_train)} | Test rows: {len(X_test)}")
    print(f"[EX5] Features: {spec['feature_cols']}")
    print(f"[EX5] RMSE: {rmse:.4f}")

    joblib.dump(pipe, MODEL_PATH)

    payload = {
        "bucket": bucket,
        "prefix": prefix,
        "rmse": float(rmse),
        "feature_cols": spec["feature_cols"],
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "target": "total_amount",
    }
    SPEC_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"[EX5] Saved model -> {MODEL_PATH}")
    print(f"[EX5] Saved spec  -> {SPEC_PATH}")


if __name__ == "__main__":
    required = [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_BUCKET_CLEAN",
        "YEAR",
        "MONTH",
    ]
    missing = [k for k in required if k not in os.environ]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")
    main()
