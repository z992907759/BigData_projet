import os

from src.data import load_parquets_from_minio
from src.features import build_dataset
from src.train import main as train_main


def test_env_vars_present():
    required = [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_BUCKET_CLEAN",
        "YEAR",
        "MONTH",
    ]
    missing = [k for k in required if k not in os.environ]
    assert not missing, f"Missing env vars: {missing}"


def test_load_and_build_dataset():
    bucket = os.environ["MINIO_BUCKET_CLEAN"]
    prefix = f"year={os.environ['YEAR']}/month={os.environ['MONTH']}/"
    df = load_parquets_from_minio(bucket=bucket, prefix=prefix)
    X, y, spec = build_dataset(df)

    assert len(X) > 0
    assert len(y) == len(X)
    assert len(spec["feature_cols"]) > 0


def test_train_runs():
    # Just checks the training script executes (and writes artifacts).
    train_main()
