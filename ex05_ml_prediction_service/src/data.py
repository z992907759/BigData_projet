import os
from typing import List

import pandas as pd
import s3fs


def _s3fs() -> s3fs.S3FileSystem:
    """Create an S3FileSystem client for Minio."""
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    return s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint},
    )


def list_parquet_keys(bucket: str, prefix: str) -> List[str]:
    """
    List parquet objects under a given prefix in Minio.

    Parameters
    ----------
    bucket : str
        Minio bucket name (e.g., 'nyc-cleaned').
    prefix : str
        Prefix inside the bucket (e.g., 'year=2025/month=01/').

    Returns
    -------
    List[str]
        List of object keys (bucket/key style) for parquet files.
    """
    fs = _s3fs()
    # s3fs expects bucket/prefix
    path = f"{bucket}/{prefix}"
    keys = fs.find(path)

    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise FileNotFoundError(f"No parquet files found under: s3a://{bucket}/{prefix}")

    return parquet_keys


def load_parquets_from_minio(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Load all parquet files found under (bucket/prefix) into a single DataFrame.

    Parameters
    ----------
    bucket : str
        Minio bucket name.
    prefix : str
        Prefix for the month partition.

    Returns
    -------
    pd.DataFrame
        Concatenated dataframe.
    """
    fs = _s3fs()
    keys = list_parquet_keys(bucket=bucket, prefix=prefix)

    dfs = []
    for k in keys:
        with fs.open(k, "rb") as f:
            dfs.append(pd.read_parquet(f))

    return pd.concat(dfs, ignore_index=True)
