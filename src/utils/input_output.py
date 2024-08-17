import polars as pl
from typing import Dict
import json


def read_parquet(path: str, **kwargs) -> pl.DataFrame:
    return pl.read_parquet(path, **kwargs)


def write_parquet(df: pl.DataFrame, path: str, **kwargs):
    df.write_parquet(path, **kwargs)
    print(f"written to {path}")


def write_json(json_object: Dict, path: str):
    with open(path, "w") as f:
        json.dump(json_object, f)
    print(f"written to {path}")
