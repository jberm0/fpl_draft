import polars as pl
from typing import Dict
import json
import os


def read_parquet(path: str, **kwargs) -> pl.DataFrame:
    return pl.read_parquet(path, **kwargs)


def write_parquet(df: pl.DataFrame, path: str, **kwargs):
    df.write_parquet(path, **kwargs)
    print(f"written to {path}")


def write_json(json_object: Dict, path: str):
    # creates new directory if doesn't exist yet
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(json_object, f)
    print(f"written to {path}")


def read_json(path):
    f = open(path)
    data = json.load(f)
    return data
