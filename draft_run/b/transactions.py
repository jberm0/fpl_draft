import polars as pl
import sys

sys.path.append("./")

from src.process.schema_independent import json_to_dict
from src.utils.env import load_env

landing_path = load_env(["landing_path"])[0]


def process_transactions(base_path):
    extension = "league_transactions.json"
    dict = json_to_dict(base_path + extension)
    df = pl.from_dict(dict)
    # .cast({"added": pl.Datetime})
    return df


if __name__ == "__main__":
    transactions = process_transactions(landing_path)
