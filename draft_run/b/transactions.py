import polars as pl
import sys

sys.path.append("./")

from src.process.schema_independent import json_to_dict
from src.utils.env import load_env
from src.utils.input_output import write_parquet

landing_path, raw_path = load_env(["landing_path", "raw_path"])


def process_transactions(base_path):
    extension = "league_transactions.json"
    dict = json_to_dict(base_path + extension).get("transactions")
    df = pl.DataFrame(dict)
    return df


if __name__ == "__main__":
    transactions = process_transactions(landing_path)
    write_parquet(transactions, raw_path + "transactions.parquet")
