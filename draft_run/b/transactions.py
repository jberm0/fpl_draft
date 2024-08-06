from dotenv.main import load_dotenv, find_dotenv
from os import getenv
import polars as pl
import sys

sys.path.append("./")

from src.process.schema_independent import json_to_dict

load_dotenv(find_dotenv())

landing_path = getenv("landing_path")


def process_transactions(base_path):
    extension = "league_transactions.json"
    dict = json_to_dict(base_path + extension)
    df = pl.from_dict(dict)
    # .cast({"added": pl.Datetime})
    return df


if __name__ == "__main__":
    transactions = process_transactions(landing_path)
