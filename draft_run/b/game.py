from dotenv.main import load_dotenv, find_dotenv
from os import getenv
import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict

load_dotenv(find_dotenv())

landing_path = getenv("landing_path")
raw_path = getenv("raw_path")


def process_gameweek(base_path):
    extension = "game_week.json"
    dict = json_to_dict(base_path + extension)
    df = pl.from_dict(dict)
    df = df.select(
        "current_event",
        "current_event_finished",
        "next_event",
        "trades_time_for_approval",
        "waivers_processed",
        pl.when(pl.col("processing_status") == "y")
        .then(True)
        .otherwise(False)
        .alias("processing_status"),
    )
    return df


if __name__ == "__main__":
    gameweek = process_gameweek(landing_path)
    gameweek.write_parquet(raw_path + "gameweek.parquet")
