import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict
from src.utils.input_output import write_parquet
from src.utils.env import load_env

landing_path, raw_path = load_env(["landing_path", "raw_path"])


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
    write_parquet(gameweek, raw_path + "gameweek.parquet")
