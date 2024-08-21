import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict
from src.utils.env import load_env

landing_path = load_env(["landing_path"])[0]


def process_trades(base_path):
    extension = "trades.json"
    dict = json_to_dict(base_path + extension)
    df = pl.from_dict(dict)
    # df = df.cast({"offer_time": pl.Datetime, "response_time": pl.Datetime})
    # df = df.select(
    #     "event",
    #     "id",
    #     "offered_entry",
    #     "offer_time",
    #     "received_entry",
    #     "response_time",
    #     "state",
    #     pl.col("tradeitem_set").list.first(),
    # )
    # df = df.unnest("tradeitem_set")
    return df


if __name__ == "__main__":
    trades = process_trades(landing_path)
    print(trades.head(100))