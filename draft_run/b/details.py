from dotenv.main import load_dotenv, find_dotenv
from os import getenv
import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict

load_dotenv(find_dotenv())

landing_path = getenv("landing_path")
raw_path = getenv("raw_path")


def process_details(base_path):
    extension = "details.json"
    dict = json_to_dict(base_path + extension)
    league_name = dict.get("league").get("name")
    league_entries = pl.DataFrame(dict.get("league_entries")).select(
        "entry_id", "entry_name", "id", "short_name"
    )
    matches = pl.DataFrame(dict.get("matches")).select(
        "event",
        "finished",
        "league_entry_1",
        "league_entry_1_points",
        "league_entry_2",
        "league_entry_2_points",
        "started",
    )
    standings = pl.DataFrame(dict.get("standings")).select(
        "last_rank",
        "league_entry",
        "matches_drawn",
        "matches_lost",
        "matches_played",
        "matches_won",
        "points_against",
        "points_for",
        "rank",
        "total",
    )

    return league_name, league_entries, matches, standings


if __name__ == "__main__":
    league_name, league_entries, matches, standings = process_details(landing_path)
    print(league_name)
    league_entries.write_parquet(raw_path + "league_entries.parquet")
    matches.write_parquet(raw_path + "h2h_fixtures.parquet")
    standings.write_parquet(raw_path + "h2h_standings.parquet")
