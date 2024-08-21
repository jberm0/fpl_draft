import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict
from src.utils.input_output import write_parquet
from src.utils.env import load_env

landing_path, raw_path = load_env(["landing_path", "raw_path"])


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
    write_parquet(league_entries, raw_path + "league_entries.parquet")
    write_parquet(matches, raw_path + "h2h_fixtures.parquet")
    write_parquet(standings, raw_path + "h2h_standings.parquet")
