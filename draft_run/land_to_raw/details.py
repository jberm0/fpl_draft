import sys

sys.path.append("././")

from src.utils.input_output import write_parquet
from src.utils.env import load_env
from src.ingestion.raw import process_details

landing_path, raw_path = load_env(["landing_path", "raw_path"])

if __name__ == "__main__":
    league_name, league_entries, matches, standings = process_details(landing_path)
    write_parquet(league_entries, raw_path + "league_entries.parquet")
    write_parquet(matches, raw_path + "h2h_fixtures.parquet")
    write_parquet(standings, raw_path + "h2h_standings.parquet")
