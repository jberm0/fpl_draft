import sys

sys.path.append("./././")

from src.utils.env import load_env
from src.utils.input_output import read_parquet, write_parquet
from src.ingestion.trusted import get_head_to_head, get_standings

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


if __name__ == "__main__":
    h2h_fixtures = read_parquet(f"{raw_path}/h2h_fixtures.parquet")
    h2h_standings = read_parquet(f"{raw_path}/h2h_standings.parquet")
    entries = read_parquet(f"{raw_path}/league_entries.parquet")

    head_to_head = get_head_to_head(h2h_fixtures, entries)
    standings = get_standings(h2h_standings, entries)

    write_parquet(head_to_head, trusted_path + "head_to_head.parquet")
    write_parquet(standings, trusted_path + "standings.parquet")
