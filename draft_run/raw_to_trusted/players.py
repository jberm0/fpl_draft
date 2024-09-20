import sys

sys.path.append("./././")

from src.utils.env import load_env
from src.utils.input_output import write_parquet
from src.ingestion.trusted import trusted_players

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


if __name__ == "__main__":
    trusted_players_df = trusted_players(raw_path)
    write_parquet(trusted_players_df, trusted_path + "players.parquet")
