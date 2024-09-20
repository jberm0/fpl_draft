import sys

sys.path.append("./././")

from src.utils.input_output import write_parquet
from src.utils.env import load_env
from src.ingestion.trusted import trusted_scores

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


if __name__ == "__main__":
    player_actions, player_points = trusted_scores(raw_path, trusted_path)
    write_parquet(player_actions, trusted_path + "player_actions.parquet")
    write_parquet(player_points, trusted_path + "player_points.parquet")
