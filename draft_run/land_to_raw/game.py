import sys

sys.path.append("././")

from src.utils.input_output import write_parquet
from src.utils.env import load_env
from src.ingestion.raw import process_gameweek

landing_path, raw_path = load_env(["landing_path", "raw_path"])

if __name__ == "__main__":
    gameweek = process_gameweek(landing_path)
    write_parquet(gameweek, raw_path + "gameweek.parquet")
