import sys

sys.path.append("./././")

from src.utils.input_output import write_parquet
from src.utils.env import load_env
from src.ingestion.trusted import trusted_match_stats

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


if __name__ == "__main__":
    matches_trusted = trusted_match_stats(raw_path, trusted_path)
    write_parquet(matches_trusted, trusted_path + "match_stats.parquet")
