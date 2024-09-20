import sys

sys.path.append("./././")

from src.utils.env import load_env
from src.utils.input_output import write_parquet
from src.ingestion.trusted import trusted_scoring

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])

if __name__ == "__main__":
    position_scoring = trusted_scoring(raw_path)
    write_parquet(position_scoring, trusted_path + "scoring.parquet")
