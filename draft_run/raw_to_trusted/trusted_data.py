import sys

sys.path.append("./././")

from src.utils.env import load_env
from src.ingestion.trusted import raw_to_trusted

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


if __name__ == "__main__":
    raw_to_trusted(raw_path, trusted_path)
