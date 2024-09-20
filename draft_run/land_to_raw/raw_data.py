import sys

sys.path.append("././")

from src.utils.env import load_env
from src.ingestion.raw import land_to_raw

landing_path, raw_path = load_env(["landing_path", "raw_path"])

if __name__ == "__main__":
    land_to_raw(raw_path, landing_path)
