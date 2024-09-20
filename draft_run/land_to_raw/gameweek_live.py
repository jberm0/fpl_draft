import sys

sys.path.append("././")

from src.utils.input_output import write_parquet
from src.utils.env import load_env
from src.ingestion.landing import get_gameweeks
from src.ingestion.raw import (
    process_live_gameweek,
    process_live_matches,
    process_live_elements,
)

landing_path, raw_path = load_env(["landing_path", "raw_path"])

gameweeks = get_gameweeks(landing_path)

if __name__ == "__main__":
    for gw_id in gameweeks:
        gw_dict = process_live_gameweek(landing_path, gw_id)
        scores, stats = process_live_elements(gw_dict, gw_id)
        write_parquet(scores, raw_path + f"live/scores/{gw_id}.parquet")
        write_parquet(stats, raw_path + f"live/stats/{gw_id}.parquet")

        matches = process_live_matches(gw_dict)
        write_parquet(matches, raw_path + f"live/matches/{gw_id}.parquet")
