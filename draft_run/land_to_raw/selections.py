import sys

sys.path.append("././")

from src.utils.input_output import write_parquet
from src.utils.env import load_env
from src.ingestion.landing import get_gameweeks, get_team_ids
from src.ingestion.raw import get_gw_team_selection

landing_path, raw_path = load_env(["landing_path", "raw_path"])

all_gameweeks = get_gameweeks(landing_path)
all_team_ids = get_team_ids(raw_path)


if __name__ == "__main__":
    for gw_id in all_gameweeks:
        selections = get_gw_team_selection(gw_id, all_team_ids)
        write_parquet(selections, raw_path + f"live/selections/{gw_id}.parquet")
