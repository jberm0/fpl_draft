import polars as pl
import sys
from typing import List

sys.path.append("././")

from src.ingestion.download_raw import get_gameweeks, get_team_ids
from src.utils.input_output import write_parquet, read_json
from src.utils.env import load_env

landing_path, raw_path = load_env(["landing_path", "raw_path"])

all_gameweeks = get_gameweeks(raw_path)
all_team_ids = get_team_ids(raw_path)


def get_gw_team_selection(gw_id: int, team_ids: List[int]):
    gw_selections = []
    for team_id in team_ids:
        dict = read_json(landing_path + f"gw_{gw_id}/{team_id}_selection.json").get(
            "picks"
        )
        selection = pl.DataFrame(dict).select(
            pl.lit(gw_id).alias("event"), pl.lit(team_id).alias("owner"), "element"
        )
        gw_selections.append(selection)
    return pl.concat(gw_selections, how="vertical")


if __name__ == "__main__":
    for gw_id in all_gameweeks:
        selections = get_gw_team_selection(gw_id, all_team_ids)
        write_parquet(selections, raw_path + f"/live/selections/{gw_id}.parquet")
