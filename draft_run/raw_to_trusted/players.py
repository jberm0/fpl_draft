import polars as pl
import sys

sys.path.append("./././")

from src.utils.env import load_env
from src.utils.input_output import read_parquet, write_parquet

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])

pl.Config(set_tbl_cols=30, set_tbl_width_chars=180)


def trusted_players():
    return read_parquet(f"{raw_path}/elements.parquet").select(
        "id",
        "web_name",
        "added",  # added to the game
        "draft_rank",
        "code",
        "status",
        "team",
        "element_type",  # position key
        "total_points",
        "event_points",  # points in current event
        "points_per_game",
        "starts",
        "minutes",
        "chance_of_playing_this_round",  # in 25% chunks
        "chance_of_playing_next_round",  # in 25% chunks
        "goals_scored",
        "assists",
        "goals_conceded",
        "clean_sheets",
        "yellow_cards",
        "red_cards",
        "own_goals",
        "penalties_missed",
        "penalties_saved",
        "saves",
        "bonus",
        "bps",
        "form",
        "form_rank",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "threat",  # impact on goals
        "threat_rank",
        "threat_rank_type",
        "creativity",  # impact on assists
        "creativity_rank",
        "creativity_rank_type",
        "influence",  # influence on outcome of match
        "influence_rank",
        "influence_rank_type",
        "ict_index",  # combination of threat, influence, creativity
        "ict_index_rank",
        "ict_index_rank_type",
        "dreamteam_count",
        "in_dreamteam",
        "news",
    )


if __name__ == "__main__":
    trusted_players_df = trusted_players()
    write_parquet(trusted_players_df, trusted_path + "players.parquet")
