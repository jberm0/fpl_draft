import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_parquet, write_parquet
from src.utils.env import load_env

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])

pl.Config(set_tbl_cols=30)


def trusted_stats():
    stats = read_parquet(f"{raw_path}/live/stats/*.parquet")

    pl_teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int64), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select(
        "web_name", "id", "team"
    )

    return (
        stats.with_columns(pl.col("player_id").cast(pl.Int64))
        .join(players, how="left", left_on="player_id", right_on="id")
        .join(pl_teams, how="left", left_on="team", right_on="id")
        .select(
            "player_id",
            "web_name",
            pl.col("short_name").alias("team"),
            "match_number",
            "event",
            "minutes",
            "goals_scored",
            "assists",
            "clean_sheets",
            "goals_conceded",
            "own_goals",
            "penalties_saved",
            "penalties_missed",
            "yellow_cards",
            "red_cards",
            "saves",
            "bonus",
            "bps",
            "influence",
            "creativity",
            "threat",
            "ict_index",
            "starts",
            "expected_goals",
            "expected_assists",
            "expected_goal_involvements",
            "expected_goals_conceded",
            "total_points",
            "in_dreamteam",
        )
    )


if __name__ == "__main__":
    trusted_stats_df = trusted_stats()
    write_parquet(trusted_stats_df, trusted_path + "stats.parquet")
