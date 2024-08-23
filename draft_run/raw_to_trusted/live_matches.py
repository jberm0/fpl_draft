import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_parquet, write_parquet
from src.utils.env import load_env

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


def trusted_match_stats():
    matches = read_parquet(f"{raw_path}/live/matches/*.parquet")
    teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int32), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select("web_name", "id")

    return (
        matches.join(players, how="left", left_on="element", right_on="id")
        .join(teams, how="left", left_on="team_id", right_on="id")
        .rename({"short_name": "team_name"})
        .join(teams, how="left", left_on="opp_id", right_on="id")
        .rename({"short_name": "opp_name"})
        .select(
            "event",
            "h_or_a",
            "team_name",
            "opp_name",
            pl.col("scored").alias("team_scored"),
            pl.col("conceded").alias("team_conceded"),
            pl.col("web_name").alias("name"),
            "element",
            "goals_scored",
            "assists",
            "own_goals",
            "penalties_saved",
            "penalties_missed",
            "yellow_cards",
            "red_cards",
            "saves",
            "bonus",
            "bps",
        )
    )


if __name__ == "__main__":
    matches_trusted = trusted_match_stats()
    write_parquet(matches_trusted, trusted_path + "match_stats.parquet")
