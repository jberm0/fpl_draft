import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_parquet, write_parquet
from src.utils.env import load_env

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])

pl.Config(set_tbl_cols=25)


def trusted_scores():
    scores = read_parquet(f"{raw_path}/live/scores/*.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64)
    )
    teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int64), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select(
        "web_name", "id", "team"
    )

    actions_and_points = (
        scores.join(players, left_on="player_id", right_on="id")
        .join(teams, left_on="team", right_on="id")
        .select(
            "event",
            "match_number",
            "player_id",
            "web_name",
            "team",
            "short_name",
            "stat",
            "value",
            "points",
        )
    )

    player_actions = actions_and_points.pivot(
        columns="stat",
        index=["event", "match_number", "player_id", "web_name", "team", "short_name"],
        values=["value"],
    )

    player_points = actions_and_points.pivot(
        columns="stat",
        index=["event", "match_number", "player_id", "web_name", "team", "short_name"],
        values=["points"],
    )

    return player_actions, player_points


if __name__ == "__main__":
    player_actions, player_points = trusted_scores()
    write_parquet(player_actions, trusted_path + "player_actions.parquet")
    write_parquet(player_points, trusted_path + "player_points.parquet")
