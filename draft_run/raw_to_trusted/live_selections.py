import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_parquet, write_parquet
from src.utils.env import load_env

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])

pl.Config(set_tbl_cols=25)


def trusted_selections():
    selections = read_parquet(f"{raw_path}/live/selections/*.parquet")
    h2h_teams = read_parquet(f"{raw_path}/league_entries.parquet").select(
        pl.col("entry_id").cast(pl.Int32), "entry_name", "short_name"
    )
    pl_teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int64), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select(
        "web_name", "id", "team"
    )

    return (
        selections.join(h2h_teams, how="left", left_on="owner", right_on="entry_id")
        .join(players, how="left", left_on="element", right_on="id")
        .join(pl_teams, how="left", left_on="team", right_on="id")
        .select(
            "event",
            "entry_name",
            pl.col("short_name").alias("owner"),
            pl.col("web_name").alias("player"),
            pl.col("short_name_right").alias("team"),
        )
    )


if __name__ == "__main__":
    trusted_selections = trusted_selections()
    write_parquet(trusted_selections, trusted_path + "selections.parquet")
