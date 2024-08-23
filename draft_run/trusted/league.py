import polars as pl
import sys

sys.path.append("./././")

from src.utils.env import load_env
from src.utils.input_output import read_parquet, write_parquet

raw_path, trusted_path = load_env(["raw_path", "trusted_path"])


def get_standings(h2h_standings, entries):
    standings = (
        h2h_standings.join(
            entries.select("id", "entry_name", "short_name"),
            left_on="league_entry",
            right_on="id",
            how="inner",
        )
        .select(
            "rank",
            "last_rank",
            pl.col("entry_name").alias("team_name"),
            pl.col("short_name").alias("owner"),
            "matches_won",
            "matches_drawn",
            "matches_lost",
            "points_against",
            "points_for",
            pl.col("total").alias("total_points"),
        )
        .sort("rank", descending=False)
    )

    return standings


def get_head_to_head(h2h_fixtures, entries):
    head_to_head = (
        h2h_fixtures.join(
            entries.select(
                "id",
                pl.col("entry_name").alias("team_name_1"),
                pl.col("short_name").alias("owner_1"),
            ),
            left_on="league_entry_1",
            right_on="id",
            how="inner",
        )
        .join(
            entries.select(
                "id",
                pl.col("entry_name").alias("team_name_2"),
                pl.col("short_name").alias("owner_2"),
            ),
            left_on="league_entry_2",
            right_on="id",
            how="inner",
        )
        .select(
            "event",
            "started",
            "finished",
            "league_entry_1",
            "team_name_1",
            "owner_1",
            "league_entry_1_points",
            "league_entry_2",
            "team_name_2",
            "owner_2",
            "league_entry_2_points",
        )
    )

    return head_to_head


if __name__ == "__main__":
    h2h_fixtures = read_parquet(f"{raw_path}/h2h_fixtures.parquet")
    h2h_standings = read_parquet(f"{raw_path}/h2h_standings.parquet")
    entries = read_parquet(f"{raw_path}/league_entries.parquet")

    head_to_head = get_head_to_head(h2h_fixtures, entries)
    standings = get_standings(h2h_standings, entries)

    write_parquet(head_to_head, trusted_path + "head_to_head.parquet")
    write_parquet(standings, trusted_path + "standings.parquet")
