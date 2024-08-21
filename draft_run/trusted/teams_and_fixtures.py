import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_parquet, write_parquet

raw_path = "data/raw"
trusted_path = "data/trusted"

fixtures_1 = read_parquet(f"{raw_path}/fixtures_1.parquet").select(
    pl.lit("+1").alias("fixture"), "team_h", "team_a"
)
fixtures_2 = read_parquet(f"{raw_path}/fixtures_2.parquet").select(
    pl.lit("+2").alias("fixture"), "team_h", "team_a"
)
fixtures_3 = read_parquet(f"{raw_path}/fixtures_3.parquet").select(
    pl.lit("+3").alias("fixture"), "team_h", "team_a"
)
teams = read_parquet(f"{raw_path}/teams.parquet")

all_fixtures = pl.concat([fixtures_1, fixtures_2, fixtures_3], how="vertical")

teams_and_fixtures = all_fixtures.join(
    teams.select("id", pl.col("name").alias("team_h_name")),
    left_on="team_h",
    right_on="id",
).join(
    teams.select("id", pl.col("name").alias("team_a_name")),
    left_on="team_a",
    right_on="id",
)

write_parquet(teams_and_fixtures, trusted_path + "/upcoming_fixtures.parquet")
