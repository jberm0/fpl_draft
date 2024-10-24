import streamlit as st
import polars as pl
from src.utils.env import load_env

trusted_path = load_env(["trusted_path"])[0]

fixtures = pl.read_parquet(f"{trusted_path}fixtures.parquet")
selections = pl.read_parquet(f"{trusted_path}selections.parquet")
player_points = pl.read_parquet(f"{trusted_path}player_points.parquet")
players = pl.read_parquet(f"{trusted_path}players.parquet")
head_to_head = pl.read_parquet(f"{trusted_path}head_to_head.parquet")
match_stats = pl.read_parquet(f"{trusted_path}match_stats.parquet")
upcoming_fixtures = pl.read_parquet(f"{trusted_path}upcoming_fixtures.parquet")


fixtures = fixtures.select(
    pl.col("event").cast(pl.Int32),
    "team_h",
    "team_h_difficulty",
    "team_a",
    "team_a_difficulty",
)

selections = selections.select(
    pl.col("event").cast(pl.Int32), "entry_name", "owner", "player", "team"
)

team_to_id = (
    selections.select("team")
    .unique()
    .with_columns(team_id=pl.col("team").rank("ordinal").cast(pl.Int64))
)

players = players.select("web_name", pl.col("team").alias("team_id"))

players_with_team = players.join(
    team_to_id, how="inner", left_on="team_id", right_on="team_id"
)

st.dataframe(players_with_team)

selected_players = selections.join(
    players_with_team,
    how="inner",
    left_on=["team", "player"],
    right_on=["team", "web_name"],
)

st.write(selected_players)

st.write(fixtures)

# upcoming fixture rating - which team has the easiest/hardest next run of fixtures

h_fixtures = fixtures.select(
    "event",
    pl.col("team_h").alias("team_id"),
    pl.col("team_a").alias("opp"),
    pl.col("team_h_difficulty").alias("fdr"),
)
a_fixtures = fixtures.select(
    "event",
    pl.col("team_a").alias("team_id"),
    pl.col("team_h").alias("opp"),
    pl.col("team_a_difficulty").alias("fdr"),
)
all_fixtures = (
    pl.concat([h_fixtures, a_fixtures])
    .join(team_to_id, how="inner", left_on="team_id", right_on="team_id")
    .join(team_to_id, how="inner", left_on="opp", right_on="team_id")
).select("event", "team", pl.col("team_right").alias("opp"), "fdr")

st.write(all_fixtures)


# who's teams have the easiest and hardest run of fixtures
