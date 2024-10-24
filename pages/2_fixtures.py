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

players = players.select("web_name", pl.col("team").alias("team_id"))

st.write(fixtures)

st.write(selections)

st.write(players)

selected_players = selections.join(
    players, how="inner", left_on="player", right_on="web_name"
)

team_to_id = (
    selected_players.select("team")
    .unique()
    .with_columns(team_id=pl.col("team").rank("ordinal").cast(pl.Int64))
)

st.write(selected_players)

st.write(team_to_id)

joined = selected_players.join(
    fixtures, how="right", left_on=["event", "team_id"], right_on=["event", "team_h"]
)

# .join(team_to_id, how="left", left_on="team_a", right_on="team_id").select(
#     "event",
#     "owner",
#     "player",
#     "team",
#     "difficulty",

# )

st.write(joined)

# TODO:
# upcoming fixture rating
# filter by selections
