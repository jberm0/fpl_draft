import streamlit as st
import polars as pl
from src.utils.env import load_env

trusted_path = load_env(["trusted_path"])[0]

st.write("# My Team")

st.markdown("## Selections")
selections = (
    pl.read_parquet(f"{trusted_path}selections.parquet")
    .filter(pl.col("owner") == "JB")
    .select("event", "team", "player")
)

points = pl.read_parquet(f"{trusted_path}player_points.parquet")

my_team = selections.join(
    points, left_on=["event", "player"], right_on=["event", "web_name"], how="left"
).select(
    "event",
    "player",
    "minutes",
    "yellow_cards",
    "clean_sheets",
    "goals_scored",
    "assists",
    "bonus",
    "own_goals",
    "goals_conceded",
    "red_cards",
    "penalties_missed",
)

my_team = my_team.select(
    *my_team.columns,
    (pl.col("goals_scored") + pl.col("assists")).alias("goal_contributions"),
    (pl.col("yellow_cards") + pl.col("red_cards")).alias("cards"),
    my_team.select(pl.exclude("event", "player", "team"))
    .sum_horizontal()
    .alias("cat_total"),
)

st.write(my_team)

st.line_chart(data=my_team, x="event", y="cat_total", color="player")
