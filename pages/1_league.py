import streamlit as st
import polars as pl
from src.utils.env import load_env

trusted_path = load_env(["trusted_path"])[0]

st.markdown("## League Standings")
standings = pl.read_parquet(f"{trusted_path}standings.parquet").select(
    "rank",
    "team_name",
    (pl.col("matches_won") + pl.col("matches_drawn") + pl.col("matches_lost")).alias(
        "P"
    ),
    "total_points",
    pl.col("matches_won").alias("W"),
    pl.col("matches_drawn").alias("D"),
    pl.col("matches_lost").alias("L"),
    "points_for",
    "points_against",
)
st.write(standings)

last_gw = pl.Series(standings.select(pl.first("P"))).to_list()[0]

st.markdown("## Fixtures and Results")
st.markdown("### Filters")
col1, col2 = st.columns(2)
gw_filter = col1.slider(
    "Gameweek", min_value=1, max_value=38, value=(last_gw, last_gw + 1), step=1
)
team_filter = col2.text_input("Search Team", value="")
fixtures = (
    pl.read_parquet(f"{trusted_path}head_to_head.parquet")
    .select(
        pl.col("event").alias("gameweek"),
        pl.col("team_name_1").alias("team_1"),
        pl.col("league_entry_1_points").alias("team_1_points"),
        pl.col("team_name_2").alias("team_2"),
        pl.col("league_entry_2_points").alias("team_2_points"),
    )
    .filter(pl.col("gameweek").is_between(gw_filter[0], gw_filter[1]))
    .filter(
        pl.col("team_1").str.contains(f"(?i){team_filter}")
        | pl.col("team_2").str.contains(f"(?i){team_filter}")
    )
)
st.write(fixtures)
