import streamlit as st
import polars as pl

data = pl.read_parquet(
    "/Users/jonah/Documents/projects/fpl_draft/data/trusted/player_points.parquet"
)
st.write(data)


data = pl.read_parquet(
    "/Users/jonah/Documents/projects/fpl_draft/data/trusted/head_to_head.parquet"
)
st.write(data)


data = pl.read_parquet(
    "/Users/jonah/Documents/projects/fpl_draft/data/trusted/match_stats.parquet"
)
st.write(data)


data = pl.read_parquet(
    "/Users/jonah/Documents/projects/fpl_draft/data/trusted/player_actions.parquet"
)
st.write(data)


data = pl.read_parquet(
    "/Users/jonah/Documents/projects/fpl_draft/data/trusted/players.parquet"
)
st.write(data)


data = pl.read_parquet(
    "/Users/jonah/Documents/projects/fpl_draft/data/trusted/scoring.parquet"
)
st.write(data)
