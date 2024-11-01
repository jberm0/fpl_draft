import streamlit as st
import polars as pl
from src.utils.env import load_env

trusted_path = load_env(["trusted_path"])[0]

fixtures = pl.read_parquet(f"{trusted_path}fixtures.parquet")
selections = pl.read_parquet(f"{trusted_path}selections.parquet")
player_points = pl.read_parquet(f"{trusted_path}player_points.parquet")
players = pl.read_parquet(f"{trusted_path}players.parquet")
standings = pl.read_parquet(f"{trusted_path}standings.parquet").select(
    (pl.col("matches_won") + pl.col("matches_drawn") + pl.col("matches_lost")).alias(
        "P"
    )
)
last_gw = pl.Series(standings.select(pl.first("P"))).to_list()[0]
team_to_id = (
    selections.select(pl.col("team").alias("team_name"))
    .unique()
    .with_columns(team_id=pl.col("team_name").rank("ordinal").cast(pl.Int64))
)

player_id_position = players.select(
    "web_name",
    "id",
    pl.col("element_type")
    .replace_strict([1, 2, 3, 4], ["GK", "DEF", "MID", "FWD"])
    .alias("position"),
)


st.write("# Waivers")

current_selections = selections.filter(pl.col("event") == last_gw)

available_players = player_points.join(
    selections,
    how="anti",
    left_on=["web_name", "short_name"],
    right_on=["player", "team"],
)

available_player_ids = pl.Series(available_players.select("player_id")).to_list()

metrics = [
    "total_points",
    "points_per_game",
    "starts",
    "minutes",
    "goals_scored",
    "assists",
    "goals_conceded",
    "clean_sheets",
    "yellow_cards",
    "red_cards",
    "own_goals",
    "penalties_missed",
    "penalties_saved",
    "saves",
    "bonus",
    "bps",
    "form",
    "form_rank",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "threat",
    "threat_rank",
    "threat_rank_type",
    "creativity",
    "creativity_rank",
    "creativity_rank_type",
    "influence",
    "influence_rank",
    "influence_rank_type",
    "ict_index",
    "ict_index_rank",
    "ict_index_rank_type",
]

waivers = (
    players.join(
        player_id_position,
        how="inner",
        left_on=["id", "web_name"],
        right_on=["id", "web_name"],
    )
    .join(team_to_id, how="inner", left_on="team", right_on="team_id")
    .filter(pl.col("id").is_in(available_player_ids))
    .filter(pl.col("status") == "a")
    .select(
        "id",
        "web_name",
        "position",
        "team_name",
        "total_points",
        "points_per_game",
        "starts",
        "minutes",
        "goals_scored",
        "assists",
        "goals_conceded",
        "clean_sheets",
        "yellow_cards",
        "red_cards",
        "own_goals",
        "penalties_missed",
        "penalties_saved",
        "saves",
        "bonus",
        "bps",
        "form",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "threat",
        "threat_rank",
        "threat_rank_type",
        "creativity",
        "creativity_rank",
        "creativity_rank_type",
        "influence",
        "influence_rank",
        "influence_rank_type",
        "ict_index",
        "ict_index_rank",
        "ict_index_rank_type",
    )
)

with st.sidebar:
    position_filter = st.selectbox("Position:", ["All", "GK", "DEF", "MID", "FWD"])
    team_filter = st.multiselect(
        "Team:",
        [
            "NFO",
            "NEW",
            "MCI",
            "ARS",
            "IPS",
            "WHU",
            "SOU",
            "LIV",
            "EVE",
            "BHA",
            "WOL",
            "BRE",
            "LEI",
            "BOU",
            "AVL",
            "TOT",
            "CHE",
            "FUL",
            "CRY",
            "MUN",
        ],
    )
    metric_columns = st.multiselect("Metrics:", metrics)


if position_filter != "All":
    waivers = waivers.filter(pl.col("position") == position_filter)


if team_filter:
    waivers = waivers.filter(pl.col("team_name").is_in(team_filter))

col1, col2 = st.columns(2)
sort_by = col1.multiselect("Sort by:", metric_columns)
desc = col2.radio("Descending:", [True, False])
limit = col1.number_input("Top N", min_value=0, step=1)

top_waivers = waivers.select(
    "web_name", "position", "team_name", "total_points", *metric_columns
)

if sort_by and limit:
    top_waivers = top_waivers.sort(sort_by, descending=desc).limit(limit)

st.write(top_waivers)
