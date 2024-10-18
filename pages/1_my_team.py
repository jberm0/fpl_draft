import streamlit as st
import polars as pl
from src.utils.env import load_env

trusted_path = load_env(["trusted_path"])[0]

selections = pl.read_parquet(f"{trusted_path}selections.parquet")
player_points = pl.read_parquet(f"{trusted_path}player_points.parquet")
players = pl.read_parquet(f"{trusted_path}players.parquet")
head_to_head = pl.read_parquet(f"{trusted_path}head_to_head.parquet")
match_stats = pl.read_parquet(f"{trusted_path}match_stats.parquet")
stats = pl.read_parquet(f"{trusted_path}stats.parquet")


my_selections = selections.filter(pl.col("owner") == "JB").select(
    "event", "team", "player"
)

player_id_position = players.select(
    "web_name",
    "id",
    pl.col("element_type")
    .replace_strict([1, 2, 3, 4], ["GK", "DEF", "MID", "FWD"])
    .alias("position"),
)

st.write("# My Team")

st.markdown("## Selections")


my_team = (
    my_selections.join(
        player_points,
        left_on=["event", "player", "team"],
        right_on=["event", "web_name", "short_name"],
        how="left",
    )
    .join(
        player_id_position,
        left_on=["player", "player_id"],
        right_on=["web_name", "id"],
        how="left",
    )
    .select(
        "event",
        "player_id",
        "player",
        "position",
        "minutes",
        "goals_scored",
        "assists",
        "clean_sheets",
        (pl.col("goals_scored") + pl.col("assists")).alias("goal_contributions"),
        (pl.col("yellow_cards") + pl.col("red_cards")).alias("cards"),
        "bonus",
        "goals_conceded",
        "yellow_cards",
        "red_cards",
        "own_goals",
        "penalties_missed",
    )
)

my_team = my_team.select(
    my_team.select(pl.exclude("event", "player", "team", "position", "player_id"))
    .sum_horizontal()
    .alias("total"),
    *my_team.columns,
)

by_position = my_team.group_by("event", "position").agg(pl.sum("total"))
st.line_chart(data=by_position, x="event", y="total", color="position")

st.write("#### Drill down into position")

position_filter = st.selectbox("Select Position", ["GK", "DEF", "MID", "FWD"])

my_team = my_team.filter(pl.col("position") == position_filter)
st.line_chart(
    data=my_team.select("position", "event", "total", "player"),
    x="event",
    y="total",
    color="player",
)


st.write("## Record")

my_points_diff = (
    head_to_head.filter(pl.col("finished"))
    .filter((pl.col("owner_1") == "JB") | (pl.col("owner_2") == "JB"))
    .select(
        "event",
        pl.when(pl.col("owner_1") == "JB")
        .then(pl.col("league_entry_1_points"))
        .otherwise(pl.col("league_entry_2_points"))
        .alias("my_points"),
        pl.when(pl.col("owner_1") == "JB")
        .then(pl.col("league_entry_2_points"))
        .otherwise(pl.col("league_entry_1_points"))
        .alias("opp_points"),
    )
    .with_columns(
        (pl.col("my_points") - pl.col("opp_points")).alias("diff"),
    )
    .with_columns((pl.when(pl.col("diff") >= 0).then(1).otherwise(0).alias("win")))
)

st.bar_chart(data=my_points_diff, x="event", y="diff", stack=True, color="win")

st.write("## Home and Away")

home_away = (
    match_stats.select("event", "h_or_a", "team_name", "name", "opp_name")
    .join(
        selections,
        right_on=["event", "team", "player"],
        left_on=["event", "team_name", "name"],
        how="inner",
    )
    .join(
        player_points,
        left_on=["event", "name", "team_name"],
        right_on=["event", "web_name", "short_name"],
        how="left",
    )
    .select(
        "event",
        "h_or_a",
        "team_name",
        "opp_name",
        "name",
        "minutes",
        "yellow_cards",
        "clean_sheets",
        "goals_scored",
        "assists",
        "bonus",
        "saves",
        "own_goals",
        "goals_conceded",
        "red_cards",
        "penalties_missed",
        "penalties_saved",
    )
)

home_away = home_away.select(
    "event",
    "h_or_a",
    "name",
    "team_name",
    "opp_name",
    home_away.select(pl.exclude("event", "h_or_a", "name", "team_name", "opp_name"))
    .sum_horizontal()
    .alias("total"),
)

st.bar_chart(
    home_away.group_by("event", "h_or_a").agg(pl.sum("total").alias("total_points")),
    x="event",
    y="total_points",
    color="h_or_a",
    stack=False,
)

st.write("## Form Guide")

st.write(stats)

team_form = my_selections.join(
    match_stats,
    left_on=["event", "player", "team"],
    right_on=["event", "name", "team_name"],
    how="left",
).join(
    stats,
    left_on=["event", "team", "element"],
    right_on=["event", "web_name", "player_id"],
    how="left",
)

st.write(team_form)


# TODO:
# form guide - who to get rid of
# next fixtures difficulty
