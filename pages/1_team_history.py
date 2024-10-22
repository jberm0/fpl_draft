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

if "selected_options" not in st.session_state:
    st.session_state["selected_options"] = []

with st.sidebar:
    owner_filter = st.selectbox(
        label="Select Owner", options=["JB", "KO", "VA", "LJ", "GN", "SH", "MM", "FM"]
    )

selections = selections.filter(pl.col("owner") == owner_filter).select(
    "event", "team", "player"
)

player_id_position = players.select(
    "web_name",
    "id",
    pl.col("element_type")
    .replace_strict([1, 2, 3, 4], ["GK", "DEF", "MID", "FWD"])
    .alias("position"),
)

team = (
    selections.join(
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

team = team.select(
    team.select(pl.exclude("event", "player", "team", "position", "player_id"))
    .sum_horizontal()
    .alias("total"),
    *team.columns,
)

by_position = team.group_by("event", "position").agg(pl.sum("total"))

points_diff = (
    head_to_head.filter(pl.col("finished"))
    .filter((pl.col("owner_1") == owner_filter) | (pl.col("owner_2") == owner_filter))
    .select(
        "event",
        pl.when(pl.col("owner_1") == owner_filter)
        .then(pl.col("league_entry_1_points"))
        .otherwise(pl.col("league_entry_2_points"))
        .alias("points"),
        pl.when(pl.col("owner_1") == owner_filter)
        .then(pl.col("league_entry_2_points"))
        .otherwise(pl.col("league_entry_1_points"))
        .alias("opp_points"),
    )
    .with_columns(
        (pl.col("points") - pl.col("opp_points")).alias("diff"),
    )
    .with_columns((pl.when(pl.col("diff") >= 0).then(1).otherwise(0).alias("win")))
)

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

metrics = [
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "starts",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "total_points",
]

team_form = (
    selections.join(
        match_stats,
        left_on=["event", "player", "team"],
        right_on=["event", "name", "team_name"],
        how="left",
    )
    .select(
        "event",
        "team",
        "player",
        "h_or_a",
        "opp_name",
        "team_scored",
        "team_conceded",
        "element",
    )
    .join(
        stats,
        left_on=["event", "team", "element"],
        right_on=["event", "team", "player_id"],
        how="left",
    )
    .join(
        player_id_position.select("id", "position"),
        left_on="element",
        right_on="id",
        how="inner",
    )
    .select(
        "event",
        "team",
        "player",
        "position",
        "h_or_a",
        "opp_name",
        "team_scored",
        "team_conceded",
        "element",
        *metrics,
    )
)


st.write("# Team Performance")

team_tab, position_tab, player_tab = st.tabs(["Team", "Position", "Player"])

position_tab.markdown("## Points by Position")

position_tab.line_chart(data=by_position, x="event", y="total", color="position")


team_tab.write("## Points Difference")
team_tab.bar_chart(data=points_diff, x="event", y="diff", stack=True, color="win")

player_tab.write("## Points by Player")
position_filter = player_tab.selectbox(
    "Select Position", ["All", "GK", "DEF", "MID", "FWD"]
)
if position_filter != "All":
    team = team.filter(pl.col("position") == position_filter)
player_tab.line_chart(
    data=team.select("position", "event", "total", "player"),
    x="event",
    y="total",
    color="player",
)

team_tab.write("## Home and Away Breakdown")

team_tab.bar_chart(
    home_away.group_by("event", "h_or_a").agg(pl.sum("total").alias("total_points")),
    x="event",
    y="total_points",
    color="h_or_a",
    stack=False,
)

player_tab.write("## Player Form Guide")

metric_filter = player_tab.multiselect(
    label="Metric Filter", options=metrics, default="total_points"
)
metrics_cumulative = team_form.select(
    "event",
    "player",
    "position",
    *[
        pl.col(metric).cum_sum().over("player").alias(f"cumulative_{metric}")
        for metric in metric_filter
    ],
)

if position_filter != "All":
    metrics_cumulative = metrics_cumulative.filter(
        pl.col("position") == position_filter
    )

player_tab.line_chart(
    data=metrics_cumulative,
    x="event",
    y=(f"cumulative_{metric}" for metric in metric_filter),
    color="player",
)

player_tab.write("## Player Quadrants")

col1, col2 = player_tab.columns(2)
y_metric = col1.selectbox(
    label="Metric Y-Axis", options=metrics, placeholder="goals_scored"
)
x_metric = col2.selectbox(
    label="Metric X-Axis", options=metrics, placeholder="expected_goals_scored"
)

if y_metric == x_metric:
    team_quadrants = (
        team_form.filter(pl.col("position") == position_filter)
        .group_by("player", "position")
        .agg(
            pl.sum(y_metric).alias(y_metric),
            pl.sum("total_points").alias("total_points"),
        )
    )
else:
    team_quadrants = (
        team_form.filter(pl.col("position") == position_filter)
        .group_by("player")
        .agg(
            pl.sum(y_metric).alias(y_metric),
            pl.sum(x_metric).alias(x_metric),
            pl.sum("total_points").alias("total_points"),
        )
    )

player_tab.scatter_chart(
    data=team_quadrants, x=x_metric, y=y_metric, color="player", size="total_points"
)
