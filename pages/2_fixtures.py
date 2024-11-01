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

st.write("# Fixture Difficulty")

standings = pl.read_parquet(f"{trusted_path}standings.parquet").select(
    (pl.col("matches_won") + pl.col("matches_drawn") + pl.col("matches_lost")).alias(
        "P"
    )
)
last_gw = pl.Series(standings.select(pl.first("P"))).to_list()[0]


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

selected_players = selections.join(
    players_with_team,
    how="inner",
    left_on=["team", "player"],
    right_on=["team", "web_name"],
)

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


prem_teams, fpl_teams = st.tabs(["Prem Teams", "FPL Teams"])

gw_filter = prem_teams.slider(
    "Gameweek Filter",
    min_value=1,
    max_value=38,
    value=(last_gw + 1, last_gw + 3),
    step=1,
)

col1, col2 = prem_teams.columns(2)

all_fixtures = (
    (
        pl.concat([h_fixtures, a_fixtures])
        .join(team_to_id, how="inner", left_on="team_id", right_on="team_id")
        .join(team_to_id, how="inner", left_on="opp", right_on="team_id")
    )
    .select("event", "team", pl.col("team_right").alias("opp"), "fdr")
    .sort("event", "team")
)

all_fixtures_filtered = all_fixtures.filter(
    pl.col("event").is_between(gw_filter[0], gw_filter[1])
)


easiest_fixtures = (
    all_fixtures_filtered.group_by("team")
    .agg(pl.sum("fdr").alias("total_fdr"), pl.col("opp").explode())
    .sort(pl.col("total_fdr"), descending=False)
    .limit(10)
)
col1.write("## Good runs")
col1.write(easiest_fixtures)

hardest_fixtures = (
    all_fixtures_filtered.group_by("team")
    .agg(pl.sum("fdr").alias("total_fdr"), pl.col("opp").explode())
    .sort(pl.col("total_fdr"), descending=True)
    .limit(10)
)
col2.write("## Tough runs")
col2.write(hardest_fixtures)


# who's teams have the easiest and hardest run of fixtures

selections_difficulty = selected_players.select(
    pl.col("event").cast(pl.Int32), "owner", "player", "team"
).join(
    all_fixtures.select(pl.col("event").cast(pl.Int32), "team", "opp", "fdr"),
    how="left",
    left_on=["event", "team"],
    right_on=["event", "team"],
)

grouped_by_owner = selections_difficulty.group_by("event", "owner").agg(
    pl.sum("fdr").alias("total_fdr")
)
fpl_teams.write("## Historic FDR by team")
fpl_teams.line_chart(grouped_by_owner, x="event", y="total_fdr", color="owner")


current_selections = selected_players.filter(pl.col("event") == last_gw)
next_fixtures = all_fixtures.filter(pl.col("event") == last_gw + 1)

selection_projection = current_selections.select("owner", "team", "player").join(
    next_fixtures.select("team", "opp", "fdr"), how="inner", on="team"
)

fdr_projection = selection_projection.group_by("owner").agg(
    pl.sum("fdr").alias("total_fdr")
)
fpl_teams.write(f"## FDR Projection for GW {last_gw+1} based on current squads")
fpl_teams.bar_chart(
    fdr_projection, x="owner", y="total_fdr", color="total_fdr", stack="center"
)

fpl_teams.write(f"## Squad Projection for GW {last_gw+1} based on current squads")
owner_filter = fpl_teams.selectbox(
    label="Select Owner", options=["JB", "KO", "VA", "LJ", "GN", "SH", "MM", "FM"]
)
opp_projection = selection_projection.filter(pl.col("owner") == owner_filter)
fpl_teams.write(opp_projection)
