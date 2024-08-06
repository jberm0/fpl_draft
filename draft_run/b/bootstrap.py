from dotenv.main import load_dotenv, find_dotenv
from os import getenv
import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict

load_dotenv(find_dotenv())

landing_path = getenv("landing_path")


def process_bootstrap(base_path):
    extension = "bootstrap-static.json"
    dict = json_to_dict(base_path + extension)
    elements = (
        pl.DataFrame(dict.get("elements"))
        .select(
            "id",
            "assists",
            "bonus",
            "bps",
            "clean_sheets",
            "creativity",
            "goals_conceded",
            "goals_scored",
            "ict_index",
            "influence",
            "minutes",
            "own_goals",
            "penalties_missed",
            "penalties_saved",
            "red_cards",
            "saves",
            "threat",
            "yellow_cards",
            "starts",
            "expected_goals",
            "expected_assists",
            "expected_goal_involvements",
            "expected_goals_conceded",
            "added",
            "chance_of_playing_next_round",
            "chance_of_playing_this_round",
            "code",
            "draft_rank",
            "dreamteam_count",
            "event_points",
            "first_name",
            "form",
            "in_dreamteam",
            "news",
            "points_per_game",
            "second_name",
            "status",
            "total_points",
            "web_name",
            "influence_rank",
            "influence_rank_type",
            "creativity_rank",
            "creativity_rank_type",
            "threat_rank",
            "threat_rank_type",
            "ict_index_rank",
            "ict_index_rank_type",
            "form_rank",
            "element_type",
            "team",
        )
        .cast(
            {
                "creativity": pl.Float64,
                "ict_index": pl.Float64,
                "influence": pl.Float64,
                "threat": pl.Float64,
                "expected_goals": pl.Float64,
                "expected_assists": pl.Float64,
                "expected_goal_involvements": pl.Float64,
                "expected_goals_conceded": pl.Float64,
                "form": pl.Float64,
                "points_per_game": pl.Float64,
                "added": pl.Datetime,
            }
        )
    )
    stats = pl.DataFrame(dict.get("element_stats")).select("name", "abbreviation")
    positions = pl.DataFrame(dict.get("element_types")).select(
        "id",
        "element_count",
        pl.col("singular_name_short").alias("position_abbv"),
        pl.col("plural_name").alias("position"),
    )
    gameweek_calendar = (
        pl.DataFrame(dict.get("events").get("data"))
        .select("id", "name", "deadline_time", "trades_time", "waivers_time")
        .cast(
            {
                "deadline_time": pl.Datetime,
                "trades_time": pl.Datetime,
                "waivers_time": pl.Datetime,
            }
        )
    )
    gameweek_0 = dict.get("events").get("current")
    gameweek_1 = dict.get("events").get("next")
    gameweek_2 = [gw for gw in dict.get("fixtures").keys()][1]
    gameweek_3 = [gw for gw in dict.get("fixtures").keys()][2]

    gameweek_1_fixtures = pl.DataFrame(
        dict.get("fixtures").get(f"{gameweek_1}")
    ).select("id", "event", "team_a", "team_h")
    gameweek_2_fixtures = pl.DataFrame(
        dict.get("fixtures").get(f"{gameweek_2}")
    ).select("id", "event", "team_a", "team_h")
    gameweek_3_fixtures = pl.DataFrame(
        dict.get("fixtures").get(f"{gameweek_3}")
    ).select("id", "event", "team_a", "team_h")

    league_rules = dict.get("settings").get("league")
    points_rules = dict.get("settings").get("scoring")
    squad_rules = dict.get("settings").get("squad")

    return (
        elements,
        stats,
        positions,
        gameweek_calendar,
        gameweek_0,
        gameweek_1,
        gameweek_1_fixtures,
        gameweek_2,
        gameweek_2_fixtures,
        gameweek_3,
        gameweek_3_fixtures,
        league_rules,
        points_rules,
        squad_rules,
    )


if __name__ == "__main__":
    (
        elements,
        stats,
        positions,
        gameweek_calendar,
        gameweek_0,
        gameweek_1,
        gameweek_1_fixtures,
        gameweek_2,
        gameweek_2_fixtures,
        gameweek_3,
        gameweek_3_fixtures,
        league_rules,
        points_rules,
        squad_rules,
    ) = process_bootstrap(landing_path)
    for x in process_bootstrap(landing_path):
        try:
            print(x.head(10))
        except AttributeError:
            pass
