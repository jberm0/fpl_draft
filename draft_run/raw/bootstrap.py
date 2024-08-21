import polars as pl
import sys

sys.path.append("././")

from src.process.schema_independent import json_to_dict
from src.utils.env import load_env
from src.utils.input_output import write_json, write_parquet

landing_path, raw_path = load_env(["landing_path", "raw_path"])


def bootstrap_dict(base_path):
    extension = "bootstrap-static.json"
    dict = json_to_dict(base_path + extension)
    return dict


def process_elements(dict):
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

    return elements


def process_stats(dict):
    stats = pl.DataFrame(dict.get("element_stats")).select("name", "abbreviation")
    return stats


def process_teams(dict):
    teams = pl.DataFrame(dict.get("teams"))
    return teams


def process_positions(dict):
    positions = pl.DataFrame(dict.get("element_types")).select(
        "id",
        "element_count",
        pl.col("singular_name_short").alias("position_abbv"),
        pl.col("plural_name").alias("position"),
    )
    return positions


def process_gameweek_calendar(dict):
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
    return gameweek_calendar


def get_gw_plus_1(dict):
    return dict.get("events").get("next")


def get_gw_plus_2(dict):
    return [gw for gw in dict.get("fixtures").keys()][1]


def get_gw_plus_3(dict):
    return [gw for gw in dict.get("fixtures").keys()][2]


def get_gw_pl_fixtures(dict, gw):
    return pl.DataFrame(dict.get("fixtures").get(f"{gw}"))


def get_rules(dict, keyword):
    assert keyword in ["league", "scoring", "squad"]
    return dict.get("settings").get(keyword)


if __name__ == "__main__":
    pl.Config(set_tbl_cols=15, set_tbl_rows=10, set_tbl_width_chars=150)
    dict = bootstrap_dict(landing_path)
    gw_1 = get_gw_plus_1(dict)
    gw_2 = get_gw_plus_2(dict)
    gw_3 = get_gw_plus_3(dict)

    elements = process_elements(dict).pipe(write_parquet, raw_path + "elements.parquet")
    stats = process_stats(dict).pipe(write_parquet, raw_path + "stats.parquet")
    teams = process_teams(dict).pipe(write_parquet, raw_path + "teams.parquet")
    positions = process_positions(dict).pipe(
        write_parquet, raw_path + "positions.parquet"
    )
    gw_calendar = process_gameweek_calendar(dict).pipe(
        write_parquet, raw_path + "gw_calendar.parquet"
    )
    fixtures_1 = get_gw_pl_fixtures(dict, gw_1).pipe(
        write_parquet, raw_path + "fixtures_1.parquet"
    )
    fixtures_2 = get_gw_pl_fixtures(dict, gw_2).pipe(
        write_parquet, raw_path + "fixtures_2.parquet"
    )
    fixtures_3 = get_gw_pl_fixtures(dict, gw_3).pipe(
        write_parquet, raw_path + "fixtures_3.parquet"
    )

    league_rules = get_rules(dict, "league")
    write_json(league_rules, raw_path + "league_rules.json")

    squad_rules = get_rules(dict, "squad")
    write_json(squad_rules, raw_path + "squad.json")

    scoring_rules = get_rules(dict, "scoring")
    write_json(scoring_rules, raw_path + "scoring.json")
