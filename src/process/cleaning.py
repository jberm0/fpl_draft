from src.process.schema_independent import json_to_dict
import polars as pl


def process_transactions(base_path, extension):
    if extension != "league_transactions.json":
        raise Exception()
    df = json_to_dict(base_path + extension)
    df = df.cast({"added": pl.Datetime})
    return df


def process_trades(base_path, extension):
    if extension != "trades.json":
        raise Exception()
    df = json_to_dict(base_path + extension)
    df = df.cast({"offer_time": pl.Datetime, "response_time": pl.Datetime})
    df = df.select(
        "event",
        "id",
        "offered_entry",
        "offer_time",
        "received_entry",
        "response_time",
        "state",
        pl.col("tradeitem_set").list.first(),
    )
    df = df.unnest("tradeitem_set")
    return df


def process_gameweek(base_path, extension):
    if extension != "game_week.json":
        raise Exception()
    df = json_to_dict(base_path + extension)
    df = df.select(
        "current_event",
        "current_event_finished",
        "next_event",
        "trades_time_for_approval",
        "waivers_processed",
        pl.when(pl.col("processing_status") == "y")
        .then(True)
        .otherwise(False)
        .alias("processing_status"),
    )
    return df


def process_details(base_path, extension):
    if extension != "details.json":
        raise Exception()
    dict = json_to_dict(base_path + extension)
    league_name = dict["league"]["name"]
    league_entries = pl.DataFrame(dict["league_entries"]).select(
        "entry_id", "entry_name", "id", "short_name"
    )
    matches = pl.DataFrame(dict["matches"]).select(
        "event",
        "finished",
        "league_entry_1",
        "league_entry_1_points",
        "league_entry_2",
        "league_entry_2_points",
        "started",
    )
    standings = pl.DataFrame(dict["standings"]).select(
        "last_rank",
        "league_entry",
        "matches_drawn",
        "matches_lost",
        "matches_played",
        "matches_won",
        "points_against",
        "points_for",
        "rank",
        "total",
    )

    return league_name, league_entries, matches, standings


def process_bootsrap(base_path, extension):
    if extension != "bootstrap.json":
        raise Exception()
    dict = json_to_dict(base_path + extension)
    elements = (
        pl.DataFrame(dict["elements"])
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
    stats = pl.DataFrame(dict["element_stats"]).select("name", "abbreviation")
    positions = pl.DataFrame(dict["element_types"]).select(
        "id",
        "element_count",
        pl.col("singular_name_short").alias("position_abbv"),
        pl.col("plural_name").alias("position"),
    )
    gameweek_calendar = (
        pl.DataFrame(dict["events"]["data"])
        .select("id", "name", "deadline_time", "trades_time", "waivers_time")
        .cast(
            {
                "deadline_time": pl.Datetime,
                "trades_time": pl.Datetime,
                "waivers_time": pl.Datetime,
            }
        )
    )
    gameweek_0 = dict["events"]["current"]
    gameweek_1 = dict["events"]["next"]
    gameweek_2 = [gw for gw in dict["fixtures"].keys()][1]
    gameweek_3 = [gw for gw in dict["fixtures"].keys()][2]

    gameweek_1_fixtures = pl.DataFrame(dict["fixtures"][f"{gameweek_1}"]).select(
        "id", "event", "team_a", "team_h"
    )
    gameweek_2_fixtures = pl.DataFrame(dict["fixtures"][f"{gameweek_2}"]).select(
        "id", "event", "team_a", "team_h"
    )
    gameweek_3_fixtures = pl.DataFrame(dict["fixtures"][f"{gameweek_3}"]).select(
        "id", "event", "team_a", "team_h"
    )

    league_rules = dict["settings"]["league"]
    points_rules = dict["settings"]["scoring"]
    squad_rules = dict["settings"]["squad"]

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
