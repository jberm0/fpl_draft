import polars as pl
from typing import List

from src.process.schema_independent import json_to_dict
from src.ingestion.landing import get_gameweeks, get_team_ids
from src.utils.input_output import read_json, write_json, write_parquet


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


def process_details(base_path):
    extension = "details.json"
    dict = json_to_dict(base_path + extension)
    league_name = dict.get("league").get("name")
    league_entries = pl.DataFrame(dict.get("league_entries")).select(
        "entry_id", "entry_name", "id", "short_name"
    )
    matches = pl.DataFrame(dict.get("matches")).select(
        "event",
        "finished",
        "league_entry_1",
        "league_entry_1_points",
        "league_entry_2",
        "league_entry_2_points",
        "started",
    )
    standings = pl.DataFrame(dict.get("standings")).select(
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


def process_gameweek(base_path):
    extension = "game_week.json"
    dict = json_to_dict(base_path + extension)
    df = pl.from_dict(dict)
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


def process_live_gameweek(landing_path, gw_id):
    dict = read_json(landing_path + f"{gw_id}/live.json")
    return dict


def process_live_elements(dict, gw_id):
    # elements level
    player_level = dict.get("elements")
    score_dfs = []
    stats_dfs = []
    for player_id in player_level:
        # explain is the scoring for a given player
        explain = player_level.get(player_id).get("explain")[0]
        # stats is the full stat report for a given player
        stats = player_level.get(player_id).get("stats")

        score_dict = pl.DataFrame(explain[0])
        number = explain[1]

        scoring_df = score_dict.with_columns(
            pl.lit(player_id).alias("player_id"),
            pl.lit(number).alias("match_number"),
            pl.lit(gw_id).alias("event"),
        )
        score_dfs.append(scoring_df)

        stats_df = pl.from_dict(stats).with_columns(
            pl.lit(player_id).alias("player_id"),
            pl.lit(number).alias("match_number"),
            pl.lit(gw_id).alias("event"),
        )
        stats_dfs.append(stats_df)

    scores_combined = pl.concat(score_dfs, how="vertical")
    stats_combined = pl.concat(stats_dfs, how="vertical")

    return scores_combined, stats_combined


def process_live_matches(dict):
    # match level
    match_level = dict.get("fixtures")

    match_dfs = []
    for match in match_level:
        match_df = (
            pl.from_dict(match)
            .select(
                "id",
                "stats",
                "team_a_score",
                "team_h_score",
                "pulse_id",
                "event",
                "team_a",
                "team_h",
            )
            .unnest("stats")
        )

        home_stats = (
            match_df.select(
                "id",
                "event",
                "pulse_id",
                pl.lit("home").alias("h_or_a"),
                pl.col("team_h").alias("team_id"),
                pl.col("team_a").alias("opp_id"),
                pl.col("team_h_score").alias("scored"),
                pl.col("team_a_score").alias("conceded"),
                "s",
                "h",
            )
            .explode("h")
            .unnest("h")
            .pivot(
                columns="s",
                index=[
                    "id",
                    "event",
                    "pulse_id",
                    "h_or_a",
                    "team_id",
                    "opp_id",
                    "scored",
                    "conceded",
                    "element",
                ],
                values="value",
            )
        )

        away_stats = (
            match_df.select(
                "id",
                "event",
                "pulse_id",
                pl.lit("away").alias("h_or_a"),
                pl.col("team_a").alias("team_id"),
                pl.col("team_h").alias("opp_id"),
                pl.col("team_a_score").alias("scored"),
                pl.col("team_h_score").alias("conceded"),
                "s",
                "a",
            )
            .explode("a")
            .unnest("a")
            .pivot(
                columns="s",
                index=[
                    "id",
                    "event",
                    "pulse_id",
                    "h_or_a",
                    "team_id",
                    "opp_id",
                    "scored",
                    "conceded",
                    "element",
                ],
                values="value",
            )
        )

        full_stats = pl.concat([home_stats, away_stats], how="vertical")
        match_dfs.append(full_stats)

    return pl.concat(match_dfs, how="vertical")


def get_gw_team_selection(landing_path, gw_id: int, team_ids: List[int]):
    gw_selections = []
    for team_id in team_ids:
        dict = read_json(landing_path + f"{gw_id}/{team_id}_selection.json").get(
            "picks"
        )
        selection = pl.DataFrame(dict).select(
            pl.lit(gw_id).alias("event"), pl.lit(team_id).alias("owner"), "element"
        )
        gw_selections.append(selection)
    return pl.concat(gw_selections, how="vertical")


def process_trades(base_path):
    extension = "trades.json"
    dict = json_to_dict(base_path + extension)
    df = pl.from_dict(dict)
    # df = df.cast({"offer_time": pl.Datetime, "response_time": pl.Datetime})
    # df = df.select(
    #     "event",
    #     "id",
    #     "offered_entry",
    #     "offer_time",
    #     "received_entry",
    #     "response_time",
    #     "state",
    #     pl.col("tradeitem_set").list.first(),
    # )
    # df = df.unnest("tradeitem_set")
    return df


def process_transactions(base_path):
    extension = "league_transactions.json"
    dict = json_to_dict(base_path + extension).get("transactions")
    df = pl.DataFrame(dict)
    return df


def bootstrap(raw_path, landing_path):
    dict = bootstrap_dict(landing_path)
    gw_1 = get_gw_plus_1(dict)
    gw_2 = get_gw_plus_2(dict)
    gw_3 = get_gw_plus_3(dict)

    process_elements(dict).pipe(write_parquet, raw_path + "elements.parquet")
    process_stats(dict).pipe(write_parquet, raw_path + "stats.parquet")
    process_teams(dict).pipe(write_parquet, raw_path + "teams.parquet")
    process_positions(dict).pipe(write_parquet, raw_path + "positions.parquet")
    process_gameweek_calendar(dict).pipe(
        write_parquet, raw_path + "gw_calendar.parquet"
    )
    get_gw_pl_fixtures(dict, gw_1).pipe(write_parquet, raw_path + "fixtures_1.parquet")
    get_gw_pl_fixtures(dict, gw_2).pipe(write_parquet, raw_path + "fixtures_2.parquet")
    get_gw_pl_fixtures(dict, gw_3).pipe(write_parquet, raw_path + "fixtures_3.parquet")

    league_rules = get_rules(dict, "league")
    write_json(league_rules, raw_path + "league_rules.json")
    squad_rules = get_rules(dict, "squad")
    write_json(squad_rules, raw_path + "squad.json")
    scoring_rules = get_rules(dict, "scoring")
    write_json(scoring_rules, raw_path + "scoring.json")


def details(raw_path, landing_path):
    league_name, league_entries, matches, standings = process_details(landing_path)
    write_parquet(league_entries, raw_path + "league_entries.parquet")
    write_parquet(matches, raw_path + "h2h_fixtures.parquet")
    write_parquet(standings, raw_path + "h2h_standings.parquet")


def gameweek(raw_path, landing_path):
    gameweek = process_gameweek(landing_path)
    write_parquet(gameweek, raw_path + "gameweek.parquet")


def gameweek_live(raw_path, landing_path):
    gameweeks = get_gameweeks(landing_path)

    for gw_id in gameweeks:
        gw_dict = process_live_gameweek(landing_path, gw_id)
        scores, stats = process_live_elements(gw_dict, gw_id)
        write_parquet(scores, raw_path + f"live/scores/{gw_id}.parquet")
        write_parquet(stats, raw_path + f"live/stats/{gw_id}.parquet")

        matches = process_live_matches(gw_dict)
        write_parquet(matches, raw_path + f"live/matches/{gw_id}.parquet")


def selections(raw_path, landing_path):
    all_gameweeks = get_gameweeks(landing_path)
    all_team_ids = get_team_ids(raw_path)

    for gw_id in all_gameweeks:
        selections = get_gw_team_selection(landing_path, gw_id, all_team_ids)
        write_parquet(selections, raw_path + f"live/selections/{gw_id}.parquet")


def transactions(raw_path, landing_path):
    transactions = process_transactions(landing_path)
    write_parquet(transactions, raw_path + "transactions.parquet")


def fixtures(raw_path, landing_path):
    fixtures = pl.DataFrame(read_json(f"{landing_path}fixtures.json")).select(
        "event",
        "team_a",
        "team_a_score",
        "team_h",
        "team_h_score",
        "team_h_difficulty",
        "team_a_difficulty",
    )
    write_parquet(fixtures, f"{raw_path}fixtures.parquet")


def land_to_raw(raw_path, landing_path):
    bootstrap(raw_path, landing_path)
    details(raw_path, landing_path)
    gameweek(raw_path, landing_path)
    gameweek_live(raw_path, landing_path)
    selections(raw_path, landing_path)
    transactions(raw_path, landing_path)
    fixtures(raw_path, landing_path)

    print("written raw data")
