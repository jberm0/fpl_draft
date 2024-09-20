import polars as pl

from src.utils.input_output import read_parquet, read_json, write_parquet
from src.process.cleaning import rename_columns_by_regex


def get_standings(h2h_standings, entries):
    standings = (
        h2h_standings.join(
            entries.select("id", "entry_name", "short_name"),
            left_on="league_entry",
            right_on="id",
            how="inner",
        )
        .select(
            "rank",
            "last_rank",
            pl.col("entry_name").alias("team_name"),
            pl.col("short_name").alias("owner"),
            "matches_won",
            "matches_drawn",
            "matches_lost",
            "points_against",
            "points_for",
            pl.col("total").alias("total_points"),
        )
        .sort("rank", descending=False)
    )

    return standings


def get_head_to_head(h2h_fixtures, entries):
    head_to_head = (
        h2h_fixtures.join(
            entries.select(
                "id",
                pl.col("entry_name").alias("team_name_1"),
                pl.col("short_name").alias("owner_1"),
            ),
            left_on="league_entry_1",
            right_on="id",
            how="inner",
        )
        .join(
            entries.select(
                "id",
                pl.col("entry_name").alias("team_name_2"),
                pl.col("short_name").alias("owner_2"),
            ),
            left_on="league_entry_2",
            right_on="id",
            how="inner",
        )
        .select(
            "event",
            "started",
            "finished",
            "league_entry_1",
            "team_name_1",
            "owner_1",
            "league_entry_1_points",
            "league_entry_2",
            "team_name_2",
            "owner_2",
            "league_entry_2_points",
        )
    )

    return head_to_head


def trusted_match_stats(raw_path, trusted_path):
    matches = read_parquet(f"{raw_path}/live/matches/*.parquet")
    teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int32), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select("web_name", "id")

    return (
        matches.join(players, how="left", left_on="element", right_on="id")
        .join(teams, how="left", left_on="team_id", right_on="id")
        .rename({"short_name": "team_name"})
        .join(teams, how="left", left_on="opp_id", right_on="id")
        .rename({"short_name": "opp_name"})
        .select(
            "event",
            "h_or_a",
            "team_name",
            "opp_name",
            pl.col("scored").alias("team_scored"),
            pl.col("conceded").alias("team_conceded"),
            pl.col("web_name").alias("name"),
            "element",
            "goals_scored",
            "assists",
            "own_goals",
            "penalties_saved",
            "penalties_missed",
            "yellow_cards",
            "red_cards",
            "saves",
            "bonus",
            "bps",
        )
    )


def trusted_scores(raw_path, trusted_path):
    scores = read_parquet(f"{raw_path}/live/scores/*.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64)
    )
    teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int64), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select(
        "web_name", "id", "team"
    )

    actions_and_points = (
        scores.join(players, left_on="player_id", right_on="id")
        .join(teams, left_on="team", right_on="id")
        .select(
            "event",
            "match_number",
            "player_id",
            "web_name",
            "team",
            "short_name",
            "stat",
            "value",
            "points",
        )
    )

    player_actions = actions_and_points.pivot(
        columns="stat",
        index=["event", "match_number", "player_id", "web_name", "team", "short_name"],
        values=["value"],
    )

    player_points = actions_and_points.pivot(
        columns="stat",
        index=["event", "match_number", "player_id", "web_name", "team", "short_name"],
        values=["points"],
    )

    return player_actions, player_points


def trusted_selections(raw_path, trusted_path):
    selections = read_parquet(f"{raw_path}/live/selections/*.parquet")
    h2h_teams = read_parquet(f"{raw_path}/league_entries.parquet").select(
        pl.col("entry_id").cast(pl.Int32), "entry_name", "short_name"
    )
    pl_teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int64), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select(
        "web_name", "id", "team"
    )

    return (
        selections.join(h2h_teams, how="left", left_on="owner", right_on="entry_id")
        .join(players, how="left", left_on="element", right_on="id")
        .join(pl_teams, how="left", left_on="team", right_on="id")
        .select(
            "event",
            "entry_name",
            pl.col("short_name").alias("owner"),
            pl.col("web_name").alias("player"),
            pl.col("short_name_right").alias("team"),
        )
    )


def trusted_stats(raw_path, trusted_path):
    stats = read_parquet(f"{raw_path}/live/stats/*.parquet")

    pl_teams = read_parquet(f"{raw_path}/teams.parquet").select(
        pl.col("id").cast(pl.Int64), "short_name"
    )
    players = read_parquet(f"{trusted_path}/players.parquet").select(
        "web_name", "id", "team"
    )

    return (
        stats.with_columns(pl.col("player_id").cast(pl.Int64))
        .join(players, how="left", left_on="player_id", right_on="id")
        .join(pl_teams, how="left", left_on="team", right_on="id")
        .select(
            "player_id",
            "web_name",
            pl.col("short_name").alias("team"),
            "match_number",
            "event",
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
            "in_dreamteam",
        )
    )


def trusted_players(raw_path):
    return read_parquet(f"{raw_path}/elements.parquet").select(
        "id",
        "web_name",
        "added",  # added to the game
        "draft_rank",
        "code",
        "status",
        "team",
        "element_type",  # position key
        "total_points",
        "event_points",  # points in current event
        "points_per_game",
        "starts",
        "minutes",
        "chance_of_playing_this_round",  # in 25% chunks
        "chance_of_playing_next_round",  # in 25% chunks
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
        "threat",  # impact on goals
        "threat_rank",
        "threat_rank_type",
        "creativity",  # impact on assists
        "creativity_rank",
        "creativity_rank_type",
        "influence",  # influence on outcome of match
        "influence_rank",
        "influence_rank_type",
        "ict_index",  # combination of threat, influence, creativity
        "ict_index_rank",
        "ict_index_rank_type",
        "dreamteam_count",
        "in_dreamteam",
        "news",
    )


def trusted_scoring(raw_path):
    scoring = read_json(f"{raw_path}/scoring.json")
    scoring_df = pl.DataFrame(scoring)

    positions_list = ["GKP", "DEF", "MID", "FWD"]

    standard_scoring = scoring_df.select(
        pl.col("*").exclude(*[f"^*_{pos}*$" for pos in positions_list])
    )

    cols = ["goals_conceded", "goals_scored", "clean_sheets"]

    position_points_dfs = []
    for pos in positions_list:
        df_r = (
            scoring_df.select(pl.col(f"^*_{pos}*$"))
            .pipe(rename_columns_by_regex, f"_{pos}", "")
            .select(pl.lit(f"{pos}").alias("position"), *cols)
        )
        df_r = pl.concat([df_r, standard_scoring], how="horizontal")
        position_points_dfs.append(df_r)

    position_scoring = pl.concat(position_points_dfs, how="vertical")

    return position_scoring


def create_teams_and_fixtures(fixtures_1, fixtures_2, fixtures_3, teams):
    all_fixtures = pl.concat([fixtures_1, fixtures_2, fixtures_3], how="vertical")

    teams_and_fixtures = all_fixtures.join(
        teams.select("id", pl.col("name").alias("team_h_name")),
        left_on="team_h",
        right_on="id",
    ).join(
        teams.select("id", pl.col("name").alias("team_a_name")),
        left_on="team_a",
        right_on="id",
    )

    return teams_and_fixtures


def league(raw_path, trusted_path):
    h2h_fixtures = read_parquet(f"{raw_path}/h2h_fixtures.parquet")
    h2h_standings = read_parquet(f"{raw_path}/h2h_standings.parquet")
    entries = read_parquet(f"{raw_path}/league_entries.parquet")

    head_to_head = get_head_to_head(h2h_fixtures, entries)
    standings = get_standings(h2h_standings, entries)

    write_parquet(head_to_head, trusted_path + "head_to_head.parquet")
    write_parquet(standings, trusted_path + "standings.parquet")


def live_matches(raw_path, trusted_path):
    matches_trusted = trusted_match_stats(raw_path, trusted_path)
    write_parquet(matches_trusted, trusted_path + "match_stats.parquet")


def live_scores(raw_path, trusted_path):
    player_actions, player_points = trusted_scores(raw_path, trusted_path)
    write_parquet(player_actions, trusted_path + "player_actions.parquet")
    write_parquet(player_points, trusted_path + "player_points.parquet")


def live_selections(raw_path, trusted_path):
    trusted_selections_df = trusted_selections(raw_path, trusted_path)
    write_parquet(trusted_selections_df, trusted_path + "selections.parquet")


def live_stats(raw_path, trusted_path):
    trusted_stats_df = trusted_stats(raw_path, trusted_path)
    write_parquet(trusted_stats_df, trusted_path + "stats.parquet")


def players(raw_path, trusted_path):
    trusted_players_df = trusted_players(raw_path)
    write_parquet(trusted_players_df, trusted_path + "players.parquet")


def scoring(raw_path, trusted_path):
    position_scoring = trusted_scoring(raw_path)
    write_parquet(position_scoring, trusted_path + "scoring.parquet")


def teams_and_fixtures(raw_path, trusted_path):
    fixtures_1 = read_parquet(f"{raw_path}/fixtures_1.parquet").select(
        pl.lit("+1").alias("fixture"), "team_h", "team_a"
    )
    fixtures_2 = read_parquet(f"{raw_path}/fixtures_2.parquet").select(
        pl.lit("+2").alias("fixture"), "team_h", "team_a"
    )
    fixtures_3 = read_parquet(f"{raw_path}/fixtures_3.parquet").select(
        pl.lit("+3").alias("fixture"), "team_h", "team_a"
    )
    teams = read_parquet(f"{raw_path}/teams.parquet")
    teams_and_fixtures = create_teams_and_fixtures(
        fixtures_1, fixtures_2, fixtures_3, teams
    )
    write_parquet(teams_and_fixtures, trusted_path + "upcoming_fixtures.parquet")


def trusted_data(raw_path, trusted_path):
    league(raw_path, trusted_path)
    live_matches(raw_path, trusted_path)
    live_scores(raw_path, trusted_path)
    live_selections(raw_path, trusted_path)
    live_stats(raw_path, trusted_path)
    players(raw_path, trusted_path)
    scoring(raw_path, trusted_path)
    teams_and_fixtures(raw_path, trusted_path)
