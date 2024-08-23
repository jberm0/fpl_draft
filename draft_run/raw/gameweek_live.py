import polars as pl
import sys

sys.path.append("././")

from src.ingestion.download_raw import get_gameweeks
from src.utils.input_output import write_parquet, read_json
from src.utils.env import load_env

landing_path, raw_path = load_env(["landing_path", "raw_path"])

gameweeks = get_gameweeks(raw_path)


def process_live_gameweek(gw_id):
    dict = read_json(landing_path + f"/gw_{gw_id}/live.json")
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
    for match in match_level[:1]:
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


if __name__ == "__main__":
    for gw_id in gameweeks:
        gw_dict = process_live_gameweek(gw_id)
        scores, stats = process_live_elements(gw_dict, gw_id)
        write_parquet(scores, raw_path + f"/live/scores/{gw_id}.parquet")
        write_parquet(stats, raw_path + f"/live/stats/{gw_id}.parquet")

        matches = process_live_matches(gw_dict)
        write_parquet(matches, raw_path + f"/live/matches/{gw_id}.parquet")
