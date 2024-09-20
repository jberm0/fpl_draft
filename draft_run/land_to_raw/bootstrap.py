import sys

sys.path.append("././")

from src.utils.env import load_env
from src.utils.input_output import write_json, write_parquet
from src.ingestion.raw import (
    bootstrap_dict,
    get_gw_plus_1,
    get_gw_plus_2,
    get_gw_plus_3,
    process_elements,
    process_stats,
    process_positions,
    process_gameweek_calendar,
    process_teams,
    get_gw_pl_fixtures,
    get_rules,
)

landing_path, raw_path = load_env(["landing_path", "raw_path"])


if __name__ == "__main__":
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
