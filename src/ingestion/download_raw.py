import sys
import polars as pl
from pprint import pprint

sys.path.append("././")

from src.utils.input_output import write_json, read_parquet

raw_path = "data/raw"


def primary_api_calls_json(base_url, base_path, league_id):
    tables_to_pull = {
        "league_transactions": {
            "api_call": base_url + f"draft/league/{league_id}/transactions",
            "write_path": base_path + "league_transactions.json",
        },
        "trades": {
            "api_call": base_url + f"/draft/league/{league_id}/trades",
            "write_path": base_path + "trades.json",
        },
        "details": {
            "api_call": base_url + f"league/{league_id}/details",
            "write_path": base_path + "details.json",
        },
        "bootstrap_static": {
            "api_call": base_url + "bootstrap-static",
            "write_path": base_path + "bootstrap-static.json",
        },
        "game_week": {
            "api_call": base_url + "game",
            "write_path": base_path + "game_week.json",
        },
        "fixtures": {
            "api_call": base_url + "fixtures",
            "write_path": base_path + "fixtures.json",
        },
    }
    return tables_to_pull


def get_team_ids():
    df = read_parquet(f"{raw_path}/league_entries.parquet")

    return pl.Series(df.select("entry_id")).to_list()


def get_gameweeks():
    df = read_parquet(f"{raw_path}/gameweek.parquet")

    return [df.select("current_event").item()]


def live_stats_api_call_json(base_url, base_path, gameweek):
    return {
        "live_stats": {
            "api_call": base_url + f"event/{gameweek}/live",
            "write_path": base_path + f"gw_{gameweek}/live.json",
        },
    }


def team_selection_api_call_json(base_url, base_path, entry_id, gameweek):
    return {
        "team_selection": {
            "api_call": base_url + f"entry/{entry_id}/event/{gameweek}",
            "write_path": base_path + f"gw_{gameweek}/{entry_id}_selection.json",
        },
    }


# def secondary_api_calls_json(base_url, base_path, entry_id, gameweek):
#     tables_to_pull = {
#         "team_selection": {
#             "api_call": base_url + f"/entry/{entry_id}/event/{gameweek}",
#             "write_path": base_path + f"{entry_id}_{gameweek}_selection.json",
#         },
#         "live_stats": {
#             "api_call": base_url + f"/event/{gameweek}/live",
#             "write_path": base_path + f"{gameweek}_live.json",
#         },
#     }
#     return tables_to_pull


def retrieve_primary_json(session, tables_to_pull, tables_selected):
    for table in tables_to_pull:
        if table in tables_selected:
            write_path = tables_to_pull[table]["write_path"]
            api_call = tables_to_pull[table]["api_call"]
            print(api_call)
            r = session.get(api_call)
            jsonResponse = r.json()
            write_json(jsonResponse, write_path)
        print(
            f"read and downloaded {table} from {api_call} and written to {write_path}"
        )


def retrieve_secondary_json(session, tables_to_pull):
    pprint(tables_to_pull)
    for table in tables_to_pull:
        print(table)
        vals = list(table.values())[0]
        write_path = vals.get("write_path")
        api_call = vals.get("api_call")
        print(api_call)
        r = session.get(api_call)
        jsonResponse = r.json()
        write_json(jsonResponse, write_path)
    print(f"read and downloaded {table} from {api_call} and written to {write_path}")
