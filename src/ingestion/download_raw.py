import sys

sys.path.append("././")

from src.utils.input_output import write_json


def api_calls_json(base_url, base_path, league_id):
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
    }
    return tables_to_pull


def retrieve_json(session, tables_selected, base_url, base_path, league_id):
    tables_to_pull = api_calls_json(base_url, base_path, league_id)
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
