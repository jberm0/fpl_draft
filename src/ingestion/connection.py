import requests
import json
from dotenv.main import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())
league_id=os.getenv("league_id")
team_id=os.getenv("team_id")
email=os.getenv("email")
password=os.getenv("password")

def api_calls_json(base_url, base_path, league_id):

        tables_to_pull = {
            "league_transactions": {
                "api_call": base_url + f"draft/league/{league_id}/transactions", 
                "write_path": base_path + "league_transactions.json"
            },
            "trades": {
                "api_call": base_url + f"/draft/league/{league_id}/trades",
                "write_path": base_path + "trades.json"
            },
            "details": {
                "api_call": base_url + f"league/{league_id}/details",
                "write_path": base_path + "details.json"
            },
            "bootstrap_static": {
                "api_call": base_url + f"bootstrap-static",
                "write_path": base_path + "bootstrap-static.json"
            },
            "game_week": {
                "api_call": base_url + f"game",
                "write_path": base_path + "game_week.json"
            }
        }
        return tables_to_pull

def retrieve_json(session, tables_selected, base_url, base_path, league_id):
        
    tables_to_pull = api_calls_json(base_url, base_path, league_id)
    for table in tables_to_pull:
        if table in tables_selected:
            print(table)
            print(tables_to_pull[table])
            write_path = tables_to_pull[table]["write_path"]
            api_call = tables_to_pull[table]["api_call"]
            r = session.get(api_call)
            jsonResponse = r.json()
            with open(write_path, 'w') as outputfile:
                json.dump(jsonResponse, outputfile)

def user_authentication(
    user_email, password, tables_selected: list, base_url, base_path, league_id
):

    session = requests.session()

    login_data = {
        "login" : user_email,
        "password" : password,
        "app" : "plfpl-web",
        "redirect_uri" : "https://fantasy.premierleague.com/a/login"
    }

    url = "https://users.premierleague.com/accounts/login/"

    session.post(url, data = login_data)
    print("logged in successfully")

    retrieve_json(session, tables_selected, base_url, base_path, league_id)
    print("successfully read tables")