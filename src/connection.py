import requests
import json
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())
league_id=os.getenv("league_id")
team_id=os.getenv("team_id")
email=os.getenv("email")
password=os.getenv("password")

print(league_id + team_id + email + password)

def api_calls_json():
        base_url = "https://draft.premierleague.com/api/"
        base_path = "/Users/jonah/Documents/projects/fpl_draft/landing/"

        tables_to_pull = {
            "league_transactions": {
                "api_call": base_url + f"draft/league/{league_id}/transactions", 
                "write_path": base_path + "league_transactions.json"
            },
            "my_transactions": {
                "api_call": base_url + f"draft/entry/{team_id}/transactions", 
                "write_path": base_path + "my_transactions.json"
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
            "league_details": {
                "api_call": base_url + f"league/{league_id}/details",
                "write_path": base_path + "league_details.json"
            },
            "game_week": {
                "api_call": base_url + f"game",
                "write_path": base_path + "game_week.json"
            }
        }
        return tables_to_pull

def retrieve_json(session, tables_selected):
        
    tables_to_pull = api_calls_json()
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

def user_authentication(user_email, password, tables_selected: list):

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

    retrieve_json(session, tables_selected)
    print("successfully read tables")



tables_to_pull = [
    "league_transactions", 
    "my_transactions", 
    "trades", 
    "details", 
    "bootstrap_static", 
    "league_details", 
    "game_week"
]
user_authentication(email, password, tables_to_pull)