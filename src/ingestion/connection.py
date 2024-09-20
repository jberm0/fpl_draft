import requests

from src.ingestion.landing import (
    retrieve_primary_json,
    get_team_ids,
    primary_api_calls_json,
    get_gameweeks,
    live_stats_api_call_json,
    team_selection_api_call_json,
    retrieve_secondary_json,
)
from src.utils.env import load_env

league_id, team_id, email, password, raw_path, landing_path = load_env(
    ["league_id", "team_id", "email", "password", "raw_path", "landing_path"]
)


def authenticate_and_pull(
    user_email, password, tables_selected: list, base_url, base_path, league_id
):
    session = requests.session()

    login_data = {
        "login": user_email,
        "password": password,
        "app": "plfpl-web",
        "redirect_uri": "https://fantasy.premierleague.com/a/login",
    }

    url = "https://users.premierleague.com/accounts/login/"

    session.post(url, data=login_data)
    print("logged in successfully")

    # primary tables are tables available at the root
    primary_tables_to_pull = primary_api_calls_json(base_url, base_path, league_id)

    retrieve_primary_json(session, primary_tables_to_pull, tables_selected)
    print("successfully read primary tables")

    team_ids = get_team_ids(raw_path)
    all_gameweeks = get_gameweeks(landing_path)

    # secondary tables are tables that use values from primary tables as part of the API call, e.g. team_id or gameweek number
    secondary_tables_to_pull = []
    for gameweek in all_gameweeks:
        secondary_tables_to_pull.append(
            live_stats_api_call_json(base_url, base_path, gameweek)
        )
        for team_id in team_ids:
            secondary_tables_to_pull.append(
                team_selection_api_call_json(base_url, base_path, team_id, gameweek)
            )

    retrieve_secondary_json(session, secondary_tables_to_pull)
    print("successfully read secondary tables")

    print("read all raw tables")
