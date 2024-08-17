import requests

from src.ingestion.download_raw import retrieve_json
from src.utils.env import load_env

league_id, team_id, email, password = load_env(
    ["league_id", "team_id", "email", "password"]
)


def user_authentication(
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

    retrieve_json(session, tables_selected, base_url, base_path, league_id)
    print("successfully read tables")
