import requests
from dotenv.main import load_dotenv, find_dotenv
import os

from src.ingestion.download_raw import retrieve_json

load_dotenv(find_dotenv())
league_id = os.getenv("league_id")
team_id = os.getenv("team_id")
email = os.getenv("email")
password = os.getenv("password")


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
