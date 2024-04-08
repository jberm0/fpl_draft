from dotenv.main import load_dotenv, find_dotenv
from os import getenv
from src.ingestion.connection import user_authentication

load_dotenv(find_dotenv())
league_id = getenv("league_id")
team_id = getenv("team_id")
email = getenv("email")
password = getenv("password")
landing_path = getenv("landing_path")
base_api_path = getenv("base_api_path")

# defining the tables to pull
tables_to_pull = [
    "league_transactions",
    "trades",
    "details",
    "bootstrap_static",
    "league_details",
    "game_week",
]

# connect and pull the tables
user_authentication(
    email, 
    password, 
    tables_to_pull,
    base_url=base_api_path,
    base_path=landing_path,
    league_id=league_id,
)

