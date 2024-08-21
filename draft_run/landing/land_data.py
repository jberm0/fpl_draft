import sys

sys.path.append("./")

from src.ingestion.connection import authenticate_and_pull
from src.utils.env import load_env

league_id, team_id, email, password, landing_path, base_api_path = load_env(
    ["league_id", "team_id", "email", "password", "landing_path", "base_api_path"]
)

# defining the tables to pull
primary_tables_to_pull = [
    "league_transactions",
    "trades",
    "details",
    "bootstrap_static",
    "league_details",
    "game_week",
]

if __name__ == "__main__":
    authenticate_and_pull(
        email,
        password,
        primary_tables_to_pull,
        base_url=base_api_path,
        base_path=landing_path,
        league_id=league_id,
    )
