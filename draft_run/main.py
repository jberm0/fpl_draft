import sys
import time

sys.path.append("././")

from src.utils.env import load_env
from src.ingestion.raw import land_to_raw
from src.ingestion.trusted import raw_to_trusted
from src.ingestion.connection import authenticate_and_pull

(
    landing_path,
    raw_path,
    trusted_path,
    league_id,
    team_id,
    email,
    password,
    landing_path,
    base_api_path,
) = load_env(
    [
        "landing_path",
        "raw_path",
        "trusted_path",
        "league_id",
        "team_id",
        "email",
        "password",
        "landing_path",
        "base_api_path",
    ]
)


if __name__ == "__main__":
    primary_tables_to_pull = [
        "league_transactions",
        "trades",
        "details",
        "bootstrap_static",
        "league_details",
        "game_week",
    ]
    authenticate_and_pull(
        email,
        password,
        primary_tables_to_pull,
        base_url=base_api_path,
        base_path=landing_path,
        league_id=league_id,
    )

    time.sleep(3)

    land_to_raw(raw_path, landing_path)

    time.sleep(3)

    raw_to_trusted(raw_path, trusted_path)

    time.sleep(3)

    print("data retrieved and written to trusted")
