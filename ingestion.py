from dotenv import load_dotenv, find_dotenv
import os
from src.connection import user_authentication

load_dotenv(find_dotenv())
league_id=os.getenv("league_id")
team_id=os.getenv("team_id")
email=os.getenv("email")
password=os.getenv("password")

tables_to_pull = [
    "league_transactions", 
    "trades", 
    "details", 
    "bootstrap_static", 
    "league_details", 
    "game_week"
]
user_authentication(email, password, tables_to_pull)