import sys

sys.path.append("././")

from src.utils.env import load_env
from src.ingestion.raw import process_trades

landing_path = load_env(["landing_path"])[0]

if __name__ == "__main__":
    trades = process_trades(landing_path)
    print(trades.head(100))
