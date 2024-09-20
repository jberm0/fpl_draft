import sys

sys.path.append("./")

from src.utils.env import load_env
from src.utils.input_output import write_parquet
from src.ingestion.raw import process_transactions

landing_path, raw_path = load_env(["landing_path", "raw_path"])


if __name__ == "__main__":
    transactions = process_transactions(landing_path)
    write_parquet(transactions, raw_path + "transactions.parquet")
