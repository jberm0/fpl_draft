# to recommend players to trade or waiver in

import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_parquet, read_json

pl.Config(set_tbl_rows=20, set_tbl_cols=20)

raw_path = "data/raw"
trusted_path = "data/trusted"


elements = read_parquet(f"{raw_path}/elements.parquet")
scoring_rules = read_json(f"{raw_path}/scoring.json")
upcoming_fixtures = read_parquet(f"{trusted_path}/upcoming_fixtures.parquet")

print(upcoming_fixtures)

# join elements and teams on id
# join elements on fixtures on id == team_h and team_a
