import polars as pl
import sys

sys.path.append("./././")

from src.utils.input_output import read_json, write_parquet
from src.process.cleaning import rename_columns_by_regex

raw_path = "data/raw"
trusted_path = "data/trusted"


def create_scoring(scoring):
    scoring_df = pl.DataFrame(scoring)

    positions_list = ["GKP", "DEF", "MID", "FWD"]

    standard_scoring = scoring_df.select(
        pl.col("*").exclude(*[f"^*_{pos}*$" for pos in positions_list])
    )

    cols = ["goals_conceded", "goals_scored", "clean_sheets"]

    position_points_dfs = []
    for pos in positions_list:
        df_r = (
            scoring_df.select(pl.col(f"^*_{pos}*$"))
            .pipe(rename_columns_by_regex, f"_{pos}", "")
            .select(pl.lit(f"{pos}").alias("position"), *cols)
        )
        df_r = pl.concat([df_r, standard_scoring], how="horizontal")
        position_points_dfs.append(df_r)

    position_scoring = pl.concat(position_points_dfs, how="vertical")

    return position_scoring


if __name__ == "__main__":
    scoring = read_json(f"{raw_path}/scoring.json")
    position_scoring = create_scoring(scoring)
    write_parquet(position_scoring, trusted_path + "/scoring.parquet")
