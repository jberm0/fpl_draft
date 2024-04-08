import json
import polars as pl

def json_to_dict(path):
    with open(path, 'r') as fh:
        dictionary = json.load(fh)
    return dictionary


def string_to_ints(df, cols: list):
    dict = {col: pl.i64 for col in cols}
    df = df.cast(dict)
    return df