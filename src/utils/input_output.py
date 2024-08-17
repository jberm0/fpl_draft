import polars as pl


def read_parquet(path: str, **kwargs) -> pl.DataFrame:
    return pl.read_parquet(path, **kwargs)


def write_parquet(df: pl.DataFrame, path: str, **kwargs):
    df.write_parquet(path, **kwargs)
    print(f"written to {path}")
