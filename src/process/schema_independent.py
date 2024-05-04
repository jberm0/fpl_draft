import json
import polars as pl

def json_to_dict(path):
    with open(path, 'r') as fh:
        dictionary = json.load(fh)
    return dictionary

