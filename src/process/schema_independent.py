import json


def json_to_dict(path):
    with open(path, "r") as fh:
        dictionary = json.load(fh)
    return dictionary
