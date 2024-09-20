from dotenv.main import load_dotenv, find_dotenv
from os import getenv

from typing import Tuple, List


def load_env(variables: List[str]) -> Tuple[str]:
    load_dotenv(find_dotenv())
    return [getenv(variable) for variable in variables]
