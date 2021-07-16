import json
from types import SimpleNamespace
from argparse import ArgumentParser


def read_and_parse_config(config_path: str):
    config_file = open(config_path, "r")
    return json.loads(config_file.read(), object_hook=lambda d: SimpleNamespace(**d))


def read_cli_args():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", help="config file path", metavar="FILE")
    return parser.parse_args()
