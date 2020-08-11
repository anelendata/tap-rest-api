#!/usr/bin/env python3
import argparse, os, sys
import simplejson as json
import singer
from singer import utils
from singer.catalog import Catalog

from .helper import Stream, get_abs_path
from .sync import sync
from .schema import discover, infer_schema

LOGGER = singer.get_logger()

SPEC_FILE = "./tap_rest_api_spec.json"
SPEC = {}

REQUIRED_CONFIG_KEYS = ["url"]
CONFIG = {}
ENDPOINTS = {}
STREAMS = {}


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


TYPES = {
    "string": str,
    "datetime": str,
    "integer": int,
    "boolean": str2bool
    }


def parse_args(spec_file, required_config_keys):
    ''' This is to replace singer's default utils.parse_args()
    https://github.com/singer-io/singer-python/blob/master/singer/utils.py

    Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    # Read default spec file
    default_spec = {}
    default_spec_file = get_abs_path("default_spec.json")
    with open(default_spec_file, "r") as f:
        default_spec.update(json.load(f))

    SPEC.update(default_spec)

    # Overwrite with the custom spec file
    custom_spec = {}

    if os.path.isfile(spec_file):
        with open(spec_file, "r") as f:
            custom_spec.update(json.load(f))

    SPEC["application"] = custom_spec.get("application", SPEC["application"])
    if custom_spec.get("args"):
        SPEC["args"].update(custom_spec.get("args"))

    parser = argparse.ArgumentParser(SPEC["application"])

    if custom_spec:
        parser.add_argument("spec_file", type=str, help="Custom spec file")

    # Capture additional args
    for arg in SPEC["args"].keys():
        parser.add_argument(
            "--" + arg,
            type=TYPES[SPEC["args"][arg]["type"]],
            default=SPEC["args"][arg].get("default"),
            help=SPEC["args"][arg].get("help"),
            required=SPEC["args"][arg].get("required", False))

    # Default singer arguments, commands, and required args
    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    # commands
    parser.add_argument(
        '-r', '--raw',
        action='store_true',
        help='Raw output at record level')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    parser.add_argument(
        '-i', '--infer_schema',
        action='store_true',
        help='Do infer schema')

    args = parser.parse_args()

    if args.config:
        args.config = utils.load_json(args.config)
    if args.state:
        if os.path.exists(args.state):
            args.state = utils.load_json(args.state)
        else:
            LOGGER.warn(args.state + " was not found.")
            args.state = {}
    else:
        args.state = {}
    if args.catalog:
        if not os.path.isfile(args.catalog):
            raise Exception("Catalog file %s not found" % args.catalog)
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args


@utils.handle_top_exception(LOGGER)
def main():
    """
    Entry point of tap_rest_api
    """
    spec_file = ""
    if len(sys.argv) > 1:
        spec_file = sys.argv[1]

    args = parse_args(spec_file, REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)

    # Overwrite config specs with commandline args
    # But we want to skip the args unspecified by the user...
    # So the trick is to go back to sys.argv and find the args begins with "--"
    # I can do this because I'm not allowing abbreviation of the args
    args_dict = args.__dict__
    for arg in args_dict.keys():
        if "--" + arg not in sys.argv and CONFIG.get(arg) is not None:
            continue
        CONFIG[arg] = args_dict[arg]

    STATE = {}

    auth_method = CONFIG.get("auth_method")
    assume_sorted = CONFIG.get("assume_sorted")
    max_page = CONFIG.get("max_page")
    filter_by_schema = CONFIG.get("filter_by_schema")

    if CONFIG.get("streams"):
        streams = CONFIG["streams"].split(",")
    elif CONFIG.get("schema"):
        streams = [CONFIG["schema"]]

    for stream in streams:
        stream = stream.strip()
        STREAMS[stream] = Stream(stream, CONFIG)

    if args.state:
        STATE.update(args.state)
        LOGGER.debug("State read: %s" % STATE)

    if args.infer_schema:
        infer_schema(CONFIG, STREAMS)
    elif args.discover:
        discover(CONFIG, STREAMS)
    elif args.catalog:
        sync(CONFIG, STREAMS, STATE, args.catalog, assume_sorted, max_page,
             auth_method, raw=args.raw, filter_by_schema=filter_by_schema)
    else:
        raise Exception("No streams were selected")


if __name__ == "__main__":
    main()
