#!/usr/bin/env python3

from requests.auth import HTTPBasicAuth
from dateutil import parser
import argparse, attr, backoff, datetime, itertools, json, os, pytz, requests, sys, time, urllib

import singer
from singer import utils
from singer.catalog import Catalog
import singer.metrics as metrics

SPEC_FILE = "./tap_rest_api_spec.json"
SPEC = {}
TYPES = {
    "string": str,
    "datetime": str,
    "integer": int
    }

REQUIRED_CONFIG_KEYS = []
# REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_datetime", "schema"]

LOGGER = singer.get_logger()

CONFIG = {"schema_dir": "../../schema", "items_per_page": 100}

ENDPOINTS = {}

USER_AGENT = 'Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36 '

STREAMS = {}

@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    kwargs = attr.ib()


def get_endpoint(endpoint, kwargs):
    '''Get the full url for the endpoint'''
    # TODO: Support multiple streams
    return CONFIG["url"].format(**kwargs)

    # Original code
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))
    return CONFIG["url"] + ENDPOINTS[endpoint].format(**kwargs)


def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        current_bookmark = CONFIG.get("start_datetime", CONFIG.get("start_index"))
        if current_bookmark is None:
            raise KeyError("Neither start_datetime or start_index is set.")
    return current_bookmark


def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path(CONFIG["schema_dir"] + "/{}.json".format(entity)))

    return schema


def nested_get(input_dict, nested_key):
    internal_dict_value = input_dict
    for k in nested_key:
        internal_dict_value = internal_dict_value.get(k, None)
        if internal_dict_value is None:
            return None
    return internal_dict_value


def get_tzinfo():
    return pytz.utc
    parser.parse(CONFIG[datetime_param]).tzinfo


def _do_filter(obj, dict_path, schema):
    if not obj:
        return None
    obj_type = nested_get(schema, dict_path + ["type"])
    obj_format = nested_get(schema, dict_path + ["format"])
    tzinfo = get_tzinfo()
    if obj_type is None:
        return None
    if type(obj_type) is list:
        obj_type = obj_type[1]

    if obj_type == "object":
        assert(type(obj) is dict and obj.keys())
        filtered = dict()
        for key in obj.keys():
            ret = _do_filter(obj[key], dict_path + ["properties", key], schema)
            if ret:
                filtered[key] = ret
    elif obj_type == "array":
        assert(type(obj) is list)
        filtered = list()
        for o in obj:
            ret = _do_filter(o, dict_path + ["items"], schema)
            if ret:
                filtered.append(ret)
    else:
        if obj_type == "string":
            filtered = str(obj)
            if obj_format == "date-time":
                filtered = parser.parse(obj).replace(tzinfo=tzinfo).isoformat()
        elif obj_type == "number":
            try:
                filtered = float(obj)
            except ValueError as e:
                LOGGER.error(str(e) + "dict_path" + str(dict_path) + " object type: " + obj_type)
                raise
        else:
            filtered = obj
    return filtered


def filter_result(row, schema):
    return _do_filter(row, [], schema)


def convert_time(row, schema):
    return _do_convert_time(row, [], schema)
    datetime_param = None
    for key in SPEC["args"].keys():
        if SPEC["args"][key]["type"] == "datetime":
            datetime_param = key

    tzinfo = parser.parse(CONFIG[datetime_param]).tzinfo
    for d in filtered:
        filtered[d] = parser.parse(row[datetime_param]).replace(tzinfo=tzinfo).isoformat()

    if filtered.get("meta_data"):
        filtered.pop("meta_data")
    if filtered.get("_links"):
        filtered.pop("_links")

    return filtered


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


@utils.backoff((backoff.expo,requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def gen_request(stream_id, url):
    with metrics.http_request_timer(stream_id) as timer:
        headers = { 'User-Agent': USER_AGENT }
        resp = requests.get(url,
                headers=headers,
                auth=HTTPBasicAuth(CONFIG["username"], CONFIG["password"]))
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()


def get_last_update(record, current):
    last_update = current
    if CONFIG.get("datetime_key"):
        key = CONFIG["datetime_key"]
        if(key in record) and (parser.parse(record[key]) > parser.parse(current)):
            last_update = record[key]
    elif CONFIG.get("index_key"):
        key = CONFIG["index_key"]
        if(key in record) and record[key] > current:
            last_update = record[key]
    else:
        raise KeyError("Neither datetime_key or index_key is set")
    return last_update

def sync_rows(STATE, catalog, schema_name, key_properties=[]):
    schema = load_schema(schema_name)
    singer.write_schema(schema_name, schema, key_properties)

    start = get_start(STATE, schema_name, "last_update")
    LOGGER.info("Only syncing %s updated since %s" % (schema_name, start))
    last_update = start
    page_number = 1
    offset_number = 0  # Offset is the number of records (vs. page)
    with metrics.record_counter(schema_name) as counter:
        while True:
            params = CONFIG
            params.update({"current_page": page_number})
            params.update({"current_offset": offset_number})
            endpoint = get_endpoint(schema_name, params)
            LOGGER.info("GET %s", endpoint)
            rows = gen_request(schema_name,endpoint)
            for row in rows:
                counter.increment()
                row = filter_result(row, schema)
                if "_etl_tstamp" in schema["properties"].keys():
                    row["_etl_tstamp"] = time.time()
                last_update = get_last_update(row, last_update)
                singer.write_record(schema_name, row)
            if len(rows) < 100:
                break
            else:
                page_number +=1
                offset_number += len(rows)

    STATE = singer.write_bookmark(STATE, schema_name, 'last_update', last_update)
    singer.write_state(STATE)
    LOGGER.info("Completed %s Sync" % schema_name)
    return STATE


def get_streams_to_sync(streams, state):
    '''Get the streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = streams
    if current_stream:
        result = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != current_stream, streams))
    if not result:
        raise Exception("Unknown stream {} in state".format(current_stream))
    return result


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for stream in remaining_streams:
        tap_stream_id = stream.tap_stream_id
        for stream_idx, annotated_stream in enumerate(annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if (hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams


def do_sync(STATE, catalogs, schema):
    '''Sync the streams that were selected'''
    remaining_streams = get_streams_to_sync(STREAMS[schema], STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [cat for cat in catalogs.streams if cat.stream == stream.tap_stream_id][0]
            STATE = sync_rows(STATE, catalog, stream.tap_stream_id)
        except Exception as e:
            LOGGER.critical(e)
            raise e


def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_discovered_schema(stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def discover_schemas(schema="orders"):
    '''Iterate through streams, push to an array and return'''
    result = {'streams': []}
    for stream in STREAMS[schema]:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({'stream': stream.tap_stream_id,
                                  'tap_stream_id': stream.tap_stream_id,
                                  'schema': load_discovered_schema(stream)})
    return result

def do_discover():
    '''JSON dump the schemas to stdout'''
    LOGGER.info("Loading Schemas")
    json.dump(discover_schemas(CONFIG["schema"]), sys.stdout, indent=4)


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
    # Read spec file
    with open(spec_file, "r") as f:
        content = f.read()
    SPEC.update(json.loads(content))

    parser = argparse.ArgumentParser(SPEC["application"])
    parser.add_argument("spec_file", type=str, help="Specification file")

    # Capture additional args
    for arg in SPEC["args"].keys():
        parser.add_argument(
            "--" + arg,
            type=TYPES[SPEC["args"][arg]["type"]],
            default=SPEC["args"][arg].get("default"),
            help=SPEC["args"][arg].get("help"),
            required=SPEC["args"][arg].get("required", False))

    # Default arguments
    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        "--schema_dir",
        type=str,
        help="Full path to the schema directory.",
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    parser.add_argument(
        "--url",
        type=str,
        help="REST API endpoint with {params}. Required in config.")

    args = parser.parse_args()
    if args.config:
        args.config = utils.load_json(args.config)
    if args.state:
        args.state = utils.load_json(args.state)
    else:
        args.state = {}
    if args.catalog:
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args


@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
    spec_file = sys.argv[1]
    args = parse_args(spec_file, REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    # Overwrite config specs with commandline args if present

    args_dict = args.__dict__
    for arg in args_dict.keys():
        if CONFIG.get(arg) and args_dict.get(arg):
            CONFIG[arg] = args_dict[arg]

    # if not CONFIG.get("end_datetime"):
    #     CONFIG["end_datetime"]  = datetime.datetime.utcnow().isoformat()

    STATE = {}
    schema = CONFIG["schema"]
    tap_stream_id = CONFIG.get("tap_stream_id", CONFIG["schema"])

    # TODO: support multiple streams
    STREAMS[schema] = [Stream(schema, CONFIG)]

    if args.state:
        STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.catalog:
        do_sync(STATE, args.catalog, schema)
    else:
        LOGGER.info("No Streams were selected")

if __name__ == "__main__":
    main()
