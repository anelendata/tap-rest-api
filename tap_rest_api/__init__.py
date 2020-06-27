#!/usr/bin/env python3

from requests.auth import HTTPBasicAuth, HTTPDigestAuth
# from requests_ntlm import HTTPNtlmAuth

import dateutil
import argparse, attr, backoff, datetime, itertools, json, os, pytz, requests, sys, time, urllib

import singer
from singer import utils
from singer.catalog import Catalog
import singer.metrics as metrics

from .json2schema import infer_schema


SPEC_FILE = "./tap_rest_api_spec.json"
SPEC = {}
TYPES = {
    "string": str,
    "datetime": str,
    "integer": int
    }

REQUIRED_CONFIG_KEYS = ["url"]

LOGGER = singer.get_logger()

CONFIG = {}

ENDPOINTS = {}

USER_AGENT = 'Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36 '

STREAMS = {}

@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    kwargs = attr.ib()


def get_endpoint(tap_stream_id, kwargs):
    """ Get the full url for the endpoint
    In addition to params passed from config values, it will create "resource"
    that is derived from tap_stream_id.
    URL can be something like:
    https://api.example.com/1/{resource}?last_update_start={start_datetime}&last_update_end={end_datetime}&items_per_page={items_per_page}&page={current_page}
    """
    params = {"resource": tap_stream_id}
    params.update(kwargs)
    return CONFIG["url"].format(**kwargs)


def get_bookmark_type():
    if CONFIG.get("timestamp_key"):
        return "timestamp"
    if CONFIG.get("datetime_key"):
        return "datetime"
    if CONFIG.get("index_key"):
        return "index"
    raise KeyError("You need to set timestamp_key, datetime_key, or index_key")


def get_start(STATE, tap_stream_id, bookmark_key):
    """
    state file, given by --state <state_file> prioritizes over the start value given by config or args
    """
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        if CONFIG.get("timestamp_key"):
            if not CONFIG.get("start_timestamp") and not CONFIG.get("start_datetime"):
                raise KeyError("timestamp_key is set but neither start_timestamp or start_datetime is set")
            current_bookmark = CONFIG.get("start_timestamp")
            if current_bookmark is None:
                current_bookmark = dateutil.parser.parse(CONFIG["start_datetime"]).timestamp()
        elif CONFIG.get("datetime_key"):
            if not CONFIG.get("start_datetime"):
                raise KeyError("datetime_key is set but start_datetime is not set")
            current_bookmark = CONFIG.get("start_datetime")
        elif CONFIG.get("index_key"):
            if not CONFIG.get("start_index"):
                raise KeyError("index_key is set but start_index is not set")
            current_bookmark = CONFIG.get("start_index")

        if current_bookmark is None:
            raise KeyError("You need to set timestamp_key, datetime_key, or index_key")
    return current_bookmark


def get_end():
    if CONFIG.get("timestamp_key"):
        end_from_config = CONFIG.get("end_timestamp")
        if end_from_config is None:
            end_from_config = dateutil.parser.parse(CONFIG["end_datetime"]).timestamp()
    elif CONFIG.get("datetime_key"):
        end_from_config = CONFIG.get("end_datetime")
    elif CONFIG.get("index_key"):
        end_from_config = CONFIG.get("end_index")
    return end_from_config


def get_last_update(record, current):
    last_update = current
    if CONFIG.get("timestamp_key"):
        key = CONFIG["timestamp_key"]
        if (key in record) and record[key] > current:
            # Handle the data with sub-seconds converted to int
            ex_digits = len(str(int(record[key]))) - 10
            last_update = record[key] / (pow(10, ex_digits))
        else:
            KeyError("timestamp_key not found in the record")
    elif CONFIG.get("datetime_key"):
        key = CONFIG["datetime_key"]
        if key not in record:
            KeyError("datetime_key not found in the record")

        record_datetime = dateutil.parser.parse(record[key])
        if record_datetime.tzinfo is None:
            record_datetime = record_datetime.replace(tzinfo=datetime.timezone.utc)

        current_datetime = dateutil.parser.parse(current)
        if current_datetime.tzinfo is None:
            current_datetime = current_datetime.replace(tzinfo=datetime.timezone.utc)

        if record_datetime > current_datetime:
            last_update = record[key]
    elif CONFIG.get("index_key"):
        key = CONFIG["index_key"]
        r_str = str(record.get(key))
        if r_str and (not current or r_str > current):
            last_update = r_str
        else:
            KeyError("index_key not found in the record")
    else:
        raise KeyError("Neither timestamp_key, datetime_key, or index_key is set")
    return last_update


def get_tzinfo():
    return pytz.utc
    # dateutil.parser.parse(CONFIG[datetime_param]).tzinfo


def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(os.path.join(CONFIG["schema_dir"], "{}.json".format(entity)))
    return schema


def nested_get(input_dict, nested_key):
    internal_dict_value = input_dict
    for k in nested_key:
        internal_dict_value = internal_dict_value.get(k, None)
        if internal_dict_value is None:
            return None
    return internal_dict_value


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
                filtered = dateutil.parser.parse(obj).replace(tzinfo=tzinfo).isoformat()
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

    tzinfo = dateutil.parser.parse(CONFIG[datetime_param]).tzinfo
    for d in filtered:
        filtered[d] = date.util.parser.parse(row[datetime_param]).replace(tzinfo=tzinfo).isoformat()

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
def gen_request(stream_id, url, auth_method="basic"):
    if not auth_method or auth_method == "no_auth":
        auth=None
    elif auth_method == "basic":
        auth=HTTPBasicAuth(CONFIG["username"], CONFIG["password"])
    elif auth_method == "digest":
        auth=HTTPDigestAuth(CONFIG["username"], CONFIG["password"])
    elif auth_method == "ntlm":
        auth=HTTPNtlmAuth(CONFIG["username"], CONFIG["password"])
    else:
        raise ValueError("Unknown auth method: " + auth_method)

    LOGGER.info("Using %s authentication method." % auth_method)

    with metrics.http_request_timer(stream_id) as timer:
        headers = { 'User-Agent': USER_AGENT }
        resp = requests.get(url,
                headers=headers,
                auth=auth)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()

def sync_rows(STATE, tap_stream_id, key_properties=[], auth_method=None, max_page=None):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, key_properties)

    bookmark_type = get_bookmark_type()

    start = get_start(STATE, tap_stream_id, "last_update")
    end = get_end()
    params = CONFIG
    if CONFIG.get("timestamp_key"):
        params.update({"start_timestamp": start})
    elif CONFIG.get("datetime_key"):
        params.update({"start_datetime": start})
    elif CONFIG.get("index_key"):
        params.update({"start_index": start})

    pretty_start = start
    pretty_end = end
    if bookmark_type == "timestamp":
        pretty_start = str(start) + " (" + str(datetime.datetime.fromtimestamp(start)) + ")"
        if end is not None:
            pretty_end = str(end) + " (" + str(datetime.datetime.fromtimestamp(end)) + ")"

    LOGGER.info("Stream %s has %s set starting %s and ending %s. I trust you set URL format contains those params. The behavior depends on the data source API's spec. I will not filter out the records outside the boundary. Every record received is will be written out." % (tap_stream_id, bookmark_type, pretty_start, pretty_end))

    last_update = start
    page_number = 1
    offset_number = 0  # Offset is the number of records (vs. page)
    etl_tstamp = int(time.time())
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            params.update({"current_page": page_number})
            params.update({"current_offset": offset_number})
            endpoint = get_endpoint(tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)
            rows = gen_request(tap_stream_id, endpoint, auth_method)
            rows = get_record_list(rows, CONFIG.get("record_list_level"))
            for row in rows:
                counter.increment()
                row = get_record(row, CONFIG.get("record_level"))
                row = filter_result(row, schema)
                if "_etl_tstamp" in schema["properties"].keys():
                    row["_etl_tstamp"] = etl_tstamp
                last_update = get_last_update(row, last_update)

                singer.write_record(tap_stream_id, row)

            LOGGER.info("Current page %d" % page_number)
            LOGGER.info("Current offset %d" % offset_number)

            if len(rows) == 0 or (max_page and page_number + 1 > max_page):
                break
            else:
                page_number +=1
                offset_number += len(rows)

    STATE = singer.write_bookmark(STATE, tap_stream_id, 'last_update', last_update)
    singer.write_state(STATE)
    return STATE


def get_streams_to_sync(streams, state):
    '''Get the streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = streams

    if current_stream:
        for key in result.keys():
            if result[key].tap_stream_id != current_stream:
                result.pop(key, None)

    if not result:
        raise Exception("Unknown stream {} in state".format(current_stream))

    return result


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for key in remaining_streams.keys():
        stream = remaining_streams[key]
        tap_stream_id = stream.tap_stream_id
        for stream_idx, annotated_stream in enumerate(annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if (hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams


def output_raw_records(STATE, tap_stream_id, key_properties=[], auth_method=None, max_page=None):
    """
    Write out the raw JSON output at the record list level to stdout
    """
    start = get_start(STATE, tap_stream_id, "last_update")
    end = get_end()

    pretty_start = start
    pretty_end = end
    last_update = start
    page_number = 1
    offset_number = 0  # Offset is the number of records (vs. page)
    etl_tstamp = int(time.time())
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            params = CONFIG
            params.update({"current_page": page_number})
            params.update({"current_offset": offset_number})
            endpoint = get_endpoint(tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)
            rows = gen_request(tap_stream_id, endpoint, auth_method)
            rows = get_record_list(rows, CONFIG.get("record_list_level"))
            for row in rows:
                counter.increment()
                row = get_record(row, CONFIG.get("record_level"))
                last_update = get_last_update(row, last_update)

                sys.stdout.write(json.dumps(row) + "\n")

            LOGGER.info("Current page %d" % page_number)
            LOGGER.info("Current offset %d" % offset_number)

            if len(rows) == 0 or (max_page and page_number + 1 > max_page):
                break
            else:
                page_number +=1
                offset_number += len(rows)


def do_sync(STATE, catalog, max_page=None, auth_method="basic", raw=False):
    """
    Sync the streams that were selected
    raw: Output raw JSON records to stdout
    """
    start_process_at = datetime.datetime.now()
    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, catalog)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        if raw:
            # Output raw JSON records only
            output_raw_records(STATE, stream.tap_stream_id, max_page=max_page, auth_method=auth_method)
            continue

        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            STATE = sync_rows(STATE, stream.tap_stream_id, max_page=max_page, auth_method=auth_method)
        except Exception as e:
            LOGGER.critical(e)
            raise e

        bookmark_type = get_bookmark_type()
        last_update = STATE["bookmarks"][stream.tap_stream_id]["last_update"]
        if bookmark_type == "timestamp":
            last_update = str(last_update) + " (" + str(datetime.datetime.fromtimestamp(last_update)) + ")"
        LOGGER.info("End sync " + stream.tap_stream_id)
        LOGGER.info("Last record's %s: %s" % (bookmark_type, last_update))

    end_process_at = datetime.datetime.now()
    LOGGER.info("Completed sync at %s" % str(end_process_at))
    LOGGER.info("Process duration: " + str(end_process_at - start_process_at))


def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_discovered_schema(stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def discover_schemas(schema):
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
    json.dump(discover_schemas(CONFIG["schema"]), sys.stdout, indent=2)


def get_record(raw_item, record_level):
    """
    Dig the items until the target schema
    """
    if not record_level:
        return raw_item

    record = raw_item
    for x in record_level.split(","):
        record = record[x]

    return record


def get_record_list(data, record_list_level):
    """
    Dig the raw data to the level that contains the list of the records
    """
    if not record_list_level:
        return data
    for x in record_list_level.split(","):
        data = data[x]
    return data


def do_infer_schema(out_catalog=True, add_tstamp=True):
    """
    Infer schema from the sample record list and write JSON schema and
    catalog files under schema directory and catalog directory.
    To fully support multiple streams, the catalog files must be consolidated
    but that is not supported in this function yet.
    """
    # TODO: Support multiple streams specified by STREAM[]
    tap_stream_id = STREAMS[list(STREAMS.keys())[0]].tap_stream_id

    params = CONFIG
    page_number = 0
    offset_number = 0
    params.update({"current_page": page_number})
    params.update({"current_offset": offset_number})
    endpoint = get_endpoint(tap_stream_id, params)
    LOGGER.info("GET %s", endpoint)
    auth_method = CONFIG.get("auth_method", "basic")
    data = gen_request(tap_stream_id, endpoint, auth_method)

    # In case the record is not at the root level
    data = get_record_list(data, CONFIG.get("record_list_level"))

    schema = infer_schema(data, CONFIG.get("record_level"))
    if add_tstamp:
        schema["properties"]["_etl_tstamp"] = {"type": ["null", "integer"]}

    with open(os.path.join(CONFIG["schema_dir"], tap_stream_id + ".json"), "w") as f:
        json.dump(schema, f, indent=2)

    if out_catalog:
        schema["selected"] = True
        catalog = {"streams": [{"stream": tap_stream_id,
                                "tap_stream_id": tap_stream_id,
                                "schema": schema
                                }]}
        with open(os.path.join(CONFIG["catalog_dir"], tap_stream_id + ".json"), "w") as f:
            json.dump(catalog, f, indent=2)


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
    with open(spec_file, "r") as f:
        custom_spec.update(json.load(f))

    SPEC["application"] = custom_spec.get("application", SPEC["application"])
    if custom_spec.get("args"):
        SPEC["args"].update(custom_spec.get("args"))

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
    if args.catalog and os.path.isfile(args.catalog):
            args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args


@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
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
    max_page = CONFIG.get("max_page")
    LOGGER.info("auth_method=%s" % auth_method)

    streams = CONFIG["streams"].split(",")
    for stream in streams:
        stream = stream.strip()
        STREAMS[stream] = Stream(stream, CONFIG)

    if args.state:
        STATE.update(args.state)
        LOGGER.info("State read: %s" % STATE)

    if args.infer_schema:
        do_infer_schema()
    elif args.discover:
        do_discover()
    elif args.catalog:
        do_sync(STATE, args.catalog, max_page, auth_method, raw=args.raw)
    else:
        LOGGER.info("No streams were selected")

if __name__ == "__main__":
    main()
