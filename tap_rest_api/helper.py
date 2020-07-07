import attr, backoff, dateutil, datetime, os, requests
from urllib.parse import quote as urlquote
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from dateutil.tz import tzoffset

import singer
from singer import utils
import singer.metrics as metrics


USER_AGENT = "Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36 "
LOGGER = singer.get_logger()


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    kwargs = attr.ib()


def get_abs_path(path):
    """Returns the absolute path"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


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


def get_init_endpoint_params(config, state, tap_stream_id):
    params = config
    start = get_start(config, state, tap_stream_id, "last_update")

    if config.get("timestamp_key"):
        params.update({"start_timestamp": start})
    elif config.get("datetime_key"):
        params.update({"start_datetime": start})
    elif config.get("index_key"):
        params.update({"start_index": start})

    params.update({"current_page": 0})
    params.update({"current_offset": 0})
    params.update({"last_update": start})

    return params


def get_endpoint(url_format, tap_stream_id, kwargs):
    """ Get the full url for the endpoint including query

    In addition to kwargs passed from config values, it will create "resource"
    that is derived from tap_stream_id.

    The special characters in query are quoted with "%XX"

    URL can be something like:
        https://api.example.com/1/{resource}? \
            last_update_start={start_datetime}&last_update_end={end_datetime}& \
            items_per_page={items_per_page}&page={current_page}
    """
    params = dict()
    for key in kwargs:
        params[key] = urlquote(str(kwargs[key]).encode("utf-8"))
    params["resource"] = urlquote(str(tap_stream_id).encode("utf-8"))
    return url_format.format(**params)


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


@utils.backoff((backoff.expo,requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def generate_request(stream_id, url, auth_method="basic", username=None, password=None):
    """
    url: URL with pre-encoded query. See get_endpoint()
    """
    if not auth_method or auth_method == "no_auth":
        auth=None
    elif auth_method == "basic":
        auth=HTTPBasicAuth(username, password)
    elif auth_method == "digest":
        auth=HTTPDigestAuth(username, password)
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


def get_bookmark_type(config):
    if config.get("timestamp_key"):
        return "timestamp"
    if config.get("datetime_key"):
        return "datetime"
    if config.get("index_key"):
        return "index"
    raise KeyError("You need to set timestamp_key, datetime_key, or index_key")


def parse_datetime_tz(datetime_str, default_tz_offset=0):
    d = dateutil.parser.parse(datetime_str)
    if not d.tzinfo:
        d = d.replace(tzinfo=tzoffset(None, default_tz_offset))
    return d


def get_start(config, state, tap_stream_id, bookmark_key):
    """
    state file, given by --state <state_file> prioritizes over the start value given by config or args
    """
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        if config.get("timestamp_key"):
            if not config.get("start_timestamp") and not config.get("start_datetime"):
                raise KeyError("timestamp_key is set but neither start_timestamp or start_datetime is set")
            current_bookmark = config.get("start_timestamp")
            if current_bookmark is None:
                current_bookmark = dateutil.parser.parse(config["start_datetime"]).timestamp()
        elif config.get("datetime_key"):
            if not config.get("start_datetime"):
                raise KeyError("datetime_key is set but start_datetime is not set")
            current_bookmark = config.get("start_datetime")
        elif config.get("index_key"):
            if not config.get("start_index"):
                raise KeyError("index_key is set but start_index is not set")
            current_bookmark = config.get("start_index")

        if current_bookmark is None:
            raise KeyError("You need to set timestamp_key, datetime_key, or index_key")
    return current_bookmark


def get_end(config):
    if config.get("timestamp_key"):
        end_from_config = config.get("end_timestamp")
        if end_from_config is None:
            end_from_config = dateutil.parser.parse(config["end_datetime"]).timestamp()
    elif config.get("datetime_key"):
        end_from_config = config.get("end_datetime")
    elif config.get("index_key"):
        end_from_config = config.get("end_index")
    return end_from_config


def get_last_update(config, record, current):
    last_update = current
    if config.get("timestamp_key"):
        key = config["timestamp_key"]
        if (key in record) and record[key] > current:
            # Handle the data with sub-seconds converted to int
            ex_digits = len(str(int(record[key]))) - 10
            last_update = record[key] / (pow(10, ex_digits))
        else:
            KeyError("timestamp_key not found in the record")
    elif config.get("datetime_key"):
        key = config["datetime_key"]
        if key not in record:
            KeyError("datetime_key not found in the record")

        record_datetime = parse_datetime_tz(record[key])
        current_datetime = parse_datetime_tz(current)

        if record_datetime > current_datetime:
            last_update = record_datetime.isoformat()
    elif config.get("index_key"):
        key = config["index_key"]
        r_str = str(record.get(key))
        if r_str and (not current or r_str > current):
            last_update = r_str
        else:
            KeyError("index_key not found in the record")
    else:
        raise KeyError("Neither timestamp_key, datetime_key, or index_key is set")
    return last_update


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
