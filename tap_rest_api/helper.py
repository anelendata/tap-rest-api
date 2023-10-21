import attr, backoff, dateutil, datetime, hashlib, os, requests
import simplejson as json
from urllib.parse import quote as urlquote
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from dateutil.tz import tzoffset

import jsonpath_ng as jsonpath

import singer
from singer import utils
import singer.metrics as metrics


USER_AGENT = ("Mozilla/5.0 (Macintosh; scitylana.singer.io) " +
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 " +
              "Safari/537.36 ")
LOGGER = singer.get_logger()


# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
# The timestamp of the record extracted from the source
EXTRACT_TIMESTAMP = "_sdc_extracted_at"
# The timestamp of the record submit to the destination
# (kept null at extraction)
BATCH_TIMESTAMP = "_sdc_batched_at"


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    kwargs = attr.ib()


def get_abs_path(path):
    """Returns the absolute path"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def parse_datetime_tz(datetime_str, default_tz_offset=0):
    d = dateutil.parser.parse(datetime_str)
    if not d.tzinfo:
        d = d.replace(tzinfo=tzoffset(None, default_tz_offset))
    return d


def human_readable(bookmark_type, t):
    readable = t
    if t is not None and bookmark_type == "timestamp":
        try:
            ds = datetime.datetime.fromtimestamp(t)
        except:
            raise Exception("bookmark type is set to timestamp, but the value {t} isn't timestamp")
        readable = f"{str(t)} ({str(ds)})"
    return readable


def _get_jsonpath(raw, path):
    jsonpath_expr = jsonpath.parse(path)
    record = [match.value for match in jsonpath_expr.find(raw)]
    return record


def get_record(raw_item, record_level):
    """
    Dig the items until the target schema
    """
    if not record_level:
        return raw_item

    record = _get_jsonpath(raw_item, record_level)
    if len(record) != 1:
        raise Exception(f"jsonpath match records: {len(record)}, expected 1.")

    return record[0]


def get_record_list(raw_data, record_list_level):
    """
    Dig the raw data to the level that contains the list of the records
    """
    if not record_list_level:
        return raw_data
    data = _get_jsonpath(raw_data, record_list_level)
    return data


def get_bookmark_type_and_key(config, stream):
    """
    If config value timestamp_key, datetime_key, or index_key is a dictionary
    and has value for the stream, it will be prioritized.
    Otherwise, timestamp_key > datetime_key > index_key
    """
    bm_type = None
    bm_key = None
    ts_keys = config.get("timestamp_key")
    dt_keys = config.get("datetime_key")
    i_keys = config.get("index_key")

    if ts_keys:
        if isinstance(ts_keys, dict) and ts_keys.get(stream):
            return "timestamp", ts_keys.get(stream)
        elif isinstance(ts_keys, str):
            bm_type = "timestamp"
            bm_key = ts_keys
    if dt_keys:
        if isinstance(dt_keys, dict) and dt_keys.get(stream):
            return "datetime", dt_keys.get(stream)
        elif isinstance(dt_keys, str) and bm_type is None:
            bm_type = "datetime"
            bm_key = dt_keys
    if i_keys:
        if isinstance(i_keys, dict) and i_keys.get(stream):
            return "index", i_keys.get(stream)
        elif isinstance(i_keys, str) and bm_type is None:
            bm_type = "index"
            bm_key = i_keys

    if bm_type and bm_key:
        return bm_type, bm_key

    raise KeyError("You need to set timestamp_key, datetime_key, or index_key")


def get_streams_to_sync(streams, state):
    '''Get the streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = dict(streams)

    if current_stream:
        for key in streams.keys():
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
        for stream_idx, annotated_stream in enumerate(
                annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if (hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams


def get_start(config, state, tap_stream_id, bookmark_key):
    """
    state file, given by --state <state_file> prioritizes over the start
    value given by config or args

    For human convenience, start_datetime (more human readable) is also looked
    up when timestamp_key is set but start_timestamp is not set.
    """
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    bookmark_type, _ = get_bookmark_type_and_key(config, tap_stream_id)
    if current_bookmark is None:
        if bookmark_type == "timestamp":
            if (not config.get("start_timestamp") and
                    not config.get("start_datetime")):
                raise KeyError("timestamp_key is set but neither " +
                               "start_timestamp or start_datetime is set")
            current_bookmark = config.get("start_timestamp")
            if current_bookmark is None:
                current_bookmark = dateutil.parser.parse(
                    config["start_datetime"]).timestamp()
            else:
                current_bookmark = get_float_timestamp(current_bookmark)
        elif bookmark_type == "datetime":
            if not config.get("start_datetime"):
                raise KeyError(
                    "datetime_key is set but start_datetime is not set")
            current_bookmark = config.get("start_datetime")
        elif bookmark_type == "index":
            if config.get("start_index") is None:
                raise KeyError("index_key is set but start_index is not set")
            current_bookmark = config.get("start_index")

    return current_bookmark


def get_end(config, tap_stream_id):
    """
    For human convenience, end_datetime (more human readable) is also looked
    up when timestamp_key is set but end_timestamp is not set.
    """
    bookmark_type, _ = get_bookmark_type_and_key(config, tap_stream_id)
    if bookmark_type == "timestamp":
        end_from_config = config.get("end_timestamp")
        if end_from_config is None:
            if config.get("end_datetime") is not None:
                end_from_config = dateutil.parser.parse(
                    config["end_datetime"]).timestamp()
            else:
                end_from_config = datetime.datetime.now().timestamp()
    elif bookmark_type == "datetime":
        end_from_config = config.get("end_datetime")
        if not end_from_config:
            end_from_config = datetime.datetime.now().isoformat()
    elif bookmark_type == "index":
        end_from_config = config.get("end_index")
    return end_from_config


def get_digest_from_record(record):
    digest = hashlib.md5(
        json.dumps(record, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return digest


def get_float_timestamp(ts):
    # Handle the data with sub-seconds converted to int
    ex_digits = len(str(int(ts))) - 10
    value = float(ts) / (pow(10, ex_digits))
    return value


def get_last_update(config, tap_stream_id, record, current):
    last_update = current
    bookmark_type, bookmark_key = get_bookmark_type_and_key(config, tap_stream_id)
    if bookmark_type == "timestamp":
        value = _get_jsonpath(record, bookmark_key)[0]
        if value:
            value = get_float_timestamp(value)
            if value > current:
                last_update = value
        else:
            KeyError("timestamp_key not found in the record")
    elif bookmark_type == "datetime":
        value = _get_jsonpath(record, bookmark_key)[0]
        if not value:
            KeyError("datetime_key not found in the record")

        record_datetime = parse_datetime_tz(value)
        current_datetime = parse_datetime_tz(current)

        if record_datetime > current_datetime:
            last_update = record_datetime.isoformat()
    elif bookmark_type == "index":
        current_index = str(_get_jsonpath(record, bookmark_key)[0])
        LOGGER.debug("Last update will be updated from %s to %s" %
                     (last_update, current_index))
        # When index is an integer, it's dangerous to compare 9 and 10 as
        # string for example.
        try:
            current_index = int(current_index)
        except ValueError:
            if type(last_update) == int:
                # When the index suddenly changes to str, fall back to string
                LOGGER.warning(
                    "Previously index was throught to be integer. Now" +
                    " it seems to be string type. %s %s" %
                    (last_update, current_index))
            last_update = str(last_update)
        if current_index and (not current or current_index > current):
            last_update = current_index
        else:
            KeyError("index_key not found in the record")
    else:
        raise KeyError(
            "Neither timestamp_key, datetime_key, or index_key is set")
    return last_update


def get_init_endpoint_params(config, state, tap_stream_id):
    bookmark_type, bookmark_key = get_bookmark_type_and_key(config, tap_stream_id)
    params = dict(config)
    start = get_start(config, state, tap_stream_id, "last_update")
    end = get_end(config, tap_stream_id)
    if bookmark_type == "timestamp":
        start_datetime = datetime.datetime.fromtimestamp(start).isoformat()
        end_datetime = datetime.datetime.fromtimestamp(end).isoformat()
        params.update({
            "start_timestamp": start,
            "end_timestamp": end,
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "start_date": start_datetime[0:10],
            "end_date": end_datetime[0:10],
            "timestamp_key": bookmark_key,
        })
    elif bookmark_type == "datetime":
        params.update({
            "start_timestamp": dateutil.parser.parse(start).timestamp(),
            "end_timestamp": dateutil.parser.parse(end).timestamp(),
            "start_datetime": start,
            "end_datetime": end,
            "start_date": start[0:10],
            "end_date": end[0:10],
            "datetime_key": bookmark_key,
        })
    elif bookmark_type == "index":
        start_datetime = config.get("start_datetime")
        start_date = start_datetime[0:10] if start_datetime else None
        end_datetime = config.get("end_datetime")
        if not end_datetime:
            end_datetime = datetime.datetime.utcnow().isoformat()
        end_date = end_datetime[0:10] if end_datetime else None
        params.update({
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "start_date": start_date,
            "end_date": end_date,
            "start_index": start,
            "end_index": end,
            "index_key": bookmark_key,
        })

    params.update(
        {
            "stream": tap_stream_id,
            "current_page": config.get("page_start", 0),
            "current_offset": config.get("offset_start", 0),
            "last_update": start,
         }
    )

    return params


def get_http_headers(config=None):
    if not config or not config.get("http_headers"):
        return {"User-Agent": USER_AGENT,
                "Content-type": "application/json"}

    headers = config["http_headers"]
    if type(headers) == str:
        headers = json.loads(headers)
    LOGGER.debug(headers)
    return headers


def get_endpoint(url_format, tap_stream_id, data):
    """ Get the full url for the endpoint including query

    In addition to data passed from config values, it will create "resource"
    that is derived from tap_stream_id.

    The special characters in query are quoted with "%XX"

    URL can be something like:
        https://api.example.com/1/{resource}? \
          last_update_start={start_datetime}&last_update_end={end_datetime}& \
          items_per_page={items_per_page}&page={current_page}
    """
    params = dict()
    for key in data:
        params[key] = urlquote(str(data[key]).encode("utf-8"))
    params["resource"] = urlquote(str(tap_stream_id).encode("utf-8"))
    return url_format.format(**params)


def _giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


@utils.backoff((requests.exceptions.RequestException,), _giveup)
@utils.ratelimit(20, 1)
def generate_request(stream_id, url, auth_method="no_auth", headers=None,
                     username=None, password=None):
    """
    url: URL with pre-encoded query. See get_endpoint()
    """
    if not auth_method or auth_method == "no_auth":
        auth = None
    elif auth_method == "basic":
        auth = HTTPBasicAuth(username, password)
    elif auth_method == "digest":
        auth = HTTPDigestAuth(username, password)
    else:
        raise ValueError("Unknown auth method: " + auth_method)

    LOGGER.info("Using %s authentication method." % auth_method)

    headers = headers or get_http_headers()

    with metrics.http_request_timer(stream_id) as timer:
        resp = requests.get(url,
                            headers=headers,
                            auth=auth)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()
