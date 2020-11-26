import dateutil, os, sys
import simplejson as json
import singer
from singer import utils

from .helper import (generate_request, get_endpoint, get_init_endpoint_params,
                     get_record, get_record_list, get_http_headers,
                     EXTRACT_TIMESTAMP, BATCH_TIMESTAMP)
import getschema
import jsonschema

LOGGER = singer.get_logger()


def validate(record, schema):
    try:
        jsonschema.validate(record, schema)
    except jsonschema.exceptions.ValidationError:
        return False
    return True


def filter_record(row, schema, on_invalid_property="force"):
    """
    Parse the result into types
    """
    return getschema.fix_type(row, schema,
                              on_invalid_property=on_invalid_property)


def load_schema(schema_dir, entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(os.path.join(schema_dir, "{}.json".format(entity)))
    return schema


def load_discovered_schema(schema_dir, stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(schema_dir, stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def _discover_schemas(schema_dir, streams, schema):
    '''Iterate through streams, push to an array and return'''
    result = {'streams': []}
    for key in streams.keys():
        stream = streams[key]
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({'stream': stream.tap_stream_id,
                                  'tap_stream_id': stream.tap_stream_id,
                                  'schema': load_discovered_schema(schema_dir,
                                                                   stream)})
    return result


def discover(config, streams):
    """
    JSON dump the schemas to stdout
    """
    LOGGER.info("Loading Schemas")
    json_str = _discover_schemas(config["schema_dir"], streams,
                                 config["schema"])
    json.dump(json_str, sys.stdout, indent=2)


def infer_schema(config, streams, out_catalog=True, add_tstamp=True):
    """
    Infer schema from the sample record list and write JSON schema and
    catalog files under schema directory and catalog directory.
    To fully support multiple streams, the catalog files must be consolidated
    but that is not supported in this function yet.
    """
    # TODO: Support multiple streams specified by STREAM[]
    tap_stream_id = streams[list(streams.keys())[0]].tap_stream_id

    params = get_init_endpoint_params(config, {}, tap_stream_id)
    endpoint = get_endpoint(config["url"], tap_stream_id, params)
    LOGGER.info("GET %s", endpoint)
    auth_method = config.get("auth_method", "basic")

    headers = get_http_headers(config)
    data = generate_request(tap_stream_id, endpoint, auth_method,
                            headers,
                            config.get("username"),
                            config.get("password"))

    # In case the record is not at the root level
    data = get_record_list(data, config.get("record_list_level"))

    schema = getschema.infer_schema(data, config.get("record_level"))
    if add_tstamp:
        timestamp_format = {"type": ["null", "string"],
                            "format": "date-time"}
        schema["properties"][EXTRACT_TIMESTAMP] = timestamp_format
        schema["properties"][BATCH_TIMESTAMP] = timestamp_format

    if not os.path.exists(config["schema_dir"]):
        os.mkdir(config["schema_dir"])

    with open(os.path.join(config["schema_dir"], tap_stream_id + ".json"),
              "w") as f:
        json.dump(schema, f, indent=2)

    if out_catalog:
        schema["selected"] = True
        catalog = {"streams": [{"stream": tap_stream_id,
                                "tap_stream_id": tap_stream_id,
                                "schema": schema
                                }]}

        if not os.path.exists(config["catalog_dir"]):
            os.mkdir(config["catalog_dir"])

        with open(os.path.join(config["catalog_dir"], tap_stream_id + ".json"),
                  "w") as f:
            json.dump(catalog, f, indent=2)
