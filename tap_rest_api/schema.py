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
    return getschema.fix_type(
        row,
        schema,
        on_invalid_property=on_invalid_property,
        date_to_datetime=True,
    )


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


def infer_schema(config, streams, out_catalog=True, add_tstamp=True, max_page=None):
    """
    Infer schema from the sample record list and write JSON schema and
    catalog files under schema directory and catalog directory.
    To fully support multiple streams, the catalog files must be consolidated
    but that is not supported in this function yet.
    """
    schemas = {}
    for stream in list(streams.keys()):
        tap_stream_id = streams[stream].tap_stream_id

        params = get_init_endpoint_params(config, {}, tap_stream_id)

        url = config.get("urls", {}).get(tap_stream_id, config["url"])
        auth_method = config.get("auth_method", "basic")
        headers = get_http_headers(config)
        records = []
        page_number = 0
        offset_number = 0
        while True:
            params.update({"current_page": page_number})
            params.update({"current_page_one_base": page_number + 1})
            params.update({"current_offset": offset_number})

            endpoint = get_endpoint(url, tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)
            data = generate_request(tap_stream_id, endpoint, auth_method,
                                    headers,
                                    config.get("username"),
                                    config.get("password"))

            # In case the record is not at the root level
            record_list_level = config.get("record_list_level")
            if isinstance(record_list_level, dict):
                record_list_level = record_list_level.get(stream)
            record_level = config.get("record_level")
            if isinstance(record_level, dict):
                record_level = record_level.get(stream)
            data = get_record_list(data, record_list_level)
            records += data

            # Exit conditions
            if len(data) < config["items_per_page"]:
                LOGGER.info(("Response is less than set item per page (%d)." +
                             "Finishing the extraction") %
                            config["items_per_page"])
                break
            if max_page and page_number + 1 >= max_page:
                LOGGER.info("Max page %d reached. Finishing the extraction." % max_page)
                break

            page_number +=1
            offset_number += len(data)


        schema = getschema.infer_schema(records, record_level)

        if add_tstamp:
            timestamp_format = {"type": ["null", "string"],
                                "format": "date-time"}
            schema["properties"][EXTRACT_TIMESTAMP] = timestamp_format
            schema["properties"][BATCH_TIMESTAMP] = timestamp_format

        if not os.path.exists(config["schema_dir"]):
            os.mkdir(config["schema_dir"])

        schemas[tap_stream_id] = schema
        with open(os.path.join(config["schema_dir"], tap_stream_id + ".json"),
                  "w") as f:
            json.dump(schema, f, indent=2)

    if not out_catalog:
        return

    catalog = {"streams": []}
    for stream in list(streams.keys()):
        tap_stream_id = streams[stream].tap_stream_id
        schema = schemas[tap_stream_id]
        schema["selected"] = True
        catalog["streams"].append({
            "stream": tap_stream_id,
            "tap_stream_id": tap_stream_id,
            "schema": schema,
        })

    if not os.path.exists(config["catalog_dir"]):
        os.mkdir(config["catalog_dir"])

    with open(os.path.join(config["catalog_dir"], "catalog.json"), "w") as f:
        json.dump(catalog, f, indent=2)
