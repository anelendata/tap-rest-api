import dateutil
import os
import sys
import simplejson as json
import singer

from singer import utils

from .helper import (generate_request, get_endpoint, get_init_endpoint_params,
                     get_record, get_record_list, get_http_headers, unnest,
                     EXTRACT_TIMESTAMP, BATCH_TIMESTAMP)
import getschema
import jsonschema

LOGGER = singer.get_logger()


class Schema(object):
    config = None

    def __init__(self, config):
        self.config = config

    @staticmethod
    def validate(record, schema):
        try:
            jsonschema.validate(record, schema)
        except jsonschema.exceptions.ValidationError:
            return False
        return True

    @staticmethod
    def filter_record(
        row,
        schema,
        on_invalid_property="force",
        drop_unknown_properties=False):
        """
        Parse the result into types
        """
        try:
            cleaned = getschema.fix_type(
                row,
                schema,
                on_invalid_property=on_invalid_property,
                drop_unknown_properties=drop_unknown_properties,
                date_to_datetime=True,
            )
        except Exception as e:
            LOGGER.debug(row)
            raise e
        return cleaned

    @staticmethod
    def safe_update(old_schema, new_schema, lock_obj=True):
        """
        lock_obj: When true, the sub-item will not be modified recursively.
        """
        lock_at = None
        if lock_obj:
            lock_at = 1

        def get(d, path):
            cur = d
            for key in path:
                cur  = cur.get(key)
                if not isinstance(cur, dict):
                    return cur
            return cur

        def update(d, path, value, force=False):
            cur = d
            par = None
            level = 0
            for key in path:
                level += 1
                if not isinstance(cur, dict):
                    raise Exception(".".join(path[0:level - 1]) + " is not a dict.")
                if not cur.get(key):
                    if level == len(path):  # leaf
                        cur[key] = value
                        # LOGGER.debug("added " + ".".join(path))
                        return
                    # Not a leaf, add a dict
                    cur[key] = dict()
                if force and not isinstance(cur[key], dict):
                    cur[key] = dict()
                par = cur
                cur = cur[key]
            LOGGER.debug("updated " + ".".join(path))
            par[path[-1]] = value

        def get_all_paths(d, path=[], level=None):
            for key, value in d.items():
                LOGGER.debug(f"key {key} level {len(path)}/{level}")
                if not isinstance(value, dict):
                    yield path + [key]
                    continue
                if level is None:
                    yield from get_all_paths(value, path=path + [key], level=None)
                    continue
                if len(path) == level:
                    yield path + [key]
                    continue
                elif len(path) < level:
                    yield from get_all_paths(value, path=path + [key], level=level)
        
        safe_schema = dict(new_schema)
        new_paths = list(get_all_paths(safe_schema, level=lock_at))
        old_paths = list(get_all_paths(old_schema, level=lock_at))

        # for path in new_paths:
        #     LOGGER.debug(".".join(path))

        added = 0
        for path in new_paths:
            if path not in old_paths:
                added += 1
                continue
            o = get(old_schema, path)
            n = get(new_schema, path)
            if (
                (not isinstance(o, dict) and o != n) or
                (isinstance(o, dict) and o.get("type") != "object" and o!= n) or
                (isinstance(o, dict) and o.get("type") == "object" and o!= n and lock_obj)
            ):
                LOGGER.warning(" Found a modified entry, but not changing at " + ".".join(path))
                update(safe_schema, path, get(old_schema, path), force=True)

        for path in old_paths:
            if path not in new_paths:
                LOGGER.warning(" Found a deleted entry, but not changing at " + ".".join(path))
                update(safe_schema, path, get(old_schema, path), force=True)

        if (added == 0):
            LOGGER.warning(" No new field has been added.")

        return safe_schema

    def load_schema(self, stream_id):
        schema_dir = self.config["schema_dir"]
        '''Returns the schema for the specified source'''
        schema = utils.load_json(os.path.join(schema_dir, stream_id + ".json"))
        return schema

    def load_discovered_schema(self, stream):
        '''Attach inclusion automatic to each schema'''
        schema_dir = self.config["schema_dir"]
        schema = self.load_schema(stream.tap_stream_id)
        for k in schema['properties']:
            schema['properties'][k]['inclusion'] = 'automatic'
        return schema

    def discover_schemas(self, streams):
        '''Iterate through streams, push to an array and return'''
        schema_dir = self.config["schema_dir"]
        result = {'streams': []}
        for key in streams.keys():
            stream = streams[key]
            LOGGER.info('Loading schema for %s', stream.tap_stream_id)
            result['streams'].append({'stream': stream.tap_stream_id,
                                    'tap_stream_id': stream.tap_stream_id,
                                    'schema': self.load_discovered_schema(stream)})
        return result

    def infer_schema(self, stream_id):
        max_page = self.config.get("max_page")
        sample_dir = self.config.get("sample_dir")

        params = get_init_endpoint_params(self.config, {}, stream_id)

        url = self.config.get("urls", {}).get(stream_id, self.config["url"])
        auth_method = self.config.get("auth_method", "basic")
        headers = get_http_headers(self.config)

        records = []
        page_number = params.get("page_start", 0)
        offset_number = params.get("offset_start", 0)
        while True:
            params.update({"current_page": page_number})
            params.update({"current_page_one_base": page_number + 1})
            params.update({"current_offset": offset_number})

            if sample_dir:
                LOGGER.info("Reading the data from file")
                with open(os.path.join(sample_dir, stream_id + ".json"), 'r') as file:
                    data = json.load(file)
            else:
                endpoint = get_endpoint(url, stream_id, params)
                LOGGER.info("GET %s", endpoint)
                data = generate_request(stream_id, endpoint, auth_method,
                                        headers,
                                        self.config.get("username"),
                                        self.config.get("password"))

            # In case the record is not at the root level
            record_list_level = self.config.get("record_list_level")
            if isinstance(record_list_level, dict):
                record_list_level = record_list_level.get(stream)
            record_level = self.config.get("record_level")
            if isinstance(record_level, dict):
                record_level = record_level.get(stream)
            data = get_record_list(data, record_list_level)

            unnest_cols = self.config.get("unnest", {}).get(stream_id, [])
            if unnest_cols:
                for i in range(0, len(data)):
                    for u in unnest_cols:
                        LOGGER.info(f"Unnesting {u['path']} to {u['target']}")
                        data[i] = unnest(data[i], u["path"], u["target"])

            records += data

            # Exit conditions
            if sample_dir:
                break
            if len(data) < self.config["items_per_page"]:
                LOGGER.info(("Response is less than set item per page (%d)." +
                            "Finishing the extraction") %
                            self.config["items_per_page"])
                break
            if max_page and page_number + 1 >= max_page:
                LOGGER.info("Max page %d reached. Finishing the extraction." % max_page)
                break

            page_number +=1
            offset_number += len(data)


        schema = getschema.infer_schema(records, record_level)

        return schema


def discover(config, streams):
    """
    JSON dump the schemas to stdout
    """
    LOGGER.info("Loading Schemas")
    schema_service = Schema(config)
    json_str = schema_service.discover_schemas(streams)
    json.dump(json_str, sys.stdout, indent=2)


def infer_schema(
    config,
    streams,
    out_catalog=True,
    add_tstamp=True,
    safe_update=True,
    ):
    """
    Infer schema from the sample record list and write JSON schema and
    catalog files under schema directory and catalog directory.

    - safe_update: When schema_dir contains existing schema and safe_update = True, it will only modify the exiting schema with append manner.
    """
    schema_service = Schema(config)
    schemas = {}
    for stream in list(streams.keys()):
        tap_stream_id = streams[stream].tap_stream_id
        LOGGER.info(f"Processing {tap_stream_id}...")
        schema = schema_service.infer_schema(tap_stream_id)

        if not os.path.exists(config["schema_dir"]):
            os.mkdir(config["schema_dir"])

        if add_tstamp:
            timestamp_format = {"type": ["null", "string"],
                                "format": "date-time"}
            schema["properties"][EXTRACT_TIMESTAMP] = timestamp_format
            schema["properties"][BATCH_TIMESTAMP] = timestamp_format

        if safe_update and os.path.exists(os.path.join(config["schema_dir"], tap_stream_id + ".json")):
            cur_schema = schema_service.load_schema(tap_stream_id)
            safe_schema = schema_service.safe_update(cur_schema, schema)
            schemas[tap_stream_id] = safe_schema
        else:
            schemas[tap_stream_id] = schema


    for stream in list(streams.keys()):
        tap_stream_id = streams[stream].tap_stream_id
        with open(os.path.join(config["schema_dir"], tap_stream_id + ".json"),
                "w") as f:
            json.dump(schemas[tap_stream_id], f, indent=2)

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
