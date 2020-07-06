import datetime, json, sys, time

import singer
import singer.metrics as metrics

from .helper import (generate_request, get_bookmark_type, get_end, get_endpoint,
                     get_last_update, get_record, get_record_list, get_selected_streams,
                     get_start, get_streams_to_sync)
from .schema import filter_result, load_schema



LOGGER = singer.get_logger()


def sync_rows(config, state, tap_stream_id, key_properties=[], auth_method=None, max_page=None, assume_sorted=True):
    """
    - max_page: Force sync to end after max_page. Mostly used for debugging.
    - assume_sorted: Trust the data to be presorted by the index/timestamp/datetime keys
                     so it is safe to finish the replication once the last update index/timestamp/datetime
                     passes the end.
    """
    schema = load_schema(config["schema_dir"], tap_stream_id)
    singer.write_schema(tap_stream_id, schema, key_properties)

    bookmark_type = get_bookmark_type(config)

    start = get_start(config, state, tap_stream_id, "last_update")
    end = get_end(config)
    params = config
    if config.get("timestamp_key"):
        params.update({"start_timestamp": start})
    elif config.get("datetime_key"):
        params.update({"start_datetime": start})
    elif config.get("index_key"):
        params.update({"start_index": start})

    pretty_start = start
    pretty_end = end
    if bookmark_type == "timestamp":
        pretty_start = str(start) + " (" + str(datetime.datetime.fromtimestamp(start)) + ")"
        if end is not None:
            pretty_end = str(end) + " (" + str(datetime.datetime.fromtimestamp(end)) + ")"

    LOGGER.info("""Stream %s has %s set starting %s and ending %s.
I trust you set URL format contains those params. The behavior depends on the data source API's spec.
I will not filter out the records outside the boundary. Every record received is will be written out.
""" % (tap_stream_id, bookmark_type, pretty_start, pretty_end))

    LOGGER.info("assume_sorted is set to %s" % assume_sorted)
    if assume_sorted:
        LOGGER.info("""I trust the data to be presorted by the index/timestamp/datetime keys.
So it is safe to finish the replication once the last update index/timestamp/datetime passes the end.
When in doubt, set this to False. Always perform post-replication dedup.""")

    last_update = start
    page_number = 1
    offset_number = 0  # Offset is the number of records (vs. page)
    etl_tstamp = int(time.time())
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            params.update({"current_page": page_number})
            params.update({"current_offset": offset_number})
            params.update({"last_update": last_update})

            endpoint = get_endpoint(config["url"], tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)

            rows = generate_request(tap_stream_id, endpoint, auth_method, config["username"], config["password"])
            rows = get_record_list(rows, config.get("record_list_level"))

            LOGGER.info("Current page %d" % page_number)
            LOGGER.info("Current offset %d" % offset_number)

            for row in rows:
                counter.increment()
                row = get_record(row, config.get("record_level"))
                row = filter_result(row, schema)
                if "_etl_tstamp" in schema["properties"].keys():
                    row["_etl_tstamp"] = etl_tstamp

                next_last_update = get_last_update(config, row, last_update)

                if not end or next_last_update < end:
                    last_update = next_last_update
                    singer.write_record(tap_stream_id, row)

            if len(rows) < config["items_per_page"]:
                LOGGER.info("Response is less than set item per page (%d). Finishing the extraction" % config["items_per_page"])
                break
            if max_page and page_number + 1 > max_page:
                LOGGER.info("Max page %d reached. Finishing the extraction.")
                break
            if assume_sorted and end and next_last_update >= end:
                LOGGER.info("Record greater than %s and assume_sorted is set. Finishing the extraction." % (end))
                break
            else:
                page_number +=1
                offset_number += len(rows)

    state = singer.write_bookmark(state, tap_stream_id, 'last_update', last_update)
    singer.write_state(state)
    return state


def output_raw_records(config, state, tap_stream_id, key_properties=[], auth_method=None, max_page=None):
    """
    Write out the raw JSON output at the record list level to stdout
    """
    start = get_start(config, state, tap_stream_id, "last_update")
    end = get_end(config)

    pretty_start = start
    pretty_end = end
    last_update = start
    page_number = 1
    offset_number = 0  # Offset is the number of records (vs. page)
    etl_tstamp = int(time.time())
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            params = config
            params.update({"current_page": page_number})
            params.update({"current_offset": offset_number})
            endpoint = get_endpoint(config["url"], tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)
            rows = generate_request(tap_stream_id, endpoint, auth_method, config["username"], config["password"])
            rows = get_record_list(rows, config.get("record_list_level"))
            for row in rows:
                counter.increment()
                row = get_record(row, config.get("record_level"))
                last_update = get_last_update(config, row, last_update)

                sys.stdout.write(json.dumps(row) + "\n")

            LOGGER.info("Current page %d" % page_number)
            LOGGER.info("Current offset %d" % offset_number)

            if len(rows) == 0 or (max_page and page_number + 1 > max_page):
                break
            else:
                page_number +=1
                offset_number += len(rows)


def sync(config, streams, state, catalog, assume_sorted=True, max_page=None, auth_method="basic", raw=False):
    """
    Sync the streams that were selected

    raw: Output raw JSON records to stdout
    """
    start_process_at = datetime.datetime.now()
    remaining_streams = get_streams_to_sync(streams, state)
    selected_streams = get_selected_streams(remaining_streams, catalog)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        if raw:
            # Output raw JSON records only
            output_raw_records(config, state, stream.tap_stream_id, max_page=max_page, auth_method=auth_method)
            continue

        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(state, stream.tap_stream_id)
        singer.write_state(state)

        try:
            state = sync_rows(config, state, stream.tap_stream_id, max_page=max_page, auth_method=auth_method, assume_sorted=assume_sorted)
        except Exception as e:
            LOGGER.critical(e)
            raise e

        bookmark_type = get_bookmark_type(config)
        last_update = state["bookmarks"][stream.tap_stream_id]["last_update"]
        if bookmark_type == "timestamp":
            last_update = str(last_update) + " (" + str(datetime.datetime.fromtimestamp(last_update)) + ")"
        LOGGER.info("End sync " + stream.tap_stream_id)
        LOGGER.info("Last record's %s: %s" % (bookmark_type, last_update))

    end_process_at = datetime.datetime.now()
    LOGGER.info("Completed sync at %s" % str(end_process_at))
    LOGGER.info("Process duration: " + str(end_process_at - start_process_at))
