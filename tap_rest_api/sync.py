import datetime, json, sys, time

import singer
import singer.metrics as metrics

from .helper import (generate_request, get_bookmark_type, get_end, get_endpoint,
                     get_init_endpoint_params, get_last_update, get_record,
                     get_record_list, get_selected_streams, get_start,
                     get_streams_to_sync)
from .schema import filter_result, load_schema


LOGGER = singer.get_logger()


def sync_rows(config, state, tap_stream_id, key_properties=[], auth_method=None,
              max_page=None, assume_sorted=True, filter_by_schema=True,
              raw_output=False):
    """
    - max_page: Force sync to end after max_page. Mostly used for debugging.
    - assume_sorted: Trust the data to be presorted by the index/timestamp/datetime keys
                     so it is safe to finish the replication once the last update index/timestamp/datetime
                     passes the end.
    """
    schema = load_schema(config["schema_dir"], tap_stream_id)

    if raw_output is False:
        singer.write_schema(tap_stream_id, schema, key_properties)

    bookmark_type = get_bookmark_type(config)

    start = get_start(config, state, tap_stream_id, "last_update")
    end = get_end(config)

    params = get_init_endpoint_params(config, state, tap_stream_id)

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
        LOGGER.info("""    I trust the data to be presorted by the index/timestamp/datetime keys.
So it is safe to finish the replication once the last update index/timestamp/datetime passes the end.
When in doubt, set this to False. Always perform post-replication dedup.""")

    LOGGER.info("filter_by_schema is set to %s." % filter_by_schema)
    if filter_by_schema is False:
        LOGGEr.info("   The fields undefined/not-conforming to the schema will be written out." % filter_by_schema)

    last_update = start
    page_number = 1
    offset_number = 0  # Offset is the number of records (vs. page)
    etl_tstamp = int(time.time())

    # When we rely on index/datetime/timestamp to parse the next GET URL,
    # we get the record we have already seen in the current process.
    # When we get last_record_extracted from state file, we can also
    # compare with the previous process to further avoiding duplicated
    # records in the target data store.
    prev_written_record = None
    last_record_extracted = singer.get_bookmark(state, tap_stream_id, "last_record_extracted")
    if last_record_extracted:
        prev_written_record = json.loads(last_record_extracted)

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
                record = get_record(row, config.get("record_level"))
                if filter_by_schema:
                    record = filter_result(record, schema)

                # It's important to compare the record before adding _etl_tstamp
                if record == prev_written_record:
                    LOGGER.debug("Skipping the duplicated row %s" % record)
                    continue

                if "_etl_tstamp" in schema["properties"].keys():
                    record["_etl_tstamp"] = etl_tstamp

                next_last_update = get_last_update(config, record, last_update)

                if not end or next_last_update < end:
                    if raw_output:
                        sys.stdout.write(json.dumps(record) + "\n")
                    else:
                        singer.write_record(tap_stream_id, record)
                    # For now, increment only when we write
                    counter.increment()
                    last_update = next_last_update
                    record.pop("_etl_tstamp")
                    prev_written_record = record

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

    state = singer.write_bookmark(state, tap_stream_id, "last_update", last_update)
    if prev_written_record:
        state = singer.write_bookmark(state, tap_stream_id, "last_record_extracted", json.dumps(prev_written_record))

    if raw_output == False:
        singer.write_state(state)

    return state


def sync(config, streams, state, catalog, assume_sorted=True, max_page=None,
         auth_method="basic", raw=False, filter_by_schema=True):
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
        LOGGER.info("Syncing %s", stream.tap_stream_id)


        singer.set_currently_syncing(state, stream.tap_stream_id)
        if raw is False:
            singer.write_state(state)

        try:
            state = sync_rows(config, state, stream.tap_stream_id, max_page=max_page,
                              auth_method=auth_method, assume_sorted=assume_sorted,
                              raw_output=raw, filter_by_schema=filter_by_schema)
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
