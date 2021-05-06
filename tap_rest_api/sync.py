import datetime, sys, time
import simplejson as json

import singer
import singer.metrics as metrics

from .helper import (generate_request, get_bookmark_type, get_end, get_endpoint,
                     get_init_endpoint_params, get_last_update, get_record,
                     get_record_list, get_selected_streams, get_start,
                     get_streams_to_sync, human_readable,
                     get_http_headers,
                     EXTRACT_TIMESTAMP)
from .schema import filter_record, load_schema, validate


LOGGER = singer.get_logger()


def sync_rows(config, state, tap_stream_id, key_properties=[], auth_method=None,
              max_page=None, assume_sorted=True, filter_by_schema=True,
              raw_output=False):
    """
    - max_page: Force sync to end after max_page. Mostly used for debugging.
    - assume_sorted: Trust the data to be presorted by the
                     index/timestamp/datetime keys
                     so it is safe to finish the replication once the last
                     update index/timestamp/datetime passes the end.
    """
    schema = load_schema(config["schema_dir"], tap_stream_id)
    params = get_init_endpoint_params(config, state, tap_stream_id)
    bookmark_type = get_bookmark_type(config)
    start = get_start(config, state, tap_stream_id, "last_update")
    end = get_end(config)

    headers = get_http_headers(config)

    if start is None:
        LOGGER.warning("None of timestamp_key, datetime_key, and index_key" +
                       " are set in conifg. Bookmarking is not available.")

    start_str = human_readable(bookmark_type, start)
    end_str = human_readable(bookmark_type, end)
    # Log the conditions
    LOGGER.info("Stream %s has %s set starting %s and ending %s." %
                (tap_stream_id, bookmark_type, start_str, end_str))
    # I trust you set URL format contains those params. The behavior depends
    # on the data source API's spec.
    # I will not filter out the records outside the boundary. Every record
    # received is will be written out.

    LOGGER.info("assume_sorted is set to %s" % assume_sorted)
    # I trust the data to be sorted by the index/timestamp/datetime keys.
    # So it is safe to finish the replication once the last
    # update index/timestamp/datetime passes the end.
    # When in doubt, set this to False. Always perform post-replication dedup.

    LOGGER.info("filter_by_schema is set to %s." % filter_by_schema)
    # The fields undefined/not-conforming to the schema will be written out.

    LOGGER.info("auth_method is set to %s" % auth_method)

    # Initialize the counters
    last_update = start

    # Offset is the number of records (vs. page)
    offset_number = params.get("current_offset", 0)
    page_number = params.get("current_page", 0)

    # When we rely on index/datetime/timestamp to parse the next GET URL,
    # we will get the record we have already seen in the current process.
    # When we get last_record_extracted from state file, we can also
    # compare with the previous process to further avoiding duplicated
    # records in the target data store.
    prev_written_record = None
    last_record_extracted = singer.get_bookmark(state, tap_stream_id,
                                                "last_record_extracted")
    if last_record_extracted:
        prev_written_record = json.loads(last_record_extracted)

    # First writ out the schema
    if raw_output is False:
        singer.write_schema(tap_stream_id, schema, key_properties)

    # Fetch and iterate over to write the records
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            params.update({"current_page": page_number})
            params.update({"current_page_one_base": page_number + 1})
            params.update({"current_offset": offset_number})
            params.update({"last_update": last_update})

            endpoint = get_endpoint(config["url"], tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)

            rows = generate_request(tap_stream_id, endpoint, auth_method,
                                    headers,
                                    config.get("username"),
                                    config.get("password"))
            rows = get_record_list(rows, config.get("record_list_level"))

            LOGGER.info("Current page %d" % page_number)
            LOGGER.info("Current offset %d" % offset_number)

            for row in rows:
                record = get_record(row, config.get("record_level"))
                if filter_by_schema:
                    record = filter_record(record, schema)

                    if not validate(record, schema):
                        LOGGER.debug("Skipping the schema invalidated row %s" % record)
                        continue

                # It's important to compare the record before adding
                # EXTRACT_TIMESTAMP
                if record == prev_written_record:
                    LOGGER.debug("Skipping the duplicated row %s" % record)
                    continue

                if EXTRACT_TIMESTAMP in schema["properties"].keys():
                    extract_tstamp = datetime.datetime.utcnow()
                    extract_tstamp = extract_tstamp.replace(
                        tzinfo=datetime.timezone.utc)
                    record[EXTRACT_TIMESTAMP] = extract_tstamp.isoformat()

                next_last_update = get_last_update(config, record, last_update)

                if not end or next_last_update < end:
                    if raw_output:
                        sys.stdout.write(json.dumps(record) + "\n")
                    else:
                        singer.write_record(tap_stream_id, record)

                    counter.increment()  # Increment only when we write
                    last_update = next_last_update

                    # prev_written_record may be persisted for the next run.
                    # EXTRACT_TIMESTAMP will be different. So popping it out
                    # before storing.
                    record.pop(EXTRACT_TIMESTAMP)
                    prev_written_record = record

            # Exit conditions
            if len(rows) < config["items_per_page"]:
                LOGGER.info(("Response is less than set item per page (%d)." +
                             "Finishing the extraction") %
                            config["items_per_page"])
                break
            if max_page and page_number + 1 >= max_page:
                LOGGER.info("Max page %d reached. Finishing the extraction." % max_page)
                break
            if assume_sorted and end and next_last_update >= end:
                LOGGER.info(("Record greater than %s and assume_sorted is" +
                             " set. Finishing the extraction.") % end)
                break

            page_number +=1
            offset_number += len(rows)

    state = singer.write_bookmark(state, tap_stream_id, "last_update",
                                  last_update)
    if prev_written_record:
        state = singer.write_bookmark(state, tap_stream_id,
                                      "last_record_extracted",
                                      json.dumps(prev_written_record))

    if raw_output == False:
        singer.write_state(state)

    return state


def sync(config, streams, state, catalog, assume_sorted=True, max_page=None,
         auth_method="basic", raw=False, filter_by_schema=True):
    """
    Sync the streams that were selected

    - assume_sorted: Assume the data to be sorted and exit the process as soon
      as a record having greater than end index/datetime/timestamp is detected.
    - max_page: Stop after making this number of API call is made.
    - auth_method: HTTP auth method (basic, no_auth, digest)
    - raw: Output raw JSON records to stdout
    - filter_by_schema: When True, check the extracted records against the
      schema and undefined/unmatching fields won't be written out.
    """
    start_process_at = datetime.datetime.now()
    remaining_streams = get_streams_to_sync(streams, state)
    selected_streams = get_selected_streams(remaining_streams, catalog)
    if len(selected_streams) < 1:
        raise Exception("No Streams selected, please check that you have a " +
                        "schema selected in your catalog")

    LOGGER.info("Starting sync. Will sync these streams: %s" %
                [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("%s Start sync" % stream.tap_stream_id)

        singer.set_currently_syncing(state, stream.tap_stream_id)
        if raw is False:
            singer.write_state(state)

        try:
            state = sync_rows(config, state, stream.tap_stream_id,
                              max_page=max_page,
                              auth_method=auth_method,
                              assume_sorted=assume_sorted,
                              raw_output=raw,
                              filter_by_schema=filter_by_schema)
        except Exception as e:
            LOGGER.critical(e)
            raise e

        bookmark_type = get_bookmark_type(config)
        last_update = state["bookmarks"][stream.tap_stream_id]["last_update"]
        if bookmark_type == "timestamp":
            last_update = str(last_update) + " (" + str(
                datetime.datetime.fromtimestamp(last_update)) + ")"
        LOGGER.info("%s End sync" % stream.tap_stream_id)
        LOGGER.info("%s Last record's %s: %s" %
                    (stream.tap_stream_id, bookmark_type, last_update))

    end_process_at = datetime.datetime.now()
    LOGGER.info("Completed sync at %s" % str(end_process_at))
    LOGGER.info("Process duration: " + str(end_process_at - start_process_at))
