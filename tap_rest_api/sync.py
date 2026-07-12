import datetime
import simplejson as json
import sys
import time

import singer
import singer.metrics as metrics

from .helper import (
    get_streams,
    generate_request,
    get_bookmark_type_and_key,
    get_end,
    get_endpoint,
    get_init_endpoint_params,
    get_last_update,
    get_float_timestamp,
    get_record,
    get_record_list,
    get_selected_streams,
    get_start,
    get_streams_to_sync,
    human_readable,
    get_http_headers,
    get_digest_from_record,
    unnest,
    EXTRACT_TIMESTAMP,
    format_datetime,
    parse_datetime_tz,
    get_windowed_endpoint_params,
    iter_window_bounds,
    get_window_seconds,
)
from .schema import Schema


LOGGER = singer.get_logger()


class Sync(object):
    config = None
    streams = None

    def __init__(self, config, state, catalog):
        self.config = config
        self.state = state
        self.catalog = catalog
        self.streams = get_streams(config)

    def sync_rows(self, current_state, tap_stream_id, key_properties=[], raw_output=False):
        """
        - max_page: Force sync to end after max_page. Mostly used for debugging.
        - assume_sorted: Trust the data to be presorted by the
                        index/timestamp/datetime keys
                        so it is safe to finish the replication once the last
                        update index/timestamp/datetime passes the end.
        """
        max_page = self.config.get("max_page")
        global_timeout = self.config.get("global_timeout")
        auth_method = self.config.get("auth_method", "basic")
        assume_sorted = self.config.get("assume_sorted", True)
        filter_by_schema = self.config.get("filter_by_schema", True)

        schema_service = Schema(self.config)
        schema = schema_service.load_schema(tap_stream_id)
        params = get_init_endpoint_params(self.config, current_state, tap_stream_id)

        dt_keys = self.config.get("datetime_keys")
        if isinstance(dt_keys, str):
            raise Exception(f"{tap_stream_id}: {dt_keys}, {self.config}")
        i_keys = self.config.get("index_keys")
        if isinstance(i_keys, str):
            raise Exception(f"{tap_stream_id}: {i_keys}, {self.config}")

        bookmark_type, _ = get_bookmark_type_and_key(self.config, tap_stream_id)

        on_invalid_property = self.config.get("on_invalid_property", "force")
        drop_unknown_properties = self.config.get("drop_unknown_properties", False)

        start = get_start(self.config, current_state, tap_stream_id, "last_update")
        end = get_end(self.config, tap_stream_id)

        headers = get_http_headers(self.config)

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
        next_last_update = None

        # Offset is the number of records (vs. page)
        offset_number = params.get("current_offset", 0)
        page_number = params.get("current_page", 0)

        # When we rely on index/datetime/timestamp to parse the next GET URL,
        # we will get the record we have already seen in the current process.
        # When we get last_record_extracted from state file, we can also
        # compare with the previous process to further avoiding duplicated
        # records in the target data store.
        prev_written_record = None
        last_record_extracted = singer.get_bookmark(current_state, tap_stream_id,
                                                    "last_record_extracted")
        if last_record_extracted:
            prev_written_record = json.loads(last_record_extracted)

        # First write out the schema
        if raw_output is False:
            singer.write_schema(tap_stream_id, schema, key_properties)

        window_seconds = get_window_seconds(self.config, tap_stream_id)

        # Fetch and iterate over to write the records
        with metrics.record_counter(tap_stream_id) as counter:
            if (window_seconds and bookmark_type in ("datetime", "timestamp")
                    and start is not None and end is not None):
                current_state = self._sync_windowed(
                    current_state, tap_stream_id, schema, start, end, bookmark_type,
                    window_seconds, prev_written_record, counter, raw_output)
            else:
                completed, last_update, prev_written_record = self._drain_pages(
                    tap_stream_id, params, schema, end, last_update,
                    prev_written_record, counter, raw_output)
                # Not windowing: advance the bookmark to the max value seen (legacy behavior).
                if bookmark_type == "timestamp" and len(str(int(last_update))) == 10:
                    last_update = int(last_update * 1000)
                current_state = singer.write_bookmark(
                    current_state, tap_stream_id, "last_update", last_update)
                if prev_written_record:
                    current_state = singer.write_bookmark(
                        current_state, tap_stream_id, "last_record_extracted",
                        json.dumps(prev_written_record))
                if raw_output is False:
                    singer.write_state(current_state)

        return current_state

    def _drain_pages(self, tap_stream_id, params, schema, end, last_update,
                     prev_written_record, counter, raw_output):
        """Paginate a single query (one window, or the whole range when not windowing)
        to exhaustion, writing every record fetched.

        Returns (completed, last_update, prev_written_record). ``completed`` is True
        only when the API signalled the natural end of data (a short/last page), or,
        with assume_sorted, when the sort passed ``end``. It is False when the run was
        cut short by global_timeout or max_page -- in which case the caller must NOT
        advance the bookmark past records that were never fetched.
        """
        max_page = self.config.get("max_page")
        global_timeout = self.config.get("global_timeout")
        auth_method = self.config.get("auth_method", "basic")
        assume_sorted = self.config.get("assume_sorted", True)
        filter_by_schema = self.config.get("filter_by_schema", True)
        on_invalid_property = self.config.get("on_invalid_property", "force")
        drop_unknown_properties = self.config.get("drop_unknown_properties", False)
        headers = get_http_headers(self.config)

        page_number = params.get("current_page", 0)
        offset_number = params.get("current_offset", 0)
        next_last_update = None
        completed = False

        while True:
            if (self.started_at and global_timeout and
                datetime.datetime.now() - self.started_at >= datetime.timedelta(seconds=global_timeout)):
                LOGGER.warning(f"Timeout {global_timeout} reached. Not doing further sync.")
                break

            params.update({"current_page": page_number})
            params.update({"current_page_one_base": page_number + 1})
            params.update({"current_offset": offset_number})
            params.update({"last_update": last_update})

            url = self.config.get("urls", {}).get(tap_stream_id, self.config["url"])
            endpoint = get_endpoint(url, tap_stream_id, params)
            LOGGER.info("GET %s", endpoint)

            rows = []
            try:
                rows = generate_request(tap_stream_id, endpoint, auth_method,
                                        headers,
                                        self.config.get("username"),
                                        self.config.get("password"))
            except Exception as e:
                if page_number == self.config.get("page_start", 0):
                    raise
                LOGGER.error(f"Endpoint responded with an error: {str(e)}")

            # In case the record is not at the root level
            record_list_level = self.config.get("record_list_level")
            if isinstance(record_list_level, dict):
                record_list_level = record_list_level.get(tap_stream_id)
            record_level = self.config.get("record_level")
            if isinstance(record_level, dict):
                record_level = record_level.get(tap_stream_id)

            rows = get_record_list(rows, record_list_level)
            if not isinstance(rows, list):
                rows = [rows]

            LOGGER.info("Current page %d" % page_number)
            LOGGER.info("Current offset %d" % offset_number)

            LOGGER.debug("    Row process started.")
            row_process_started_at = datetime.datetime.now()
            for row in rows:
                record = get_record(row, record_level)

                unnest_config = self.config.get("unnest", {})
                if unnest_config is None:
                    unnest_config = {}
                unnest_cols = unnest_config.get(tap_stream_id, [])
                for u in unnest_cols:
                    record = unnest(record, u["path"], u["target"])

                if filter_by_schema:
                    record = Schema.filter_record(
                            record,
                            schema,
                            on_invalid_property=on_invalid_property,
                            drop_unknown_properties=drop_unknown_properties,
                            )

                valid, reason = Schema.validate(record, schema)
                if not valid:
                    LOGGER.warning(f"Skipping the schema invalidated (Reason: {reason}) row:\n  {json.dumps(record)}\n\n")
                    continue

                # It's important to compare the record before adding EXTRACT_TIMESTAMP
                digest = get_digest_from_record(record)
                digest_dict = {"digest": digest}
                if (prev_written_record == record or
                        prev_written_record == digest_dict):
                    LOGGER.info(
                        "Skipping the duplicated row with "
                        f"digest {digest}"
                    )
                    continue

                if EXTRACT_TIMESTAMP in schema["properties"].keys():
                    extract_tstamp = datetime.datetime.utcnow()
                    extract_tstamp = extract_tstamp.replace(
                        tzinfo=datetime.timezone.utc)
                    record[EXTRACT_TIMESTAMP] = extract_tstamp.isoformat()

                try:
                    next_last_update = get_last_update(self.config, tap_stream_id, record, last_update)
                except Exception as e:
                    LOGGER.error(f"Error with the record:\n    {row}\n    message: {e}")
                    raise

                if not end or next_last_update < end:
                    if raw_output:
                        sys.stdout.write(json.dumps(record) + "\n")
                    else:
                        singer.write_record(tap_stream_id, record)

                    counter.increment()  # Increment only when we write
                    last_update = next_last_update

                    # prev_written_record may be persisted for the next run.
                    # EXTRACT_TIMESTAMP will be different. So popping it out before storing.
                    # It is only added when the schema declares it, so pop with a
                    # default to avoid KeyError when the schema omits it.
                    record.pop(EXTRACT_TIMESTAMP, None)
                    digest = get_digest_from_record(record)
                    prev_written_record = {"digest": digest}

            row_process_sec = datetime.datetime.now() - row_process_started_at
            LOGGER.debug(f"    row process completed in {row_process_sec} seconds.")

            # Exit conditions
            if len(rows) < self.config["items_per_page"]:
                LOGGER.info(("Response is less than set item per page (%d)." +
                            "Finishing the extraction") %
                            self.config["items_per_page"])
                completed = True
                break
            if max_page and page_number + 1 >= max_page:
                LOGGER.info("Max page %d reached. Finishing the extraction." % max_page)
                break
            if assume_sorted and end and (next_last_update and next_last_update >= end):
                LOGGER.info(("Record greater than %s and assume_sorted is" +
                            " set. Finishing the extraction.") % end)
                completed = True
                break

            page_number += 1
            offset_number += len(rows)

        return completed, last_update, prev_written_record

    def _sync_windowed(self, current_state, tap_stream_id, schema, start, end,
                       bookmark_type, window_seconds, prev_written_record,
                       counter, raw_output):
        """Replicate in contiguous, fully-drained time windows.

        The bookmark is checkpointed to a window's (exclusive) upper bound only after
        that window is completely drained. A window cut short by a timeout leaves the
        bookmark at the last completed boundary, so the next run resumes there -- no
        leapfrog past unfetched records, no silent loss. Requires the URL to bound
        both ends of the bookmark field, e.g.
        ...__gte={start_datetime}&...__lt={end_datetime}.
        """
        if bookmark_type == "timestamp":
            start_epoch = get_float_timestamp(start)
            end_epoch = get_float_timestamp(end)
        else:  # datetime
            start_epoch = parse_datetime_tz(start).timestamp()
            end_epoch = parse_datetime_tz(end).timestamp()

        for w_start, w_end in iter_window_bounds(start_epoch, end_epoch, window_seconds):
            params = get_windowed_endpoint_params(self.config, tap_stream_id, w_start, w_end)
            # Exclusive upper bound in the bookmark's native format, used both as the
            # checkpoint value and as the per-record write-gate (keeps windows half-open
            # even if the URL uses an inclusive __lte).
            if bookmark_type == "timestamp":
                gate_end = params["end_timestamp"]
                checkpoint = params["end_timestamp"]
            else:
                gate_end = params["end_datetime"]
                checkpoint = params["end_datetime"]

            LOGGER.info("Window %s [%s, %s)" %
                        (tap_stream_id, params["start_datetime"], params["end_datetime"]))

            completed, _last_update, prev_written_record = self._drain_pages(
                tap_stream_id, params, schema, gate_end, params["last_update"],
                prev_written_record, counter, raw_output)

            if not completed:
                LOGGER.warning(
                    "Window ending %s did not fully drain (timeout/max_page). Leaving "
                    "the bookmark at the last completed window and stopping so the next "
                    "run resumes here." % checkpoint)
                break

            if bookmark_type == "timestamp" and len(str(int(checkpoint))) == 10:
                checkpoint = int(checkpoint * 1000)
            current_state = singer.write_bookmark(
                current_state, tap_stream_id, "last_update", checkpoint)
            if prev_written_record:
                current_state = singer.write_bookmark(
                    current_state, tap_stream_id, "last_record_extracted",
                    json.dumps(prev_written_record))
            if raw_output is False:
                singer.write_state(current_state)
            LOGGER.info("Checkpoint: window drained; bookmark advanced to %s" % checkpoint)

        return current_state


    def sync(self, raw=False):
        """
        Sync the streams that were selected

        - max_page: Stop after making this number of API call is made.
        - assume_sorted: Assume the data to be sorted and exit the process as soon
        as a record having greater than end index/datetime/timestamp is detected.
        - auth_method: HTTP auth method (basic, no_auth, digest)
        - filter_by_schema: When True, check the extracted records against the
        schema and undefined/unmatching fields won't be written out.
        - raw: Output raw JSON records to stdout
        """
        global_timeout = self.config.get("global_timeout")
        if global_timeout:
            LOGGER.info(f"Global timeout is set {global_timeout} seconds.")

        dt_keys = self.config.get("datetime_keys")
        if isinstance(dt_keys, str):
            raise Exception(f"Invalid datetime_keys in config: {dt_keys}")
        i_keys = self.config.get("index_keys")
        if isinstance(i_keys, str):
            raise Exception(f"Invalid index_keys in config: {i_keys}")

        self.started_at = datetime.datetime.now()
        remaining_streams = get_streams_to_sync(self.streams, self.state)
        selected_streams = get_selected_streams(remaining_streams, self.catalog)

        if len(selected_streams) < 1:
            raise Exception("No Streams selected, please check that you have a " +
                            "schema selected in your catalog")

        LOGGER.info("Starting sync. Will sync these streams: %s" %
                    [stream.tap_stream_id for stream in selected_streams])

        if not self.state.get("bookmarks"):
            self.state["bookmarks"] = {}
        for stream in selected_streams:
            LOGGER.info("%s Start sync" % stream.tap_stream_id)

            current_state = dict(self.state)
            singer.set_currently_syncing(current_state, stream.tap_stream_id)
            if raw is False:
                singer.write_state(current_state)

            try:
                self.sync_rows(current_state, stream.tap_stream_id, raw_output=raw)
            except Exception as e:
                LOGGER.critical(e)
                raise e

            if not self.state["bookmarks"].get(stream.tap_stream_id):
                self.state["bookmarks"][stream.tap_stream_id] = current_state["bookmarks"][stream.tap_stream_id]
            else:
                self.state["bookmarks"][stream.tap_stream_id].update(
                    current_state["bookmarks"][stream.tap_stream_id])
            if raw is False:
                singer.write_state(self.state)

            bookmark_type, _ = get_bookmark_type_and_key(self.config, stream.tap_stream_id)
            last_update = self.state["bookmarks"][stream.tap_stream_id]["last_update"]
            if bookmark_type == "timestamp":
                last_update = str(last_update) + " (" + str(
                    datetime.datetime.fromtimestamp(get_float_timestamp(last_update))) + ")"
            LOGGER.info("%s End sync" % stream.tap_stream_id)
            LOGGER.info("%s Last record's %s: %s" %
                        (stream.tap_stream_id, bookmark_type, last_update))

        ended_at = datetime.datetime.now()
        LOGGER.info("Completed sync at %s" % str(ended_at))
        LOGGER.info("Process duration: " + str(ended_at - self.started_at))


def sync(config, state, catalog, raw=False):
    sync_service = Sync(config, state, catalog)
    sync_service.sync()
