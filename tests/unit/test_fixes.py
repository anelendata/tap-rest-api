"""Regression tests for anelendata/tap-rest-api issues #38, #39, #40."""
import datetime

import pytest


# --- #39: assert(cond, "msg") always passed (tuple truthiness) ----------------

def test_bookmark_key_type_assert_fires_on_non_string():
    """Non-string *_key must now raise (the parenthesized asserts were dead)."""
    import tap_rest_api.helper as H
    with pytest.raises(AssertionError):
        H.get_bookmark_type_and_key({"datetime_key": 123}, "orders")
    with pytest.raises(AssertionError):
        H.get_bookmark_type_and_key({"timestamp_key": ["x"]}, "orders")
    with pytest.raises(AssertionError):
        H.get_bookmark_type_and_key({"index_key": 5}, "orders")


def test_bookmark_key_type_assert_allows_valid():
    import tap_rest_api.helper as H
    assert H.get_bookmark_type_and_key(
        {"datetime_keys": {"orders": "modified"}}, "orders") == ("datetime", "modified")
    assert H.get_bookmark_type_and_key(
        {"datetime_key": "modified", "datetime_keys": {"orders": "modified"}},
        "orders") == ("datetime", "modified")


# --- #38: get_end() must use UTC "now", not naive local now() -----------------

def test_get_end_datetime_uses_utcnow(monkeypatch):
    import tap_rest_api.helper as H
    fixed_utc = datetime.datetime(2026, 1, 1, 20, 0, 0)
    fixed_local = datetime.datetime(2026, 1, 1, 12, 0, 0)  # a non-UTC host, 8h behind

    class FakeDT(datetime.datetime):
        @classmethod
        def utcnow(cls):
            return fixed_utc
        @classmethod
        def now(cls, tz=None):
            return fixed_local

    monkeypatch.setattr(H.datetime, "datetime", FakeDT)
    cfg = {"datetime_keys": {"orders": "modified"},
           "url_param_datetime_format": "%Y-%m-%dT%H:%M:%S.%f"}
    end = H.get_end(cfg, "orders")
    assert end == "2026-01-01T20:00:00.000000"          # from utcnow()
    assert end != "2026-01-01T12:00:00.000000"          # not local now()


def test_get_end_timestamp_uses_utc_aware_now(monkeypatch):
    import tap_rest_api.helper as H
    fixed = datetime.datetime(2026, 1, 1, 20, 0, 0, tzinfo=datetime.timezone.utc)

    class FakeDT(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            # the fix must pass an explicit tz (UTC), not call a naive now()
            assert tz is not None
            return fixed

    monkeypatch.setattr(H.datetime, "datetime", FakeDT)
    cfg = {"timestamp_keys": {"orders": "ts"}}
    assert H.get_end(cfg, "orders") == fixed.timestamp()


# --- #40: unconditional record.pop(EXTRACT_TIMESTAMP) KeyError'd -------------

def test_drain_pages_no_keyerror_when_schema_omits_extract_timestamp(monkeypatch):
    """A schema without _sdc_extracted_at must not crash on the post-write pop."""
    import tap_rest_api.sync as S
    import tap_rest_api.schema as SC

    def fake_request(stream, endpoint, *a, **k):
        return [{"id": 1, "modified": "2026-01-01T00:00:00.000000"}] \
            if "page=1" in endpoint else []

    written = []
    monkeypatch.setattr(S, "generate_request", fake_request)
    monkeypatch.setattr(SC.Schema, "validate", staticmethod(lambda rec, sch: (True, None)))
    monkeypatch.setattr(S.singer, "write_record", lambda stream, rec: written.append(rec))

    cfg = {
        "streams": "orders",
        "url": "http://x/orders?page={current_page_one_base}",
        "datetime_keys": {"orders": "modified"},
        "url_param_datetime_format": "%Y-%m-%dT%H:%M:%S.%f",
        "items_per_page": 100,
        "assume_sorted": False,
        "filter_by_schema": False,
        "auth_method": "no_auth",
        "page_start": 0,
        "offset_start": 0,
    }
    s = S.Sync(cfg, {}, None)
    s.started_at = datetime.datetime.now()
    # schema intentionally has NO _sdc_extracted_at property
    schema = {"type": "object",
              "properties": {"id": {"type": "integer"},
                             "modified": {"type": "string", "format": "date-time"}}}
    params = dict(cfg, current_page=0, current_offset=0,
                  last_update="2026-01-01T00:00:00.000000")
    with S.metrics.record_counter("orders") as counter:
        completed, last_update, prev = s._drain_pages(
            "orders", params, schema, None, "2026-01-01T00:00:00.000000",
            None, counter, raw_output=False)

    assert completed is True
    assert len(written) == 1
    assert "_sdc_extracted_at" not in written[0]
