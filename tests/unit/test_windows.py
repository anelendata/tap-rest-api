import datetime
import json
import urllib.parse as urlparse

import pytest

from tap_rest_api.helper import (
    iter_window_bounds,
    get_windowed_endpoint_params,
    format_datetime,
    parse_datetime_tz,
)


def _win_end(cfg, start_str, k, size=3600):
    """The kth window's exclusive end, computed exactly as the tap does (via the
    same epoch round-trip), so assertions are independent of the host timezone."""
    start_epoch = parse_datetime_tz(start_str).timestamp()
    return format_datetime(cfg, datetime.datetime.fromtimestamp(start_epoch + k * size))


def test_windows_contiguous_and_clamped():
    # last window is clamped to the end; windows are contiguous and half-open
    assert list(iter_window_bounds(0, 10, 3)) == [(0.0, 3.0), (3.0, 6.0), (6.0, 9.0), (9.0, 10.0)]


def test_windows_exact_multiple():
    assert list(iter_window_bounds(0, 9, 3)) == [(0.0, 3.0), (3.0, 6.0), (6.0, 9.0)]


def test_windows_empty_when_start_ge_end():
    assert list(iter_window_bounds(5, 5, 3)) == []
    assert list(iter_window_bounds(7, 5, 3)) == []


def test_windows_single_when_window_larger_than_range():
    assert list(iter_window_bounds(0, 2, 3600)) == [(0.0, 2.0)]


def test_windows_cover_without_gaps_or_overlaps():
    start, end, size = 100.0, 100.0 + 3 * 3600 + 500, 3600
    windows = list(iter_window_bounds(start, end, size))
    # contiguous: each window's end is the next window's start
    assert all(windows[i][1] == windows[i + 1][0] for i in range(len(windows) - 1))
    # covers exactly [start, end]
    assert windows[0][0] == start
    assert windows[-1][1] == end


def test_windows_reject_nonpositive_size():
    with pytest.raises(ValueError):
        list(iter_window_bounds(0, 10, 0))
    with pytest.raises(ValueError):
        list(iter_window_bounds(0, 10, -5))


def test_windowed_params_datetime_bookmark():
    cfg = {
        "datetime_keys": {"orders": "modified"},
        "url_param_datetime_format": "%Y-%m-%dT%H:%M:%S.%f",
        "page_start": 0,
        "offset_start": 0,
    }
    w_start = datetime.datetime(2026, 1, 1, 0, 0, 0).timestamp()
    w_end = w_start + 3600
    params = get_windowed_endpoint_params(cfg, "orders", w_start, w_end)
    # bounds are formatted for the URL and are exactly one hour apart
    assert params["start_datetime"] == "2026-01-01T00:00:00.000000"
    assert params["end_datetime"] == "2026-01-01T01:00:00.000000"
    assert params["datetime_key"] == "modified"
    # pagination resets; last_update seeds the window start
    assert params["current_page"] == 0
    assert params["current_offset"] == 0
    assert params["last_update"] == params["start_datetime"]


def test_windowed_params_requires_time_bookmark():
    cfg = {"index_keys": {"orders": "id"}}
    with pytest.raises(ValueError):
        get_windowed_endpoint_params(cfg, "orders", 0, 3600)


def _windowing_config():
    return {
        "streams": "orders",
        "url": ("http://x/orders?page={current_page_one_base}"
                "&modified__gte={start_datetime}&modified__lt={end_datetime}"),
        "datetime_keys": {"orders": "modified"},
        "url_param_datetime_format": "%Y-%m-%dT%H:%M:%S.%f",
        "items_per_page": 100,
        "assume_sorted": False,
        "filter_by_schema": False,
        "auth_method": "no_auth",
        "page_start": 0,
        "offset_start": 0,
    }


def test_sync_windowed_checkpoints_each_drained_window(monkeypatch):
    """Three 1h windows -> three checkpoints, bookmark advancing to each window end."""
    import tap_rest_api.sync as S
    import tap_rest_api.schema as SC

    # Fake API honoring the ?modified__gte=&modified__lt= filter: one record per
    # window dated at the window start; page 2 is empty (signals the window's end).
    def fake_request(stream, endpoint, *a, **k):
        q = urlparse.parse_qs(urlparse.urlparse(endpoint).query)
        if q.get("page", ["1"])[0] != "1":
            return []
        return [{"id": 1, "modified": q["modified__gte"][0]}]

    monkeypatch.setattr(S, "generate_request", fake_request)
    monkeypatch.setattr(SC.Schema, "validate", staticmethod(lambda rec, sch: (True, None)))
    states = []
    monkeypatch.setattr(S.singer, "write_state", lambda st: states.append(json.loads(json.dumps(st))))
    monkeypatch.setattr(S.singer, "write_record", lambda *a, **k: None)

    s = S.Sync(_windowing_config(), {}, None)
    s.started_at = datetime.datetime.now()
    schema = {"type": "object",
              "properties": {"id": {"type": "integer"},
                             "modified": {"type": "string", "format": "date-time"},
                             "_sdc_extracted_at": {"type": "string", "format": "date-time"}}}
    with S.metrics.record_counter("orders") as counter:
        final = s._sync_windowed(
            {}, "orders", schema,
            "2026-01-01T00:00:00.000000", "2026-01-01T03:00:00.000000",
            "datetime", 3600, None, counter, raw_output=False)

    cfg = _windowing_config()
    bm = lambda st: st["bookmarks"]["orders"]["last_update"]
    assert len(states) == 3
    assert bm(states[0]) == _win_end(cfg, "2026-01-01T00:00:00.000000", 1)
    assert bm(states[1]) == _win_end(cfg, "2026-01-01T00:00:00.000000", 2)
    assert bm(final) == _win_end(cfg, "2026-01-01T00:00:00.000000", 3)


def test_sync_windowed_does_not_advance_past_incomplete_window(monkeypatch):
    """If a window is cut short, the bookmark stays at the last fully-drained window."""
    import tap_rest_api.sync as S

    calls = {"n": 0}

    def fake_drain(self, stream, params, schema, end, last_update, prev, counter, raw):
        calls["n"] += 1
        completed = (calls["n"] == 1)  # window 1 drains; window 2 is cut short
        return completed, last_update, prev

    monkeypatch.setattr(S.Sync, "_drain_pages", fake_drain)
    states = []
    monkeypatch.setattr(S.singer, "write_state", lambda st: states.append(json.loads(json.dumps(st))))

    s = S.Sync(_windowing_config(), {}, None)
    s.started_at = datetime.datetime.now()
    final = s._sync_windowed(
        {}, "orders", {},
        "2026-01-01T00:00:00.000000", "2026-01-01T03:00:00.000000",
        "datetime", 3600, None, None, raw_output=False)

    # only window 1 checkpointed; bookmark left at window-1 end (not leapfrogged forward)
    assert len(states) == 1
    assert (final["bookmarks"]["orders"]["last_update"]
            == _win_end(_windowing_config(), "2026-01-01T00:00:00.000000", 1))
    assert calls["n"] == 2  # tried window 2, saw it incomplete, stopped
