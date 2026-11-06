"""Cursor paging on a datetime bookmark: `start={last_update}&page=1` (issue #45).

The fake API sorts by `modified`, includes records at exactly `start` (like an
inclusive `start_datetime` filter) and pages by page number. Records can be
changed between requests to reproduce a list that shifts while the tap reads it.
"""
import datetime
import urllib.parse as urlparse

import pytest

from tap_rest_api.helper import get_init_endpoint_params, parse_datetime_tz

T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
PAGE_URL = "http://x/orders?sort=modified&start={start_datetime}&per_page={items_per_page}&page={current_page_one_base}"
CURSOR_URL = "http://x/orders?sort=modified&start={last_update}&per_page={items_per_page}&page=1"


def ts(sec):
    return (T0 + datetime.timedelta(seconds=sec)).isoformat()


class FakeApi:
    def __init__(self, records, per_page, on_request=None):
        self.records = {r["id"]: dict(r) for r in records}
        self.per_page = per_page
        self.on_request = on_request  # called with (api, request number) before serving
        self.requests = []

    def __call__(self, stream, endpoint, *a, **k):
        if self.on_request:
            self.on_request(self, len(self.requests))
        self.requests.append(endpoint)
        q = urlparse.parse_qs(urlparse.urlparse(endpoint).query)
        start = parse_datetime_tz(q["start"][0])
        page = int(q["page"][0])
        rows = sorted((r for r in self.records.values() if parse_datetime_tz(r["modified"]) >= start),
                      key=lambda r: (r["modified"], r["id"]))
        return [dict(r) for r in rows[(page - 1) * self.per_page: page * self.per_page]]

    def touch(self, rid, sec):
        """The record is updated: it moves to the end of the sorted list."""
        self.records[rid]["modified"] = ts(sec)


def config(url, per_page):
    return {
        "streams": "orders",
        "url": url,
        "datetime_keys": {"orders": "modified"},
        "start_datetime": ts(0),
        "items_per_page": per_page,
        "assume_sorted": True,
        "filter_by_schema": False,
        "auth_method": "no_auth",
        "page_start": 0,
        "offset_start": 0,
    }


def drain(monkeypatch, cfg, api, end=None):
    import tap_rest_api.sync as S
    import tap_rest_api.schema as SC

    written = []
    monkeypatch.setattr(S, "generate_request", api)
    monkeypatch.setattr(SC.Schema, "validate", staticmethod(lambda rec, sch: (True, None)))
    monkeypatch.setattr(S.singer, "write_record", lambda stream, rec: written.append(rec))
    s = S.Sync(cfg, {}, None)
    s.started_at = datetime.datetime.now()
    schema = {"type": "object", "properties": {"id": {"type": "integer"}, "modified": {"type": "string"}}}
    params = get_init_endpoint_params(cfg, {}, "orders")
    with S.metrics.record_counter("orders") as counter:
        completed, last_update, _ = s._drain_pages(
            "orders", params, schema, end or ts(10_000), params["last_update"], None, counter, raw_output=False)
    return completed, last_update, written


def versions(written):
    return [(r["id"], r["modified"]) for r in written]


def records(n, sec_of=lambda i: i):
    return [{"id": i, "modified": ts(sec_of(i))} for i in range(n)]


def shift_after_first_page(api, n):
    # After page 1 is served, record 0 (already read) is updated and moves to the end.
    if n == 1:
        api.touch(0, 500)


def test_page_paging_skips_a_record_when_the_list_shifts(monkeypatch):
    """The problem: record 3 moves onto page 1 after page 1 was read, and is never read."""
    api = FakeApi(records(9), per_page=3, on_request=shift_after_first_page)
    completed, _, written = drain(monkeypatch, config(PAGE_URL, 3), api)
    assert completed
    assert 3 not in {i for i, _ in versions(written)}


def test_cursor_paging_reads_every_record_when_the_list_shifts(monkeypatch):
    api = FakeApi(records(9), per_page=3, on_request=shift_after_first_page)
    completed, last_update, written = drain(monkeypatch, config(CURSOR_URL, 3), api)
    assert completed
    got = versions(written)
    assert {i for i, _ in got} == set(range(9))
    assert (0, ts(500)) in got          # the moved record's new version is read too
    assert len(got) == len(set(got))    # nothing written twice
    assert parse_datetime_tz(last_update) == parse_datetime_tz(ts(500))
    # every request is page 1, starting at the last record written
    assert all("page=1" in u for u in api.requests)


def test_cursor_paging_reads_a_tie_split_across_pages_once(monkeypatch):
    """Records 2-4 share one second, across the page boundary at 4."""
    api = FakeApi(records(10, lambda i: 2 if 2 <= i <= 4 else i), per_page=4)
    completed, _, written = drain(monkeypatch, config(CURSOR_URL, 4), api)
    assert completed
    got = versions(written)
    assert sorted(i for i, _ in got) == list(range(10))
    assert len(got) == len(set(got))


def test_cursor_paging_with_unstable_tie_order(monkeypatch):
    """The API returns tied records in a different order on each request."""
    class Shuffling(FakeApi):
        # Still sorted by modified, but ties come back in the opposite order on every
        # other request.
        def __call__(self, stream, endpoint, *a, **k):
            self.requests.append(endpoint)
            q = urlparse.parse_qs(urlparse.urlparse(endpoint).query)
            start, page = parse_datetime_tz(q["start"][0]), int(q["page"][0])
            sign = 1 if len(self.requests) % 2 else -1
            rows = sorted((r for r in self.records.values() if parse_datetime_tz(r["modified"]) >= start),
                          key=lambda r: (r["modified"], sign * r["id"]))
            return [dict(r) for r in rows[(page - 1) * self.per_page: page * self.per_page]]

    api = Shuffling(records(12, lambda i: 3 if 3 <= i <= 6 else i), per_page=5)
    completed, _, written = drain(monkeypatch, config(CURSOR_URL, 5), api)
    assert completed
    assert sorted(i for i, _ in versions(written)) == list(range(12))
    assert len(written) == 12


def test_cursor_paging_raises_when_a_full_page_shares_one_value(monkeypatch):
    api = FakeApi(records(8, lambda i: 1 if i <= 4 else i), per_page=3)
    with pytest.raises(RuntimeError, match="more than items_per_page"):
        drain(monkeypatch, config(CURSOR_URL, 3), api)
    # it stops before repeating the request
    assert len(set(api.requests)) == len(api.requests)


def test_cursor_paging_stops_at_end(monkeypatch):
    api = FakeApi(records(10), per_page=3)
    completed, last_update, written = drain(monkeypatch, config(CURSOR_URL, 3), api, end=ts(5))
    assert completed
    assert [i for i, _ in versions(written)] == [0, 1, 2, 3, 4]
    assert parse_datetime_tz(last_update) == parse_datetime_tz(ts(4))


def test_page_paging_unchanged_without_a_shift(monkeypatch):
    api = FakeApi(records(7), per_page=3)
    completed, _, written = drain(monkeypatch, config(PAGE_URL, 3), api)
    assert completed
    assert [i for i, _ in versions(written)] == list(range(7))
    assert [urlparse.parse_qs(urlparse.urlparse(u).query)["page"][0] for u in api.requests] == ["1", "2", "3"]
