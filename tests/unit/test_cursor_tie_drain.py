"""Cursor paging when more records share one bookmark value than fit on a page.

The fake API behaves like Chargify's subscriptions_components list, as checked against
the API on 2026-11-18: `start_datetime` and `end_datetime` are both inclusive, values are
whole seconds, `sort` is `updated_at` (ties in a fixed order that isn't the id) or `id`,
and pages are numbered from 1.
"""
import datetime
import functools
import urllib.parse as urlparse

import pytest

from tap_rest_api.helper import get_init_endpoint_params, parse_datetime_tz

T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
BASE = "http://x/subscriptions_components.json?date_field=updated_at&direction=asc&per_page={items_per_page}"
PAGE_URL = BASE + "&sort=updated_at&start_datetime={start_datetime}&page={current_page_one_base}"
CURSOR_URL = BASE + "&sort=updated_at&start_datetime={last_update}&page=1"
TIE_URL = BASE + "&sort=id&start_datetime={tie_datetime}&end_datetime={tie_datetime}&page={tie_page}"


def ts(sec):
    return (T0 + datetime.timedelta(seconds=sec)).isoformat()


@functools.lru_cache(maxsize=None)
def parsed(value):
    return parse_datetime_tz(value)


class FakeApi:
    def __init__(self, records, per_page, on_request=None, tie_key=None):
        self.records = {r["id"]: dict(r) for r in records}
        self.per_page = per_page
        self.on_request = on_request  # called with (api, request number) before serving
        self.requests = []
        # Order of tied records with sort=updated_at: fixed, but not the id.
        self.tie_key = tie_key or (lambda rid: (rid * 7919) % 1009)

    def __call__(self, stream, endpoint, *a, **k):
        if self.on_request:
            self.on_request(self, len(self.requests))
        self.requests.append(endpoint)
        q = {k: v[0] for k, v in urlparse.parse_qs(urlparse.urlparse(endpoint).query).items()}
        start = parsed(q["start_datetime"])
        end = parsed(q["end_datetime"]) if "end_datetime" in q else None
        page, per_page = int(q["page"]), int(q["per_page"])
        rows = [r for r in self.records.values()
                if parsed(r["updated_at"]) >= start and (end is None or parsed(r["updated_at"]) <= end)]
        if q["sort"] == "id":
            rows.sort(key=lambda r: r["id"])
        else:
            rows.sort(key=lambda r: (parsed(r["updated_at"]), self.tie_key(r["id"])))
        return [dict(r) for r in rows[(page - 1) * per_page: page * per_page]]

    def touch(self, rid, sec):
        """The record is updated: its updated_at moves to a later second."""
        self.records[rid]["updated_at"] = ts(sec)

    def tie_requests(self):
        return [u for u in self.requests if "sort=id" in u]


def config(url, per_page, tie=True):
    cfg = {
        "streams": "subscriptions_components",
        "url": url,
        "datetime_keys": {"subscriptions_components": "updated_at"},
        "start_datetime": ts(0),
        "items_per_page": per_page,
        "assume_sorted": True,
        "filter_by_schema": False,
        "auth_method": "no_auth",
        "page_start": 0,
        "offset_start": 0,
    }
    if tie:
        cfg["tie_urls"] = {"subscriptions_components": TIE_URL}
    return cfg


SCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}}}


def patch(monkeypatch, api, written):
    import tap_rest_api.sync as S
    import tap_rest_api.schema as SC
    monkeypatch.setattr(S, "generate_request", api)
    monkeypatch.setattr(SC.Schema, "validate", staticmethod(lambda rec, sch: (True, None)))
    monkeypatch.setattr(S.singer, "write_record", lambda stream, rec: written.append(rec))
    return S


def drain(monkeypatch, cfg, api, end=None, timeout_after=None):
    written = []
    S = patch(monkeypatch, api, written)
    s = S.Sync(cfg, {}, None)
    s.started_at = datetime.datetime.now()
    if timeout_after is not None:
        # global_timeout trips once `timeout_after` requests have been made
        cfg["global_timeout"] = 1
        real = datetime.datetime

        class Clock(real):
            @classmethod
            def now(cls, tz=None):
                return real.now(tz) + (datetime.timedelta(hours=1)
                                       if len(api.requests) >= timeout_after else datetime.timedelta(0))
        monkeypatch.setattr(S.datetime, "datetime", Clock)
    params = get_init_endpoint_params(cfg, {}, "subscriptions_components")
    with S.metrics.record_counter("subscriptions_components") as counter:
        completed, last_update, _ = s._drain_pages(
            "subscriptions_components", params, SCHEMA, end or ts(100_000),
            params["last_update"], None, counter, raw_output=False)
    return completed, last_update, written


def versions(written):
    return [(r["id"], parse_datetime_tz(r["updated_at"])) for r in written]


def ids(written):
    return {r["id"] for r in written}


def burst(n_before=50, tie=311, n_after=50, tie_sec=100):
    """Records one a second, then `tie` records created in one second (a new CKSource
    subscription gets about 311 components at once), then more records."""
    recs = [{"id": i, "updated_at": ts(i)} for i in range(n_before)]
    recs += [{"id": 1000 + i, "updated_at": ts(tie_sec)} for i in range(tie)]
    recs += [{"id": 5000 + i, "updated_at": ts(tie_sec + 1 + i)} for i in range(n_after)]
    return recs


def test_without_tie_urls_cursor_paging_raises_on_a_311_record_second(monkeypatch):
    """Tap 0.2.23 as deployed for invoices: the run fails, and every later run fails at
    the same second, because the bookmark can't move past it."""
    api = FakeApi(burst(), per_page=200)
    with pytest.raises(RuntimeError, match="more than items_per_page"):
        drain(monkeypatch, config(CURSOR_URL, 200, tie=False), api)


def test_tie_drain_reads_a_311_record_second_once(monkeypatch):
    api = FakeApi(burst(), per_page=200)
    completed, last_update, written = drain(monkeypatch, config(CURSOR_URL, 200), api)
    assert completed
    assert ids(written) == set(api.records)
    assert len(written) == len(api.records)  # nothing written twice
    assert parse_datetime_tz(last_update) == parse_datetime_tz(ts(150))
    # two passes over the tie: 2 pages each
    assert len(api.tie_requests()) == 4


def test_tie_split_from_the_first_cursor_page_is_not_written_twice(monkeypatch):
    # 150 records before the tie: the first cursor page holds 50 of the tie's records
    api = FakeApi(burst(n_before=150), per_page=200)
    completed, _, written = drain(monkeypatch, config(CURSOR_URL, 200), api)
    assert completed
    assert ids(written) == set(api.records)
    assert len(written) == len(api.records)


def test_ties_in_consecutive_seconds(monkeypatch):
    recs = [{"id": i, "updated_at": ts(10)} for i in range(450)]
    recs += [{"id": 1000 + i, "updated_at": ts(11)} for i in range(260)]
    recs += [{"id": 2000 + i, "updated_at": ts(12 + i)} for i in range(5)]
    api = FakeApi(recs, per_page=200)
    completed, last_update, written = drain(monkeypatch, config(CURSOR_URL, 200), api)
    assert completed
    assert ids(written) == set(api.records)
    assert len(written) == len(api.records)
    assert parse_datetime_tz(last_update) == parse_datetime_tz(ts(16))


def test_a_record_changing_during_the_tie_read_is_recovered_by_the_second_pass(monkeypatch):
    """A 700-record second. The cursor pages already wrote its 200 highest ids (ties come
    back highest id first). After tie page 1 is served, record 1000 on it changes: it
    leaves the second, the records behind it move up one place, and 1200 slips onto page 1
    unread. Pass 2 finds it."""
    state = {"done": False}

    def change_during_tie(api, n):
        if len(api.tie_requests()) == 1 and not state["done"]:
            api.touch(1000, 900)
            state["done"] = True

    api = FakeApi(burst(tie=700), per_page=200, on_request=change_during_tie, tie_key=lambda rid: -rid)
    completed, _, written = drain(monkeypatch, config(CURSOR_URL, 200), api)
    assert completed
    assert ids(written) == set(api.records)
    assert (1000, parse_datetime_tz(ts(900))) in versions(written)  # the new version, read later
    assert [r["id"] for r in written].count(1200) == 1
    # 699 records are 4 pages: pass 1 missed 1200, pass 2 found it, pass 3 found nothing
    assert len(api.tie_requests()) == 12


def test_without_the_second_pass_the_slipped_record_is_lost(monkeypatch):
    """Same as above with tie_max_passes=1, to show what the second pass is for."""
    state = {"done": False}

    def change_during_tie(api, n):
        if len(api.tie_requests()) == 1 and not state["done"]:
            api.touch(1000, 900)
            state["done"] = True

    cfg = config(CURSOR_URL, 200)
    cfg["tie_max_passes"] = 1
    api = FakeApi(burst(tie=700), per_page=200, on_request=change_during_tie, tie_key=lambda rid: -rid)
    with pytest.raises(RuntimeError, match="still changing"):
        drain(monkeypatch, cfg, api)


def test_page_number_paging_loses_records_when_read_records_change(monkeypatch):
    """The deployed sync: records already read change during the run and every record
    behind them moves up one place."""
    def churn(api, n):
        if n in (1, 2):
            api.touch(n, 900 + n)  # records 1 and 2 were on page 1

    api = FakeApi(burst(), per_page=200, on_request=churn)
    completed, _, written = drain(monkeypatch, config(PAGE_URL, 200, tie=False), api)
    assert completed
    assert len(set(api.records) - ids(written)) == 2  # two never read, not even at a later version


def test_cursor_paging_with_tie_drain_keeps_every_record_under_the_same_churn(monkeypatch):
    def churn(api, n):
        if n in (1, 2):
            api.touch(n, 900 + n)

    api = FakeApi(burst(), per_page=200, on_request=churn)
    completed, _, written = drain(monkeypatch, config(CURSOR_URL, 200), api)
    assert completed
    assert ids(written) == set(api.records)
    for n in (1, 2):
        assert (n, parse_datetime_tz(ts(900 + n))) in versions(written)


def test_timeout_during_the_tie_leaves_the_cursor_at_the_tie(monkeypatch):
    api = FakeApi(burst(), per_page=200)
    # requests: cursor page 1, cursor page 2 (the tie), then tie page 1 trips the timeout
    completed, last_update, written = drain(
        monkeypatch, config(CURSOR_URL, 200), api, timeout_after=3)
    assert not completed
    assert parse_datetime_tz(last_update) == parse_datetime_tz(ts(100))


def test_a_tie_at_or_after_the_end_is_not_read(monkeypatch):
    api = FakeApi(burst(), per_page=200)
    completed, last_update, written = drain(monkeypatch, config(CURSOR_URL, 200), api, end=ts(100))
    assert completed
    assert api.tie_requests() == []
    assert parse_datetime_tz(last_update) < parse_datetime_tz(ts(100))


def test_a_tie_url_returning_other_seconds_fails(monkeypatch):
    cfg = config(CURSOR_URL, 200)
    cfg["tie_urls"] = {"subscriptions_components": BASE + "&sort=id&start_datetime={tie_datetime}&page={tie_page}"}
    api = FakeApi(burst(), per_page=200)
    with pytest.raises(RuntimeError, match="not at the tie value"):
        drain(monkeypatch, cfg, api)


def test_records_that_keep_changing_fail_the_run(monkeypatch):
    counter = {"next": 0}

    def keep_changing(api, n):
        if api.tie_requests() and api.requests[-1].count("sort=id"):
            # every tie request: one more record at the tie value appears
            counter["next"] += 1
            api.records[90000 + counter["next"]] = {"id": 90000 + counter["next"], "updated_at": ts(100)}

    api = FakeApi(burst(), per_page=200, on_request=keep_changing)
    with pytest.raises(RuntimeError, match="still changing"):
        drain(monkeypatch, config(CURSOR_URL, 200), api)


def test_full_sync_saves_a_bookmark_past_a_fully_read_tie(monkeypatch):
    """sync_rows end to end: the tie is the newest second, so the bookmark is one second
    after it, and the next run reads nothing again."""
    import tap_rest_api.sync as S
    recs = [{"id": i, "updated_at": ts(i)} for i in range(5)] + \
           [{"id": 1000 + i, "updated_at": ts(50)} for i in range(250)]
    api = FakeApi(recs, per_page=200)
    written, states = [], []
    patch(monkeypatch, api, written)
    monkeypatch.setattr(S.singer, "write_state", lambda st: states.append(st))
    monkeypatch.setattr(S.singer, "write_schema", lambda *a, **k: None)
    monkeypatch.setattr(S.Schema, "load_schema", lambda self, stream: SCHEMA)
    cfg = config(CURSOR_URL, 200)
    cfg["end_datetime"] = ts(100_000)
    s = S.Sync(cfg, {}, None)
    s.started_at = datetime.datetime.now()
    state = s.sync_rows({}, "subscriptions_components")
    bookmark = state["bookmarks"]["subscriptions_components"]["last_update"]
    assert parse_datetime_tz(bookmark) == parse_datetime_tz(ts(51))
    assert len(written) == 255

    written.clear()
    s2 = S.Sync(cfg, {}, None)
    s2.started_at = datetime.datetime.now()
    s2.sync_rows(state, "subscriptions_components")
    assert written == []
