"""Token (cursor) pagination: next_page_token_level + next_page_url(s)."""
import datetime

import jinja2
import pytest

import tap_rest_api.helper as H


# --- helpers ------------------------------------------------------------------

def test_token_level_str_dict_and_unset():
    assert H.get_next_page_token_level({"next_page_token_level": "$.next"}, "a") == "$.next"
    cfg = {"next_page_token_level": {"a": "$.next"}}
    assert H.get_next_page_token_level(cfg, "a") == "$.next"
    assert H.get_next_page_token_level(cfg, "b") is None
    assert H.get_next_page_token_level({}, "a") is None


def test_get_next_page_token():
    cfg = {"next_page_token_level": "$.nextPageToken"}
    assert H.get_next_page_token(cfg, "s", {"nextPageToken": "abc", "issues": []}) == "abc"
    # missing / null / empty all end paging
    assert H.get_next_page_token(cfg, "s", {"isLast": True}) is None
    assert H.get_next_page_token(cfg, "s", {"nextPageToken": None}) is None
    assert H.get_next_page_token(cfg, "s", {"nextPageToken": ""}) is None
    # nested path, and a non-JSON-object response
    nested = {"next_page_token_level": "$.meta.cursor.next"}
    assert H.get_next_page_token(nested, "s", {"meta": {"cursor": {"next": "n2"}}}) == "n2"
    assert H.get_next_page_token(cfg, "s", "not json") is None
    # not configured for this stream
    assert H.get_next_page_token({}, "s", {"nextPageToken": "abc"}) is None


def test_get_next_page_endpoint_quotes_token():
    cfg = {"next_page_url": "{{ endpoint }}&nextPageToken={{ next_page_token }}"}
    url = H.get_next_page_endpoint(cfg, "s", {"x": 1}, "http://h/s?maxResults=50", "a/b+c==")
    assert url == "http://h/s?maxResults=50&nextPageToken=a%2Fb%2Bc%3D%3D"


def test_get_next_page_endpoint_params_and_per_stream_override():
    cfg = {
        "next_page_url": "{{ endpoint }}&cursor={{ next_page_token }}",
        "next_page_urls": {"issues": "http://h/{{ resource }}/{{ custom_id }}?token={{ next_page_token }}"},
    }
    data = {"custom_id": "42"}
    assert H.get_next_page_endpoint(cfg, "issues", data, "http://first", "t") == "http://h/issues/42?token=t"
    assert H.get_next_page_endpoint(cfg, "boards", data, "http://first", "t") == "http://first&cursor=t"


def test_get_next_page_endpoint_errors():
    with pytest.raises(ValueError):
        H.get_next_page_endpoint({"next_page_token_level": "$.n"}, "s", {}, "http://first", "t")
    # An unknown variable is an error, not a silently empty string
    with pytest.raises(jinja2.exceptions.UndefinedError):
        H.get_next_page_endpoint({"next_page_url": "{{ endpoint }}&t={{ typo }}"}, "s", {}, "http://first", "t")


# --- Sync._drain_pages ----------------------------------------------------------

PAGES = {
    None: {"issues": [{"id": 1}, {"id": 2}], "nextPageToken": "t/1+="},
    "t%2F1%2B%3D": {"issues": [{"id": 3}, {"id": 4}], "nextPageToken": "t2"},
    "t2": {"issues": [{"id": 5}], "isLast": True},
}


def _jira_like_config():
    return {
        "schema": "issues",
        "url": "http://h/rest/software/1.0/sprint/{custom_id}/issue?maxResults=2",
        "next_page_token_level": "$.nextPageToken",
        "next_page_url": "{{ endpoint }}&nextPageToken={{ next_page_token }}",
        "record_list_level": "issues[*]",
        "index_key": "id",
        "start_index": 0,
        # Deliberately smaller than the pages: with token paging a short page must
        # not end the extraction, only a missing token does.
        "items_per_page": 3,
        "filter_by_schema": False,
        "auth_method": "no_auth",
        "custom_id": "42",
    }


def _drain(monkeypatch, cfg, fake_request):
    import tap_rest_api.sync as S
    import tap_rest_api.schema as SC

    written = []
    monkeypatch.setattr(S, "generate_request", fake_request)
    monkeypatch.setattr(SC.Schema, "validate", staticmethod(lambda rec, sch: (True, None)))
    monkeypatch.setattr(S.singer, "write_record", lambda stream, rec: written.append(rec))
    s = S.Sync(cfg, {}, None)
    s.started_at = datetime.datetime.now()
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    params = dict(cfg, current_page=0, current_offset=0, last_update=0)
    with S.metrics.record_counter("issues") as counter:
        completed, _, _ = s._drain_pages(
            "issues", params, schema, None, 0, None, counter, raw_output=False)
    return completed, written


def _token_of(endpoint):
    return endpoint.split("nextPageToken=")[1] if "nextPageToken=" in endpoint else None


def test_drain_pages_follows_tokens_until_missing(monkeypatch):
    requested = []

    def fake_request(stream, endpoint, *a, **k):
        requested.append(endpoint)
        return PAGES[_token_of(endpoint)]

    completed, written = _drain(monkeypatch, _jira_like_config(), fake_request)

    assert completed is True
    assert [r["id"] for r in written] == [1, 2, 3, 4, 5]
    first = "http://h/rest/software/1.0/sprint/42/issue?maxResults=2"
    # The token is appended to the FIRST page URL each time; tokens don't accumulate
    assert requested == [first,
                         first + "&nextPageToken=t%2F1%2B%3D",
                         first + "&nextPageToken=t2"]


def test_drain_pages_failed_token_page_is_not_completed(monkeypatch):
    def fake_request(stream, endpoint, *a, **k):
        if _token_of(endpoint):
            raise RuntimeError("503")
        return PAGES[None]

    completed, written = _drain(monkeypatch, _jira_like_config(), fake_request)

    assert completed is False
    assert [r["id"] for r in written] == [1, 2]


def test_drain_pages_stops_on_repeated_token(monkeypatch):
    calls = []

    def fake_request(stream, endpoint, *a, **k):
        calls.append(endpoint)
        return {"issues": [{"id": len(calls)}], "nextPageToken": "same"}

    completed, written = _drain(monkeypatch, _jira_like_config(), fake_request)

    assert completed is False
    assert len(calls) == 2


def test_drain_pages_offset_paging_unchanged_without_token_level(monkeypatch):
    cfg = _jira_like_config()
    del cfg["next_page_token_level"]
    calls = []

    def fake_request(stream, endpoint, *a, **k):
        calls.append(endpoint)
        return PAGES[None]  # 2 rows < items_per_page 3 -> short page ends it

    completed, written = _drain(monkeypatch, cfg, fake_request)

    assert completed is True
    assert len(calls) == 1
    assert [r["id"] for r in written] == [1, 2]


# --- Schema.infer_schema --------------------------------------------------------

def test_infer_schema_follows_tokens(monkeypatch):
    import tap_rest_api.schema as SC

    requested = []

    def fake_request(stream, endpoint, *a, **k):
        requested.append(endpoint)
        return PAGES[_token_of(endpoint)]

    monkeypatch.setattr(SC, "generate_request", fake_request)
    schema = SC.Schema(_jira_like_config()).infer_schema("issues")

    assert len(requested) == 3
    assert "id" in schema["properties"]
