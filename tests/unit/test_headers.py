from tap_rest_api.helper import get_http_headers, USER_AGENT


DEFAULT_HEADERS = {"User-Agent": USER_AGENT,
                   "Content-type": "application/json"}


def test_default():
    h = get_http_headers()
    assert h == DEFAULT_HEADERS


def test_agent_overwrite():
    ua = ("Mozilla/5.1 (Macintosh; scitylana.singer.io) " +
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 " +
          "Safari/537.36 ")
    config = {"http_headers": {"User-Agent": ua,
                               "Conetnt-type": "application/json",
                               "Bearer": "xxxxyyyy"}}

    h = get_http_headers(config)

    assert h == config["http_headers"]
