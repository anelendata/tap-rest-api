# tap-rest-api

A configurable REST API tap.

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Rest API
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

Usage:

1. Create Config file from sample-config.json

```
{
  "url":"https://example.com/",
  "consumer_key":"ck_woocommerce",
  "consumer_secret":"cs_woocommerce",
  "start_datetime": <ISO8601-Date-String>,
  "end_datetime": <ISO8601-Date-String>,
  "schema_dir": <full_path/schema_dir>
}
```

- start date will determine how far back in your order history the tap will go
	- this is only relevant for the initial run, progress afterwards will be bookmarked

2. Discover

```
$ tap_rest_api --config config.json --discover >> catalog.json
```

- Run the above to discover the data points the tap supports for each of Rest API's endpoints

3. Select Streams

```
    {
       "schema": {
            "properties": {...},
            "type": "object",
            "selected": true
        },
        "stream": "stream_name",
        "tap_stream_id": "stream_id"
    }
```
- Add ```"selected":true``` within the schema object to select the stream

4.Run the tap

```
$ tap_rest_api --config config.json --catalog catalog.json
```

---

Copyright &copy; 2019~ Anelen Co., LLC
