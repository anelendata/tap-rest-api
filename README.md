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

1. Create the spec for config file:

Example:

```
{
    "application": "Some example API",
    "args": {
        "prod_stage":
        {
            "type": "string",
            "default": "stage",
            "help": "This will fill the URL as https://{prod_stage}api.example.com"
        },
        "api_version":
        {
            "type": "integer",
            "default": 1,
            "help": "This will fill the API version as https://api.example.com/v/{api_version}/"
        }
    }
}
```

Note: Currently, you need to create this file even if you don't want to modify the default config specs.
In such cases, please provide an empty args object:

```
{
    "application": "Some example API",
    "args": {}
}
```

The args that are reserved default can be found [default_spec.json](./tap_rest_api/default_spec.json)


2. Create Config file based on the spec:

Example:
```
{
  "url":"https://example.com/v1/some_resource",
  "username":"xxxx",
  "password":"yyyy",
  "datetime_key": "last_modified_at",
  "start_datetime": "2020-04-01 00:00:00Z",
  "end_datetime": "2020-05-01 00:00:00Z",
  "schema_dir": <path_to_schema_dir>
}
```

Note: URL can contain parameters from config values and the following run-time variables:

- resource: The current resource you are accessing based on Tap Stream ID provided by [streams default config variable](./tap_rest_api/default_spec.json).
- current_page: The current page if the endpoint supports paging
- current_offset: Offset by the number of rows to skip

Example:

```
http://api.example.com/v1/{resource}?offset={current_offset}&limit={items_per_page}
```

In the above example,
- {items_per_page} is substituted by the config value.
- {resource} and {current_offset} is substituted by the runtime value based on the current stream and paging.

3. Create schema and catalog files

```
$ tap_rest_api spec.json --infer_schema --config config.json --schema_dir ./schema
```

4.Run the tap

```
$ tap_rest_api spec.json --config config.json --catalog catalog.json
```

---

Copyright &copy; 2019~ Anelen Co., LLC
