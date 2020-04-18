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

1. Create the spec for config file from sample-spec.json

Example (spec.json):

```
{
    "application": "Some example API",
    "args": {
        "username":
        {
            "type": "string",
            "default": null,
            "help": "Username for REST API Basic Auth"
        },
        "password":
        {
            "type": "string",
            "default": null,
            "help": "Password for REST API Basic Auth"
        }
    }
}
```

The args that are reserved default can be found [default_spec.json](./default_spec.json)


2. Create Config file from sample-config.json

Example:
```
{
  "url":"https://example.com/v1/customers",
  "username":"xxxx",
  "password":"yyyy",
  "datetime_key": "last_modified_at",
  "start_datetime": "2020-04-01 00:00:00Z",
  "end_datetime": "2020-05-01 00:00:00Z",
  "schema_dir": <path_to_schema_dir>
}
```

Note: url can contain parameters from config values and the following run-time variables:

- current_page: The current page if the endpoint supports paging
- current_offset: Offset by the number of rows to skip

Example:

```
http://example.com/v1/customers?offset={current_offset}&limit={items_per_page}
```

In the above example, {items_per_page} and {current_offset} is substituted by the config value and the runtime value, respectively.

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
