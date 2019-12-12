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
        },
        "start_index":
        {
            "type": "integer",
            "default": 0,
            "help": "Starting index number"
        },
        "index_key":
        {
            "type": "string",
            "default": null,
            "help": "Index key name"
        },
        "start_at":
        {
            "type": "datetime",
            "default": null,
            "help": "Start time in ISO 8601 format"
        },
        "end_at":
        {
            "type": "datetime",
            "default": null,
            "help": "End time in ISO 8601 format"
        }
    }
}
```

The following args are reserved:

```
{
    "args":
    {
        "schema_dir":
        {
            "type": "string",
            "default": null,
            "help": "Path to the schema directory"
        },
        "catalog_dir":
        {
            "type": "string",
            "default": null,
            "help": "Path to the catalog directory"
        },
        "start_at":
        {
            "type": "string",
            "default": null,
            "help": "Start time in ISO 8601 format"
        },
        "end_at":
        {
            "type": "string",
            "default": null,
            "help": "End time in ISO 8601 format"
        },
        "items_per_page":
        {
            "type": "integer",
            "default": null,
            "help": "# of items per page if API supports paging"
        },
        "max_page":
        {
            "type": "integer",
            "default": null,
            "help": "If set, stop polling after max_page"
        },
        "auth_method":
        {
            "type": "string",
            "default": "basic",
            "help": "HTTP request authentication method: basic, digest, or ntlm"
        }
    }
}
```

2. Create Config file from sample-config.json

```
{
  "url":"https://example.com/v1/customers",
  "username":"xxxx",
  "password":"yyyy",
  "start_datetime": <ISO8601-Date-String>,
  "end_datetime": <ISO8601-Date-String>,
  "schema_dir": <path_to_schema_dir>
}
```

- start date will determine how far back in your order history the tap will go
	- this is only relevant for the initial run, progress afterwards will be bookmarked

Note: url can contain parameters from config values and the following run-time variables:

- current_page: The current page if the endpoint supports paging
- current_offset: Offset by the number of rows to skip

Example: http://example.com/v1/customers?offset={current_offset}&limit={items_per_page}

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
