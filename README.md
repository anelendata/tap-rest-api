# tap_rest_api

A configurable REST API singer.io tap.

## What is it?

tap_rest_api is a [Singer](https://singer.io) tap that produces JSON-formatted
data following the [Singer spec](https://github.com/singer-io/getting-started).

This tap:

- Pulls JSON records from Rest API
- Automatically infers the schema and generate JSON-schema file.
- Incrementally pulls data based on the input state. (singer.io bookmark specification)

The stdout from this program is intended by consumed by singer.io target program as:

```
tap_rest_api | target-csv
```

## How to use it: USGS data example

The following example is created using [USGS Earthquake Events data](https://earthquake.usgs.gov/fdsnws/event/1/).

The record looks like:

`curl https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2014-01-01&endtime=2014-01-02&minmagnitude=1`

See [examples/usgs/sample_records.json](https://raw.githubusercontent.com/anelendata/tap_rest_api/master/examples/usgs/sample_records.json)

### Step 1: Default spec

Anything defined here can be added to tap configuration file or to the
command-line argument:

- [default_spec.json](https://github.com/anelendata/tap_rest_api/blob/master/tap_rest_api/default_spec.json)

### Step 2: [Optional] Create a custom spec for config file:

If you would like to define more configuration variables, create a spec
file. Anything you define overwrites the default spec.

A spec file example (./examples/usgs/custom_spec.json):
```
{
    "args": {
        "min_magnitude":
        {
            "type": "integer",
            "default": "0",
            "help": "Filter based on the minimum magnitude."
        }
    }
}
```

### Step 3. Create Config file based on the spec:

[Example](https://github.com/anelendata/tap_rest_api/tree/master/examples/usgs/tap_config.json):
```
{
  "url":"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_datetime}&endtime={end_datetime}&minmagnitude={min_magnitude}&limit={items_per_page}&offset={current_offset}&eventtype=earthquake&orderby=time-asc",
  "timestamp_key": "time",
  "minmagnitude": 1,
  "schema": "earthquakes",
  "record_list_level": "features",
  "record_level": "properties",
  "items_per_page": 100,
  "offset_start": 1,
  "auth_method": "no_auth"
}
```

#### Parameters

You can use `{<config>}` notion to insert the value specified at the config to URL.

Also notice the URL can contain parameters from config values and the following run-time variables:

- current_offset: Offset by the number of records to skip
- current_page: The current page if the endpoint supports paging
- last_update: The last retrieved value of the column specified by index_key, timestamp_key, or datetime_key

#### Record list level and record level

- record_list_level:
  Some API wraps a set of records under a property. Others responds a newline separated JSONs.
  For the former, we need to specify a key so the tap can find the record level. In USGS example,
  we find "features" as the property that lists the records.
- record_level:
  Under the individual record, there may be another layer of properties that separates
  the data and meta data and we may only be interested in the former. If this is the case,
  we can specify record_level.

Limitations: Currently, both record_list_level and record_level are a single string,
making impossible to go down more than one level.

### Step 4. Create schema and catalog files

```
$ tap_rest_api custom_spec.json --config config/tap_config.json --schema_dir ./schema --catalog_dir ./catalog --start_datetime="2020-08-06" --infer_schema 
```

The schema and catalog files are created under schema and catalog directories, respectively.

Note:

- If no customization needed, you can omit the spec file (custom_spec.json)
- `start_dateime` and `end_datetime` are copied to `start_timestamp` and `end_timestamp`.
- `end_timestamp` and `end_datetime` are automatically set as UTC now if not present in the config file or command-line argument.

### Step 5. Run the tap

```
$ tap_rest_api ./custom_spec.json --config config/tap_config.json --schema_dir ./schema --catalog_dir ./catalog --start_datetime="2020-08-06" --catalog ./catalog/earthquakes.json
```

## Authentication

The example above does not require login. tap_rest_api currently supports
basic auth. If this is needed add something like:

```
{
  "auth_method": "basic",
  "username": "my_username",
  "password": "my_password",
  ...
}
```

Or add those at the commands line:

```
tap_rest_api config/custom_spec.json --config config/tap_config.json --schema_dir ./config/schema --catalog ./config/catalog/some_catalog.json --start_datetime="2020-08-06" --username my_username --password my_password --auth_method basic
```

## State

This tap emits [state](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file).
The command also takes a state file input with `--state <file-name>` option.
The tap itself does not output a state file. It anticipate the target program or a downstream process to fianlize the state safetly and produce a state file.

---

Copyright &copy; 2020~ Anelen Co., LLC
