[![Build Status](https://travis-ci.com/anelendata/tap_rest_api.svg?branch=master)](https://travis-ci.com/anelendata/tap_rest_api)

# tap_rest_api

A configurable REST API singer.io tap.

## What is it?

tap_rest_api is a [Singer](https://singer.io) tap that produces JSON-formatted
data following the [Singer spec](https://github.com/singer-io/getting-started).

This tap:

- Pulls JSON records from Rest API
- Automatically infers the schema and generate JSON-schema and Singer catalog
  file.
- Incrementally pulls data based on the input state. (singer.io bookmark specification)

The stdout from this program is intended by consumed by singer.io target program as:

```
tap_rest_api | target-csv
```

## How to use it

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
  "record_list_level": "features",
  "record_level": "properties",
  "schema": "earthquakes",
  "items_per_page": 100,
  "offset_start": 1,
  "auth_method": "no_auth"
  "min_magnitude": 1,
}
```

Below are some key concepts in the configuration file.

#### Parametric URL

You can use `{<config_varable_name>}` notion to insert the value specified at the config to URL.

In addition to the config variables listed in
[default_spec.json](https://github.com/anelendata/tap_rest_api/blob/master/tap_rest_api/default_spec.json)
and the custom spec file, the URL also can contain parameters from the following run-time variables:

- current_offset: Offset by the number of records to skip
- current_page: The current page if the endpoint supports paging
- last_update: The last retrieved value of the column specified by index_key, timestamp_key, or datetime_key
  (See next section)

#### timestamp_key, datetime_key, index_key

If you want to use timestamp, datetime, index in the parameterized URL or
want to use a field in those types as a bookmark, one of either timestamp_key,
datetime_key, or index_key must be set to indicate which field in the record
corresponds to the data type.

- timestamp_key: POSIX timestamp
- datetime_key: ISO 8601 formatted datetime (it can be truncated to date and etc)
  It works when the character between the date and time components is " " instead of "T".
- index_key: A sequential index (integer or string)

When you specify timestamp_key, datetime_key, or index_key in the config,
you also need to set start_timestamp, start_datetime, or start_index in
config or as a command-line argument.

Optionally, you can set end_timestamp, end_datetime, or end_index to indicate
so the process stops once such threashold is encounterd, assuming the data
is sorted by the field.

For human convenience, start/end_datetime (more human readable) is also looked
up when timestamp_key is set but start/end_timestamp is not set.

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

## Raw output

If you want to use this tap outside Singer framework, set `--raw` in the
commandline argument. Then the process write out the records as
newline-separated JSON.

A use case for this mode is when you expect the schema to change or inconsistent
and you rather want to extract and clean up post-loading.
([Example](https://articles.anelen.co/elt-google-cloud-storage-bigquery/)

---

Copyright &copy; 2020~ Anelen Co., LLC
