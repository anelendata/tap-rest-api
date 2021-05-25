[![Build Status](https://travis-ci.com/anelendata/tap-rest-api.svg?branch=master)](https://travis-ci.com/anelendata/tap-rest-api)

💥 New in 0.2.0: Set record_list_level and record_level, index_key, datetime_key, and timestamp_key with jsonpath.

# tap-rest-api

A configurable REST API singer.io tap.

## What is it?

tap-rest-api is a [Singer](https://singer.io) tap that produces JSON-formatted
data following the [Singer spec](https://github.com/singer-io/getting-started).

This tap:

- Pulls JSON records from Rest API
- Automatically infers the schema and generate JSON-schema and Singer catalog
  file.
- Incrementally pulls data based on the input state. (singer.io bookmark specification)

The stdout from this program is intended by consumed by singer.io target program as:

```
tap-rest-api | target-csv
```

## How to use it

Install:

```
pip install tap-rest-api
```

The following example is created using [USGS Earthquake Events data](https://earthquake.usgs.gov/fdsnws/event/1/).

`curl https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2014-01-01&endtime=2014-01-02&minmagnitude=1`

```
{
  "type": "FeatureCollection",
  "features": [
    {
      "geometry": {
        "type": "Point",
        "coordinates": [
          -116.7776667,
          33.6633333,
          11.008
        ]
      },
      "type": "Feature",
      "properties": {
        "rms": 0.09,
        "code": "11408890",
        "cdi": null,
        "sources": ",ci,",
        "nst": 39,
        "tz": -480,
        "title": "M 1.3 - 10km SSW of Idyllwild, CA",
        ...
        "mag": 1.29,
        ...
        "place": "10km SSW of Idyllwild, CA",
        "time": 1388620296020,
        "mmi": null
      },
      "id": "ci11408890"
    },
    ...
  ]
}
```
[examples/usgs/sample_records.json](https://raw.githubusercontent.com/anelendata/tap-rest-api/master/examples/usgs/sample_records.json)

In the following steps, we will atempt to extract `properties` section of
the record type `Feature` as Singer record.

### Step 1: Default spec

Anything defined here can be added to tap configuration file or to the
command-line argument:

- [default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json)

### Step 2: [Optional] Create a custom spec for config file:

If you would like to define more configuration variables, create a spec file.
Here is an
[example] (https://github.com/anelendata/tap-rest-api/blob/master/examples/usgs/custom_spec.json):
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

Anything you define here overwrites
[default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json).

### Step 3. Create Config file:

**Please note jsonpath specification is supported version 0.2.0 and later only.**

Now create a cofnig file. Note the difference between spec file and config file.
The role of spec file is to create or alter the config specs, and the role of
the config file is to provide the values to the config variables. When a value
is not specified in the config file, the default value defined in the spec
file is used.

[Example](https://github.com/anelendata/tap-rest-api/tree/master/examples/usgs/config/tap_config.json):

```
{
  "url":"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_datetime}&endtime={end_datetime}&minmagnitude={min_magnitude}&limit={items_per_page}&offset={current_offset}&eventtype=earthquake&orderby=time-asc",
  "record_list_level": "features[*]",
  "timestamp_key": "properties.time",
  "schema": "earthquakes",
  "items_per_page": 100,
  "offset_start": 1,
  "auth_method": "no_auth",
  "min_magnitude": 1
}
```

Below are some key concepts in the configuration file.

#### Parametric URL

You can use `{<config_varable_name>}` notion to insert the value specified at the config to URL.

In addition to the config variables listed in
[default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json)
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

In USGS example, the individual record contains the top level objects `properties`
and `geometry`. The timestamp key is `time` defined under `properties`, so the config
value `timestamp_key` is set as `properties.time`, following
[jsonpath](https://goessner.net/articles/JsonPath/) specification.

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
  For the former, we need to specify a key so the tap can find the record level.
  The USGS earthquake response is a single JSON object example. The records are listed under
  features object. So the config value `record_list_level` is set as a jsonpath `features[*]`.

- record_level:
  Under the individual record, there may be another layer of properties that separates
  the data and meta data and we may only be interested in the former. If this is the case,
  we can specify record_level. In USGS example, we can ignore `geometry` object and output
  only the content of `properties` object. Set a jsonpath to `record_level` config value
  to achieve this:

```
{
  "url":"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_datetime}&endtime={end_datetime}&minmagnitude={min_magnitude}&limit={items_per_page}&offset={current_offset}&eventtype=earthquake&orderby=time-asc",
  "record_list_level": "features[*]",
  "record_level": "properties",
  "timestamp_key": "time",
  "schema": "earthquakes",
  "items_per_page": 100,
  "offset_start": 1,
  "auth_method": "no_auth",
  "min_magnitude": 1
}
```

### Step 4. Create schema and catalog files

```
$ tap-rest-api custom_spec.json --config config/tap_config.json --schema_dir ./schema --catalog_dir ./catalog --start_datetime="2020-08-06" --infer_schema
```

The schema and catalog files are created under schema and catalog directories, respectively.

Note:

- If no customization needed, you can omit the spec file (custom_spec.json)
- `start_dateime` and `end_datetime` are copied to `start_timestamp` and `end_timestamp`.
- `end_timestamp` and `end_datetime` are automatically set as UTC now if not present in the config file or command-line argument.

### Step 5. Run the tap

```
$ tap-rest-api ./custom_spec.json --config config/tap_config.json --start_datetime="2020-08-06" --catalog ./catalog/earthquakes.json
```

## Authentication

The example above does not require login. tap-rest-api currently supports
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
tap-rest-api config/custom_spec.json --config config/tap_config.json --schema_dir ./config/schema --catalog ./config/catalog/some_catalog.json --start_datetime="2020-08-06" --username my_username --password my_password --auth_method basic
```

## Custom http-headers

In addition to the authentication method, you can specify the http header
in config file:

Example:

```
...
"http_headers":
    {
      "User-Agent": "Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
      "Content-type": "application/json",
      "Authorization": "Bearer <some-key>"
    },
...
```

Here is the default value:
```
{
  "User-Agent": "Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
  "Content-type": "application/json"
}
```

When you define http_headers config value, the default value is nullified.
So you should redefine "User-Agent" and "Content-type" when you need them.

## State

This tap emits [state](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file).
The command also takes a state file input with `--state <file-name>` option.
The tap itself does not output a state file. It anticipate the target program or a downstream process to fianlize the state safetly and produce a state file.

## Raw output mode

If you want to use this tap outside Singer framework, set `--raw` in the
commandline argument. Then the process write out the records as
newline-separated JSON.

A use case for this mode is when you expect the schema to change or inconsistent
and you rather want to extract and clean up post-loading.
([Example](https://articles.anelen.co/elt-google-cloud-storage-bigquery/))

# About this project

This project is developed by 
ANELEN and friends. Please check out the ANELEN's
[open innovation philosophy and other projects](https://anelen.co/open-source.html)

![ANELEN](https://avatars.githubusercontent.com/u/13533307?s=400&u=a0d24a7330d55ce6db695c5572faf8f490c63898&v=4)
---

Copyright &copy; 2020~ Anelen Co., LLC
