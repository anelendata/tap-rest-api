[![PyPI version](https://img.shields.io/pypi/v/tap-rest-api.svg)](https://pypi.org/project/tap-rest-api/)
[![Python versions](https://img.shields.io/pypi/pyversions/tap-rest-api.svg)](https://pypi.org/project/tap-rest-api/)

# tap-rest-api

A configurable REST API [Singer](https://singer.io) tap.

> See [HISTORY.md](HISTORY.md) for the changelog. **Latest (0.2.18):** bounded,
> fully-drained time windows for safe incremental replication — see
> [Incremental replication: `assume_sorted` and windowing](#incremental-replication-assume_sorted-and-windowing).

## Contents

- [What is it?](#what-is-it)
- [Install](#install)
- [Quick start (USGS example)](#quick-start-usgs-example)
- [Configuration](#configuration)
  - [Parametric URL](#parametric-url)
  - [Bookmark keys: `timestamp_key`, `datetime_key`, `index_key`](#bookmark-keys-timestamp_key-datetime_key-index_key)
  - [Incremental replication: `assume_sorted` and windowing](#incremental-replication-assume_sorted-and-windowing)
  - [Multi-stream bookmark keys](#multi-stream-bookmark-keys)
  - [Record list level and record level](#record-list-level-and-record-level)
  - [unnest](#unnest)
- [Authentication](#authentication)
- [Custom http-headers](#custom-http-headers)
- [Multiple streams](#multiple-streams)
- [State](#state)
- [Raw output mode](#raw-output-mode)
- [Schema validation and cleanups](#schema-validation-and-cleanups)
- [About this project](#about-this-project)

## What is it?

tap-rest-api is a [Singer](https://singer.io) tap that produces JSON-formatted
data following the [Singer spec](https://github.com/singer-io/getting-started).

This tap:

- Pulls JSON records from a REST API.
- Automatically infers the schema and generates a JSON schema and a Singer catalog file.
- Incrementally pulls data based on the input state (the singer.io bookmark specification).

The stdout of this program is intended to be consumed by a singer.io target:

```
tap-rest-api | target-csv
```

## Install

```
pip install tap-rest-api
```

## Quick start (USGS example)

The following example uses [USGS Earthquake Events data](https://earthquake.usgs.gov/fdsnws/event/1/):

`curl https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2014-01-01&endtime=2014-01-02&minmagnitude=1`

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "geometry": {
        "type": "Point",
        "coordinates": [-116.7776667, 33.6633333, 11.008]
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
        "mag": 1.29,
        "place": "10km SSW of Idyllwild, CA",
        "time": 1388620296020,
        "mmi": null
      },
      "id": "ci11408890"
    }
  ]
}
```
(see [examples/usgs/sample_records.json](https://raw.githubusercontent.com/anelendata/tap-rest-api/master/examples/usgs/sample_records.json))

In the following steps, we will attempt to extract the `properties` section of
each `Feature` record as a Singer record.

### Step 1: Default spec

Anything defined here can be added to the tap configuration file or passed as a
command-line argument:

- [default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json)

### Step 2: [Optional] Create a custom spec for the config file

If you want to define more configuration variables, create a spec file. Here is an
[example](https://github.com/anelendata/tap-rest-api/blob/master/examples/usgs/custom_spec.json):

```json
{
    "args": {
        "min_magnitude": {
            "type": "integer",
            "default": "0",
            "help": "Filter based on the minimum magnitude."
        }
    }
}
```

Anything you define here overwrites
[default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json).

### Step 3: Create the config file

Note the difference between the spec file and the config file: the spec file
creates or alters the config *specs*, while the config file provides the *values*
for the config variables. When a value is not specified in the config file, the
default value defined in the spec file is used.

**Note: the jsonpath specification is supported in version 0.2.0 and later only.**

[Example](https://github.com/anelendata/tap-rest-api/tree/master/examples/usgs/config/tap_config.json):

```json
{
  "url": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_datetime}&endtime={end_datetime}&minmagnitude={min_magnitude}&limit={items_per_page}&offset={current_offset}&eventtype=earthquake&orderby=time-asc",
  "record_list_level": "features[*]",
  "timestamp_key": "properties.time",
  "schema": "earthquakes",
  "items_per_page": 100,
  "offset_start": 1,
  "auth_method": "no_auth",
  "min_magnitude": 1
}
```

Below are some key concepts used in the configuration file.

#### Parametric URL

Use the `{<config_variable_name>}` notation to insert a value from the config into
the URL.

In addition to the config variables listed in
[default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json)
and the custom spec file, the URL can also contain these run-time variables:

- `current_offset`: Offset by the number of records to skip.
- `current_page`: The current page, if the endpoint supports paging (`current_page_one_base` for 1-based paging).
- `last_update`: The last retrieved value of the field specified by `index_key`, `timestamp_key`, or `datetime_key` (see below).
- `start_datetime` / `end_datetime`, `start_timestamp` / `end_timestamp`, `start_index` / `end_index`: The bounds of the range being replicated.

#### Bookmark keys: `timestamp_key`, `datetime_key`, `index_key`

If you want to use a timestamp, datetime, or index in the parameterized URL, or
want to use a field of one of those types as a bookmark, set exactly one of
`timestamp_key`, `datetime_key`, or `index_key` to indicate which field in the
record corresponds to the data type:

- `timestamp_key`: POSIX timestamp.
- `datetime_key`: ISO 8601 formatted datetime (it can be truncated to a date, etc.).
  It also works when the separator between the date and time components is a space (" ") instead of "T".
- `index_key`: A sequential index (integer or string).

In the USGS example, each record contains the top-level objects `properties` and
`geometry`. The timestamp key `time` lives under `properties`, so `timestamp_key`
is set to `properties.time`, following the
[jsonpath](https://goessner.net/articles/JsonPath/) specification.

When you specify `timestamp_key`, `datetime_key`, or `index_key`, you must also
set `start_timestamp`, `start_datetime`, or `start_index` (in the config or as a
command-line argument) as the initial lower bound.

Optionally set `end_timestamp`, `end_datetime`, or `end_index` as an upper bound.
For human convenience, `start_datetime` / `end_datetime` are also consulted when
`timestamp_key` is set but `start_timestamp` / `end_timestamp` are not. When the
upper bound is not set, the datetime/timestamp end defaults to **UTC now**.

#### Incremental replication: `assume_sorted` and windowing

On each run the tap bookmarks the highest bookmark-field value it has written and,
via the input [state](#state), resumes from there on the next run.

**`assume_sorted`** (default `true`) tells the tap that the response is already
sorted by the bookmark field, so it may stop as soon as it encounters a value past
the end bound. If the API cannot guarantee that ordering — for example, it filters
on one field but can only sort by another — set `assume_sorted: false` so the tap
drains every page.

**Bounded time windows** (`window_size_seconds` / `window_size_hours`, *new in
0.2.18*): for a `datetime` or `timestamp` bookmark, set one of these to replicate
the range `[bookmark, end)` in contiguous, half-open time windows. The bookmark is
checkpointed to a window's upper bound **only after that window has been completely
drained**, so a run cut short by a timeout or error leaves the bookmark at the last
completed window instead of leaping past records that were never fetched. This keeps
the incremental safe even when `assume_sorted` is `false` and the API is not sorted
by the bookmark field.

Requirements:

- Use with a `datetime` or `timestamp` bookmark.
- The URL must bound **both** ends of the bookmark field (`__gte`/`__lt`, or whatever
  the API uses for `{start_datetime}` and `{end_datetime}`).

```json
{
  "assume_sorted": false,
  "window_size_hours": 6,
  "url": ".../items?page={current_page_one_base}&modified__gte={start_datetime}&modified__lt={end_datetime}"
}
```

When `window_size_*` is unset, the tap issues a single open-ended request (the
original behavior).

#### Multi-stream bookmark keys

`timestamp_keys`, `datetime_keys`, and `index_keys` (plural) are dictionaries used
when you want a different bookmark type per stream, keyed by stream ID:

```json
{
  "datetime_keys": {
    "some_stream": "modified_at"
  }
}
```

See [Multiple streams](#multiple-streams) for the full multi-stream setup.

#### Record list level and record level

- `record_list_level`:
  Some APIs wrap the set of records under a property; others respond with
  newline-separated JSON. For the former, specify a key so the tap can find the
  record level. The USGS response is a single JSON object whose records are listed
  under `features`, so `record_list_level` is set to the jsonpath `features[*]`.

- `record_level`:
  Under an individual record there may be another layer of properties separating
  the data from metadata, and you may only want the former. If so, specify
  `record_level`. In the USGS example we can ignore the `geometry` object and output
  only the contents of `properties` by setting `record_level` to a jsonpath:

```json
{
  "url": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_datetime}&endtime={end_datetime}&minmagnitude={min_magnitude}&limit={items_per_page}&offset={current_offset}&eventtype=earthquake&orderby=time-asc",
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

#### unnest

To flatten a nested record, the config below grabs
`record["some_nested_col"]["modified_at"]` and puts it in `record["modified_at"]`:

```json
{
  "unnest": {
    "some_stream": [
      {
        "path": "$.some_nested_col.modified_at",
        "target": "modified_at"
      }
    ]
  }
}
```

Note: the schema and catalog must reflect the schema *after* unnesting. To help
with this, `infer_schema` also applies this transformation before determining the
schema.

### Step 4: Create the schema and catalog files

```
$ tap-rest-api custom_spec.json --config config/tap_config.json --schema_dir ./schema --catalog_dir ./catalog --start_datetime="2020-08-06" --infer_schema
```

The schema and catalog files are created under the schema and catalog directories,
respectively. By default `--safe-schema-update=true`, meaning `infer_schema`
modifies the existing schema in an append manner and does not overwrite the data
types or sub-items of existing fields. To overwrite everything, either remove the
existing schema JSON files under `--schema_dir` or set `--safe_schema_update=false`.

Notes:

- If no customization is needed, you can omit the spec file (`custom_spec.json`).
- `start_datetime` and `end_datetime` are copied to `start_timestamp` and `end_timestamp`.
- `end_timestamp` and `end_datetime` default to UTC now when not present in the config file or command-line argument.
- When inferring the schema, you can use `--sample_dir <directory>` to read sample data from files instead of the API. Each file must be named `sample_dir/stream_name.json`, and its format must match the raw response from the REST API.

### Step 5: Run the tap

```
$ tap-rest-api ./custom_spec.json --config config/tap_config.json --start_datetime="2020-08-06" --catalog ./catalog/earthquakes.json
```

## Configuration

The reference for the configuration file is spread across the [Quick start](#quick-start-usgs-example)
subsections above ([Parametric URL](#parametric-url),
[bookmark keys](#bookmark-keys-timestamp_key-datetime_key-index_key),
[incremental replication and windowing](#incremental-replication-assume_sorted-and-windowing),
[record levels](#record-list-level-and-record-level), [unnest](#unnest)) and the
sections below ([Authentication](#authentication),
[Custom http-headers](#custom-http-headers), [Multiple streams](#multiple-streams),
[Schema validation and cleanups](#schema-validation-and-cleanups)). The full list
of options with their defaults is in
[default_spec.json](https://github.com/anelendata/tap-rest-api/blob/master/tap_rest_api/default_spec.json).

## Authentication

The example above does not require login. tap-rest-api currently supports basic
auth. If this is needed, add something like:

```json
{
  "auth_method": "basic",
  "username": "my_username",
  "password": "my_password"
}
```

Or add those on the command line:

```
tap-rest-api config/custom_spec.json --config config/tap_config.json --schema_dir ./config/schema --catalog ./config/catalog/some_catalog.json --start_datetime="2020-08-06" --username my_username --password my_password --auth_method basic
```

## Custom http-headers

In addition to the authentication method, you can specify the http headers in the
config file:

```json
{
  "http_headers": {
    "User-Agent": "Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
    "Content-type": "application/json",
    "Authorization": "Bearer <some-key>"
  }
}
```

Here is the default value:

```json
{
  "User-Agent": "Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
  "Content-type": "application/json"
}
```

When you define the `http_headers` config value, the default value is nullified,
so you should redefine `User-Agent` and `Content-type` when you need them.

## Multiple streams

tap-rest-api supports settings for multiple streams.

- `url` is a string, used as the default.
- `urls` is a dictionary that overrides the default `url` for the stream ID given as the dictionary key.
- `{stream}` can be used as a parameter in the URL.
- `timestamp_keys`, `datetime_keys`, `index_keys` can be set as dictionaries. If a stream ID exists as a key in one of them, it is used; otherwise the key falls back to the string-valued `timestamp_key` > `datetime_key` > `index_key` (in that priority).
- `datetime_key`, `timestamp_key`, and `index_key` are set as strings and are the default bookmark keys.
- Active streams must be defined as comma-separated stream IDs, either in the config file or via `--stream <streams>`.
- Streams must be registered in the catalog file with `selected: true` ([example](https://github.com/anelendata/tap-rest-api/blob/master/examples/usgs/catalog/earthquakes.json)).

Here is an example for the [Chargify API](https://developers.chargify.com/docs/api-docs):

```json
{
  "url": "https://{{ subdomain }}.chargify.com/{stream}.json?direction=asc&per_page={items_per_page}&page={current_page_one_base}&date_field={datetime_key}&start_datetime={start_datetime}",
  "urls": {
    "events": "https://{{ subdomain }}.chargify.com/events.json?direction=asc&per_page={items_per_page}&page={current_page_one_base}&date_field=created_at&since_id={start_index}",
    "price_points": "https://{{ subdomain }}.chargify.com/products_price_points.json?direction=asc&per_page={items_per_page}&page={current_page_one_base}&filter[date_field]=updated_at&filter[start_datetime]={start_datetime}&filter[end_datetime]={end_datetime}",
    "segments": "https://{{ subdomain }}.chargify.com/components/{{ component_id }}/price_points/{{ price_point_id }}/segments.json?per_page={items_per_page}&page={current_page_one_base}",
    "statements": "https://{{ subdomain }}.chargify.com/statements.json?direction=asc&per_page={items_per_page}&page={current_page_one_base}&sort=created_at",
    "transactions": "https://{{ subdomain }}.chargify.com/transactions.json?direction=asc&per_page={items_per_page}&page={current_page_one_base}&since_id={start_index}&order_by=id",
    "customers_meta": "https://{{ subdomain }}.chargify.com/customers/metadata.json?direction=asc&date_field=updated_at&per_page={items_per_page}&page={current_page_one_base}&with_deleted=true&start_datetime={start_datetime}&end_datetime={end_datetime}",
    "subscriptions_meta": "https://{{ subdomain }}.chargify.com/subscriptions/metadata.json?direction=asc&date_field=updated_at&per_page={items_per_page}&page={current_page_one_base}&with_deleted=true&start_datetime={start_datetime}&end_datetime={end_datetime}"
  },
  "streams": "components,coupons,customers,events,invoices,price_points,products,product_families,subscriptions,subscriptions_components,transactions",
  "auth_method": "basic",
  "username": "{{ api_key }}",
  "password": "x",
  "record_list_level": {
    "customers_meta": "$.metadata[*]",
    "invoices": "$.invoices[*]",
    "price_points": "$.price_points[*]",
    "segments": "$.segments[*]",
    "subscriptions_components": "$.subscriptions_components[*]",
    "subscriptions_meta": "$.metadata[*]"
  },
  "record_level": {
    "components": "$.component",
    "coupons": "$.coupon",
    "customers": "$.customer",
    "events": "$.event",
    "product_families": "$.product_family",
    "products": "$.product",
    "statements": "$.statement",
    "subscriptions": "$.subscription",
    "transactions": "$.transaction"
  },
  "datetime_key": {
    "components": "updated_at",
    "coupons": "updated_at",
    "customers": "updated_at",
    "invoices": "updated_at",
    "price_points": "updated_at",
    "product_families": "updated_at",
    "products": "updated_at",
    "subscriptions": "updated_at",
    "subscriptions_components": "updated_at"
  },
  "index_key": {
    "events": "id",
    "transactions": "id",
    "segments": "id",
    "statements": "id",
    "customers_meta": "id",
    "subscriptions_meta": "id"
  },
  "items_per_page": 200
}
```

## State

This tap emits [state](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file).
The command also takes a state file as input with the `--state <file-name>` option.
The tap itself does not write a state file; it expects the target program or a
downstream process to finalize the state safely and produce the state file.

## Raw output mode

If you want to use this tap outside the Singer framework, set `--raw` on the
command line. The process then writes out the records as newline-separated JSON.

A use case for this mode is when you expect the schema to change or be inconsistent
and you would rather extract and clean up after loading.
([Example](https://articles.anelen.co/elt-google-cloud-storage-bigquery/))

## Schema validation and cleanups

- `on_invalid_property`: Behavior when schema validation fails.
  - `"raise"`: Raise an exception.
  - `"null"`: Impute with null.
  - `"force"` (default): Keep the record value as-is (string). This may fail in the Singer target.
- `drop_unknown_properties`: If true, records exclude unknown (sub-)properties before being written to stdout. Default is false.

Config example:

```json
{
  "on_invalid_property": "force",
  "drop_unknown_properties": true
}
```

# About this project

This project is developed by ANELEN and friends. Please check out ANELEN's
[open innovation philosophy and other projects](https://anelen.co/open-source.html).

![ANELEN](https://avatars.githubusercontent.com/u/13533307?s=400&u=a0d24a7330d55ce6db695c5572faf8f490c63898&v=4)

---

Copyright &copy; 2020~ Anelen Co., LLC
