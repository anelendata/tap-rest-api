## History

### 0.2.6 (2021-07-11)

- Update getschema to 0.2.6 to fix a wrong rejection of null object when it's allowed.


### 0.2.5 (2021-06-04)

- Update getschema to 0.2.5 to fix a bad null conversion

### 0.2.4 (2021-05-25)

- fix: Infer schema mode produces null record that causes "CRITICAL list index out of range" (#16)

### 0.2.3 (2021-05-06)

- fix: missing variable for max_page logging

### 0.2.2 (2021-05-03)

- fix: end_datetime is not honored when timestamp_key is used #12

### 0.2.1 (2021-05-02)

- doc: add missing release history entry

### 0.2.0 (2021-05-02)

- feature: Set record_list_level and record_level, index_key, datetime_key, and timestamp_key with jsonpath.

### 0.1.3 (2020-12-22)

- Bump getschema version to 0.1.2 so it allows empty object (dict) entries

### 0.1.2 (2020-12-05)

- When filter_by_schema: true in config, clean the record and filter out
  invalid record against the schema.
- Externalized json2schema.py as [getschema](https://pypi.org/project/getschema/)

### 0.1.1 (2020-11-08)

- Custom header (See README.md)
- Raise when an invalid type is set in schema
- Treat numbers with leading zeros as string

### 0.1.0b2 (2020-08-12)

Project description update only.

### 0.1.0b1 (2020-08-12)

Change repository and command name to tap-rest-api (from underscore)

### 0.1.0b0 (2020-08-11)

Beta release. The first pypi package build.
