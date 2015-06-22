# Salesforce output plugin for Embulk

Embulk output plugin to load into Salesforce.com.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **username**: salesforce username (string, required)
- **password**: salesforce password (string, required)
- **batch_size**: batch size (string, default: 200)
- **login_endpoint**: login endpoint (string, default: https://login.salesforce.com)
- **sobject**: salesforce object API name (string, required)
- **upsert_key**: upsert API field name (string, default: null)
- **action**: output action that is "insert", "update", "upsert" or "delete" (string, default: insert)
- **result_dir**: directory for resulting csv(success and error file). If the directory is not exist, the plugin show error. If not specified, resulting csv is not created. (string, default: null)
- **version**: API version (string, default: "34.0")

## Example
The column names must be salesforce API field name.  

```yaml
in:
  type: file
  path_prefix: /path/to/salesforce_
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: ','
    quote: '"'
    escape: ''
    skip_header_lines: 1
    comment_line_marker: null
    allow_extra_columns: true
    allow_optional_columns: false
    columns:
    - {name: Name, type: string}
    - {name: Date__c, type: timestamp, format: '%Y-%m-%d'}
    - {name: DateTime__c, type: timestamp, format: '%Y-%m-%d %H:%M:%S%Z'}
    - {name: Number__c, type: double}
    - {name: Reference__c, type: string}
out:
  type: salesforce
  username: hoge@example.com
  password: fuga
  sobject: Account
  action: insert
```


## Build

```
$ ./gradlew gem
```
