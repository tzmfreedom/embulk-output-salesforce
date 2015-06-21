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
- **action**: output action that is "insert", "update", "upsert" or "delete". (string, required)
- **result_dir**: directory for resulting csv(success and error file). (string, default: "./target")
- **version**: API version(string, default: "34.0")

## Example

```yaml
out:
  type: salesforce
  username: hoge@example.com
  password: fuga
  action: insert
```


## Build

```
$ ./gradlew gem
```
