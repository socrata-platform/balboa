# Balboa Common

Common components between all balboa projects.

* Standard Configuration Formatting
* Message and Metric Data model

## Metrics Message Format

### Protobuf

TODO - Currently used but being deprecated

### Json Message Format

For convenience and testing purposes plain ol JSON is flowing through the message broker.

#### Version

```json
{
  "entityId":"<entity-id>",
  "timestamp":1431970648000,
  "metrics":
    "<metric-name>": { "value":1,"type":"aggregate" },
    "<metric-name>": { "value":200,"type":"absolute" },
    ...
}
```

Notes:

