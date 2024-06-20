# duckdb_protobuf

A duckdb plugin for parsing sequences of protobuf messages.

## quick start

download the latest release

start duckdb with `-unsigned` flag to allow loading unsigned libraries

```bash
duckdb -unsigned
```

or if you're using the jdbc connector, you can do this with `allow_unsigned_extensions` jdbc connection property.

next load the plugin

```sql
LOAD '/Users/martin/projects/duckdb_protobuf/target/debug/libduckdb_protobuf.dylib';
```

and start shredding up your protobufs!

```sql
SELECT *
FROM protobuf(
    descriptors = './descriptor.pb',
    files = './scrape/data/SceneVersion/**/*.bin',
    message_type = 'test_server.v1.GetUserSceneVersionResponse'
)
LIMIT 10;
```

## configuration

* `descriptors` - path to the protobuf descriptor file.