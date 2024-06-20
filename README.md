# duckdb_protobuf

a duckdb extension for parsing sequences of protobuf messages encoded in either 
the standard varint delimited format or a u32 big endian delimited format.

## quick start

ensure you're using duckdb 0.10.1

```
$ duckdb -version
v0.10.1 4a89d97db8
```

currently only works with duckdb 0.10.1 due to breaking changes in the C ABI
in versions following. waiting for the rust bindings to merge support for 1.0 to
upgrade.

start duckdb with `-unsigned` flag to allow loading unsigned libraries

```bash
$ duckdb -unsigned
```

or if you're using the jdbc connector, you can do this with the
`allow_unsigned_extensions` jdbc connection property.

next load the extension

```sql
LOAD '/Users/martin/projects/duckdb_protobuf/target/debug/libduckdb_protobuf.dylib';
```

now start shredding up your protobufs!

```sql
SELECT *
FROM protobuf(
    descriptors = './descriptor.pb',
    files = './scrape/data/SceneVersion/**/*.bin',
    message_type = 'test_server.v1.GetUserSceneVersionResponse'
)
LIMIT 10;
```

## why

for some workloads which generate discrete data (think scraping a grpc API for
users, or sampling a click stream) its desirable to store data in its 
original form without applying a transform step. sometimes in this state, 
you don't know what questions to ask yet, but you want to try a bunch of cuts 
over your data to get a better sense of what's inside.

`duckdb_protobuf` allows for making a new choice along the
flexibility-performance tradeoff continuum for fast exploration with little 
import complexity

## configuration

* `descriptors`: path to the protobuf descriptor file. Generated using something
  like `protoc --descriptor_set_out=descriptor.pb ...`
* `files`: glob pattern for the files to read. Uses the [`glob`][glob] crate 
  for evaluating globs.
* `message_type`: the fully qualified message type to parse.

## features

* converts `google.protobuf.Timestamp` messages to duckdb timestamp
* supports nested messages with repeating fields

## limitations

* doesn't support a few types (bytes, {s,}fixed{32,64}, sint{32,64}), 
  contributions welcome!
* execution is single threaded (limitations of the rust bindings)

i'm releasing this to understand how other folks are using protobuf
sequences and duckdb. as such, i'm open to PRs, issues and other high 
information feedback.

[glob]: https://docs.rs/glob/latest/glob/