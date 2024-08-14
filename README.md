# duckdb_protobuf

a duckdb extension for parsing sequences of protobuf messages encoded in either 
the standard varint delimited format or a u32 big endian delimited format.

## quick start

ensure you're using duckdb 1.0.0

```bash
$ duckdb -version
v1.0.0 1f98600c2c
```

start duckdb with `-unsigned` flag to allow loading unsigned libraries.

```bash
$ duckdb -unsigned
```

or if you're using the jdbc connector, you can do this with the
`allow_unsigned_extensions` jdbc connection property.

now install the extension:

```sql
INSTALL protobuf from 'https://duckdb.0xcaff.xyz';
```

next load it (you'll need to do this once for every session you want to use 
the extension)

```sql
LOAD protobuf;
```

and start shredding up your protobufs!

```sql
SELECT *
FROM protobuf(
    descriptors = './descriptor.pb',
    files = './scrape/data/SceneVersion/**/*.bin',
    message_type = 'test_server.v1.GetUserSceneVersionResponse',
    delimiter = 'BigEndianFixed'
)
LIMIT 10;
```

if you want builds for a platform or version which currently doesn't have
builds, please open an issue.

<details>
  <summary>install from file</summary>

download the latest version from
[releases](https://github.com/0xcaff/duckdb_protobuf/releases). if you're on
macOS, blow away the quarantine params with the following to allow the file
to be loaded

```sh
$ xattr -d com.apple.quarantine /Users/martin/Downloads/protobuf.duckdb_extension
```

next load the extension

```sql
LOAD '/Users/martin/Downloads/protobuf.duckdb_extension';
```
</details>

## why

sometimes you want to land your row primary data in a format with a well-defined
structure and pretty good decode performance and poke around without a load
step. maybe you're scraping an endpoint which returns protobuf responses, you're
figuring out the schema as you go and iteration speed matters much more than 
query performance.

`duckdb_protobuf` allows for making a new choice along the
flexibility-performance tradeoff continuum for fast exploration of protobuf 
streams with little upfront load complexity or time.

## configuration

* `descriptors`: path to the protobuf descriptor file. Generated using something
  like `protoc --descriptor_set_out=descriptor.pb ...`
* `files`: glob pattern for the files to read. Uses the [`glob`][glob] crate 
  for evaluating globs.
* `message_type`: the fully qualified message type to parse.
* `delimiter`: specifies where one message starts and the next one begins
  * `BigEndianFixed`: every message is prefixed with a u32 big endian value 
    specifying its length. files are a sequence of messages
  * `Varint`: every message is prefixed with a protobuf Varint value
    ([encoding](https://protobuf.dev/programming-guides/encoding/#varints)). 
    files are a sequence of messages
  * `SingleMessagePerFile`: each file contains a single message

## features

* converts `google.protobuf.Timestamp` messages to duckdb timestamp
* supports nested messages with repeating fields

## limitations

* doesn't support a few types (bytes, maps, {s,}fixed{32,64}, sint{32,64}), 
  contributions and even feedback that these field types are used is welcome!
* execution is single threaded (limitations of the rust bindings)
* no community plugin due to lack of out of tree build support from duckdb 
  community repository

i'm releasing this to understand how other folks are using protobuf streams and
duckdb. i'm open to PRs, issues and other feedback.

[glob]: https://docs.rs/glob/latest/glob/