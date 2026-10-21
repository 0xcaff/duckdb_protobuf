# duckdb_protobuf

a duckdb extension for parsing sequences of protobuf messages encoded in either 
the standard varint delimited format or a u32 big endian delimited format.

## quick start

ensure you're using duckdb 1.1.0 for support with the latest features. if you
need new features on an old versions, please open an issue.

```bash
$ duckdb -version
v1.1.0 fa5c2fe15f
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

## building with Nix

The Linux Nix build uses [cargo-dyndrv](https://github.com/obsidiansystems/cargo-dyndrv)
to build each Rust crate with dynamic derivations, without import from derivation
(IFD). Nixpkgs follows cargo-dyndrv's pinned input so its compiler and build hooks
stay compatible with the builder. A local cargo-dyndrv patch supplies the Cargo
metadata variables used by this workspace, caps dependency lints, and links
`cdylib` dependencies using their Rust libraries.

Both the Nix client and the daemon performing the build need support for
`builder-rpc-v0` (Nix 2.36 or a recent development build; Nix 2.35 is too old).
Enable the experimental features in the daemon's `nix.conf` and restart the
daemon after changing it:

```ini
extra-experimental-features = ca-derivations dynamic-derivations
```

Build the extension with IFD disabled:

```sh
nix build --extra-experimental-features 'ca-derivations dynamic-derivations' \
  --option allow-import-from-derivation false
```

The extension is written to `result/linux_amd64/protobuf.duckdb_extension` on
x86_64 Linux or `result/linux_arm64/protobuf.duckdb_extension` on AArch64 Linux.
The build preserves the patched DuckDB dependencies and appends the extension
metadata automatically. The artifact targets the `v0.0.1` C API and needs a
compatible DuckDB 1.1.x build with a matching platform tag.

cargo-dyndrv does not currently support macOS, so the Nix extension package is
only exposed on Linux. The development shell and `vendorScript` remain available
on Apple Silicon macOS. Intel macOS is no longer supported by the pinned
Nixpkgs. Use the existing Cargo/Makefile build on macOS.

For local development, populate the patched workspace dependencies first:

```sh
nix develop
vendor-deps
cargo build --package duckdb_protobuf --package duckdb_metadata_bin
```

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
  for evaluating globs. A pattern matching no files returns zero rows, with
  the schema defined by `descriptors` and `message_type`. Invalid glob patterns
  and errors reading matching files still fail the query.
* `message_type`: the fully qualified message type to parse.
* `delimiter`: specifies where one message starts and the next one begins
  * `BigEndianFixed`: every message is prefixed with a u32 big endian value 
    specifying its length. files are a sequence of messages
  * `Varint`: every message is prefixed with a protobuf Varint value
    ([encoding](https://protobuf.dev/programming-guides/encoding/#varints)). 
    files are a sequence of messages
  * `SingleMessagePerFile`: each file contains a single message
* `filename`, `position` and `size`: boolean values enabling columns which add
  source information about where the messages originated from

## features

* converts `google.protobuf.Timestamp` messages to duckdb timestamp
* supports nested messages with repeating fields
* scales decoding across as many threads as duckdb allows
* supports projection pushdown (for first level of columns) ensuring only
  necessary columns are decoded.

## limitations

* doesn't support a few types (bytes, maps, {s,}fixed{32,64}, sint{32,64}), 
  contributions and even feedback that these field types are used is welcome!

i'm releasing this to understand how other folks are using protobuf streams and
duckdb. i'm open to PRs, issues and other feedback.

[glob]: https://docs.rs/glob/latest/glob/
