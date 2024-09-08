# A Makefile for building stuff during development. This is not used for the CI
# builds. These two are separate concepts with different constraints (which
# target is specified, cross compilation, etc.). Using this as a task runner (a
# la just).

DUCKDB_PLATFORM := osx_arm64
DUCKDB_EXTENSION_VERSION := v0.0.1
DUCKDB_VERSION := v1.0.0

ifeq ($(DUCKDB_PLATFORM),windows_amd64)
	LIBRARY_OUTPUT := duckdb_protobuf.dll
endif
ifeq ($(DUCKDB_PLATFORM),osx_arm64)
	LIBRARY_OUTPUT := libduckdb_protobuf.dylib
endif
ifeq ($(DUCKDB_PLATFORM),linux_amd64)
	LIBRARY_OUTPUT := libduckdb_protobuf.so
endif

packages/vendor/duckdb:
	mkdir -p packages/vendor/duckdb
	curl -L https://crates.io/api/v1/crates/duckdb/1.0.0/download | tar --strip-components=1 -xz -C packages/vendor/duckdb
	patch --strip=1 --directory=packages/vendor/duckdb < patches/duckdb+1.0.0.patch

packages/vendor/duckdb-loadable-macros:
	mkdir -p packages/vendor/duckdb-loadable-macros
	curl -L https://crates.io/api/v1/crates/duckdb-loadable-macros/0.1.1/download | tar --strip-components=1 -xz -C packages/vendor/duckdb-loadable-macros

packages/vendor/libduckdb-sys:
	mkdir -p packages/vendor/libduckdb-sys
	curl -L https://crates.io/api/v1/crates/libduckdb-sys/1.0.0/download | tar --strip-components=1 -xz -C packages/vendor/libduckdb-sys

vendor: packages/vendor/duckdb packages/vendor/duckdb-loadable-macros packages/vendor/libduckdb-sys

debug: vendor
	cargo build --package duckdb_protobuf
	cargo run \
		--package duckdb_metadata_bin \
		--bin duckdb_metadata \
		-- \
		--input target/debug/$(LIBRARY_OUTPUT) \
		--output target/debug/protobuf.duckdb_extension \
		--extension-version $(DUCKDB_EXTENSION_VERSION) \
		--duckdb-version $(DUCKDB_VERSION) \
		--platform $(DUCKDB_PLATFORM)

release: vendor
	cargo build --package duckdb_protobuf --release
	cargo run \
		--package duckdb_metadata_bin \
		--bin duckdb_metadata \
		-- \
		--input target/release/$(LIBRARY_OUTPUT) \
		--output target/release/protobuf.duckdb_extension \
		--extension-version $(DUCKDB_EXTENSION_VERSION) \
		--duckdb-version $(DUCKDB_VERSION) \
		--platform $(DUCKDB_PLATFORM)

benchmark: vendor
	cargo build --package nop_benchmark --release
	cargo run \
		--package duckdb_metadata_bin \
		--bin duckdb_metadata \
		-- \
		--input target/release/libnop_benchmark.dylib \
		--output target/release/nop_benchmark.duckdb_extension \
		--extension-version $(DUCKDB_EXTENSION_VERSION) \
		--duckdb-version $(DUCKDB_VERSION) \
		--platform $(DUCKDB_PLATFORM)

	duckdb \
		-unsigned \
		-cmd "FORCE INSTALL 'target/release/nop_benchmark.duckdb_extension'" \
		-no-stdin

test: release
	cargo test --package duckdb_protobuf

install: release
	duckdb \
		-unsigned \
		-cmd "FORCE INSTALL 'target/release/protobuf.duckdb_extension'" \
		-no-stdin

.PHONY: test release debug vendor