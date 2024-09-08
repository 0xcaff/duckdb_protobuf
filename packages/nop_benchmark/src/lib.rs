mod vtab;

use std::error::Error;
use std::ffi::{c_char, c_void};

use crate::vtab::NopBenchmark;
use duckdb::ffi;
use duckdb::Connection;
use duckdb_loadable_macros::duckdb_entrypoint;

#[duckdb_entrypoint]
fn nop_benchmark_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function::<NopBenchmark>("nop_benchmark")?;

    Ok(())
}
