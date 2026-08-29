mod filtered_dynamic_message;
mod io;
mod read;
mod types;
mod vtab;

use std::error::Error;

use crate::vtab::ProtobufVTab;
use duckdb::{duckdb_entrypoint_c_api, Connection};

#[duckdb_entrypoint_c_api(ext_name = "protobuf", min_duckdb_version = "v1.5.5")]
pub fn protobuf_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function_local_init::<ProtobufVTab>("protobuf")?;

    Ok(())
}
