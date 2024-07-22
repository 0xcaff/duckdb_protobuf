use anyhow::Result;
use duckdb::{Config, Connection};
use prost::Message;
use prost_types::Timestamp;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        compile_protos().expect("Failed to compile protobufs");
        generate_test_data().expect("Failed to generate test data");
    });
}

fn compile_protos() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = "tests/protos/user.proto";
    let out_dir = "tests/generated";

    std::fs::create_dir_all(out_dir)?;

    prost_build::Config::new()
        .out_dir(out_dir)
        .compile_protos(&[proto_path], &[Path::new("tests/protos")])?;

    Ok(())
}

fn generate_test_data() -> Result<(), Box<dyn std::error::Error>> {
    // Include the generated Rust code for the protobuf messages
    mod user {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/generated/user.rs"
        ));
    }

    // Create some example User messages
    let users = [
        user::User {
            name: "Alice".to_string(),
            id: 1,
            created_at: Some(current_timestamp()),
        },
        user::User {
            name: "Bob".to_string(),
            id: 2,
            created_at: Some(current_timestamp()),
        },
        user::User {
            name: "Charlie".to_string(),
            id: 3,
            created_at: Some(current_timestamp()),
        },
    ];

    // Serialize the messages to binary files
    for (i, user) in users.iter().enumerate() {
        let mut buf = Vec::new();
        user.encode(&mut buf)?;

        let mut file = File::create(format!("./tests/data/user_{}.bin", i))?;
        file.write_all(&buf)?;
    }

    Ok(())
}

fn current_timestamp() -> Timestamp {
    let start = SystemTime::now();
    let since_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    Timestamp {
        seconds: since_epoch.as_secs() as i64,
        nanos: since_epoch.subsec_nanos() as i32,
    }
}

#[test]
fn test_setup_creates_files() {
    setup();

    for i in 0..3 {
        let file_path = format!("./tests/data/user_{}.bin", i);
        assert!(
            Path::new(&file_path).exists(),
            "File {} should exist",
            file_path
        );
    }
}

#[test]
#[ignore = "FIXME: The file is not a DuckDB extension. The metadata at the end of the file is invalid"]
fn test_query_protobuf_data() -> Result<()> {
    // Call the setup function to generate test data
    setup();

    // Connect to DuckDB
    let config = Config::default().allow_unsigned_extensions()?;
    let conn = Connection::open_in_memory_with_flags(config)?;

    // Load the protobuf extension
    // FIXME: The file is not a DuckDB extension. The metadata at the end of the file is invalid
    conn.execute("LOAD './target/release/libduckdb_protobuf.dylib'", [])?;

    // Query the protobuf data
    let mut stmt = conn.prepare(
        "
        SELECT *
        FROM protobuf(
            descriptors = './tests/generated/descriptor.pb',
            files = './tests/data/user_*.bin',
            message_type = 'user.User',
            delimiter = 'BigEndianFixed'
        )
        LIMIT 10;
    ",
    )?;

    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let id: i32 = row.get(1)?;
        let created_at: String = row.get(2)?;
        println!("name: {}, id: {}, created_at: {}", name, id, created_at);
    }

    Ok(())
}
