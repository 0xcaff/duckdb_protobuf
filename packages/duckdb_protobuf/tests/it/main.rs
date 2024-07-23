use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Once;

use anyhow::Result;
use duckdb::{Config, Connection};
use prost::Message;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        compile_protos().expect("Failed to compile protobufs");
        generate_test_data().expect("Failed to generate test data");
    });
}

fn compile_protos() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = "tests/protos/user.proto";
    let descriptor_dir = "tests/generated";
    let out_dir = "tests/src";

    std::fs::create_dir_all(descriptor_dir)?;
    std::fs::create_dir_all(out_dir)?;

    prost_build::Config::new()
        .out_dir(out_dir)
        .file_descriptor_set_path("tests/generated/descriptor.pb")
        .compile_protos(&[proto_path], &[Path::new("tests/protos")])?;

    Ok(())
}

fn generate_test_data() -> Result<(), Box<dyn std::error::Error>> {
    // Include the generated Rust code for the protobuf messages
    mod user {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/src/user.rs"));
    }

    // Create some example User messages
    let users = [
        user::User {
            name: "Alice".to_string(),
            id: 1,
        },
        user::User {
            name: "Bob".to_string(),
            id: 2,
        },
        user::User {
            name: "Charlie".to_string(),
            id: 3,
        },
    ];

    let out_dir = "tests/generated/data";
    std::fs::create_dir_all(out_dir)?;

    // Serialize the messages to binary files
    for (i, user) in users.iter().enumerate() {
        let mut buf = Vec::new();
        user.encode(&mut buf)?;

        let mut file = File::create(format!("{out_dir}/user_{}.bin", i))?;
        file.write_all(&buf)?;

        println!("Generated test data for user {}: {:?}", i, user);
    }

    Ok(())
}

#[test]
fn test_setup_creates_files() {
    setup();

    for i in 0..3 {
        let file_path = format!("tests/generated/data/user_{}.bin", i);
        assert!(
            Path::new(&file_path).exists(),
            "File {} should exist",
            file_path
        );
    }
}

#[test]
fn test_query_protobuf_data() -> Result<()> {
    setup();

    let config = Config::default().allow_unsigned_extensions()?;
    let conn = Connection::open_in_memory_with_flags(config)?;

    conn.execute("LOAD '../../target/release/protobuf.duckdb_extension'", [])?;
    println!("DuckDB extension loaded successfully.");

    let mut stmt = conn.prepare(
        "
            SELECT * FROM protobuf(
                descriptors = './tests/generated/descriptor.pb',
                files = './tests/generated/data/**/*.bin',
                message_type = 'user.User',
                delimiter = 'SingleMessagePerFile'
            )
            LIMIT 10;
        ",
    )?;

    let mut rows = stmt.query([])?;

    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let id: i32 = row.get(1)?;
        results.push((name, id));
    }
    println!("Query result: {results:?}");

    assert_eq!(results.len(), 3, "Expected 3 rows");
    assert_eq!(results[0].0, "Alice", "Expected first name to be 'Alice'");
    assert_eq!(results[0].1, 1, "Expected first id to be 1");
    Ok(())
}
