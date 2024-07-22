use anyhow::Result;
use duckdb::{Config, Connection};
use prost::Message;
use prost_types::Timestamp;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        compile_protos().expect("Failed to compile protobufs");
        generate_test_data().expect("Failed to generate test data");
        compile_duckdb_extension().expect("Failed to compile DuckDB extension");
        attach_metadata().expect("Failed to attach metadata");
    });
}

fn compile_protos() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = "tests/protos/user.proto";
    let out_dir = "tests/generated";

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

fn compile_duckdb_extension() -> Result<()> {
    Command::new("cargo")
        .args(["build", "--release"])
        .status()?;

    Ok(())
}

fn attach_metadata() -> Result<()> {
    let target_dir = "../../target/release";
    let library_output = if cfg!(target_os = "macos") {
        "libduckdb_protobuf.dylib"
    } else if cfg!(target_os = "linux") {
        "libduckdb_protobuf.so"
    } else {
        unimplemented!("Unsupported platform");
    };

    Command::new("cargo")
        .args([
            "run",
            "--package",
            "duckdb_metadata_bin",
            "--bin",
            "duckdb_metadata",
            "--",
            "--input",
            &format!("{}/{}", target_dir, library_output),
            "--output",
            &format!("{}/protobuf.duckdb_extension", target_dir),
            "--extension-version",
            "v0.0.1",
            "--duckdb-version",
            "v1.0.0",
            "--platform",
            if cfg!(target_os = "macos") {
                "osx_arm64"
            } else if cfg!(target_os = "linux") {
                "linux_amd64"
            } else {
                unimplemented!("Unsupported platform")
            },
        ])
        .status()?;

    println!("Metadata attached successfully.");

    Ok(())
}

#[test]
fn test_setup_creates_files() {
    setup();

    for i in 0..3 {
        let file_path = format!("./tests/generated/data/user_{}.bin", i);
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

    // FIXME: Error: Query returned no rows
    let val = conn.query_row(
        "
SELECT * 
FROM protobuf(
    descriptors = './tests/generated/descriptor.pb',
    files = './tests/generated/data/**/*.bin',
    message_type = 'user.User', 
    delimiter = 'BigEndianFixed')
LIMIT 10;",
        [],
        |row| <(String,)>::try_from(row),
    )?;

    println!("Query result: {:?}", val);

    // FIXME: not results yet too. 🥺
    //     let mut stmt = conn.prepare(
    //         "
    // SELECT *
    // FROM protobuf(
    //     descriptors = './tests/generated/descriptor.pb',
    //     files = './tests/generated/data/user_*.bin',
    //     message_type = 'user.User',
    //     delimiter = 'BigEndianFixed'
    // )
    // LIMIT 10;
    //     ",
    //     )?;
    //
    //     let mut rows = stmt.query([])?;
    //
    //     while let Some(row) = rows.next()? {
    //         let name: String = row.get(0)?;
    //         let id: i32 = row.get(1)?;
    //         let created_at: String = row.get(2)?;
    //         println!("name: {}, id: {}, created_at: {}", name, id, created_at);
    //     }

    Ok(())
}
