use anyhow::{Context, Result};
use clap::Parser;
use std::fs::File;
use std::io;
use std::path::PathBuf;

use duckdb_metadata::{pad_32, MetadataFields};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long)]
    output: PathBuf,

    #[clap(long)]
    input: PathBuf,

    #[clap(long)]
    extension_version: String,

    #[clap(long)]
    duckdb_version: String,

    /// Full list on https://duckdb.org/docs/extensions/working_with_extensions.html#platforms
    #[clap(long)]
    platform: String,

    #[clap(long, default_value = "4")]
    metadata_version: String,

    #[clap(long, default_value = "CPP")]
    extension_abi_type: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let extension_version =
        pad_32(args.extension_version.as_bytes()).context("extension_version")?;
    let duckdb_version = pad_32(args.duckdb_version.as_bytes()).context("duckdb_version")?;
    let platform = pad_32(args.platform.as_bytes()).context("platform")?;
    let metadata_version = pad_32(args.metadata_version.as_bytes()).context("metadata_version")?;
    let extension_abi_type =
        pad_32(args.extension_abi_type.as_bytes()).context("extension_abi_type")?;

    let metadata_fields = MetadataFields {
        meta_8: [0; 32],
        meta_7: [0; 32],
        meta_6: [0; 32],
        extension_abi_type,
        extension_version,
        duckdb_version,
        platform,
        metadata_version,
        signature: [0; 256],
    };

    let mut input_file = File::open(&args.input)
        .with_context(|| format!("failed to open input file: {:?}", args.input))?;

    let mut output_file = File::create(&args.output)
        .with_context(|| format!("failed to create output file: {:?}", args.output))?;

    io::copy(&mut input_file, &mut output_file)?;

    metadata_fields
        .write(&mut output_file)
        .context("failed to write metadata to output file")?;

    println!("output generated {:?}", args.output);

    Ok(())
}
