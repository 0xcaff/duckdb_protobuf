use anyhow::{Context, Result};
use clap::Parser;
use std::fs::File;
use std::io;
use std::path::PathBuf;

use duckdb_metadata::{pad_32, MetadataFields};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    output: PathBuf,

    #[clap(short, long)]
    input: PathBuf,

    #[clap(short, long)]
    extension_version: String,

    #[clap(short, long)]
    duckdb_version: String,

    /// Full list on https://duckdb.org/docs/extensions/working_with_extensions.html#platforms
    #[clap(short, long)]
    platform: String,

    #[clap(short, long, default_value = "4")]
    metadata_version: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let extension_version =
        pad_32(args.extension_version.as_bytes()).context("extension_version")?;
    let duckdb_version = pad_32(args.duckdb_version.as_bytes()).context("duckdb_version")?;
    let platform = pad_32(args.platform.as_bytes()).context("platform")?;
    let metadata_version = pad_32(args.metadata_version.as_bytes()).context("metadata_version")?;

    let metadata_fields = MetadataFields {
        meta_8: [0; 32],
        meta_7: [0; 32],
        meta_6: [0; 32],
        meta_5: [0; 32],
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
