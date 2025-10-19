//! nemo-fmt, an auto-formatter for rules files

#![deny(
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts
)]
#![warn(
    missing_docs,
    unused_import_braces,
    unused_qualifications,
    unused_extern_crates,
    variant_size_differences
)]

use std::{
    fs::{read_to_string, write},
    path::PathBuf,
};

use clap::Parser as _;

use cli::CliApp;
use nemo::parser::{ast::ProgramAST, Parser};

pub mod cli;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error(transparent)]
    Nemo(#[from] nemo::error::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("Failed to parse the file")]
    Parsing,
}

async fn format_file(file: &PathBuf, dry_run: bool) -> Result<bool, Error> {
    log::info!("formatting {file:?}");
    let input = read_to_string(file)?;
    let parser = Parser::initialize(&input);
    let ast = parser.parse().map_err(|_| Error::Parsing)?;

    log::debug!("{ast}");

    let formatted = ast.pretty_print(0).ok_or(Error::Parsing)?;

    if formatted != input {
        log::info!("writing updated file {file:?}");
        if dry_run {
            log::info!("doing a dry run, printing new contents");
            print!("{formatted}")
        } else {
            write(file, &formatted)?;
        }
        Ok(true)
    } else {
        // file unchanged
        Ok(false)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cli = CliApp::parse();
    cli.logging.initialize_logging();
    log::info!("Version: {}", clap::crate_version!());
    log::debug!("Rule files: {:?}", cli.files);

    for file in &cli.files {
        format_file(file, cli.dry_run).await?;
    }

    Ok(())
}
