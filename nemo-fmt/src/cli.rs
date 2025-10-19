//! CLI arguments for nemo-fmt

use std::path::PathBuf;

use nemo_cli::cli::LoggingArgs;

/// CLI arguments for nemo-fmt
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    #[command(flatten)]
    pub(crate) logging: LoggingArgs,
    #[arg(value_parser, required = true)]
    pub(crate) files: Vec<PathBuf>,
    /// Don't write the file, just print the changed contents.
    #[arg(short = 'n', long = "dry-run")]
    pub(crate) dry_run: bool,
}
