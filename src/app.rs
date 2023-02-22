//! Contains structures and functionality for the binary

pub mod cli;
pub mod shell;
use std::borrow::BorrowMut;

pub use self::cli::*;
pub use self::shell::*;
use clap::{Args, Parser, Subcommand};
use stage2::error::Error;
use stage2::logical::model::Program;
use stage2::physical::dictionary::Dictionary;

/// Application state
struct AppState<Dict: Dictionary> {
    /// parsed program data
    program: Program<Dict>,
}

impl<Dict: Dictionary> AppState<Dict> {
    fn new(program: Program<Dict>) -> Self {
        Self { program }
    }
}

/// Stage 2 tool
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct App {
    #[command(subcommand)]
    command: Command,
    #[command(flatten)]
    global_options: GlobalOptions,
}

impl App {
    /// Application logic, based on the parsed data inside the [`App`] object
    pub fn run(&mut self) -> Result<(), Error> {
        self.init_logging();
        match &mut self.command {
            Command::Materialize(cliapp) => cliapp.run(),
            Command::Shell => Shell::default().run(),
        }
    }
    /// Initialising Logging
    ///
    /// Sets the logging verbosity to the given log-level in the following order:
    ///  * `Info`, `Debug`, `Trace`; depending on the count of `-v`
    ///  * `Error` when `-q` is used
    ///  * The `RUST_LOG` environment variable value
    ///  * `Warn` otherwise
    fn init_logging(&self) {
        let mut builder = env_logger::Builder::new();
        let builder = builder.parse_default_env();
        let builder = if let Some(ref level) = self.global_options.log_level {
            builder.parse_filters(level)
        } else if self.global_options.quiet {
            builder.filter_level(log::LevelFilter::Error)
        } else {
            builder.filter_level(match self.global_options.verbose {
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                3 => log::LevelFilter::Trace,
                _ => log::LevelFilter::Warn,
            })
        };
        builder.init();
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Stage 2 CLI
    Materialize(CliApp),
    /// Stage 2 Shell
    Shell,
}

#[derive(Args, Debug)]
struct GlobalOptions {
    /// Sets the verbosity of logging if the flags -v and -q are not used
    #[arg(global = true, long = "log", value_parser=clap::builder::PossibleValuesParser::new(["error", "warn", "info", "debug", "trace"]), group = "verbosity")]
    log_level: Option<String>,
    /// Sets log verbosity (multiple times means more verbose)
    #[arg(global = true, short, long, action = clap::builder::ArgAction::Count, group = "verbosity")]
    verbose: u8,
    /// Sets log verbosity to only log errors
    #[arg(global = true, short, long, group = "verbosity")]
    quiet: bool,
    /// Overwrite existing files. This will remove all files in the given output directory
    #[arg(global = true, long = "overwrite-results", default_value = "false")]
    overwrite: bool,
    /// Gzip output files
    #[arg(global = true, short, long = "gzip", default_value = "false")]
    gz: bool,
}
