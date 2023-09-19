//! Contains structures and functionality for the binary
use std::path::PathBuf;

use nemo::{error::Error, io::OutputFileManager};

const DEFAULT_OUTPUT_DIRECTORY: &str = "results";

/// Cli Arguments related to logging
#[derive(clap::Args, Debug)]
pub struct LoggingArgs {
    /// Sets the verbosity of logging if the flags -v and -q are not used
    #[arg(long = "log", value_parser=clap::builder::PossibleValuesParser::new(["error", "warn", "info", "debug", "trace"]), group = "verbosity")]
    log_level: Option<String>,
    /// Sets log verbosity (multiple times means more verbose)
    #[arg(short, long, action = clap::builder::ArgAction::Count, group = "verbosity")]
    verbose: u8,
    /// Sets log verbosity to only log errors
    #[arg(short, long, group = "verbosity")]
    quiet: bool,
}

impl LoggingArgs {
    /// Initialising Logging
    ///
    /// Sets the logging verbosity to the given log-level in the following order:
    ///  * `Info`, `Debug`, `Trace`; depending on the count of `-v`
    ///  * `Error` when `-q` is used
    ///  * The `NMO_LOG` environment variable value
    ///  * `Warn` otherwise
    pub fn initialize_logging(&self) {
        let mut builder = env_logger::Builder::new();
        builder.parse_env("NMO_LOG");
        if let Some(ref level) = self.log_level {
            builder.parse_filters(level);
        } else if self.quiet {
            builder.filter_level(log::LevelFilter::Error);
        } else if self.verbose > 0 {
            builder.filter_level(match self.verbose {
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                3 => log::LevelFilter::Trace,
                _ => log::LevelFilter::Warn,
            });
        }
        builder.init();
    }
}

/// Cli arguments related to file output
#[derive(Debug, clap::Args)]
pub struct OutputArgs {
    /// Save results to files. (Also see --output-dir)
    #[arg(short, long = "save-results")]
    save_results: bool,
    /// Specify directory for output files. (Only relevant if --save-results is set.)
    #[arg(short='D', long = "output-dir", default_value = DEFAULT_OUTPUT_DIRECTORY, requires="save_results")]
    output_directory: PathBuf,
    /// Overwrite existing files in --output-dir. (Only relevant if --save-results is set.)
    #[arg(
        short,
        long = "overwrite-results",
        default_value = "false",
        requires = "save_results"
    )]
    overwrite: bool,
    /// Gzip output files
    #[arg(
        short,
        long = "gzip",
        default_value = "false",
        requires = "save_results"
    )]
    gz: bool,
}

impl OutputArgs {
    /// Creates an output file manager with the current options
    pub fn initialize_output_manager(self) -> Result<Option<OutputFileManager>, Error> {
        if !self.save_results {
            if self.output_directory != PathBuf::from(DEFAULT_OUTPUT_DIRECTORY) {
                log::warn!(
                    "Ignoring output directory `{:?}` since `--save-results` is false",
                    self.output_directory
                );
            }

            if self.gz {
                log::warn!(
                    "Ignoring gz-compression of output files `{:?}` since `--save-results` is false",
                    self.gz
                );
            }

            return Ok(None);
        }

        Ok(Some(OutputFileManager::try_new(
            self.output_directory,
            self.overwrite,
            self.gz,
        )?))
    }
}

/// Nemo CLI
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// Arguments related to logging
    #[command(flatten)]
    pub logging: LoggingArgs,
    /// One or more rule program files
    #[arg(value_parser, required = true)]
    pub rules: Vec<PathBuf>,
    /// Arguments related to output
    #[command(flatten)]
    pub output: OutputArgs,
    /// Override @output directives and save every IDB predicate
    #[arg(
        long = "write-all-idb-predicates",
        default_value = "false",
        requires = "save_results"
    )]
    pub write_all_idb_predicates: bool,
    /// Display detailed timing information
    #[arg(long = "detailed-timing", default_value = "false")]
    pub detailed_timing: bool,
    /// Display detailed memory information
    #[arg(long = "detailed-memory", default_value = "false")]
    pub detailed_memory: bool,
    /// Specify directory for input files.
    #[arg(short = 'I', long = "input-dir")]
    pub input_directory: Option<PathBuf>,
    /// Specify a fact, the origin of which should be explained
    #[arg(long = "trace")]
    pub trace_fact: Option<String>,
}
