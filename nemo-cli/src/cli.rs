//! Contains structures and functionality for the binary
use std::path::PathBuf;

use clap::ArgAction;
use nemo::{error::Error, execution::execution_parameters::ExportParameters, io::ExportManager};

/// Default export directory.
const DEFAULT_OUTPUT_DIRECTORY: &str = "results";

/// Possible settings for the export option.
#[derive(clap::ValueEnum, Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) enum Exporting {
    /// Export data as specified in program
    #[default]
    Keep,
    /// Disable all exports.
    None,
    /// Export all IDB predicates (those used in rule heads).
    Idb,
    /// Export all EDB predicates (those for which facts are given or imported).
    Edb,
    /// Export all predicates.
    All,
}

impl From<Exporting> for ExportParameters {
    fn from(val: Exporting) -> Self {
        match val {
            Exporting::Keep => ExportParameters::Keep,
            Exporting::None => ExportParameters::None,
            Exporting::Idb => ExportParameters::Idb,
            Exporting::Edb => ExportParameters::Edb,
            Exporting::All => ExportParameters::All,
        }
    }
}

/// Possible settings for the fact-printing option.
#[derive(clap::ValueEnum, Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) enum FactPrinting {
    /// Do not print facts for any predicate.
    #[default]
    None,
    /// Print facts for all IDB predicates (those used in rule heads).
    Idb,
    /// Print facts for all EDB predicates (those for which facts are given or imported).
    Edb,
    /// Print facts for all predicates.
    All,
}

impl FactPrinting {
    /// Whether printing is enabled for some predicates
    #[allow(dead_code)]
    pub(crate) fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Possible settings for the reporting option.
#[derive(clap::ValueEnum, Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) enum Reporting {
    /// Disable reporting.
    None,
    /// Print short report if no other results are printed. Otherwise disable reporting.
    #[default]
    Auto,
    /// Print short report.
    Short,
    /// Print short report and detailed memory timing.
    Time,
    /// Print short report and detailed memory usage.
    Mem,
    /// Print short report and all details on timing and memory usage.
    All,
}

/// Cli Arguments related to logging
#[derive(clap::Args, Debug)]
pub(crate) struct LoggingArgs {
    /// Increase log verbosity (multiple uses increase verbosity further)
    #[arg(short, long, action = clap::builder::ArgAction::Count, group = "verbosity")]
    verbose: u8,
    /// Reduce log verbosity to show only errors (equivalent to --log error)
    #[arg(short, long, group = "verbosity")]
    quiet: bool,
    /// Set log verbosity (default is "warn")
    #[arg(long = "log", value_parser=clap::builder::PossibleValuesParser::new(["error", "warn", "info", "debug", "trace"]), group = "verbosity")]
    log_level: Option<String>,
}

impl LoggingArgs {
    /// Initialising Logging
    ///
    /// Sets the logging verbosity to the given log-level in the following order:
    ///  * `Info`, `Debug`, `Trace`; depending on the count of `-v`
    ///  * `Error` when `-q` is used
    ///  * The `NMO_LOG` environment variable value
    ///  * `Warn` otherwise
    #[allow(dead_code)]
    pub(crate) fn initialize_logging(&self) {
        let mut builder = env_logger::Builder::new();

        // Default log level
        builder.filter_level(log::LevelFilter::Warn);

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
pub(crate) struct OutputArgs {
    /// Override export directives in the program
    #[arg(short, long = "export", value_enum, default_value_t)]
    pub(crate) export_setting: Exporting,
    /// Base directory for exporting files
    #[arg(short='D', long = "export-dir", default_value = DEFAULT_OUTPUT_DIRECTORY)]
    export_directory: PathBuf,
    /// Replace any existing files during export
    #[arg(short, long = "overwrite-results", default_value = "false")]
    overwrite: bool,
    /// Use gzip to compress exports by default;
    /// does not affect export directives that already specify a compression
    #[arg(short, long = "gzip", default_value = "false")]
    gz: bool,
    /// Print all facts for the select predicates
    #[arg(long = "print-facts", value_enum, default_value_t)]
    pub(crate) print_facts_setting: FactPrinting,
}

impl OutputArgs {
    /// Creates an output file manager with the current options
    #[allow(dead_code)]
    pub(crate) fn export_manager(&self) -> Result<ExportManager, Error> {
        let export_manager = ExportManager::default()
            .set_base_path(self.export_directory.clone())
            .overwrite(self.overwrite)
            .compress(self.gz);
        Ok(export_manager)
    }
}

/// Cli arguments related to experiments
#[derive(Debug, clap::Args)]
pub(crate) struct ExperimentArgs {
    #[arg(long = "x-create-queries")]
    pub create_queries: bool,
}

/// Cli arguments related to tracing
#[derive(Debug, clap::Args)]
pub(crate) struct TracingArgs {
    /// Facts for which a derivation trace should be computed;
    /// multiple facts can be separated by a semicolon, e.g. "P(a, b);Q(c)".
    #[arg(long = "trace", value_delimiter = ';', group = "trace-input")]
    pub(crate) facts: Option<Vec<String>>,
    /// Specify one or multiple input files for the facts that should be traced.
    /// The file format is the same as for the "trace" CLI argument.
    #[arg(long = "trace-input-file", value_parser, group = "trace-input")]
    pub(crate) input_file: Option<Vec<PathBuf>>,
    /// File to export the trace to
    #[arg(long = "trace-output", requires = "trace-input")]
    pub(crate) output_file: Option<PathBuf>,
}

/// Cli arguments related to type 1 tracing
#[derive(Debug, clap::Args)]
pub(crate) struct TracingTreeArgs {
    /// Query formatted as json specifying the facts that should be traced
    #[arg(long = "trace-tree")]
    pub(crate) trace_tree_json: Option<String>,
}

/// Cli arguments related to type 2 tracing
#[derive(Debug, clap::Args)]
pub(crate) struct TracingNodeArgs {
    /// Query formatted as json specifying the facts that should be traced
    #[arg(long = "trace-node")]
    pub(crate) trace_node_json: Option<String>,
}

/// Nemo CLI
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// One or more rule program files
    #[arg(value_parser, required = true)]
    pub(crate) rules: Vec<PathBuf>,
    /// Arguments related to output
    #[command(flatten)]
    pub(crate) output: OutputArgs,
    /// Base directory for importing files (default is working directory)
    #[arg(short = 'I', long = "import-dir")]
    pub(crate) import_directory: Option<PathBuf>,
    /// Arguments related to tracing
    #[command(flatten)]
    pub(crate) tracing: TracingArgs,
    /// Arguments related to advanded tracing (tree queries)
    #[command(flatten)]
    pub(crate) tracing_tree: TracingTreeArgs,
    /// Arguments related to advanded tracing (node queries)
    #[command(flatten)]
    pub(crate) tracing_node: TracingNodeArgs,
    /// Control amount of reporting printed by the program
    #[arg(long = "report", value_enum, default_value_t)]
    pub(crate) reporting: Reporting,
    /// Arguments related to logging
    #[command(flatten)]
    pub(crate) logging: LoggingArgs,
    /// Overwrite global parameters in the rule file
    #[arg(long = "param", value_parser = parse_key_val, action = ArgAction::Append)]
    pub(crate) parameters: Vec<ParamKeyValue>,
    /// Disable warnings when validating rule files
    #[arg(long = "no-warnings")]
    pub(crate) disable_warnings: bool,
    /// Experiments
    #[command(flatten)]
    pub(crate) experiments: ExperimentArgs,
}

/// Key-Value pair for global variable
#[derive(Debug, Clone)]
pub struct ParamKeyValue {
    /// Key: Global variable
    pub key: String,
    /// Value
    pub value: String,
}

/// Parse key value pairs.
fn parse_key_val(s: &str) -> Result<ParamKeyValue, String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid key=value: {s}"));
    }
    Ok(ParamKeyValue {
        key: parts[0].to_owned(),
        value: parts[1].to_owned(),
    })
}
