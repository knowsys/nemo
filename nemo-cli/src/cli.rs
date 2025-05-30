//! Contains structures and functionality for the binary
use std::path::PathBuf;

use nemo::{
    error::Error, io::ExportManager, rule_model::components::term::primitive::ground::GroundTerm,
};

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
    /// Control amount of reporting printed by the program
    #[arg(long = "report", value_enum, default_value_t)]
    pub(crate) reporting: Reporting,
    /// Arguments related to logging
    #[command(flatten)]
    pub(crate) logging: LoggingArgs,
    /// Overwrite global parameters in the rule file
    #[arg(long = "param")]
    pub(crate) parameters: Vec<ParamKeyValue>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParamKeyValue {
    #[allow(dead_code)]
    pub(crate) key: String,
    #[allow(dead_code)]
    pub(crate) value: GroundTerm,
}

impl clap::builder::ValueParserFactory for ParamKeyValue {
    type Parser = ParamKeyValueParser;

    fn value_parser() -> Self::Parser {
        ParamKeyValueParser
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ParamKeyValueParser;
impl clap::builder::TypedValueParser for ParamKeyValueParser {
    type Value = ParamKeyValue;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let inner = clap::builder::StringValueParser::new();
        let val = inner.parse_ref(cmd, arg, value)?;
        let Some((key, value)) = val.split_once('=') else {
            return Err(clap::Error::new(clap::error::ErrorKind::InvalidValue).with_cmd(cmd));
        };

        let Ok(value) = GroundTerm::parse(value) else {
            return Err(clap::Error::new(clap::error::ErrorKind::InvalidValue).with_cmd(cmd));
        };

        Ok(ParamKeyValue {
            key: key.into(),
            value,
        })
    }
}
