//! Contains structures and functionality for the binary

use clap::Parser;
use stage2::error::Error;
use stage2::io::parser::{all_input_consumed, RuleParser};
use stage2::logical::execution::ExecutionEngine;
use stage2::logical::model::Program;
use stage2::logical::types::{DefaultLogicalTypeCollection, LogicalTypeCollection};
use stage2::meta::TimedCode;
use stage2::physical::dictionary::Dictionary;
use std::fs::read_to_string;
use std::path::PathBuf;

/// Application state
struct AppState<Dict: Dictionary, LogicalTypes: LogicalTypeCollection> {
    /// parsed program data
    program: Program<Dict, LogicalTypes>,
}

impl<Dict: Dictionary, LogicalTypes: LogicalTypeCollection> AppState<Dict, LogicalTypes> {
    fn new(program: Program<Dict, LogicalTypes>) -> Self {
        Self { program }
    }
}

const DEFAULT_OUTPUT_DIRECTORY: &str = "results";

/// Stage 2 CLI
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// Sets the verbosity of logging if the flags -v and -q are not used
    #[arg(long = "log", value_parser=clap::builder::PossibleValuesParser::new(["error", "warn", "info", "debug", "trace"]), group = "verbosity")]
    log_level: Option<String>,
    /// Sets log verbosity (multiple times means more verbose)
    #[arg(short, long, action = clap::builder::ArgAction::Count, group = "verbosity")]
    verbose: u8,
    /// Sets log verbosity to only log errors
    #[arg(short, long, group = "verbosity")]
    quiet: bool,
    /// One or more rule program files
    #[arg(value_parser, required = true)]
    rules: Vec<PathBuf>,
    /// Save results
    #[arg(short, long = "save-results")]
    save_results: bool,
    /// output directory
    #[arg(short, long = "output", default_value = DEFAULT_OUTPUT_DIRECTORY)]
    output_directory: PathBuf,
    /// Overwrite existing files. This will remove all files in the given output directory
    #[arg(long = "overwrite-results", default_value = "false")]
    overwrite: bool,
    /// Gzip output files
    #[arg(short, long = "gzip", default_value = "false")]
    gz: bool,
}

#[cfg(feature = "no-prefixed-string-dictionary")]
type Dict = stage2::physical::dictionary::StringDictionary;
#[cfg(not(feature = "no-prefixed-string-dictionary"))]
type Dict = stage2::physical::dictionary::PrefixedStringDictionary;

type LogicalTypes = DefaultLogicalTypeCollection;

impl CliApp {
    /// Application logic, based on the parsed data inside the [`CliApp`] object
    pub fn run(&mut self) -> Result<(), Error> {
        TimedCode::instance().sub("Reading & Preprocessing").start();
        self.init_logging();
        log::info!("Version: {}", clap::crate_version!());
        log::debug!("Rule files: {:?}", self.rules);

        if self.output_directory != PathBuf::from(DEFAULT_OUTPUT_DIRECTORY) && !self.save_results {
            log::warn!(
                "Ignoring output directory `{:?}` since `--save-results` is false",
                self.output_directory
            );
        }
        if !self.save_results && self.gz {
            log::warn!(
                "Ignoring gz-compression of output files `{:?}` since `--save-results` is false",
                self.gz
            );
        }

        let app_state = self.parse_rules::<Dict, LogicalTypes>()?;

        let mut exec_engine = ExecutionEngine::initialize(app_state.program);

        TimedCode::instance().sub("Reading & Preprocessing").stop();
        TimedCode::instance().sub("Reasoning").start();

        log::info!("Reasoning ... ");
        exec_engine.execute()?;
        log::info!("Reasoning done");

        TimedCode::instance().sub("Reasoning").stop();

        if self.save_results {
            TimedCode::instance()
                .sub("Output & Final Materialization")
                .start();
            log::info!("writing output");
            let csv_writer = stage2::io::csv::CSVWriter::try_new(
                &self.output_directory,
                self.overwrite,
                self.gz,
            )?;
            // TODO fix cloning
            let dict = exec_engine.get_dict().clone();
            exec_engine.idb_predicates()?.try_for_each(|(pred, trie)| {
                let pred_name = pred
                    .name(&dict)
                    .expect("All predicates shall depend on the dictionary");
                if let Some(trie) = trie {
                    csv_writer.write_predicate(&pred_name, trie, &dict)
                } else {
                    csv_writer.create_file(&pred_name).map(|_| ())
                }
            })?;

            TimedCode::instance()
                .sub("Output & Final Materialization")
                .stop();
        }
        Ok(())
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
        let builder = if let Some(ref level) = self.log_level {
            builder.parse_filters(level)
        } else if self.quiet {
            builder.filter_level(log::LevelFilter::Error)
        } else {
            builder.filter_level(match self.verbose {
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                3 => log::LevelFilter::Trace,
                _ => log::LevelFilter::Warn,
            })
        };
        builder.init();
    }

    /// Parsing of all rule files, defined in [`Self::rules`]. Links internally used and needed dictionaries into the program state.
    ///
    /// Note: Currently all rule files will be merged into one big set of rules and parsed afterwards, this needs to be fixed when #59 and #60 is done.
    fn parse_rules<Dict: Dictionary, LogicalTypes: LogicalTypeCollection>(
        &mut self,
    ) -> Result<AppState<Dict, LogicalTypes>, Error> {
        log::info!("Parsing rules ...");
        let parser = RuleParser::new();
        let mut inputs: Vec<String> = Vec::new();

        self.rules.iter().try_for_each(|file| {
            inputs.push(read_to_string(file)?);
            std::io::Result::Ok(())
        })?;
        let input = inputs.iter_mut().fold(String::new(), |mut acc, item| {
            acc.push_str(item);
            acc
        });
        let program = all_input_consumed(parser.parse_program())(&input)?;
        log::info!("Rules parsed");
        log::trace!("{:?}", program);
        Ok(AppState::new(program))
    }
}
