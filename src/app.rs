//! Contains structures and functionality for the binary
use std::{fs::read_to_string, io::ErrorKind, path::PathBuf};

use clap::Parser;
use colored::Colorize;

use nemo::{
    error::Error,
    io::{
        parser::{all_input_consumed, RuleParser},
        OutputFileManager,
    },
    logical::{
        execution::{
            selection_strategy::strategy_round_robin::StrategyRoundRobin, ExecutionEngine,
        },
        model::Program,
    },
    meta::{timing::TimedDisplay, TimedCode},
};

/// Application state
struct AppState {
    /// parsed program data
    program: Program,
}

impl AppState {
    fn new(program: Program) -> Self {
        Self { program }
    }
}

const DEFAULT_OUTPUT_DIRECTORY: &str = "results";

/// Nemo 2 CLI
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
    #[arg(short, long = "gzip", default_value = "false")]
    gz: bool,
    /// Display detailed timing information
    #[arg(long = "detailed-timing", default_value = "false")]
    detailed_timing: bool,
}

impl CliApp {
    fn print_finished_message(&self, new_facts: usize) {
        let overall_time = TimedCode::instance().total_system_time().as_millis();
        let reading_time = TimedCode::instance()
            .sub("Reading & Preprocessing")
            .total_system_time()
            .as_millis();
        let loading_time = TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .total_system_time()
            .as_millis();
        let execution_time = TimedCode::instance()
            .sub("Reasoning")
            .total_system_time()
            .as_millis();

        let loading_preprocessing = reading_time + loading_time;
        let reasoning_time = execution_time - loading_time;
        let writing_time = if self.save_results {
            TimedCode::instance()
                .sub("Output & Final Materialization")
                .total_system_time()
                .as_millis()
        } else {
            0
        };

        let max_string_len = vec![loading_preprocessing, reading_time, writing_time]
            .iter()
            .map(|t| t.to_string().len())
            .max()
            .expect("Vector is not empty")
            + 2; // for the unit ms

        println!(
            "Reasoning completed in {}{}. Derived {} facts.",
            overall_time.to_string().green().bold(),
            "ms".green().bold(),
            new_facts.to_string().green().bold(),
        );

        println!(
            "   {0: <14} {1:>max_string_len$}ms",
            "Loading input:", loading_preprocessing
        );
        println!(
            "   {0: <14} {1:>max_string_len$}ms",
            "Reasoning:", reasoning_time
        );
        if self.save_results {
            println!(
                "   {0: <14} {1:>max_string_len$}ms",
                "Saving output:", writing_time
            );
        }
    }

    fn print_timing_stats() {
        println!(
            "\n{}",
            TimedCode::instance().create_tree_string(
                "nemo",
                &[
                    TimedDisplay::default(),
                    TimedDisplay::default(),
                    TimedDisplay::new(nemo::meta::timing::TimedSorting::LongestThreadTime, 0)
                ]
            )
        );
    }

    /// Application logic, based on the parsed data inside the [`CliApp`] object
    pub fn run(&mut self) -> Result<(), Error> {
        TimedCode::instance().start();
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

        let app_state = self.parse_rules()?;

        self.prevent_accidential_overwrite(&app_state)?;

        let mut exec_engine = ExecutionEngine::<StrategyRoundRobin>::initialize(app_state.program)?;
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
            let file_manager =
                OutputFileManager::try_new(&self.output_directory, self.overwrite, self.gz)?;

            let idb_tables = exec_engine.combine_results()?;

            for (pred, table_id) in idb_tables {
                let mut writer = file_manager.create_file_writer(&pred)?;

                if let Some(id) = table_id {
                    exec_engine.write_predicate_to_disk(&mut writer, id)?;
                }
            }

            TimedCode::instance()
                .sub("Output & Final Materialization")
                .stop();
        }

        TimedCode::instance().stop();

        self.print_finished_message(exec_engine.count_derived_facts());

        if self.detailed_timing {
            Self::print_timing_stats()
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
    fn parse_rules(&mut self) -> Result<AppState, Error> {
        log::info!("Parsing rules ...");
        let parser = RuleParser::new();
        let mut inputs: Vec<String> = Vec::new();

        self.rules.iter().try_for_each(|file| {
            inputs.push(read_to_string(file).map_err(|err| {
                Error::IOReading {
                    error: err,
                    filename: file
                        .clone()
                        .into_os_string()
                        .into_string()
                        .expect("Path shall be representable as string"),
                }
            })?);
            Ok::<(), Error>(())
        })?;
        let input = inputs.iter_mut().fold(String::new(), |mut acc, item| {
            acc.push_str(item);
            acc
        });
        let program = all_input_consumed(parser.parse_program())(&input)?;
        program.check_for_unsupported_features()?;

        log::info!("Rules parsed");
        log::trace!("{:?}", program);

        Ok(AppState::new(program))
    }

    /// Checks if results shall be saved without allowing to overwrite
    /// Returns an Error if files are existing without being allowed to overwrite them
    fn prevent_accidential_overwrite(&self, app_state: &AppState) -> Result<(), Error> {
        if self.save_results && !self.overwrite {
            app_state
                .program
                .idb_predicates()
                .iter()
                .try_for_each(|pred| {
                    let output_file_manager = OutputFileManager::try_new(
                        &self.output_directory,
                        self.overwrite,
                        self.gz,
                    )?;
                    let file = output_file_manager.get_output_file_name(pred);
                    let meta_info = file.metadata();
                    if let Err(err) = meta_info {
                        if err.kind() == ErrorKind::NotFound {
                            Ok::<(), Error>(())
                        } else {
                            Err(Error::IO(err))
                        }
                    } else {
                        Err(Error::IOExists {
                            error: ErrorKind::AlreadyExists.into(),
                            filename: file
                                .to_str()
                                .expect("Path is expected to be valid utf-8")
                                .to_string(),
                        })
                    }
                })
        } else {
            Ok(())
        }
    }
}
