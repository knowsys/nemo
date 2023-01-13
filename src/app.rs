//! Contains structures and functionality for the binary

use clap::Parser;
use stage2::error::Error;
use stage2::io::parser::RuleParser;
use stage2::logical::execution::ExecutionEngine;
use stage2::logical::model::Program;
use stage2::meta::TimedCode;
use stage2::physical::dictionary::PrefixedStringDictionary;
use std::cell::RefCell;
use std::fs::read_to_string;
use std::path::PathBuf;
use std::rc::Rc;

/// Application state
struct AppState {
    /// Name dictionary
    name_dict: Rc<RefCell<PrefixedStringDictionary>>,
    /// Constant dictionary
    constants_dict: Rc<RefCell<PrefixedStringDictionary>>,
    /// parsed program data
    program: Program,
}

impl AppState {
    fn new(
        names: Rc<RefCell<PrefixedStringDictionary>>,
        constants: Rc<RefCell<PrefixedStringDictionary>>,
        program: Program,
    ) -> Self {
        Self {
            name_dict: names,
            constants_dict: constants,
            program,
        }
    }
}

/// Stage 2 CLI
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// Sets the verbosity of logging if the flags -v and -q are not used
    #[arg(long = "log", env, value_parser=clap::builder::PossibleValuesParser::new(["error","warn","info","debug","trace"]))]
    rust_log: Option<String>,
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
    #[arg(short, long = "output", default_value = "results")]
    output_directory: PathBuf,
}

impl CliApp {
    /// Application logic, based on the parsed data inside the [`CliApp`] object
    pub fn run(&mut self) -> Result<(), Error> {
        TimedCode::instance().sub("Reading & Preprocessing").start();
        self.init_logging();
        log::info!("Version: {}", clap::crate_version!());
        log::debug!("Rule files: {:?}", self.rules);

        let app_state = self.parse_rules()?;

        let mut exec_engine = ExecutionEngine::initialize(app_state.program);

        TimedCode::instance().sub("Reading & Preprocessing").stop();
        TimedCode::instance().sub("Reasoning").start();

        log::info!("Reasoning ... ");
        exec_engine.execute();
        log::info!("Reasoning done");

        TimedCode::instance().sub("Reasoning").stop();

        if self.save_results {
            TimedCode::instance()
                .sub("Output & Final Materialization")
                .start();
            log::info!("writing output");
            let mut csv_writer = stage2::io::csv::CSVWriter::try_new(
                &mut exec_engine,
                &self.output_directory,
                &app_state.name_dict,
                &app_state.constants_dict,
            )?;
            csv_writer.write();
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
        let log_level = match self.verbose {
            1 => log::LevelFilter::Info,
            2 => log::LevelFilter::Debug,
            3 => log::LevelFilter::Trace,
            _ => {
                if self.quiet {
                    log::LevelFilter::Error
                } else if let Some(rust_log) = self.rust_log.clone() {
                    match rust_log.as_str() {
                        "error" => log::LevelFilter::Error,
                        "info" => log::LevelFilter::Info,
                        "debug" => log::LevelFilter::Debug,
                        "trace" => log::LevelFilter::Trace,
                        _ => log::LevelFilter::Warn,
                    }
                } else {
                    log::LevelFilter::Warn
                }
            }
        };
        env_logger::builder().filter_level(log_level).init();
    }

    /// Parsing of all rule files, defined in [`Self::rules`]. Links internally used and needed dictionaries into the program state.
    ///
    /// Note: Currently all rule files will be merged into one big set of rules and parsed afterwards, this needs to be fixed when #59 and #60 is done.
    fn parse_rules(&mut self) -> Result<AppState, Error> {
        log::info!("Parsing rules ...");
        let parser = RuleParser::new();
        let mut inputs: Vec<String> = Vec::new();

        self.rules.iter().for_each(|file| {
            if !file.try_exists().unwrap_or_else(|_| {
                // path/file access right errors
                // use of panic to avoid nested function call in expect!-macro
                panic!(
                    "Cannot check existence of \"{}\"",
                    file.as_os_str()
                        .to_str()
                        .expect("PathBuf should be initialised correctly")
                )
            }) {
                // file existence error
                log::error!(
                    "Rule-file \"{}\" does not exist",
                    file.as_os_str()
                        .to_str()
                        .expect("PathBuf should be initialised correctly")
                );
            } else {
                // file exists and can be read; add input String
                inputs.push(read_to_string(file).expect("File should be existing and readable"));
            }
        });
        let input = inputs.iter_mut().fold(String::new(), |mut acc, item| {
            acc.push_str(item);
            acc
        });
        let (remain, program) = parser.parse_program()(&input).expect("Parsing of rules failed!");
        if !remain.is_empty() {
            log::debug!("Parsing of rules failed, check trace for the remaining string");
            log::trace!("Remaining string:\n{}", remain);
            return Err(Error::ProgramParse);
        }
        log::info!("Rules parsed");
        log::trace!("{:?}", program);
        Ok(AppState::new(
            Rc::new(RefCell::new(parser.clone_dict_names())),
            Rc::new(RefCell::new(parser.clone_dict_constants())),
            program,
        ))
    }
}
