/*!
  Binary for the CLI of stage2
*/

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
#![feature(macro_metavar_expr)]
#![feature(is_sorted)]

use clap::Parser;
use stage2::error::Error;
use stage2::io::parser::RuleParser;
use stage2::logical::execution::{execution_engine, ExecutionEngine};
use stage2::logical::model::Program;
use stage2::meta::timing::TimedDisplay;
use stage2::meta::TimedCode;
use stage2::physical::tabular::traits::table::Table;
use std::fs::{read_to_string, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct CliApp {
    /// Sets the verbosity of logging to 'error', 'warn', 'info', 'debug' or 'trace' if the flags -v and -q are not used
    #[arg(long = "rust_log", env, value_parser=clap::builder::PossibleValuesParser::new(["error","warn","info","debug","trace"]))]
    rust_log: Option<String>,
    /// Sets log verbosity (multiple times means more verbose)
    #[arg(short, action = clap::builder::ArgAction::Count, group = "verbosity")]
    verbose: u8,
    /// Sets log verbosity to only log errors
    #[arg(short, group = "verbosity")]
    quiet: bool,
    /// Rule program
    #[arg(short, required = true)]
    rules: Vec<PathBuf>,
}

impl CliApp {
    /// Application logic, based on the parsed data inside the [`CliApp`] object
    fn run(&self) -> Result<(), Error> {
        self.init_logging();
        log::info!("Version: {}", clap::crate_version!());
        log::debug!("Rule files: {:?}", self.rules);

        let program = self.parse_rules()?;
        log::trace!("{:?}", program);

        let mut exec_engine = ExecutionEngine::initialize(program);

        log::info!("Reasoning ... ");
        exec_engine.execute();

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

    /// Parsing of all rule files, defined in [`Self::rules`]
    ///
    /// Note: Currently all rule files will be merged into one big set of rules and parsed afterwards, this needs to be fixed when #59 and #60 is done.
    fn parse_rules(&self) -> Result<Program, Error> {
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
        let x = parser.parse_program()(&input).expect("Parsing of rules failed!");
        if x.0.is_empty() {
            log::info!("Rules parsed");
            Ok(x.1)
        } else {
            log::debug!("Parsing of rules failed, check trace for the remaining string");
            log::trace!("Remaining string:\n{}", x.0);
            Err(Error::ProgramParseError)
        }
    }
}

fn main() {
    let app = CliApp::parse();
    if let Err(err) = app.run() {
        eprintln!("Application Error: {err}");
        std::process::exit(0);
    }
    std::process::exit(1);
    env_logger::init();

    TimedCode::instance().start();
    TimedCode::instance().sub("Reading & Preprocessing").start();

    // let mut input: String = "".to_string();
    // stdin().read_to_string(&mut input).unwrap();
    // let input = read_to_string("test-files/snomed-el-noconst.rls").unwrap();
    // let input = read_to_string("test-files/medmed-el-noconst.rls").unwrap();
    // let input = read_to_string("test-files/galen-el-without-constants.rls").unwrap();
    // let input = read_to_string("test-files/snomed-el-noaux.rls").unwrap();

    let input = read_to_string("test-files/galen-el-noaux.rls").unwrap();
    let save_results = false;

    let parser = RuleParser::new();
    let mut parser_function = parser.parse_program();
    let program = parser_function(&input).unwrap().1;

    log::info!("Parsed program.");
    log::debug!("{:?}", program);

    let mut exec_engine = ExecutionEngine::initialize(program);

    TimedCode::instance().sub("Reading & Preprocessing").stop();
    TimedCode::instance().sub("Reasoning").start();

    log::info!("Executing ...");

    exec_engine.execute();

    TimedCode::instance().sub("Reasoning").stop();
    TimedCode::instance().sub("Save").start();

    let dict = exec_engine.get_dict().clone();
    if save_results {
        log::info!("Results:");
        for (predicate, trie) in exec_engine.get_results() {
            let predicate_string = parser
                .resolve_identifier(&predicate)
                .expect("should have been interned");

            log::info!("{}: {} entries", predicate_string, trie.row_num());

            let predicate_file_name = predicate_string.replace(['/', ':'], "_");

            let path = PathBuf::from(format!("out/{predicate_file_name}.csv"));
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.clone())
            {
                Ok(mut file) => {
                    write!(file, "{}", trie.debug(&dict)).expect("should succeed");
                    log::info!("wrote {path:?}");
                }
                Err(e) => log::warn!("error writing: {e:?}"),
            }
        }
    }

    TimedCode::instance().sub("Save").stop();
    TimedCode::instance().stop();

    println!(
        "\n{}",
        TimedCode::instance().create_tree_string(
            "stage2",
            &[
                TimedDisplay::default(),
                TimedDisplay::default(),
                TimedDisplay::new(stage2::meta::timing::TimedSorting::LongestTime, 0)
            ]
        )
    );
}
