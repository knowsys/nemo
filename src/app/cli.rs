//! CLI structures and logic

use crate::app::AppState;
use clap::Parser;
use stage2::error::Error;
use stage2::io::parser::{all_input_consumed, RuleParser};
use stage2::logical::execution::ExecutionEngine;
use stage2::meta::TimedCode;
use stage2::physical::dictionary::Dictionary;
use std::fs::read_to_string;
use std::path::PathBuf;

use super::GlobalOptions;

const DEFAULT_OUTPUT_DIRECTORY: &str = "results";

/// Stage 2 CLI
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct CliApp {
    /// One or more rule program files
    #[arg(value_parser, required = true)]
    rules: Vec<PathBuf>,
    /// Save results
    #[arg(short, long = "save-results")]
    save_results: bool,
    /// output directory
    #[arg(short, long = "output", default_value = DEFAULT_OUTPUT_DIRECTORY)]
    output_directory: PathBuf,
    #[command(flatten)]
    global: GlobalOptions,
}

#[cfg(feature = "no-prefixed-string-dictionary")]
type Dict = stage2::physical::dictionary::StringDictionary;
#[cfg(not(feature = "no-prefixed-string-dictionary"))]
type Dict = stage2::physical::dictionary::PrefixedStringDictionary;

impl CliApp {
    /// Application logic, based on the parsed data inside the [`CliApp`] object
    pub fn run(&mut self) -> Result<(), Error> {
        TimedCode::instance().sub("Reading & Preprocessing").start();
        log::info!("Version: {}", clap::crate_version!());
        log::debug!("Rule files: {:?}", self.rules);

        if self.output_directory != PathBuf::from(DEFAULT_OUTPUT_DIRECTORY) && !self.save_results {
            log::warn!(
                "Ignoring output directory `{:?}` since `--save-results` is false",
                self.output_directory
            );
        }
        if !self.save_results && self.global.gz {
            log::warn!(
                "Ignoring gz-compression of output files `{:?}` since `--save-results` is false",
                self.global.gz
            );
        }

        let app_state = self.parse_rules::<Dict>()?;

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
                self.global.overwrite,
                self.global.gz,
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
    /// Parsing of all rule files, defined in [`Self::rules`]. Links internally used and needed dictionaries into the program state.
    ///
    /// Note: Currently all rule files will be merged into one big set of rules and parsed afterwards, this needs to be fixed when #59 and #60 is done.
    fn parse_rules<Dict: Dictionary>(&mut self) -> Result<AppState<Dict>, Error> {
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
