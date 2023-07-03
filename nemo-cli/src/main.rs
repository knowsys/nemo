/*!
  Binary for the CLI of nemo: nmo
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

pub mod cli;

use std::fs::read_to_string;

use clap::Parser;
use cli::CliApp;
use colored::Colorize;
use nemo::{
    error::{Error, ReadingError},
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{parser::parse_program, RecordWriter},
    meta::{timing::TimedDisplay, TimedCode},
    model::OutputPredicateSelection,
};

fn print_finished_message(new_facts: usize, saving: bool) {
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

    // NOTE: for some reason the subtraction produced on overflow for me once when running the tests; so better safe than sorry now :)
    let loading_preprocessing = reading_time.saturating_add(loading_time);
    let reasoning_time = execution_time.saturating_sub(loading_time);

    let writing_time = if saving {
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

    if saving {
        println!(
            "   {0: <14} {1:>max_string_len$}ms",
            "Saving output:", writing_time
        );
    }
}

fn run(mut cli: CliApp) -> Result<(), Error> {
    TimedCode::instance().start();
    TimedCode::instance().sub("Reading & Preprocessing").start();

    log::info!("Parsing rules ...");

    if cli.rules.len() > 1 {
        return Err(Error::MultipleFilesNotImplemented);
    }

    let rules = cli.rules.pop().ok_or(Error::NoInput)?;
    let rules_content = read_to_string(rules.clone()).map_err(|err| ReadingError::IOReading {
        error: err,
        filename: rules,
    })?;

    let mut program = parse_program(rules_content)?;

    log::info!("Rules parsed");
    log::trace!("{:?}", program);

    if cli.write_all_idb_predicates {
        program.force_output_predicate_selection(OutputPredicateSelection::AllIDBPredicates)
    }

    let output_manager = cli.output.initialize_output_manager()?;

    if let Some(output_manager) = &output_manager {
        output_manager.prevent_accidental_overwrite(program.output_predicates())?;
    }

    let mut engine: DefaultExecutionEngine = ExecutionEngine::initialize(program)?;

    TimedCode::instance().sub("Reading & Preprocessing").stop();
    TimedCode::instance().sub("Reasoning").start();

    log::info!("Reasoning ... ");

    engine.execute()?;

    log::info!("Reasoning done");

    TimedCode::instance().sub("Reasoning").stop();

    if let Some(output_manager) = &output_manager {
        TimedCode::instance()
            .sub("Output & Final Materialization")
            .start();
        log::info!("writing output");

        for predicate in engine.program().output_predicates() {
            let mut writer = output_manager.create_file_writer(&predicate)?;

            let Some(record_iter) = engine.output_serialization(predicate)? else {
                continue;
            };
            for record in record_iter {
                writer.write_record(record)?;
            }
        }

        TimedCode::instance()
            .sub("Output & Final Materialization")
            .stop();
    }

    TimedCode::instance().stop();

    print_finished_message(engine.count_derived_facts(), output_manager.is_some());

    if cli.detailed_timing {
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

    Ok(())
}

fn main() {
    let cli = cli::CliApp::parse();

    cli.logging.initialize_logging();
    log::info!("Version: {}", clap::crate_version!());
    log::debug!("Rule files: {:?}", cli.rules);

    run(cli).unwrap_or_else(|err| {
        log::error!("{} {err}", "error:".red().bold());
        std::process::exit(1)
    })
}
