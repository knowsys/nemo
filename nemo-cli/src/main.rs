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

use std::fs::{read_to_string, File};

use clap::Parser;
use cli::CliApp;
use colored::Colorize;
use nemo::{
    error::{Error, ReadingError},
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{
        parser::{parse_fact, parse_program},
        resource_providers::ResourceProviders,
        ImportManager,
    },
    meta::timing::{TimedCode, TimedDisplay},
    model::ExportDirective,
};

use crate::cli::{EXPORT_ALL, EXPORT_EDB, EXPORT_IDB, EXPORT_NONE};

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

    let max_string_len = [loading_preprocessing, reading_time, writing_time]
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
    let rules_content = read_to_string(rules.clone()).map_err(|err| ReadingError::IoReading {
        error: err,
        filename: rules.to_string_lossy().to_string(),
    })?;

    let mut program = parse_program(rules_content)?;

    log::info!("Rules parsed");
    log::trace!("{:?}", program);

    let traced_facts = cli
        .tracing
        .traced_facts
        .map(|f| f.into_iter().map(parse_fact).collect::<Result<Vec<_>, _>>())
        .transpose()?;

    if let Some(ref value) = cli.output.export_setting {
        program.clear_exports();

        let mut additional_exports = Vec::new();
        match value.as_str() {
            EXPORT_IDB => {
                for predicate in program.idb_predicates() {
                    additional_exports.push(ExportDirective::default(predicate));
                }
            }
            EXPORT_EDB => {
                for predicate in program.edb_predicates() {
                    additional_exports.push(ExportDirective::default(predicate));
                }
            }
            EXPORT_ALL => {
                for predicate in program.predicates() {
                    additional_exports.push(ExportDirective::default(predicate));
                }
            }
            EXPORT_NONE => {}
            _ => {
                unreachable!("invalid option is filtered by argument parser");
            }
        }
        program.add_exports(additional_exports);
    }

    let export_manager = cli.output.export_manager()?;
    // Validate exports even if we do not intend to write data:
    for export in program.exports() {
        export_manager.validate(export)?;
    }

    let import_manager =
        ImportManager::new(ResourceProviders::with_base_path(cli.import_directory));

    let mut engine: DefaultExecutionEngine = ExecutionEngine::initialize(&program, import_manager)?;

    TimedCode::instance().sub("Reading & Preprocessing").stop();
    TimedCode::instance().sub("Reasoning").start();

    log::info!("Reasoning ... ");

    engine.execute()?;

    log::info!("Reasoning done");

    TimedCode::instance().sub("Reasoning").stop();

    if !export_manager.write_disabled() {
        TimedCode::instance()
            .sub("Output & Final Materialization")
            .start();
        log::info!("writing output");

        for export_directive in program.exports() {
            if let Some(arity) = engine.predicate_arity(export_directive.predicate()) {
                export_manager.export_table(
                    export_directive,
                    engine.predicate_rows(export_directive.predicate())?,
                    arity,
                )?;
            }
        }

        TimedCode::instance()
            .sub("Output & Final Materialization")
            .stop();
    }

    TimedCode::instance().stop();

    print_finished_message(
        engine.count_facts_of_derived_predicates(),
        !export_manager.write_disabled(),
    );

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

    if cli.detailed_memory {
        println!("\n{}", engine.memory_usage());
    }

    if let Some(facts) = traced_facts {
        let (trace, handles) = engine.trace(program.clone(), facts.clone());

        match cli.tracing.output_file {
            Some(output_file) => {
                let filename = output_file.to_string_lossy().to_string();
                let trace_json = trace.json(&handles);

                let mut json_file = File::create(output_file)?;
                if serde_json::to_writer(&mut json_file, &trace_json).is_err() {
                    return Err(Error::SerializationError { filename });
                }
            }
            None => {
                for (fact, handle) in facts.into_iter().zip(handles) {
                    if let Some(tree) = trace.ascii_tree_string(handle) {
                        println!("\n{}", tree);
                    } else {
                        println!("\n{fact} was not derived");
                    }
                }
            }
        }
    }

    Ok(())
}

fn main() {
    let cli = CliApp::parse();

    cli.logging.initialize_logging();
    log::info!("Version: {}", clap::crate_version!());
    log::debug!("Rule files: {:?}", cli.rules);

    run(cli).unwrap_or_else(|err| {
        log::error!("{} {err}", "error:".red().bold());
        std::process::exit(1)
    })
}
