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

pub mod cli;
pub mod error;

use std::fs::{read_to_string, File};

use clap::Parser;
use colored::Colorize;

use cli::{CliApp, Exporting, Reporting};

use error::CliError;
use nemo::{
    error::Error,
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{resource_providers::ResourceProviders, ImportManager},
    meta::timing::{TimedCode, TimedDisplay},
    rule_model::{
        self,
        components::{
            fact::Fact,
            import_export::{file_formats::FileFormat, ExportDirective},
            tag::Tag,
            term::map::Map,
            ProgramComponent,
        },
        error::ValidationErrorBuilder,
        program::Program,
    },
};

fn default_export(predicate: Tag) -> ExportDirective {
    ExportDirective::new(
        predicate,
        FileFormat::CSV,
        Map::empty_unnamed(),
        Default::default(),
    )
}

/// Set exports according to command-line parameter.
/// This disables all existing exports.
fn override_exports(program: &mut Program, value: Exporting) {
    if value == Exporting::Keep {
        return;
    }

    program.clear_exports();

    let mut additional_exports = Vec::new();
    match value {
        Exporting::Idb => {
            for predicate in program.derived_predicates() {
                additional_exports.push(default_export(predicate));
            }
        }
        Exporting::Edb => {
            for predicate in program.import_predicates() {
                additional_exports.push(default_export(predicate));
            }
        }
        Exporting::All => {
            for predicate in program.all_predicates() {
                additional_exports.push(default_export(predicate));
            }
        }
        Exporting::None => {}
        Exporting::Keep => unreachable!("already checked above"),
    }
    program.add_exports(additional_exports);
}

/// Prints short summary message.
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

    // NOTE: for some reason the subtraction produced an overflow for me once when running the tests; so better safe than sorry now :)
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
        "Data import:", loading_preprocessing
    );
    println!(
        "   {0: <14} {1:>max_string_len$}ms",
        "Reasoning:", reasoning_time
    );

    if saving {
        println!(
            "   {0: <14} {1:>max_string_len$}ms",
            "Data export:", writing_time
        );
    }
}

/// Prints detailed timing information.
fn print_timing_details() {
    println!(
        "\nTiming report:\n\n{}",
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

/// Prints detailed memory information.
fn print_memory_details(engine: &DefaultExecutionEngine) {
    println!("\nMemory report:\n\n{}", engine.memory_usage());
}

/// Retrieve all facts that need to be traced from the cli arguments.
fn parse_trace_facts(cli: &CliApp) -> Result<Vec<String>, Error> {
    let mut facts = cli.tracing.facts.clone().unwrap_or_default();

    if let Some(input_files) = &cli.tracing.input_file {
        for input_file in input_files {
            let file_content = read_to_string(input_file)?;
            facts.extend(file_content.split(';').map(str::to_string));
        }
    }

    Ok(facts)
}

/// Deal with tracing
fn handle_tracing(
    cli: &CliApp,
    engine: &mut DefaultExecutionEngine,
    program: Program,
) -> Result<(), CliError> {
    let tracing_facts = parse_trace_facts(cli)?;
    if !tracing_facts.is_empty() {
        let mut facts = Vec::<Fact>::with_capacity(tracing_facts.len());
        for fact_string in &tracing_facts {
            let fact = Fact::parse(fact_string).map_err(|_| CliError::TracingInvalidFact {
                fact: fact_string.clone(),
            })?;
            let mut builder = ValidationErrorBuilder::default();
            if fact.validate(&mut builder).is_none() {
                return Err(CliError::TracingInvalidFact {
                    fact: fact_string.clone(),
                });
            }

            facts.push(fact);
        }

        let (trace, handles) = engine.trace(program, facts)?;

        match &cli.tracing.output_file {
            Some(output_file) => {
                let filename = output_file.to_string_lossy().to_string();
                let trace_json = trace.json(&handles);

                let mut json_file = File::create(output_file)?;
                if serde_json::to_writer(&mut json_file, &trace_json).is_err() {
                    return Err(CliError::SerializationError { filename });
                }
            }
            None => {
                for (fact, handle) in tracing_facts.into_iter().zip(handles) {
                    if let Some(tree) = trace.tree(handle) {
                        println!("\n{}", tree.to_ascii_art());
                    } else {
                        println!("\n{fact} was not derived");
                    }
                }
            }
        }
    }

    Ok(())
}

fn run(mut cli: CliApp) -> Result<(), CliError> {
    TimedCode::instance().start();
    TimedCode::instance().sub("Reading & Preprocessing").start();

    log::info!("Parsing rules ...");

    if cli.rules.len() > 1 {
        return Err(CliError::MultipleFilesNotImplemented);
    }

    let program_file = cli.rules.pop().ok_or(CliError::NoInput)?;
    let program_filename = program_file.to_string_lossy().to_string();
    let program_content =
        read_to_string(program_file.clone()).map_err(|err| CliError::IoReading {
            error: err,
            filename: program_filename.clone(),
        })?;

    let program_ast = match nemo::parser::Parser::initialize(
        &program_content,
        program_filename.clone(),
    )
    .parse()
    {
        Ok(program) => program,
        Err((_program, report)) => {
            report.eprint()?;
            return Err(CliError::ProgramParsing {
                filename: program_filename.clone(),
            });
        }
    };

    let mut program = match rule_model::translation::ASTProgramTranslation::initialize(
        &program_content,
        program_filename.clone(),
    )
    .translate(&program_ast)
    {
        Ok(program) => program,
        Err(report) => {
            report.eprint()?;
            return Err(CliError::ProgramParsing {
                filename: program_filename,
            });
        }
    };
    override_exports(&mut program, cli.output.export_setting);
    log::info!("Rules parsed");

    let export_manager = cli.output.export_manager()?;
    let import_manager = ImportManager::new(ResourceProviders::with_base_path(
        cli.import_directory.clone(),
    ));

    let mut engine: DefaultExecutionEngine =
        ExecutionEngine::initialize(program.clone(), import_manager)?;

    for (predicate, handler) in engine.exports() {
        export_manager.validate(&predicate, &*handler)?;
    }

    TimedCode::instance().sub("Reading & Preprocessing").stop();

    TimedCode::instance().sub("Reasoning").start();
    log::info!("Reasoning ... ");
    engine.execute()?;
    log::info!("Reasoning done");
    TimedCode::instance().sub("Reasoning").stop();

    let mut stdout_used = false;

    if !export_manager.write_disabled() {
        TimedCode::instance()
            .sub("Output & Final Materialization")
            .start();
        log::info!("writing output");

        for (predicate, handler) in engine.exports() {
            stdout_used |= export_manager.export_table(
                &predicate,
                &*handler,
                engine.predicate_rows(&predicate)?,
            )?;
        }

        TimedCode::instance()
            .sub("Output & Final Materialization")
            .stop();
    }

    TimedCode::instance().stop();

    let (print_summary, print_times, print_memory) = match cli.reporting {
        Reporting::All => (true, true, true),
        Reporting::Short => (true, false, false),
        Reporting::Time => (true, true, false),
        Reporting::Mem => (true, false, true),
        Reporting::None => (false, false, false),
        Reporting::Auto => (!stdout_used, false, false),
    };

    if print_summary {
        print_finished_message(
            engine.count_facts_of_derived_predicates(),
            !export_manager.write_disabled(),
        );
    }
    if print_times {
        print_timing_details();
    }
    if print_memory {
        print_memory_details(&engine);
    }

    handle_tracing(&cli, &mut engine, program)
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
