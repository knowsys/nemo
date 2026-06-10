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

#[cfg(target_os = "linux")]
pub mod cgroup;
pub mod cli;
pub mod error;
pub mod tracing;

use std::io::{Write, stdout};

use clap::Parser;
use colored::Colorize;

use cli::{CliApp, FactPrinting, Reporting};

use error::CliError;
use nemo::{
    datavalues::AnyDataValue,
    error::Error,
    execution::{
        DefaultExecutionEngine, ExecutionEngine, execution_parameters::ExecutionParameters,
        planning::normalization::program::NormalizedProgram,
    },
    io::{ImportManager, resource_providers::ResourceProviders},
    meta::timing::{TimedCode, TimedDisplay},
    rule_file::RuleFile,
    rule_model::components::{fact::Fact, tag::Tag, term::Term},
};
use tracing::handle_tracing;

fn print_facts_for_table<W: Write>(
    writer: &mut W,
    mut table: impl Iterator<Item = Vec<AnyDataValue>>,
    predicate: Tag,
) -> Result<(), Error> {
    table
        .try_for_each(|row| {
            writeln!(
                writer,
                "{}",
                Fact::new(predicate.clone(), row.into_iter().map(Term::ground))
            )
        })
        .map_err(Error::IO)
}

fn predicates_to_print_facts_for(
    print_facts_setting: FactPrinting,
    program: &NormalizedProgram,
) -> Vec<Tag> {
    match print_facts_setting {
        FactPrinting::None => Vec::new(),
        FactPrinting::Idb => program.derived_predicates().iter().cloned().collect(),
        FactPrinting::Edb => program.import_predicates().collect(),
        FactPrinting::All => program.all_predicates().collect(),
    }
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

async fn run(mut cli: CliApp) -> Result<(), CliError> {
    TimedCode::instance().start();
    TimedCode::instance().sub("Reading & Preprocessing").start();

    log::info!("Parsing rules ...");

    if cli.rules.len() > 1 {
        return Err(CliError::MultipleFilesNotImplemented);
    }

    let program_path = cli.rules.pop().ok_or(CliError::NoInput)?;
    let program_file = RuleFile::load(program_path)?;

    let export_manager = cli.output.export_manager()?;
    let import_manager = ImportManager::new(ResourceProviders::with_base_path(
        cli.import_directory.clone(),
    ));

    let mut execution_parameters = ExecutionParameters::default();
    execution_parameters.set_export_parameters(cli.output.export_setting.into());
    execution_parameters.set_import_manager(import_manager);

    if let Err(parameter) = execution_parameters.set_global(
        cli.parameters
            .drain(..)
            .map(|parameter| (parameter.key, parameter.value)),
    ) {
        return Err(CliError::InvalidParameter { parameter });
    }

    let (mut engine, warnings) = ExecutionEngine::from_file(program_file, execution_parameters)
        .await?
        .into_pair();
    warnings.eprint(cli.disable_warnings)?;

    log::info!("Rules parsed");

    for (predicate, handler) in engine.exports() {
        export_manager.validate(&predicate, &handler)?;
    }

    TimedCode::instance().sub("Reading & Preprocessing").stop();

    TimedCode::instance().sub("Reasoning").start();
    log::info!("Reasoning ... ");
    engine.execute().await?;
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
                &handler,
                engine.predicate_rows(&predicate).await?,
            )?;
        }

        TimedCode::instance()
            .sub("Output & Final Materialization")
            .stop();
    }

    if cli.output.print_facts_setting.is_enabled() {
        TimedCode::instance().sub("Printing Facts").start();
        log::info!("Printing facts");

        let mut stdout = Box::new(stdout().lock());

        for predicate in
            predicates_to_print_facts_for(cli.output.print_facts_setting, engine.chase_program())
        {
            if let Some(table) = engine.predicate_rows(&predicate).await? {
                print_facts_for_table(&mut stdout, table, predicate)?;
            }
        }

        TimedCode::instance().sub("Printing Facts").stop();
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
            engine.count_facts_in_memory_for_derived_predicates(),
            !export_manager.write_disabled(),
        );
    }
    if print_times {
        print_timing_details();
    }
    if print_memory {
        print_memory_details(&engine);
    }

    handle_tracing(&cli, &mut engine).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = CliApp::parse();

    let disable_warnings = cli.disable_warnings;

    cli.logging.initialize_logging();
    log::info!("Version: {}", clap::crate_version!());
    log::debug!("Rule files: {:?}", cli.rules);

    setup_cgroup(&cli).await;

    if let Err(error) = run(cli).await {
        if let CliError::NemoError(Error::ProgramReport(report)) = error {
            let _ = report.eprint(disable_warnings);

            if report.contains_errors() {
                std::process::exit(1);
            }
        } else {
            log::error!("{} {error}", "error:".red().bold());
            std::process::exit(1);
        }
    }
}

#[cfg(target_os = "linux")]
async fn setup_cgroup(cli: &CliApp) {
    let mut builder = cgroup::CgroupBuilder::new("app-nemo-{pid}.scope");

    if let Some(seconds) = cli.limits.time_seconds {
        builder.time_limit(seconds);
    }

    if let Some(mebibytes) = cli.limits.memory_mebibytes {
        builder.memory_limit(mebibytes);
    }

    if let Err(error) = builder.create_scope().await {
        if cli.limits.requested() {
            // if a resource limit was requested, this is an error.
            log::error!("Failed to create cgroup. Resource limits cannot be enforced: {error}");
        } else {
            log::warn!(
                "Failed to create cgroup. This is not fatal and can safely be ignored: {error}"
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn setup_cgroup(cli: &CliApp) {
    if cli.limits.requested() {
        Log::warn!("Resource limits are only supported on Linux.");
    }
}
