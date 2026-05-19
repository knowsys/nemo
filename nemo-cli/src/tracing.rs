//! Code related to tracing of facts

use std::fs::{File, read_to_string};

use nemo::{
    error::Error,
    execution::{
        DefaultExecutionEngine,
        tracing::{node_query::TableEntriesForTreeNodesQuery, tree_query::TreeForTableQuery},
    },
    rule_model::components::{ComponentBehavior, fact::Fact},
};

use crate::{cli::CliApp, error::CliError};

/// Retrieve all facts that need to be traced from the cli arguments.
pub(crate) fn parse_trace_facts(cli: &CliApp) -> Result<Vec<String>, Error> {
    let mut facts = cli.tracing.facts.clone().unwrap_or_default();

    if let Some(input_files) = &cli.tracing.input_file {
        for input_file in input_files {
            let file_content = read_to_string(input_file)?;
            facts.extend(file_content.split(';').map(str::to_string));
        }
    }

    Ok(facts)
}

pub(crate) async fn handle_tracing(
    cli: &CliApp,
    engine: &mut DefaultExecutionEngine,
) -> Result<(), CliError> {
    trace_selected_facts(cli, engine).await?;
    trace_tree_query(cli, engine).await?;
    trace_node_query(cli, engine).await
}

/// Trace selected facts, if given on the command line
pub(crate) async fn trace_selected_facts(
    cli: &CliApp,
    engine: &mut DefaultExecutionEngine,
) -> Result<(), CliError> {
    let (trace, handles) = if cli.tracing.trace_all_idb_facts {
        let predicates = engine
            .chase_program()
            .derived_predicates()
            .iter()
            .filter_map(|predicate| {
                let count = engine
                    .count_facts_in_memory_for_predicate(predicate)
                    .unwrap_or_default();
                (count > 0).then_some((predicate, count))
            })
            .collect::<Vec<_>>();
        let total_facts = predicates.iter().map(|(_, count)| count).sum::<usize>();
        log::info!(
            "Starting tracing of {total_facts} for {} predicates",
            predicates.len()
        );

        let predicates = predicates
            .iter()
            .map(|&(predicate, _)| predicate.clone())
            .collect::<Vec<_>>();

        engine.trace_predicates(&predicates).await?
    } else {
        let tracing_facts = parse_trace_facts(cli)?;
        if tracing_facts.is_empty() {
            return Ok(());
        }

        log::info!("Starting tracing of {} facts...", tracing_facts.len());
        let mut facts = Vec::<Fact>::with_capacity(tracing_facts.len());
        for fact_string in &tracing_facts {
            let fact = Fact::parse(fact_string).map_err(|_| CliError::TracingInvalidFact {
                fact: fact_string.clone(),
            })?;
            if fact.validate().is_err() {
                return Err(CliError::TracingInvalidFact {
                    fact: fact_string.clone(),
                });
            }

            facts.push(fact);
        }

        engine.trace_facts(facts).await?
    };

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
            for handle in handles {
                if let Some(tree) = trace.tree(handle) {
                    println!("\n{}", tree.to_ascii_art());
                } else {
                    let fact = trace.get_fact(handle).fact();
                    println!("\n{fact} was not derived");
                }
            }
        }
    }

    Ok(())
}

/// Produce a trace for a tree query, if given on the command line.
pub(crate) async fn trace_tree_query(
    cli: &CliApp,
    engine: &mut DefaultExecutionEngine,
) -> Result<(), CliError> {
    if let Some(query_file) = &cli.tracing_tree.trace_tree_json {
        let query_string = read_to_string(query_file)?;

        let tree_query: TreeForTableQuery =
            serde_json::from_str(&query_string).map_err(|error| {
                CliError::TracingInvalidJsonInput {
                    error: error.to_string(),
                }
            })?;

        let result = engine.trace_tree(tree_query).await?;

        let json = serde_json::to_string_pretty(&result).expect("json serialization failed");
        println!("{json}");
    }

    Ok(())
}

/// Produce a trace for a node query, if given on the command line.
pub(crate) async fn trace_node_query(
    cli: &CliApp,
    engine: &mut DefaultExecutionEngine,
) -> Result<(), CliError> {
    if let Some(query_file) = &cli.tracing_node.trace_node_json {
        let query_string = read_to_string(query_file)?;

        let node_query: TableEntriesForTreeNodesQuery = serde_json::from_str(&query_string)
            .map_err(|error| CliError::TracingInvalidJsonInput {
                error: error.to_string(),
            })?;

        let result = engine.trace_node(&node_query).await?;

        let json = serde_json::to_string_pretty(&result).expect("json serialization failed");
        println!("{json}");
    }

    Ok(())
}
