//! This module contains code to execute the baseline experiments

use std::{
    fs,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{
    error::Error,
    execution::{
        ExecutionEngine,
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{
            node_query::{TableEntriesForTreeNodesQuery, TableEntriesForTreeNodesQueryInner},
            trace::{ExecutionTrace, ExecutionTraceTree, TraceFactHandle},
        },
    },
};

const REPEAT_MEASUREMENT: usize = 3;
const TIMEOUT: u64 = 4 * 60; // seconds

#[derive(Debug, Default)]
struct ExperimentBaselineTracingResults {
    /// Times for computing the traces
    times_tracing: Vec<Duration>,
}

impl ExperimentBaselineTracingResults {
    pub fn write_content(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        let mut content_execute = String::default();

        for index in 0..REPEAT_MEASUREMENT {
            content_execute += &format!(",{}", self.times_tracing[index].as_millis());
        }

        writeln!(writer, "TRACING,,{}", content_execute)
    }
}

#[derive(Debug, Default)]
struct ExperimentBaselineMatchingResults {
    /// Experiment name
    name: String,

    /// Times for matching the trees
    times_matching: Vec<Duration>,
    /// Number of matching trees
    num_matches: usize,
}

impl ExperimentBaselineMatchingResults {
    pub fn write_header(writer: &mut dyn Write) -> std::io::Result<()> {
        let mut header_execute = String::default();

        for index in 0..REPEAT_MEASUREMENT {
            header_execute += &format!(",execute-{}", index + 1);
        }

        writeln!(writer, "name,matches,{}", header_execute)
    }

    pub fn write_content(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        let mut content_execute = String::default();

        for index in 0..REPEAT_MEASUREMENT {
            content_execute += &format!(",{}", self.times_matching[index].as_millis());
        }

        writeln!(
            writer,
            "{},{},{}",
            self.name, self.num_matches, content_execute,
        )
    }
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    fn tree_matches_query_inner(
        tree: &ExecutionTraceTree,
        query: &TableEntriesForTreeNodesQueryInner,
    ) -> bool {
        match tree {
            ExecutionTraceTree::Fact(_) => {
                if query.next.is_some() {
                    return false;
                }
            }
            ExecutionTraceTree::Rule(application, trees) => {
                if let Some(next) = &query.next {
                    if application.rule_index != next.rule {
                        return false;
                    }

                    if next.children.len() != trees.len() {
                        return false;
                    }

                    for (next_tree, next_query) in trees.iter().zip(next.children.iter()) {
                        if !Self::tree_matches_query_inner(next_tree, next_query) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
        }

        true
    }

    /// Check whether a [ExecutionTraceTree] matches
    /// the given [TableEntriesForTreeNodesQuery]
    fn tree_matches_query(
        tree: &ExecutionTraceTree,
        query: &TableEntriesForTreeNodesQuery,
    ) -> bool {
        let predicate = match tree {
            ExecutionTraceTree::Fact(fact) => fact.predicate(),
            ExecutionTraceTree::Rule(application, _trees) => {
                application.rule.head()[application.head_index].predicate()
            }
        };

        if predicate.to_string() != query.predicate {
            return false;
        }

        Self::tree_matches_query_inner(tree, &query.inner)
    }

    /// Execute baseline experiments.
    pub async fn experiment_baseline(&mut self, directory: &PathBuf) -> Result<(), Error> {
        let facts = self.all_facts().await;

        let mut trace: Option<ExecutionTrace> = None;
        let mut handles = Vec::<TraceFactHandle>::default();

        let mut result_tracing = ExperimentBaselineTracingResults::default();

        for _ in 0..REPEAT_MEASUREMENT {
            let time = Instant::now();

            let (current_trace, current_handles) = self.trace(facts.clone(), Some(TIMEOUT)).await?;

            result_tracing.times_tracing.push(time.elapsed());

            trace = Some(current_trace);
            handles = current_handles;
        }

        let trace = trace.unwrap();

        let mut query_files = Vec::default();

        for entry in fs::read_dir(directory).expect("failed to read directory") {
            let entry = entry.expect("failed to read directory entry");
            let path = entry.path();

            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension == "json" {
                        query_files.push(path);
                    }
                }
            }
        }

        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        ExperimentBaselineMatchingResults::write_header(&mut handle).unwrap();
        result_tracing.write_content(&mut handle)?;

        for file in query_files {
            let mut experiment_results = ExperimentBaselineMatchingResults::default();
            experiment_results.name = file.file_name().unwrap().to_str().unwrap().to_string();

            let query = fs::read_to_string(file).expect("failed to read file");

            let node_query: TableEntriesForTreeNodesQuery =
                serde_json::from_str(&query).expect("failed to parse query");

            for _ in 0..REPEAT_MEASUREMENT {
                let time = Instant::now();

                let mut num_matches: usize = 0;

                for trace_handle in &handles {
                    let Some(tree) = trace.tree(*trace_handle) else {
                        continue;
                    };

                    if Self::tree_matches_query(&tree, &node_query) {
                        num_matches += 1;
                    }
                }

                experiment_results.num_matches = num_matches;
                experiment_results.times_matching.push(time.elapsed());

                experiment_results.write_content(&mut handle)?;
            }
        }

        Ok(())
    }
}
