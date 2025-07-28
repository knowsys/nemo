//! This module contains code for executing experiments regarding node queries.

use std::{
    fmt::Display,
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::execution::{
    ExecutionEngine, selection_strategy::strategy::RuleSelectionStrategy,
    tracing::node_query::TableEntriesForTreeNodesQuery,
};

#[derive(Debug, Default)]
struct _ExperimentNodeQueryResults {
    /// Time for preparing the execution plan that implements the query
    time_prepare: Duration,
    /// Time for the actual evaluation of the query
    time_execute: Duration,
    /// Time to compute the json answer
    time_answer: Duration,

    /// Size of the query, number of nodes
    query_size: usize,
    /// Total number of results
    answer_size: usize,
}

impl Display for _ExperimentNodeQueryResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "prepare: {} ms\n",
            self.time_prepare.as_millis()
        ))?;
        f.write_fmt(format_args!(
            "execute: {} ms\n",
            self.time_execute.as_millis()
        ))?;
        f.write_fmt(format_args!(
            "prepare: {} ms\n",
            self.time_answer.as_millis()
        ))?;
        f.write_fmt(format_args!("query size: {}\n", self.query_size))?;
        f.write_fmt(format_args!("answer size: {}", self.answer_size))
    }
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Experiment trying all queries provided in a directory.
    pub fn experiment_node_queries(&mut self, directory: &PathBuf) {
        println!("Start experiment");

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

        for (index, file) in query_files.iter().enumerate() {
            println!("file: {}", file.to_str().unwrap());
            let query = fs::read_to_string(file).expect("failed to read file");

            let node_query: TableEntriesForTreeNodesQuery =
                serde_json::from_str(&query).expect("failed to parse query");

            let time = Instant::now();
            let _result = self.execute_node_query(node_query);
            println!("time: {} ms", time.elapsed().as_millis());

            // println!("{result}");

            if index % 100 == 0 {
                println!("Experiment: {}/{}", index, query_files.len());
            }
        }
    }
}
