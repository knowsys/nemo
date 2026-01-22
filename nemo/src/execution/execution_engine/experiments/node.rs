//! This module contains code for executing experiments regarding node queries.

use std::{
    fmt::Display,
    fs,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::execution::{
    ExecutionEngine, selection_strategy::strategy::RuleSelectionStrategy,
    tracing::node_query::TableEntriesForTreeNodesQuery,
};

const REPEAT_MEASUREMENT: usize = 3;

#[derive(Debug, Default)]
struct ExperimentNodeQueryResults {
    /// Experiment name
    name: String,

    /// Time for preparing the execution plan that implements the query
    times_prepare: Vec<Duration>,
    /// Time for the actual evaluation of the query
    times_execute: Vec<Duration>,
    /// Time to compute the json answer
    times_answer: Vec<Duration>,

    /// Size of the query, number of nodes
    query_size: usize,
    /// Maximum depth of query,
    query_depth: usize,
    /// Number of root results
    answer_size: usize,
}

impl Display for ExperimentNodeQueryResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "prepare: {} ms\n",
            self.times_prepare[0].as_millis()
        ))?;
        f.write_fmt(format_args!(
            "execute: {} ms\n",
            self.times_execute[0].as_millis()
        ))?;
        f.write_fmt(format_args!(
            "answer: {} ms\n",
            self.times_answer[0].as_millis()
        ))?;
        f.write_fmt(format_args!("query size: {}\n", self.query_size))?;
        f.write_fmt(format_args!("answer size: {}", self.answer_size))
    }
}

impl ExperimentNodeQueryResults {
    pub fn write_header(writer: &mut dyn Write) -> std::io::Result<()> {
        // let mut header_prepare = String::default();
        let mut header_execute = String::default();
        // let mut header_answer = String::default();

        for index in 0..REPEAT_MEASUREMENT {
            // header_prepare += &format!(",prepare-{}", index + 1);
            header_execute += &format!(",execute-{}", index + 1);
            // header_answer += &format!(",answer-{}", index + 1);
        }

        writeln!(
            writer,
            "name,size_query,depth_query,size_answer{}",
            // header_prepare,
            header_execute,
            // header_answer
        )
    }

    pub fn write_content(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        // let mut content_prepare = String::default();
        let mut content_execute = String::default();
        // let mut content_answer = String::default();

        for index in 0..REPEAT_MEASUREMENT {
            // content_prepare += &format!(",{}", self.times_prepare[index].as_millis());
            content_execute += &format!(",{}", self.times_execute[index].as_millis());
            // content_answer += &format!(",{}", self.times_answer[index].as_millis());
        }

        writeln!(
            writer,
            "{},{},{},{}{}",
            self.name,
            self.query_size,
            self.query_depth,
            self.answer_size,
            // content_prepare,
            content_execute,
            // content_answer
        )
    }
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    async fn perform_experiment(
        &mut self,
        query: &TableEntriesForTreeNodesQuery,
        result: &mut ExperimentNodeQueryResults,
    ) {
        let (reset_permanent, reset_storage) = self.table_manager.database().current_id();
        let program = self.program.clone();

        for _ in 0..REPEAT_MEASUREMENT {
            // // PREPARE
            // let time = Instant::now();
            // let response = self.trace_node_prepare_response(query);
            // result.times_prepare.push(time.elapsed());

            // EXECUTE
            let time = Instant::now();
            let manager = self
                .trace_node_execute(query, &program)
                .await
                .expect("trace_node_execute failed");
            result.times_execute.push(time.elapsed());

            // // ANSWER
            // let time = Instant::now();
            // self.node_query_answer(&manager, response)
            //     .unwrap_or_default();
            // result.times_answer.push(time.elapsed());

            // POST
            result.answer_size = manager
                ._results()
                .map(|id| self.table_manager.database().count_rows_in_memory(id))
                .sum();

            self.table_manager
                .delete_tables_from(reset_permanent, reset_storage);
        }
    }

    /// Experiment trying all queries provided in a directory.
    pub async fn experiment_node_queries(&mut self, directory: &PathBuf) {
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
        ExperimentNodeQueryResults::write_header(&mut handle).unwrap();

        for file in query_files.iter() {
            let mut experiment_results = ExperimentNodeQueryResults::default();
            experiment_results.name = file.file_name().unwrap().to_str().unwrap().to_string();

            let query = fs::read_to_string(file).expect("failed to read file");

            let node_query: TableEntriesForTreeNodesQuery =
                serde_json::from_str(&query).expect("failed to parse query");
            experiment_results.query_size = node_query.num_nodes();
            experiment_results.query_depth = node_query.max_depth();

            self.perform_experiment(&node_query, &mut experiment_results)
                .await;

            experiment_results.write_content(&mut handle).unwrap();
        }
    }
}
