//! This module contains code for executing experiments regarding node queries.

use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{
    execution::{
        execution_engine::NodeQueryRuleResult, planning::plan_tracing_rule::RuleTracingStrategy,
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::node_query::TableEntriesForTreeNodesQuery, ExecutionEngine,
    },
    table_manager::SubtableExecutionPlan,
};

#[derive(Debug, Default)]
struct ExperimentNodeQueryResults {
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

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    fn execute_node_query(
        &mut self,
        query: TableEntriesForTreeNodesQuery,
    ) -> ExperimentNodeQueryResults {
        let mut result = ExperimentNodeQueryResults::default();
        let time = Instant::now();

        // PREPARE QUERY

        let NodeQueryRuleResult {
            rule,
            step,
            elements,
        } = self.query_to_rule(query);
        let mut variable_order = rule.default_order();

        let trace_strategy =
            RuleTracingStrategy::initialize(&rule, self.rule_history.clone()).unwrap();

        let mut execution_plan = SubtableExecutionPlan::default();

        trace_strategy.add_plan(
            &self.table_manager,
            &mut execution_plan,
            &mut variable_order,
            step,
        );

        // EXECUTE QUERY

        result.time_prepare = time.elapsed();
        let time = Instant::now();

        let answer = self
            .table_manager
            .execute_plan_trie(execution_plan)
            .unwrap();

        // COMPUTE ANSWER

        result.time_execute = time.elapsed();
        let time = Instant::now();

        let num_rows = answer
            .iter()
            .map(|table| table.num_rows())
            .collect::<Vec<_>>();

        result.time_answer = time.elapsed();
        result
    }

    /// Experiment trying all queries provided in a directory.
    pub fn experiment_node_queries(&mut self, directory: &PathBuf) {
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

        for file in query_files {
            let query = fs::read_to_string(file).expect("failed to read file");

            let node_query: TableEntriesForTreeNodesQuery =
                serde_json::from_str(&query).expect("failed to parse query");
            let result = self.execute_node_query(node_query);
        }
    }
}
