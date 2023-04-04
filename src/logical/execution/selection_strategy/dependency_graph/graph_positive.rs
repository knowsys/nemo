//! Module for defining a graph that checks
//! when the application of a rule might lead to the application of another.

use crate::logical::{model::Rule, program_analysis::analysis::RuleAnalysis};

use super::graph_constructor::{DependencyGraph, DependencyGraphConstructor};

/// Constructor for a dependency graph that has an edge from one rule to another
/// if the application of the first might lead to an applicatin of the second.
#[derive(Debug)]
pub struct DependencyGraphPositive {
    x: Vec<usize>,
}

impl DependencyGraphConstructor for DependencyGraphPositive {
    fn build_graph(rules: &[Rule], rule_analyses: &[RuleAnalysis]) -> DependencyGraph {
        todo!()
    }
}
