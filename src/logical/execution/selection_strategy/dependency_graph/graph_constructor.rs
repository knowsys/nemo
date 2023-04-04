//! Trait that defines a dependency graph constructor.

use petgraph::{prelude::GraphMap, Directed};

use crate::logical::{model::Rule, program_analysis::analysis::RuleAnalysis};

/// Graph that represents a prioritization between rules.
pub type DependencyGraph = GraphMap<usize, (), Directed>;

/// Defines the trait for constructors of depedency graphs.
pub trait DependencyGraphConstructor {
    /// Given a list of rules and some additional information,
    /// construct the dependency graph.
    fn build_graph(rules: &[Rule], rule_analyses: &[RuleAnalysis]) -> DependencyGraph;
}
