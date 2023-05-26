//! Trait that defines a dependency graph constructor.

use petgraph::{adj::NodeIndex, Directed, Graph};

use crate::logical::{model::chase_model::ChaseRule, program_analysis::analysis::RuleAnalysis};

/// Graph that represents a prioritization between rules.
pub type DependencyGraph = Graph<NodeIndex<usize>, (), Directed>;

/// Defines the trait for constructors of depedency graphs.
pub trait DependencyGraphConstructor: std::fmt::Debug {
    /// Given a list of rules and some additional information,
    /// construct the dependency graph.
    fn build_graph(rules: Vec<&ChaseRule>, rule_analyses: Vec<&RuleAnalysis>) -> DependencyGraph;
}
