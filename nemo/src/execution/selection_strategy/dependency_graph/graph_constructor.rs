//! Trait that defines a dependency graph constructor.

use petgraph::{Directed, Graph, adj::NodeIndex};

use crate::execution::planning::normalization::rule::NormalizedRule;

/// Graph that represents a prioritization between rules.
pub type DependencyGraph = Graph<NodeIndex<usize>, (), Directed>;

/// Defines the trait for constructors of depedency graphs.
pub trait DependencyGraphConstructor: std::fmt::Debug {
    /// Given a list of rules and some additional information,
    /// construct the dependency graph.
    fn build_graph(rules: &[&NormalizedRule]) -> DependencyGraph;
}
