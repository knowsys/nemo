//! Trait that defines a dependency graph constructor.

use petgraph::{adj::NodeIndex, Directed, Graph};

use crate::{
    chase_model::analysis::program_analysis::RuleAnalysis, rule_model::components::tag::Tag,
};

/// Graph that represents a prioritization between rules.
pub type DependencyGraph = Graph<NodeIndex<usize>, (), Directed>;

pub trait PositivePredicateAnalysis {
    fn positive_body_predicates(&self) -> impl Iterator<Item = &Tag>;
    fn head_predicates(&self) -> impl Iterator<Item = &Tag>;
}

impl PositivePredicateAnalysis for &RuleAnalysis {
    fn positive_body_predicates(&self) -> impl Iterator<Item = &Tag> {
        self.positive_body_predicates.iter()
    }

    fn head_predicates(&self) -> impl Iterator<Item = &Tag> {
        self.head_predicates.iter()
    }
}

/// Defines the trait for constructors of depedency graphs.
pub trait DependencyGraphConstructor<T>: std::fmt::Debug {
    /// Given a list of rules and some additional information,
    /// construct the dependency graph.
    fn build_graph(rule_analyses: &[T]) -> DependencyGraph;
}
