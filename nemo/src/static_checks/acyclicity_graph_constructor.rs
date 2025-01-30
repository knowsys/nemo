//! Functionality that provides methods to build the (JointAcyclicityGraph / WeakAcyclicityGraph) of a
/// RuleSet.
use crate::static_checks::acyclicity_graphs::{
    AcyclicityGraphBuilder, JointAcyclicityGraph, WeakAcyclicityGraph,
};
use crate::static_checks::rule_set::RuleSet;

/// This Trait provides methods of some RuleSet to build the (WeakAcyclicityGraph /
/// JointAcyclicityGraph).
pub trait AcyclicityGraphConstructor<'a> {
    /// Builds the JointAcyclicityGraph.
    fn joint_acyclicity_graph(&'a self) -> JointAcyclicityGraph<'a>;
    /// Builds the WeakAcyclicityGraph.
    fn weak_acyclicity_graph(&'a self) -> WeakAcyclicityGraph<'a>;
}

impl<'a> AcyclicityGraphConstructor<'a> for RuleSet {
    fn joint_acyclicity_graph(&'a self) -> JointAcyclicityGraph<'a> {
        JointAcyclicityGraph::build_graph(self)
    }

    fn weak_acyclicity_graph(&'a self) -> WeakAcyclicityGraph<'a> {
        WeakAcyclicityGraph::build_graph(self)
    }
}
