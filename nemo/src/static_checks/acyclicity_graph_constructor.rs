use crate::static_checks::acyclicity_graphs::{
    AcyclicityGraphBuilder, JointlyAcyclicityGraph, WeaklyAcyclicityGraph,
};
use crate::static_checks::rule_set::RuleSet;

pub trait AcyclicityGraphConstructor<'a> {
    fn weakly_acyclicity_graph(&'a self) -> WeaklyAcyclicityGraph<'a>;
    fn jointly_acyclicity_graph(&'a self) -> JointlyAcyclicityGraph<'a>;
}

impl<'a> AcyclicityGraphConstructor<'a> for RuleSet {
    fn jointly_acyclicity_graph(&'a self) -> JointlyAcyclicityGraph<'a> {
        JointlyAcyclicityGraph::build_graph(self)
    }

    fn weakly_acyclicity_graph(&'a self) -> WeaklyAcyclicityGraph<'a> {
        WeaklyAcyclicityGraph::build_graph(self)
    }
}
