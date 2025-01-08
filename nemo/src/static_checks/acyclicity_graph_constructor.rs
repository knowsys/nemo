use crate::static_checks::acyclicity_graphs::{
    AcyclicityGraphBuilder, JointAcyclicityGraph, WeakAcyclicityGraph,
};
use crate::static_checks::rule_set::RuleSet;

pub trait AcyclicityGraphConstructor<'a> {
    fn weak_acyclicity_graph(&'a self) -> WeakAcyclicityGraph<'a>;
    fn joint_acyclicity_graph(&'a self) -> JointAcyclicityGraph<'a>;
}

impl<'a> AcyclicityGraphConstructor<'a> for RuleSet {
    fn joint_acyclicity_graph(&'a self) -> JointAcyclicityGraph<'a> {
        JointAcyclicityGraph::build_graph(self)
    }

    fn weak_acyclicity_graph(&'a self) -> WeakAcyclicityGraph<'a> {
        WeakAcyclicityGraph::build_graph(self)
    }
}
