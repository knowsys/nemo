use crate::rule_model::components::term::primitive::variable::Variable;
use crate::static_checks::acyclicity_graphs::acyclicity_graphs_internal::{
    AcyclicityGraphBuilderInternal, CycleExtendedInternal, WeaklyAcyclicityGraphEdgeType,
};
use crate::static_checks::{positions::Position, rule_set::RuleSet};

use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::{BTreeSet, HashSet};

mod acyclicity_graphs_internal;

pub type JointlyAcyclicityGraph<'a> = DiGraphMap<&'a Variable, ()>;

pub type WeaklyAcyclicityGraph<'a> = DiGraphMap<Position<'a>, WeaklyAcyclicityGraphEdgeType>;

pub trait AcyclicityGraphBuilder<'a>: AcyclicityGraphBuilderInternal<'a> {
    fn build_graph(rule_set: &'a RuleSet) -> Self;
}

impl<'a> AcyclicityGraphBuilder<'a> for JointlyAcyclicityGraph<'a> {
    fn build_graph(rule_set: &'a RuleSet) -> Self {
        JointlyAcyclicityGraph::build_graph_internal(rule_set)
    }
}

impl<'a> AcyclicityGraphBuilder<'a> for WeaklyAcyclicityGraph<'a> {
    fn build_graph(rule_set: &'a RuleSet) -> Self {
        WeaklyAcyclicityGraph::build_graph_internal(rule_set)
    }
}

pub trait CycleExtended<N>: CycleExtendedInternal<N>
where
    N: NodeTrait,
{
    fn cycles(&self) -> HashSet<BTreeSet<N>>;
    fn is_cyclic(&self) -> bool;
}

impl<N, E> CycleExtended<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles(&self) -> HashSet<BTreeSet<N>> {
        self.cycles_internal()
    }

    fn is_cyclic(&self) -> bool {
        self.is_cyclic_internal()
    }
}
