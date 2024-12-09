use crate::rule_model::components::term::primitive::variable::Variable;
use crate::static_checks::acyclicity_graphs::acyclicity_graphs_internal::{
    AcyclicityGraphBuilderInternal, AcyclicityGraphCycleInternal,
    WeaklyAcyclicityGraphCycleInternal, WeaklyAcyclicityGraphEdgeType,
};
use crate::static_checks::{positions::Position, rule_set::RuleSet};

use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::HashSet;

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

type Cycle<N> = Vec<N>;

type Cycles<N> = HashSet<Cycle<N>>;

pub trait AcyclicityGraphCycle<N>: AcyclicityGraphCycleInternal<N> {
    fn cycles(&self) -> Cycles<N>;
    fn is_cyclic(&self) -> bool;
}

impl<N, E> AcyclicityGraphCycle<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles(&self) -> Cycles<N> {
        self.cycles_internal()
    }

    fn is_cyclic(&self) -> bool {
        self.is_cyclic_internal()
    }
}

pub trait WeaklyAcyclicityGraphCycle<N>: WeaklyAcyclicityGraphCycleInternal<N> {
    fn contains_cycle_with_special_edge(&self) -> bool;
}

impl<'a> WeaklyAcyclicityGraphCycle<Position<'a>> for WeaklyAcyclicityGraph<'a> {
    fn contains_cycle_with_special_edge(&self) -> bool {
        self.contains_cycle_with_special_edge_internal()
    }
}
