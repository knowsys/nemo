use crate::rule_model::components::term::primitive::variable::Variable;
use crate::static_checks::acyclicity_graphs::acyclicity_graphs_internal::{
    AcyclicityGraphBuilderInternal, AcyclicityGraphCycleInternal,
    WeaklyAcyclicityGraphCycleInternal, WeaklyAcyclicityGraphEdgeType,
};
use crate::static_checks::{positions::Position, rule_set::RuleSet};

use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::HashSet;
use std::slice::Iter;

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

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Cycle<N>(Vec<N>);

impl<N> Cycle<N>
where
    N: NodeTrait,
{
    fn contains(&self, node: &N) -> bool {
        self.0.contains(node)
    }

    fn first(&self) -> Option<&N> {
        self.0.first()
    }

    fn get(&self, i: usize) -> Option<&N> {
        self.0.get(i)
    }

    fn iter(&self) -> Iter<N> {
        self.0.iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn new() -> Self {
        Cycle(Vec::<N>::new())
    }

    fn pop(&mut self) -> Option<N> {
        self.0.pop()
    }

    fn push(&mut self, node: N) {
        self.0.push(node)
    }
}

pub trait AcyclicityGraphCycle<N>: AcyclicityGraphCycleInternal<N> {
    fn cycles(&self) -> HashSet<Cycle<N>>;
    fn is_cyclic(&self) -> bool;
}

impl<N, E> AcyclicityGraphCycle<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles(&self) -> HashSet<Cycle<N>> {
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
