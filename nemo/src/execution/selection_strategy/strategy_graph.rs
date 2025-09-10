//! Defines a rule execution strategy which respects certain dependencies between rules.

use std::marker::PhantomData;

use crate::{
    chase_model::analysis::program_analysis::RuleAnalysis,
    execution::selection_strategy::strategy::MetaStrategy,
};

use super::{
    dependency_graph::graph_constructor::DependencyGraphConstructor,
    strategy::{RuleSelectionStrategy, SelectionStrategyError},
};

/// Defines a rule execution strategy which respects certain dependencies between rules
#[derive(Debug)]
pub struct StrategyDependencyGraph<GraphConstructor, SubStrategy: RuleSelectionStrategy> {
    _constructor: PhantomData<GraphConstructor>,

    ordered_sccs: Vec<Vec<usize>>,
    substrategies: Vec<SubStrategy>,

    current_scc_index: usize,
}

impl<
        GraphConstructor: for<'a> DependencyGraphConstructor<&'a RuleAnalysis>,
        SubStrategy: RuleSelectionStrategy,
    > RuleSelectionStrategy for StrategyDependencyGraph<GraphConstructor, SubStrategy>
{
    fn new(rule_analyses: Vec<&RuleAnalysis>) -> Result<Self, SelectionStrategyError> {
        let dependency_graph = GraphConstructor::build_graph(&rule_analyses);
        let graph_scc = petgraph::algo::condensation(dependency_graph, true);
        let scc_sorted = petgraph::algo::toposort(&graph_scc, None)
            .expect("The input graph is assured to be acyclic");

        let mut ordered_sccs = Vec::new();
        let mut substrategies = Vec::new();

        for scc in scc_sorted {
            let scc_rule_indices = graph_scc[scc].clone();

            let sub_analyses: Vec<&RuleAnalysis> =
                scc_rule_indices.iter().map(|&i| rule_analyses[i]).collect();

            ordered_sccs.push(scc_rule_indices);
            substrategies.push(SubStrategy::new(sub_analyses)?);
        }

        Ok(Self {
            _constructor: PhantomData,
            ordered_sccs,
            substrategies,
            current_scc_index: 0,
        })
    }

    fn next_rule(&mut self, mut new_derivations: Option<bool>) -> Option<usize> {
        while self.current_scc_index < self.ordered_sccs.len() {
            if let Some(substrategy_next_rule) =
                self.substrategies[self.current_scc_index].next_rule(new_derivations)
            {
                return Some(self.ordered_sccs[self.current_scc_index][substrategy_next_rule]);
            } else {
                self.current_scc_index += 1;
                new_derivations = None;
            }
        }

        None
    }
}

impl<
        GraphConstructor: for<'a> DependencyGraphConstructor<&'a RuleAnalysis>,
        SubStrategy: RuleSelectionStrategy,
    > MetaStrategy for StrategyDependencyGraph<GraphConstructor, SubStrategy>
{
    fn current_scc(&self) -> &[usize] {
        &self.ordered_sccs[self.current_scc_index]
    }
}
