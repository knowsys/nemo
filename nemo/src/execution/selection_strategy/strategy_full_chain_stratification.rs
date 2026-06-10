//! Defines the execution strategy for chain-stratified rulesets by which rule applications respect a precedence.

pub mod util {
    pub mod atom;
    pub mod database;
    pub mod encode;
    pub mod extend;
    pub mod ordered_atoms;
    mod unify;
}
mod reliances {
    pub mod aggr;
    pub mod negr;
    pub mod posr;
    pub mod restr;
    pub mod self_restr;

    #[cfg(test)]
    mod test;
}
mod reliance_memoization;

use std::collections::{HashMap, HashSet};

use crate::execution::planning::normalization::rule::NormalizedRule;
use crate::execution::selection_strategy::strategy_full_chain_stratification::{
    reliance_memoization::RelianceMemoization, util::atom::Predicate,
};

use super::strategy::{RuleSelectionStrategy, SelectionStrategyError};

use strum_macros::EnumCount;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, EnumCount)]
#[repr(usize)]
enum EdgeLabel {
    Positive,
    Restraint,
    Negation,
    Aggregation,
}

impl crate::util::labeled_graph::SpecialEdgeLabel for EdgeLabel {
    fn is_special(&self) -> bool {
        *self != EdgeLabel::Positive
    }
}

type Graph = crate::util::labeled_graph::LabeledGraph<usize, EdgeLabel, petgraph::Directed>;

/// Defines a strategy where rule are divided into different strata which are executed in succession.
/// Entering a new statum implies that the table for every negated atom will not get any new elements.
#[derive(Debug)]
pub struct StrategyFullChainStratification<SubStrategy: RuleSelectionStrategy> {
    ordered_strata: Vec<Vec<usize>>, // the 0th stratum is "special" and contains all the Datalog rules --> Datalog-first chase
    substrategies: Vec<SubStrategy>,

    current_stratum: usize,
}

impl<SubStrategy: RuleSelectionStrategy> StrategyFullChainStratification<SubStrategy> {
    /// Compute predicate-based dependencies in the ruleset
    fn build_dependency_graph(rules: &Vec<&NormalizedRule>) -> Graph {
        let mut predicate_to_rules_body_positive = HashMap::<Predicate, Vec<usize>>::new();
        let mut predicate_to_rules_body_negative = HashMap::<Predicate, Vec<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Predicate, Vec<usize>>::new();
        let mut predicate_to_rules_aggregation = HashMap::<Predicate, Vec<usize>>::new();

        let rule_count = rules.len();

        for (rule_index, rule) in rules.iter().enumerate() {
            for (body_predicate, _) in rule.predicates_positive() {
                let indices = predicate_to_rules_body_positive
                    .entry(body_predicate.name().to_string())
                    .or_default();

                indices.push(rule_index);
            }

            if let Some(head_index) = rule.aggregate_index() {
                let indices = predicate_to_rules_aggregation
                    .entry(rule.head()[head_index].predicate().name().to_string())
                    .or_default();

                indices.push(rule_index);
            }

            for (body_predicate, _) in rule.predicates_negative() {
                let indices = predicate_to_rules_body_negative
                    .entry(body_predicate.name().to_string())
                    .or_default();

                indices.push(rule_index);
            }

            for (head_predicate, _) in rule.predicates_head() {
                let indices = predicate_to_rules_head
                    .entry(head_predicate.name().to_string())
                    .or_default();

                indices.push(rule_index);
            }
        }

        let mut dependency_graph = Graph::default();

        for rule_index in 0..rule_count {
            dependency_graph.add_node(rule_index);
        }

        for (head_predicate, head_rules) in predicate_to_rules_head {
            if let Some(body_rules) = predicate_to_rules_body_positive.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        dependency_graph.add_edge(head_index, body_index, EdgeLabel::Positive);
                    }
                }
            }

            for &existential_head_index in head_rules.iter().filter(|&&i| rules[i].is_existential())
            {
                for &head_index in &head_rules {
                    dependency_graph.add_edge(
                        head_index,
                        existential_head_index,
                        EdgeLabel::Restraint,
                    );
                }
            }

            if let Some(body_rules) = predicate_to_rules_body_negative.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        dependency_graph.add_edge(head_index, body_index, EdgeLabel::Negation);
                    }
                }
            }
        }

        for (head_predicate, head_rules) in predicate_to_rules_aggregation {
            if let Some(body_rules) = predicate_to_rules_body_positive.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        dependency_graph.add_edge(head_index, body_index, EdgeLabel::Aggregation);
                    }
                }
            }
        }

        dependency_graph
    }

    /// Compute reliances within the given SCC of the dependency graph
    /// fails if the special edges are cyclic
    fn build_reliance_graph<'b, 'a: 'b>(
        mem: &'b mut RelianceMemoization<'a>,
        dependency_graph: &Graph,
        stratum: &Vec<usize>,
    ) -> Result<Graph, SelectionStrategyError> {
        let mut reliance_graph = Graph::default();

        for &rule_index in stratum {
            reliance_graph.add_node(rule_index);
        }

        let stratum_set = stratum.iter().copied().collect::<HashSet<_>>();

        for &rule1_index in stratum {
            for (&label, &rule2_index) in dependency_graph
                .edges_outgoing(&rule1_index)
                .filter(|(_, rule2_index)| stratum_set.contains(&rule2_index))
            {
                if let Some(_) = mem.get_one(rule1_index, rule2_index, label) {
                    if reliance_graph.has_path_via_special(
                        reliance_graph.node_unchecked(&rule1_index),
                        reliance_graph.node_unchecked(&rule1_index),
                    ) {
                        return Err(SelectionStrategyError::NonStratifiedProgram);
                    }
                    reliance_graph.add_edge(rule1_index, rule1_index, label);
                }
            }
        }

        Ok(reliance_graph)
    }

    /// Compute chains within the given SCC of the reliance graph
    /// chain graph will not use EdgeLabel::Positive
    fn build_chain_graph(
        mem: &mut RelianceMemoization,
        reliance_graph: &Graph,
        stratum: &Vec<usize>,
    ) -> Result<Graph, SelectionStrategyError> {
        let _ = RelianceMemoization::get_all; // this will be used here...
        todo!()
    }
}

impl<SubStrategy: RuleSelectionStrategy> RuleSelectionStrategy
    for StrategyFullChainStratification<SubStrategy>
{
    /// Create new [StrategyFullChainStratification].
    fn new(rules: Vec<&NormalizedRule>) -> Result<Self, SelectionStrategyError> {
        // prepare empty vec for strata and add placeholder for 0th Datalog-stratum (will be filled if ruleset is determined to be chain-stratifiable below)
        // since TarjanSCC traverses SCCs in reverse topological order, this vec will be reversed before usage
        let mut strata = vec![vec![]];

        // prepare maps to memoize encoded / canonized rules and rule pairs
        let mut mem = RelianceMemoization::new(&rules);

        // reusable state for inner Tarjan's algorithm
        let mut tarjan = petgraph::algo::TarjanScc::new();

        // compute the dependency graph and prepare hash map of nodes to SCC identifiers
        let dependency_graph = Self::build_dependency_graph(&rules);

        dependency_graph.decompose_and_refine(
            &mut strata,
            &mut petgraph::algo::TarjanScc::new(),
            |depg_stratum, strata| {
                let reliance_graph =
                    Self::build_reliance_graph(&mut mem, &dependency_graph, &depg_stratum)?;

                reliance_graph.decompose_and_refine(strata, &mut tarjan, |relg_stratum, strata| {
                    let chain_graph =
                        Self::build_chain_graph(&mut mem, &reliance_graph, &relg_stratum)?;

                    let chaing_strata = chain_graph.layer().expect(
                        "chain graph should be acyclic if no SelectionStrategyError was raised",
                    );

                    for chaing_stratum in chaing_strata {
                        strata.push(chaing_stratum);
                    }
                    Ok(())
                })
            },
        )?;

        let mut substrategies = Vec::with_capacity(strata.len());

        // TODO: pay closer attention to which Datalog rules are still active --> split strata into Datalog and non-Datalog part by adding a next_datalog_rule method?

        // extract 0th separate Datalog stratum to be applied exhaustively after each non-Datlog rule application, since we need to ensure Datalog-first chase
        let datalog_rules;
        // fill the placeholder in the strata list
        (strata[0], datalog_rules) = rules
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, rule)| rule.is_datalog())
            .unzip();
        substrategies.push(SubStrategy::new(datalog_rules)?);

        // reverse the strata, as they were pushed in reverse topological order above
        strata[1..].reverse();

        for stratum in &mut strata[1..] {
            stratum.sort();

            let mut sub_rules = Vec::with_capacity(stratum.len());

            // remove all Datalog rules from higher strata (they are redundant there, as they already live in the "special" 0th stratum)
            stratum.retain(|&i| {
                let rule = rules[i];
                if rule.is_datalog() {
                    false
                } else {
                    sub_rules.push(rule);
                    true
                }
            });

            sub_rules.shrink_to_fit();

            substrategies.push(SubStrategy::new(sub_rules)?);
        }

        if strata.len() > 1 {
            log::info!("Stratified program: {strata:?}")
        }

        Ok(Self {
            ordered_strata: strata,
            substrategies,
            current_stratum: 0,
        })
    }

    fn next_rule(&mut self, mut new_derivations: Option<bool>) -> Option<usize> {
        while self.current_stratum < self.ordered_strata.len() {
            if self.current_stratum != 0 && new_derivations == Some(true) {
                // prefer Datalog rules: after each rule application from a higher strate, exhaustively apply rules from the special 0th stratum
                if let Some(substrategy_next_datalog_rule) = self.substrategies[0].next_rule(None) {
                    return Some(self.ordered_strata[0][substrategy_next_datalog_rule]);
                }
            }
            if let Some(substrategy_next_rule) =
                self.substrategies[self.current_stratum].next_rule(new_derivations)
            {
                return Some(self.ordered_strata[self.current_stratum][substrategy_next_rule]);
            } else {
                self.current_stratum += 1;
                new_derivations = None;
            }
        }

        None
    }
}
