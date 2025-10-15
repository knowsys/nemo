//! Defines the execution strategy by which each rule is applied in the order it appears.

use std::collections::HashMap;

use petgraph::Directed;

use crate::{
    execution::planning_new::normalization::rule::NormalizedRule, rule_model::components::tag::Tag,
    util::labeled_graph::LabeledGraph,
};

use super::strategy::{RuleSelectionStrategy, SelectionStrategyError};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum EdgeLabel {
    Positive,
    Negative,
}

type NegationGraph = LabeledGraph<usize, EdgeLabel, Directed>;

/// Defines a strategy where rule are divided into different strata
/// which are executed in succession.
/// Entering a new statum implies that the table for every negated atom
/// will not get any new elements.
#[derive(Debug)]
pub struct StrategyStratifiedNegation<SubStrategy: RuleSelectionStrategy> {
    ordered_strata: Vec<Vec<usize>>,
    substrategies: Vec<SubStrategy>,

    current_stratum: usize,
}

impl<SubStrategy: RuleSelectionStrategy> StrategyStratifiedNegation<SubStrategy> {
    fn build_graph(rules: &Vec<&NormalizedRule>) -> NegationGraph {
        let mut predicate_to_rules_body_positive = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_body_negative = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Tag, Vec<usize>>::new();

        let rule_count = rules.len();

        for (rule_index, rule) in rules.iter().enumerate() {
            for (body_predicate, _) in rule.predicates_positive() {
                let indices = if rule.contains_aggregates() {
                    // An aggregate in a head means that the head predicates need to be in a higher stratum than the body predicates
                    // This is the same as when all body literals are negative
                    // Therefore, we can easily compute strata for aggregates by acting if all body atoms in the rule were negated
                    predicate_to_rules_body_negative
                        .entry(body_predicate)
                        .or_default()
                } else {
                    // No aggregates in the rule
                    predicate_to_rules_body_positive
                        .entry(body_predicate)
                        .or_default()
                };

                indices.push(rule_index);
            }

            for (body_predicate, _) in rule.predicates_negative() {
                let indices = predicate_to_rules_body_negative
                    .entry(body_predicate)
                    .or_default();

                indices.push(rule_index);
            }

            for (head_predicate, _) in rule.predicates_head() {
                let indices = predicate_to_rules_head.entry(head_predicate).or_default();

                indices.push(rule_index);
            }
        }

        let mut graph = NegationGraph::default();

        for rule_index in 0..rule_count {
            graph.add_node(rule_index);
        }

        for (head_predicate, head_rules) in predicate_to_rules_head {
            if let Some(body_rules) = predicate_to_rules_body_positive.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        graph.add_edge(head_index, body_index, EdgeLabel::Positive);
                    }
                }
            }

            if let Some(body_rules) = predicate_to_rules_body_negative.get(&head_predicate) {
                for &head_index in &head_rules {
                    for &body_index in body_rules {
                        graph.add_edge(head_index, body_index, EdgeLabel::Negative);
                    }
                }
            }
        }

        graph
    }
}

impl<SubStrategy: RuleSelectionStrategy> RuleSelectionStrategy
    for StrategyStratifiedNegation<SubStrategy>
{
    /// Create new [StrategyStratifiedNegation].
    fn new(rules: Vec<&NormalizedRule>) -> Result<Self, SelectionStrategyError> {
        let graph = Self::build_graph(&rules);

        if let Some(mut strata) = graph.stratify(&[EdgeLabel::Negative]) {
            let mut substrategies = Vec::new();

            for stratum in &mut strata {
                stratum.sort();

                let sub_rules = stratum.iter().map(|i| rules[*i]).collect::<Vec<_>>();

                substrategies.push(SubStrategy::new(sub_rules)?);
            }

            for stratum in &mut strata {
                stratum.sort();
            }

            if strata.len() > 1 {
                log::info!("Stratified program: {strata:?}")
            }

            Ok(Self {
                ordered_strata: strata,
                substrategies,
                current_stratum: 0,
            })
        } else {
            Err(SelectionStrategyError::NonStratifiedProgram)
        }
    }

    fn next_rule(&mut self, mut new_derivations: Option<bool>) -> Option<usize> {
        while self.current_stratum < self.ordered_strata.len() {
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
