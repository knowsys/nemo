//! Defines the execution strategy by which each rule is applied in the order it appears.

use std::collections::HashMap;

use petgraph::Directed;

use crate::logical::{
    model::{chase_model::ChaseRule, Identifier},
    program_analysis::analysis::RuleAnalysis,
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
    fn build_graph(rule_analyses: &Vec<&RuleAnalysis>) -> NegationGraph {
        let mut predicate_to_rules_body_positive = HashMap::<Identifier, Vec<usize>>::new();
        let mut predicate_to_rules_body_negative = HashMap::<Identifier, Vec<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Identifier, Vec<usize>>::new();

        for (rule_index, rule_analysis) in rule_analyses.iter().enumerate() {
            for body_predicate in &rule_analysis.positive_body_predicates {
                let indices = predicate_to_rules_body_positive
                    .entry(body_predicate.clone())
                    .or_insert(Vec::new());

                indices.push(rule_index);
            }

            for body_predicate in &rule_analysis.negative_body_predicates {
                let indices = predicate_to_rules_body_negative
                    .entry(body_predicate.clone())
                    .or_insert(Vec::new());

                indices.push(rule_index);
            }

            for head_predicate in &rule_analysis.head_predicates {
                let indices = predicate_to_rules_head
                    .entry(head_predicate.clone())
                    .or_insert(Vec::new());

                indices.push(rule_index);
            }
        }

        let mut graph = NegationGraph::default();

        let rule_count = rule_analyses.len();
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
    /// Create new [`StrategyStratifiedNegation`].
    fn new(
        rules: Vec<&ChaseRule>,
        rule_analyses: Vec<&RuleAnalysis>,
    ) -> Result<Self, SelectionStrategyError> {
        let graph = Self::build_graph(&rule_analyses);

        if let Some(mut strata) = graph.stratify(&[EdgeLabel::Negative]) {
            let mut substrategies = Vec::new();

            for stratum in &mut strata {
                stratum.sort();

                let sub_rules: Vec<&ChaseRule> = stratum.iter().map(|&i| rules[i]).collect();
                let sub_analyses: Vec<&RuleAnalysis> =
                    stratum.iter().map(|&i| rule_analyses[i]).collect();

                substrategies.push(SubStrategy::new(sub_rules, sub_analyses)?);
            }

            for stratum in &mut strata {
                stratum.sort();
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
