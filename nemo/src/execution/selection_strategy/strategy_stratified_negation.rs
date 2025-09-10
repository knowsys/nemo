//! Defines the execution strategy by which each rule is applied in the order it appears.

use std::collections::HashMap;

use petgraph::Directed;

use crate::{
    chase_model::analysis::program_analysis::RuleAnalysis,
    execution::selection_strategy::strategy::MetaStrategy, rule_model::components::tag::Tag,
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
/// Entering a new stratum implies that the table for every negated atom
/// will not get any new elements.
#[derive(Debug)]
pub struct StrategyStratifiedNegation<SubStrategy: RuleSelectionStrategy> {
    ordered_strata: Vec<Vec<usize>>,
    substrategies: Vec<SubStrategy>,

    current_stratum: usize,
}

impl<SubStrategy: RuleSelectionStrategy> StrategyStratifiedNegation<SubStrategy> {
    fn build_graph(rule_analyses: &[&RuleAnalysis]) -> NegationGraph {
        let mut predicate_to_rules_body_positive = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_body_negative = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Tag, Vec<usize>>::new();

        for (rule_index, rule_analysis) in rule_analyses.iter().enumerate() {
            for body_predicate in &rule_analysis.positive_body_predicates {
                let indices = if rule_analysis.has_aggregates {
                    // An aggregate in a head means that the head predicates need to be in a higher stratum than the body predicates
                    // This is the same as when all body literals are negative
                    // Therefore, we can easily compute strata for aggregates by acting if all body atoms in the rule were negated
                    predicate_to_rules_body_negative
                        .entry(body_predicate.clone())
                        .or_default()
                } else {
                    // No aggregates in the rule
                    predicate_to_rules_body_positive
                        .entry(body_predicate.clone())
                        .or_default()
                };

                indices.push(rule_index);
            }

            for body_predicate in &rule_analysis.negative_body_predicates {
                let indices = predicate_to_rules_body_negative
                    .entry(body_predicate.clone())
                    .or_default();

                indices.push(rule_index);
            }

            for head_predicate in &rule_analysis.head_predicates {
                let indices = predicate_to_rules_head
                    .entry(head_predicate.clone())
                    .or_default();

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
    /// Create new [StrategyStratifiedNegation].
    fn new(rule_analyses: Vec<&RuleAnalysis>) -> Result<Self, SelectionStrategyError> {
        let graph = Self::build_graph(&rule_analyses);

        if let Some(mut strata) = graph.stratify(&[EdgeLabel::Negative]) {
            let mut substrategies = Vec::new();

            for stratum in &mut strata {
                stratum.sort();

                let sub_analyses: Vec<&RuleAnalysis> =
                    stratum.iter().map(|&i| rule_analyses[i]).collect();

                substrategies.push(SubStrategy::new(sub_analyses)?);
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

impl<SubStrategy: MetaStrategy> MetaStrategy for StrategyStratifiedNegation<SubStrategy> {
    fn current_scc(&self) -> &[usize] {
        self.substrategies[self.current_stratum].current_scc()
    }
}
