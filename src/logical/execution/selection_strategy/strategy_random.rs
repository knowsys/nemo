//! Defines the execution strategy that selects rules randomly.

use std::collections::HashSet;

use rand::Rng;

use crate::logical::{model::Rule, program_analysis::analysis::RuleAnalysis};

use super::strategy::RuleSelectionStrategy;

/// Defines a strategy that selects rules randomly.
#[derive(Debug)]
pub struct StrategyRandom {
    rule_count: usize,
    no_derivations: HashSet<usize>,
}

impl RuleSelectionStrategy for StrategyRandom {
    /// Create new [`StrategyRandom`].
    fn new(_rules: &[Rule], rule_analyses: &[RuleAnalysis]) -> Self {
        Self {
            rule_count: rule_analyses.len(),
            no_derivations: HashSet::new(),
        }
    }

    fn next_rule(&mut self, new_derivations: Option<bool>) -> Option<usize> {
        let new_derivations = if let Some(new) = new_derivations {
            new
        } else {
            // If it is the first rule application then either return the first rule
            // or finish the computation if the program is empty.
            return if self.rule_count > 0 { Some(0) } else { None };
        };

        if new_derivations {
            self.no_derivations.clear();
        }

        if self.no_derivations.len() == self.rule_count {
            return None;
        }

        let mut random_index = rand::thread_rng().gen_range(0..self.rule_count);
        while self.no_derivations.contains(&random_index) {
            random_index = (random_index + 1) % self.rule_count;
        }

        Some(random_index)
    }
}
