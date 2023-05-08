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

    current_index: usize,
}

impl RuleSelectionStrategy for StrategyRandom {
    /// Create new [`StrategyRandom`].
    fn new(_rules: Vec<&Rule>, rule_analyses: Vec<&RuleAnalysis>) -> Self {
        Self {
            rule_count: rule_analyses.len(),
            no_derivations: HashSet::new(),
            current_index: 0,
        }
    }

    fn next_rule(&mut self, new_derivations: Option<bool>) -> Option<usize> {
        if self.rule_count == 0 {
            return None;
        }

        if let Some(new) = new_derivations {
            if new {
                self.no_derivations.clear();
            } else {
                self.no_derivations.insert(self.current_index);
            }
        }

        if self.no_derivations.len() == self.rule_count {
            return None;
        }

        let mut random_index = rand::thread_rng().gen_range(0..self.rule_count);
        while self.no_derivations.contains(&random_index) {
            random_index = (random_index + 1) % self.rule_count;
        }

        self.current_index = random_index;

        Some(random_index)
    }
}
