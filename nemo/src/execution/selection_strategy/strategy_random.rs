//! Defines the execution strategy that selects rules randomly.

use std::collections::HashSet;

use rand::Rng;

use crate::chase_model::{analysis::program_analysis::RuleAnalysis, components::rule::ChaseRule};

use super::strategy::{RuleSelectionStrategy, SelectionStrategyError};

/// Defines a strategy that selects rules randomly.
#[derive(Debug)]
#[allow(dead_code)]
pub struct StrategyRandom {
    rule_count: usize,
    no_derivations: HashSet<usize>,

    current_index: usize,
}

impl RuleSelectionStrategy for StrategyRandom {
    /// Create new [StrategyRandom].
    fn new(
        _rules: Vec<&ChaseRule>,
        rule_analyses: Vec<&RuleAnalysis>,
    ) -> Result<Self, SelectionStrategyError> {
        Ok(Self {
            rule_count: rule_analyses.len(),
            no_derivations: HashSet::new(),
            current_index: 0,
        })
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
