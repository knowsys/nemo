//! Defines the execution strategy by which each rule is applied in the order it appears.

use crate::{model::chase_model::ChaseRule, program_analysis::analysis::RuleAnalysis};

use super::strategy::{RuleSelectionStrategy, SelectionStrategyError};

/// Defines a strategy whereby each rule is applied one after another in the order they appear in the rule file.
/// Once every rule was applied it loops back to the first one.
/// If a round is completed without new derivations, the execution stops.
/// One exception to this are self-recursive rules, which will be applied exhaustively.
#[derive(Debug)]
pub struct StrategyRoundRobin {
    rule_count: usize,
    self_recursive: Vec<bool>,

    without_derivation: usize,
    current_rule_index: usize,
}

impl RuleSelectionStrategy for StrategyRoundRobin {
    /// Create new [`StrategyRoundRobin`].
    fn new(
        _rules: Vec<&ChaseRule>,
        rule_analyses: Vec<&RuleAnalysis>,
    ) -> Result<Self, SelectionStrategyError> {
        let self_recursive = rule_analyses.iter().map(|a| a.is_recursive).collect();

        Ok(Self {
            rule_count: rule_analyses.len(),
            self_recursive,
            without_derivation: 0,
            current_rule_index: 0,
        })
    }

    fn next_rule(&mut self, new_derivations: Option<bool>) -> Option<usize> {
        if self.rule_count == 0 {
            return None;
        }

        // Only switch to a different rule if the rule is not
        // recursive or if there are no new derivations
        let mut update_rule_index = !self.self_recursive[self.current_rule_index];

        match new_derivations {
            Some(true) => self.without_derivation = 0,
            Some(false) => {
                self.without_derivation += 1;
                // no new derivations, switch to a different rule
                update_rule_index = true;
            }
            None => {
                // If it is the first rule application then either return the first rule
                // or finish the computation if the program is empty.
                return (self.rule_count > 0).then_some(0);
            }
        }

        // Finish the computation if every rule has been applied with no derivation
        if self.without_derivation >= self.rule_count {
            return None;
        }

        if update_rule_index {
            self.current_rule_index = (self.current_rule_index + 1) % self.rule_count;
        }

        Some(self.current_rule_index)
    }
}
