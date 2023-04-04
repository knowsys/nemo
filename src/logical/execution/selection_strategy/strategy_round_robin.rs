//! Defines the execution strategy by which each rule is applied in the order it appears.

use crate::logical::{model::Rule, program_analysis::analysis::RuleAnalysis};

use super::strategy::RuleSelectionStrategy;

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
    fn new(_rules: &[Rule], rule_analyses: &[RuleAnalysis]) -> Self {
        let self_recursive = rule_analyses.iter().map(|a| a.is_recursive).collect();

        Self {
            rule_count: rule_analyses.len(),
            self_recursive,
            without_derivation: 0,
            current_rule_index: 0,
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
            self.without_derivation = 0;
        } else {
            self.without_derivation += 1;
        }

        // Finish the computation if every rule has been applied with no derivation
        if self.without_derivation >= self.rule_count {
            return None;
        }

        // If we have a derivation and the rule is recursive we want to stay on the same rule
        let update_rule_index = !new_derivations || !self.self_recursive[self.current_rule_index];

        if update_rule_index {
            self.current_rule_index = (self.current_rule_index + 1) % self.rule_count;
        }

        Some(self.current_rule_index)
    }
}
