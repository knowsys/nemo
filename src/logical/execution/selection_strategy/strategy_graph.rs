//! Defines a rule execution strategy which respects certain dependencies between rules.

use crate::logical::{model::Rule, program_analysis::analysis::RuleAnalysis};

use super::strategy::RuleSelectionStrategy;

/// Defines a rule execution strategy which respects certain dependencies between rules
#[derive(Debug)]
pub struct StrategyDependencyGraph {
    x: Vec<usize>,
}

impl RuleSelectionStrategy for StrategyDependencyGraph {
    fn new(rules: &[Rule], rule_analyses: &[RuleAnalysis]) -> Self {
        todo!()
    }

    fn next_rule(&mut self, new_derivations: Option<bool>) -> Option<usize> {
        todo!()
    }
}
