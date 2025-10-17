//! This module defines [VariableGenerator].

use crate::rule_model::components::term::primitive::variable::Variable;

/// Object that generates fresh variables
#[derive(Debug, Default, Copy, Clone)]
pub struct VariableGenerator {
    /// Current unique variable id
    id: usize,
}

impl VariableGenerator {
    /// Generate a fresh universal variable.
    pub fn universal(&mut self, name: &str) -> Variable {
        self.id += 1;

        Variable::universal(&format!("_{name}_{}", self.id))
    }
}
