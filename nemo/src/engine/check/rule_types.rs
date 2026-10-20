//! The types of one rule's variables.

use nemo_physical::value::Type;

use crate::model::Span;

/// The types one rule's variables can hold,
/// once the rule's conditions, bindings and computed values narrowed them.
#[derive(Debug, Clone)]
pub struct RuleTypes {
    /// The rule's source position.
    span: Span,
    /// Each named variable and its type, by the variable's name without the `?`.
    variables: Vec<(String, Type)>,
}

impl RuleTypes {
    /// Creates the types of a rule's variables.
    pub(crate) fn new(span: Span, variables: Vec<(String, Type)>) -> Self {
        Self { span, variables }
    }

    /// Returns the rule's source position.
    pub fn span(&self) -> Span {
        self.span
    }

    /// Returns the type of a variable, by its name without the `?`,
    /// or `None` for a name the rule's body does not bind.
    pub fn variable(&self, name: &str) -> Option<&Type> {
        self.variables
            .iter()
            .find(|(variable, _)| variable == name)
            .map(|(_, datatype)| datatype)
    }

    /// Returns each variable the rule's body binds, by its name without the `?`, with its type.
    pub fn variables(&self) -> impl Iterator<Item = (&str, &Type)> {
        self.variables
            .iter()
            .map(|(name, datatype)| (name.as_str(), datatype))
    }
}
