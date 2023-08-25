use super::Identifier;

/// Aggregate occurring in a predicate in the head
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Aggregate {
    pub(crate) aggregate_identifier: Identifier,
    pub(crate) variable_identifiers: Vec<Identifier>,
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{}({:?})",
            &self.aggregate_identifier, self.variable_identifiers
        )
    }
}
