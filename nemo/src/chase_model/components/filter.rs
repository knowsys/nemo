//! This module defines [ChaseFilter].

use crate::rule_model::origin::Origin;

use super::{term::operation_term::OperationTerm, ChaseComponent};

/// Indicates that a new value must be created according to [OperationTerm].
///
/// The result will be "stored" in the given variable.
#[derive(Debug, Clone)]
pub(crate) struct ChaseFilter {
    /// Origin of this component
    origin: Origin,

    /// Operation the will be evaluated
    filter: OperationTerm,
}

impl ChaseFilter {
    /// Create a new [ChaseFilter].
    pub(crate) fn new(filter: OperationTerm) -> Self {
        Self {
            origin: Origin::default(),
            filter,
        }
    }

    /// Return the filter that is being applied.
    pub(crate) fn filter(&self) -> &OperationTerm {
        &self.filter
    }
}

impl ChaseComponent for ChaseFilter {
    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }
}
