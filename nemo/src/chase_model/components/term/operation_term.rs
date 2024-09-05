//! This module defines [Operation] and [OperationTerm].

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::term::{operation::operation_kind::OperationKind, primitive::Primitive},
        origin::Origin,
    },
};

/// Operation
///
/// An action or computation performed on [Term]s.
/// This can include for example arithmetic or string operations.
#[derive(Debug, Clone)]
pub(crate) struct Operation {
    /// Origin of this component
    origin: Origin,

    /// The kind of operation
    kind: OperationKind,
    /// The input arguments for the operation
    subterms: Vec<OperationTerm>,
}

impl Operation {
    /// Create a new [Operation].
    pub(crate) fn new(kind: OperationKind, subterms: Vec<OperationTerm>) -> Self {
        Self {
            origin: Origin::default(),
            kind,
            subterms,
        }
    }
}

impl ChaseComponent for Operation {
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

/// Term that can be evaluated
#[derive(Debug, Clone)]
pub(crate) enum OperationTerm {
    Primitive(Primitive),
    Operation(Operation),
}
