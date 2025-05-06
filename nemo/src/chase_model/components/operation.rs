//! This module defines [ChaseOperation].

use crate::rule_model::{
    components::{
        term::primitive::{variable::Variable, Primitive},
        IterablePrimitives, IterableVariables,
    },
    origin::Origin,
};

use super::{term::operation_term::OperationTerm, ChaseComponent};

/// Indicates that a new value must be created according to [OperationTerm].
///
/// The result will be "stored" in the given variable.
#[derive(Debug, Clone)]
pub(crate) struct ChaseOperation {
    /// Origin of this component
    origin: Origin,

    /// Variable that will hold the result of this operation
    output_variable: Variable,
    /// Operation the will be evaluated
    operation: OperationTerm,
}

impl ChaseOperation {
    /// Create a new [ChaseOperation].
    pub(crate) fn new(output_variable: Variable, operation: OperationTerm) -> Self {
        Self {
            origin: Origin::default(),
            output_variable,
            operation,
        }
    }

    /// Return the variable which associated with the result of this constructor.
    pub(crate) fn variable(&self) -> &Variable {
        &self.output_variable
    }

    /// Return the operation that is being evaluated.
    pub(crate) fn operation(&self) -> &OperationTerm {
        &self.operation
    }
}

impl ChaseComponent for ChaseOperation {
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

impl IterableVariables for ChaseOperation {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(
            Some(self.variable())
                .into_iter()
                .chain(self.operation.variables()),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            Some(&mut self.output_variable)
                .into_iter()
                .chain(self.operation.variables_mut()),
        )
    }
}

impl IterablePrimitives for ChaseOperation {
    type TermType = Primitive;

    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        self.operation.primitive_terms()
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        self.operation.primitive_terms_mut()
    }
}
