//! This module defines [Operation] and [OperationTerm].

use crate::rule_model::components::{
    term::{
        operation::operation_kind::OperationKind,
        primitive::{variable::Variable, Primitive},
    },
    IterablePrimitives, IterableVariables,
};

/// Operation
///
/// An action or computation performed on [Term]s.
/// This can include for example arithmetic or string operations.
#[derive(Debug, Clone)]
pub(crate) struct Operation {
    /// The kind of operation
    kind: OperationKind,
    /// The input arguments for the operation
    subterms: Vec<OperationTerm>,
}

impl Operation {
    /// Create a new [Operation].
    pub(crate) fn new(kind: OperationKind, subterms: Vec<OperationTerm>) -> Self {
        Self { kind, subterms }
    }

    /// Return the kind of operation.
    pub(crate) fn operation_kind(&self) -> OperationKind {
        self.kind
    }

    /// Return the list of subterms.
    pub(crate) fn subterms(&self) -> &Vec<OperationTerm> {
        &self.subterms
    }
}

impl IterableVariables for Operation {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.subterms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.subterms
                .iter_mut()
                .flat_map(|term| term.variables_mut()),
        )
    }
}

impl IterablePrimitives for Operation {
    type TermType = Primitive;

    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(self.subterms.iter().flat_map(|term| term.primitive_terms()))
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        Box::new(
            self.subterms
                .iter_mut()
                .flat_map(|term| term.primitive_terms_mut()),
        )
    }
}

/// Term that can be evaluated
#[derive(Debug, Clone)]
pub(crate) enum OperationTerm {
    Primitive(Primitive),
    Operation(Operation),
}

impl IterableVariables for OperationTerm {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        match self {
            OperationTerm::Primitive(primitive) => primitive.variables(),
            OperationTerm::Operation(operation) => operation.variables(),
        }
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        match self {
            OperationTerm::Primitive(primitive) => primitive.variables_mut(),
            OperationTerm::Operation(operation) => operation.variables_mut(),
        }
    }
}

impl IterablePrimitives for OperationTerm {
    type TermType = Primitive;

    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        match self {
            OperationTerm::Primitive(primitive) => Box::new(Some(primitive).into_iter()),
            OperationTerm::Operation(operation) => operation.primitive_terms(),
        }
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        match self {
            OperationTerm::Primitive(primitive) => Box::new(Some(primitive).into_iter()),
            OperationTerm::Operation(operation) => operation.primitive_terms_mut(),
        }
    }
}
