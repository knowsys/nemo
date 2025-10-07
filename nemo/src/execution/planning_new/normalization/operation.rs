//! This module defines [Operation] and [OperationTerm].

use std::fmt::Display;

use crate::{
    rule_model::components::{
        IterableVariables,
        term::{
            operation::operation_kind::OperationKind,
            primitive::{Primitive, ground::GroundTerm, variable::Variable},
        },
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// An operation performed on [Primitive]s,
/// for example arithmetic or string operations.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Primitive term
    Primitive(Primitive),
    /// Operation
    Opreation {
        /// Type of operation
        kind: OperationKind,
        /// Input to the opreation
        subterms: Vec<Operation>,
    },
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Primitive(primitive) => primitive.fmt(f),
            Operation::Opreation { kind, subterms } => {
                let terms = DisplaySeperatedList::display(
                    subterms.iter(),
                    &format!("{} ", syntax::SEQUENCE_SEPARATOR),
                );

                f.write_str(&format!("{:?}({})", kind, terms))
            }
        }
    }
}

impl Operation {
    /// Create a new [Operation] of the form [Variable] = [Operation].
    pub fn new_assignment(variable: Variable, operation: Operation) -> Self {
        Self::Opreation {
            kind: OperationKind::Equal,
            subterms: vec![Self::Primitive(Primitive::Variable(variable)), operation],
        }
    }

    /// Creata a new [Operation] that is simply a variable.
    pub fn new_variable(variable: Variable) -> Self {
        Self::Primitive(Primitive::Variable(variable))
    }

    /// Create a new [Operation] that is simply a ground term.
    pub fn new_ground(ground: GroundTerm) -> Self {
        Self::Primitive(Primitive::Ground(ground))
    }

    /// Return an iterator over all variables within this operation.
    pub fn variables(&self) -> Box<dyn Iterator<Item = &Variable> + '_> {
        match self {
            Operation::Primitive(primitive) => primitive.variables(),
            Operation::Opreation { kind: _, subterms } => {
                Box::new(subterms.iter().flat_map(|term| term.variables()))
            }
        }
    }
}

impl Operation {
    /// Normalize a [crate::rule_model::components::term::Term]
    /// that can occur in the body of a rule.
    ///
    /// # Panics
    /// Panics if rule is ill-formed and one of the following conditions is met:
    ///     * Term contains any structued subterms (like tuples or maps)
    ///     * Term contains any aggregation
    pub fn normalize_body_term(term: &crate::rule_model::components::term::Term) -> Self {
        match term {
            crate::rule_model::components::term::Term::Primitive(primitive) => {
                Self::Primitive(primitive.clone())
            }
            crate::rule_model::components::term::Term::Operation(operation) => {
                Self::normalize_body_operation(operation)
            }
            _ => {
                panic!("invalid program: operation in body contains structured terms or aggregates")
            }
        }
    }

    /// Normalize a [crate::rule_model::components::term::operation::Operation]
    /// that can occur in the body of a rule.
    ///
    /// # Panics
    /// Panics if rule is ill-formed and one of the following conditions is met:
    ///     * Term contains any structued subterms (like tuples or maps)
    ///     * Term contains any aggregation
    pub fn normalize_body_operation(
        operation: &crate::rule_model::components::term::operation::Operation,
    ) -> Self {
        let kind = operation.operation_kind();
        let subterms = operation
            .terms()
            .map(Self::normalize_body_term)
            .collect::<Vec<_>>();

        Self::Opreation { kind, subterms }
    }

    /// Normalize a [crate::rule_model::components::term::Term]
    /// that can occur in the head of a rule.
    ///
    ///   /// Panics if rule is ill-formed and one of the following conditions is met:
    ///     * Term contains any structued subterms (like tuples or maps)
    ///     * Recursive aggregates
    pub fn normalize_head_term(
        term: &crate::rule_model::components::term::Term,
    ) -> (
        Self,
        Option<&crate::rule_model::components::term::aggregate::Aggregate>,
    ) {
        match term {
            crate::rule_model::components::term::Term::Primitive(primitive) => {
                (Self::Primitive(primitive.clone()), None)
            }
            crate::rule_model::components::term::Term::Operation(operation) => {
                let kind = operation.operation_kind();
                let (subterms, aggregate): (Vec<_>, Vec<_>) =
                    operation.terms().map(Self::normalize_head_term).unzip();
                let aggregate = aggregate.iter().find_map(|a| *a);

                (Self::Opreation { kind, subterms }, aggregate)
            }
            crate::rule_model::components::term::Term::Aggregate(aggregate) => {
                // Has the same name as defined in `normalize_aggregate`
                let aggregate_variable = Variable::universal("_AGGREGATE_OUT");

                (
                    Self::Primitive(Primitive::Variable(aggregate_variable)),
                    Some(aggregate),
                )
            }
            _ => {
                panic!("invalid program: operation in body contains structured terms or aggregates")
            }
        }
    }

    /// Normalize a [crate::rule_model::components::term::operation::Operation]
    /// that can occur in the head  of a rule.
    ///
    /// # Panics
    /// Panics if rule is ill-formed and one of the following conditions is met:
    ///     * Term contains any structued subterms (like tuples or maps)
    ///     * Recursive aggregates
    pub fn normalize_head_operation(
        operation: &crate::rule_model::components::term::operation::Operation,
    ) -> (
        Self,
        Option<&crate::rule_model::components::term::aggregate::Aggregate>,
    ) {
        let kind = operation.operation_kind();
        let (subterms, aggregate): (Vec<_>, Vec<_>) =
            operation.terms().map(Self::normalize_head_term).unzip();
        let aggregate = aggregate.iter().find_map(|a| *a);

        (Self::Opreation { kind, subterms }, aggregate)
    }
}
