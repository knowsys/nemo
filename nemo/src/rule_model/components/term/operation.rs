//! This module defines [Operation].

pub mod operation_kind;

use std::{fmt::Display, hash::Hash};

use operation_kind::OperationKind;

use crate::rule_model::{
    components::{IterableVariables, ProgramComponent},
    origin::Origin,
};

use super::{primitive::variable::Variable, Term};

/// Operation
///
/// An action or computation performed on [Term]s.
/// This can include for example arithmetic or string operations.
#[derive(Debug, Clone, Eq)]
pub struct Operation {
    /// Origin of this component
    origin: Origin,

    /// The kind of operation
    kind: OperationKind,
    /// The input arguments for the operation
    subterms: Vec<Term>,
}

impl Operation {
    /// Create a new [Operation]
    pub fn new(kind: OperationKind, subterms: Vec<Term>) -> Self {
        Self {
            origin: Origin::default(),
            kind,
            subterms,
        }
    }

    /// Create a new [Operation] giving the string name of the operation.
    pub fn new_from_name(operation: &str, subterms: Vec<Term>) -> Option<Self> {
        Some(Self::new(OperationKind::from_name(operation)?, subterms))
    }
}

// Helper functions related to the display implementation
impl Operation {
    /// Puts braces around `term` if it has a lower precendence than `self`.
    fn format_braces_priority(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        term: &Term,
    ) -> std::fmt::Result {
        let need_braces = if let Term::Operation(other) = term {
            self.kind.precedence() > other.kind.precedence()
        } else {
            false
        };

        if need_braces {
            self.format_braces(f, term)
        } else {
            write!(f, "{}", term)
        }
    }

    /// Put braces around the input term.
    fn format_braces(&self, f: &mut std::fmt::Formatter<'_>, term: &Term) -> std::fmt::Result {
        write!(f, "({})", term)
    }

    /// Formats the arguments of an operation as a delimiter separated list.
    fn format_operation_arguments(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        terms: &[Term],
        delimiter: &str,
    ) -> std::fmt::Result {
        for (index, term) in terms.iter().enumerate() {
            self.format_braces_priority(f, term)?;

            if index < terms.len() - 1 {
                f.write_str(delimiter)?;
            }
        }

        Ok(())
    }

    /// Returns the infix symbol corresponding to the operation
    /// or `None` if this operation should never be displayed as an infix operation.
    fn infix_representation(&self) -> Option<&str> {
        Some(match &self.kind {
            OperationKind::NumericSum => "+",
            OperationKind::NumericSubtraction => "-",
            OperationKind::NumericProduct => "*",
            &OperationKind::NumericDivision => "/",
            _ => return None,
        })
    }

    /// Format operation in the usual <name>(<arg1>, <arg2>, ...) style
    fn format_operation(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.kind)?;
        self.format_operation_arguments(f, &self.subterms, ",")?;
        f.write_str(")")
    }

    /// Format operation that is more naturally written in an infix style <left> <op> <right>.
    fn format_infix_operation(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        operation: &str,
        left: &Term,
        right: &Term,
    ) -> std::fmt::Result {
        self.format_braces_priority(f, left)?;
        write!(f, " {} ", operation)?;
        self.format_braces_priority(f, right)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(infix) = self.infix_representation() {
            if self.subterms.len() == 2 {
                return self.format_infix_operation(f, infix, &self.subterms[0], &self.subterms[1]);
            }
        }

        self.format_operation(f)
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.subterms == other.subterms
    }
}

impl PartialOrd for Operation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.kind.partial_cmp(&other.kind) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.subterms.partial_cmp(&other.subterms)
    }
}

impl Hash for Operation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.subterms.hash(state);
    }
}

impl ProgramComponent for Operation {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

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

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramValidationError>
    where
        Self: Sized,
    {
        todo!()
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
