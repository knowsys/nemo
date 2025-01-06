//! This module defines [Operation].

pub mod operation_kind;

use std::{collections::HashMap, fmt::Display, hash::Hash};

use operation_kind::OperationKind;

use crate::{
    chase_model::translation::ProgramChaseTranslation,
    execution::{
        planning::operations::operation::operation_term_to_function_tree,
        rule_execution::VariableTranslation,
    },
    rule_model::{
        components::{
            IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
        },
        error::{validation_error::ValidationErrorKind, ValidationErrorBuilder},
        origin::Origin,
    },
};

use super::{
    primitive::{ground::GroundTerm, variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

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

    /// Return an iterator over the arguments of this operation.
    pub fn arguments(&self) -> impl Iterator<Item = &Term> {
        self.subterms.iter()
    }

    /// Return the [OperationKind] of this operation.
    pub fn operation_kind(&self) -> OperationKind {
        self.kind
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        self.kind.return_type()
    }

    /// Check whether this operation has the form of an assignment of a variable to a term.
    /// If so return the variable and the term as a pair or `None` otherwise.
    ///
    /// # Panics
    /// Panics if this component is invalid.
    pub fn variable_assignment(&self) -> Option<(&Variable, &Term)> {
        if self.kind != OperationKind::Equal {
            return None;
        }

        let left = self.subterms.first().expect("invalid program component");
        let right = self.subterms.get(1).expect("invalid program component");

        if let Term::Primitive(Primitive::Variable(variable)) = left {
            return Some((variable, right));
        } else if let Term::Primitive(Primitive::Variable(variable)) = right {
            return Some((variable, left));
        }

        None
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.subterms.iter().all(Term::is_ground)
    }

    /// Reduce constant expressions returning a copy of the reduced [Term].
    pub fn reduce(&self) -> Term {
        if !self.is_ground() {
            return Term::Operation(Self {
                origin: self.origin,
                kind: self.kind,
                subterms: self.subterms.iter().map(Term::reduce).collect(),
            });
        }

        let chase_operation_term = ProgramChaseTranslation::build_operation_term(self);

        let empty_translation = VariableTranslation::new();
        let function_tree =
            operation_term_to_function_tree(&empty_translation, &chase_operation_term);

        let stack_program = nemo_physical::function::evaluation::StackProgram::from_function_tree(
            &function_tree,
            &HashMap::default(),
            None,
        );

        match stack_program.evaluate_data(&[]) {
            Some(result) => Term::from(GroundTerm::new(result)),
            None => Term::Operation(self.clone()),
        }
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
                f.write_fmt(format_args!("{delimiter} "))?;
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
            OperationKind::NumericDivision => "/",
            OperationKind::Equal => "=",
            OperationKind::Unequals => "!=",
            OperationKind::NumericGreaterthan => ">",
            OperationKind::NumericGreaterthaneq => ">=",
            OperationKind::NumericLessthan => "<",
            OperationKind::NumericLessthaneq => "<=",
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        if !self.kind.num_arguments().validate(self.subterms.len()) {
            builder.report_error(
                self.origin,
                ValidationErrorKind::OperationArgumentNumber {
                    used: self.subterms.len(),
                    expected: self.kind.num_arguments().to_string(),
                },
            );

            return None;
        }

        for argument in self.arguments() {
            argument.validate(builder)?;
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Operation
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

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::{
            parse::ComponentParseError,
            term::{operation::operation_kind::OperationKind, Term},
        },
        translation::TranslationComponent,
    };

    use super::Operation;

    impl Operation {
        fn parse(input: &str) -> Result<Operation, ComponentParseError> {
            let Term::Operation(op) = Term::parse(input)? else {
                return Err(ComponentParseError::ParseError);
            };

            Ok(op)
        }
    }

    #[test]
    fn parse_operation() {
        let operaton = Operation::parse("2 * 5").unwrap();

        assert_eq!(
            Operation::new(
                OperationKind::NumericProduct,
                vec![Term::from(2), Term::from(5)]
            ),
            operaton
        );
    }
}
