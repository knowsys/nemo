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
            ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
            IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
            component_iterator, component_iterator_mut,
        },
        error::{ValidationReport, validation_error::ValidationError},
        origin::Origin,
        pipeline::id::ProgramComponentId,
    },
};

use super::{
    Term,
    primitive::{Primitive, ground::GroundTerm, variable::Variable},
    value_type::ValueType,
};

/// Operation
///
/// An action or computation performed on [Term]s.
/// This can include for example arithmetic or string operations.
#[derive(Debug, Clone)]
pub struct Operation {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

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
            id: ProgramComponentId::default(),
            kind,
            subterms,
        }
    }

    /// Return an iterator over the arguments of this operation.
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
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

    /// Reduce this term by evaluating the expression
    /// returning a new [Term] with the same [Origin] as `self`.
    ///
    /// This function does nothing if `self` contains any variable.
    ///
    /// Returns `None` if any intermediate result is undefined.
    pub fn reduce(&self) -> Option<Term> {
        if !self.is_ground() {
            return Some(Term::Operation(self.clone()));
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

        stack_program
            .evaluate_data(&[])
            .map(|result| Term::from(GroundTerm::new(result)))
    }

    /// Check wether this term can be reduced to a ground value,
    /// except for global variables that need to be resolved.
    ///
    /// This is the case if
    ///     * This term does not contain non-global variables.
    ///     * This term does not contain undefined intermediate values.
    pub fn is_resolvable(&self) -> bool {
        if self.is_ground() {
            return self.reduce().is_some();
        }

        for term in self.terms() {
            if term.variables().any(|variable| variable.is_global()) {
                continue;
            }

            if !term.is_resolvable() {
                return false;
            }
        }

        true
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
            write!(f, "{term}")
        }
    }

    /// Put braces around the input term.
    fn format_braces(&self, f: &mut std::fmt::Formatter<'_>, term: &Term) -> std::fmt::Result {
        write!(f, "({term})")
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
        write!(f, " {operation} ")?;
        self.format_braces_priority(f, right)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(infix) = self.infix_representation()
            && self.subterms.len() == 2
        {
            return self.format_infix_operation(f, infix, &self.subterms[0], &self.subterms[1]);
        }

        self.format_operation(f)
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.subterms == other.subterms
    }
}

impl Eq for Operation {}

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

impl ComponentBehavior for Operation {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Operation
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        if let Some(variable) = self.variables().find(|variable| variable.is_anonymous()) {
            report.add(variable, ValidationError::OperationAnonymous);
        }

        if !self.kind.num_arguments().validate(self.subterms.len()) {
            report.add(
                self,
                ValidationError::OperationArgumentNumber {
                    used: self.subterms.len(),
                    expected: self.kind.num_arguments().to_string(),
                },
            );

            // If this check fails, self.reduce will panic, therefore we need to return here!
            return report.result();
        }

        if self.reduce().is_none() {
            report.add(self, ValidationError::InvalidGroundOperation);
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Operation {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Operation {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Operation {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let subterm_iterator = component_iterator(self.subterms.iter());
        Box::new(subterm_iterator)
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let subterm_iterator = component_iterator_mut(self.subterms.iter_mut());
        Box::new(subterm_iterator)
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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
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
            ComponentBehavior,
            term::{Term, operation::operation_kind::OperationKind},
        },
        translation::TranslationComponent,
    };

    use super::Operation;

    impl Operation {
        fn parse(input: &str) -> Result<Operation, ()> {
            let term = Term::parse(input).map_err(|_| ())?;

            if let Term::Operation(operation) = term {
                Ok(operation)
            } else {
                Err(())
            }
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

    #[test]
    fn invalid_grounded_operations() {
        let invalid_operations = vec![
            "2 * \"5\"",
            "STRBEFORE(\"123\",123)",
            "URIDECODE(Hello%2GWorld)",
            "STRLEN(123)",
            "LOG(1,\"3\")",
        ];

        for string in invalid_operations {
            let operation = Operation::parse(string).unwrap();
            assert!(operation.validate().is_err());
        }
    }
}
