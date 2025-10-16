//! This module defines [Operation] and [OperationTerm].

use std::fmt::Display;

use nemo_physical::{
    datavalues::AnyDataValue, function::tree::FunctionTree,
    tabular::operations::OperationColumnMarker,
};

use crate::{
    execution::planning::VariableTranslation,
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

    /// Return an iterator over all [AnyDataValue]s within this operation.
    pub fn datavalues(&self) -> Box<dyn Iterator<Item = AnyDataValue> + '_> {
        match self {
            Operation::Primitive(Primitive::Ground(ground)) => {
                Box::new(std::iter::once(ground.value()))
            }
            Operation::Primitive(Primitive::Variable(_)) => Box::new(std::iter::empty()),
            Operation::Opreation { kind: _, subterms } => {
                Box::new(subterms.iter().flat_map(|term| term.datavalues()))
            }
        }
    }

    /// Check whether this operation has the form of an assignment of a variable to a term.
    /// If so return the variable and the term as a pair or `None` otherwise.
    ///
    /// # Panics
    /// Panics if this component is invalid.
    pub fn variable_assignment(&self) -> Option<(&Variable, &Self)> {
        if let Self::Opreation { kind, subterms } = self {
            if matches!(kind, OperationKind::Equal) {
                let left = subterms.first().expect("invalid program component");
                let right = subterms.get(1).expect("invalid program component");

                if let Self::Primitive(Primitive::Variable(variable)) = left {
                    return Some((variable, right));
                } else if let Self::Primitive(Primitive::Variable(variable)) = right {
                    return Some((variable, left));
                }
            }
        }
        None
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

/// Macro for creating a binary [FunctionTree] node
macro_rules! binary {
    ($func:ident, $vec:ident) => {{
        // Get ownership of the last two elements of the vector.
        let right = $vec
            .pop()
            .expect("expected at least two elements in the vector");
        let left = $vec
            .pop()
            .expect("expected at least two elements in the vector");

        // Call the function with the two arguments.
        FunctionTree::$func(left, right)
    }};
}

/// Macro for creating a unary [FunctionTree] node
macro_rules! unary {
    ($func:ident, $vec:ident) => {{
        // Get ownership of the last two elements of the vector.
        let sub = $vec
            .pop()
            .expect("expected at least two elements in the vector");

        // Call the function with the two arguments.
        FunctionTree::$func(sub)
    }};
}

impl Operation {
    /// Translate this [Operation] into a [FunctionTree] (used in the physical layer).
    pub fn function_tree(
        &self,
        translation: &VariableTranslation,
    ) -> FunctionTree<OperationColumnMarker> {
        match self {
            Operation::Primitive(primitive) => match primitive {
                Primitive::Variable(variable) => FunctionTree::reference(
                    *translation
                        .get(variable)
                        .expect("Every variable must be known"),
                ),
                Primitive::Ground(ground_term) => FunctionTree::constant(ground_term.value()),
            },
            Operation::Opreation { kind, subterms } => {
                let mut sub = subterms
                    .iter()
                    .map(|term| term.function_tree(translation))
                    .collect::<Vec<_>>();

                match kind {
                    OperationKind::Equal => binary!(equals, sub),
                    OperationKind::Unequals => binary!(unequals, sub),
                    OperationKind::LanguageString => binary!(language_string, sub),
                    OperationKind::NumericSubtraction => binary!(numeric_subtraction, sub),
                    OperationKind::NumericDivision => binary!(numeric_division, sub),
                    OperationKind::NumericLogarithm => binary!(numeric_logarithm, sub),
                    OperationKind::NumericPower => binary!(numeric_power, sub),
                    OperationKind::NumericRemainder => binary!(numeric_remainder, sub),
                    OperationKind::NumericGreaterthaneq => binary!(numeric_greaterthaneq, sub),
                    OperationKind::NumericGreaterthan => binary!(numeric_greaterthan, sub),
                    OperationKind::NumericLessthaneq => binary!(numeric_lessthaneq, sub),
                    OperationKind::NumericLessthan => binary!(numeric_lessthan, sub),
                    OperationKind::StringCompare => binary!(string_compare, sub),
                    OperationKind::StringContains => binary!(string_contains, sub),
                    OperationKind::StringBefore => binary!(string_before, sub),
                    OperationKind::StringAfter => binary!(string_after, sub),
                    OperationKind::StringStarts => binary!(string_starts, sub),
                    OperationKind::StringEnds => binary!(string_ends, sub),
                    OperationKind::StringRegex => binary!(string_regex, sub),
                    OperationKind::StringLevenshtein => binary!(string_levenshtein, sub),
                    OperationKind::BitShl => binary!(bit_shl, sub),
                    OperationKind::BitShru => binary!(bit_shru, sub),
                    OperationKind::BitShr => binary!(bit_shr, sub),
                    OperationKind::StringSubstring => {
                        if sub.len() == 2 {
                            let start = sub.pop().expect("length must be 2");
                            let string = sub.pop().expect("length must be 2");

                            FunctionTree::string_substring(string, start)
                        } else {
                            let length = sub.pop().expect("length must be 3");
                            let start = sub.pop().expect("length must be 3");
                            let string = sub.pop().expect("length must be 3");

                            FunctionTree::string_substring_length(string, start, length)
                        }
                    }
                    OperationKind::BooleanNegation => unary!(boolean_negation, sub),
                    OperationKind::CastToDouble => unary!(casting_to_double, sub),
                    OperationKind::CastToFloat => unary!(casting_to_float, sub),
                    OperationKind::CastToInteger => unary!(casting_to_integer64, sub),
                    OperationKind::CastToIRI => unary!(casting_to_iri, sub),
                    OperationKind::CanonicalString => unary!(canonical_string, sub),
                    OperationKind::CheckIsInteger => unary!(check_is_integer, sub),
                    OperationKind::CheckIsFloat => unary!(check_is_float, sub),
                    OperationKind::CheckIsDouble => unary!(check_is_double, sub),
                    OperationKind::CheckIsIri => unary!(check_is_iri, sub),
                    OperationKind::CheckIsNumeric => unary!(check_is_numeric, sub),
                    OperationKind::CheckIsNull => unary!(check_is_null, sub),
                    OperationKind::CheckIsString => unary!(check_is_string, sub),
                    OperationKind::Datatype => unary!(datatype, sub),
                    OperationKind::LanguageTag => unary!(languagetag, sub),
                    OperationKind::NumericAbsolute => unary!(numeric_absolute, sub),
                    OperationKind::NumericCosine => unary!(numeric_cosine, sub),
                    OperationKind::NumericCeil => unary!(numeric_ceil, sub),
                    OperationKind::NumericFloor => unary!(numeric_floor, sub),
                    OperationKind::NumericNegation => unary!(numeric_negation, sub),
                    OperationKind::NumericRound => unary!(numeric_round, sub),
                    OperationKind::NumericSine => unary!(numeric_sine, sub),
                    OperationKind::NumericSquareroot => unary!(numeric_squareroot, sub),
                    OperationKind::NumericTangent => unary!(numeric_tangent, sub),
                    OperationKind::StringLength => unary!(string_length, sub),
                    OperationKind::StringReverse => unary!(string_reverse, sub),
                    OperationKind::StringLowercase => unary!(string_lowercase, sub),
                    OperationKind::StringUppercase => unary!(string_uppercase, sub),
                    OperationKind::StringUriEncode => unary!(string_uriencode, sub),
                    OperationKind::StringUriDecode => unary!(string_uridecode, sub),
                    OperationKind::LexicalValue => unary!(lexical_value, sub),
                    OperationKind::NumericSum => FunctionTree::numeric_sum(sub),
                    OperationKind::NumericProduct => FunctionTree::numeric_product(sub),
                    OperationKind::BitAnd => FunctionTree::bit_and(sub),
                    OperationKind::BitOr => FunctionTree::bit_or(sub),
                    OperationKind::BitXor => FunctionTree::bit_xor(sub),
                    OperationKind::BooleanConjunction => FunctionTree::boolean_conjunction(sub),
                    OperationKind::BooleanDisjunction => FunctionTree::boolean_disjunction(sub),
                    OperationKind::NumericMinimum => FunctionTree::numeric_minimum(sub),
                    OperationKind::NumericMaximum => FunctionTree::numeric_maximum(sub),
                    OperationKind::NumericLukasiewicz => FunctionTree::numeric_lukasiewicz(sub),
                    OperationKind::StringConcatenation => FunctionTree::string_concatenation(sub),
                }
            }
        }
    }
}
