//! This module contains a helper function to translate [Term] into [FunctionTree]

use nemo_physical::{function::tree::FunctionTree, tabular::operations::OperationColumnMarker};

use crate::{
    chase_model::components::term::operation_term::{Operation, OperationTerm},
    execution::rule_execution::VariableTranslation,
    rule_model::components::term::{
        operation::operation_kind::OperationKind, primitive::Primitive,
    },
};

/// Helper function to translate a [OperationTerm] into a [FunctionTree].
pub(crate) fn operation_term_to_function_tree(
    translation: &VariableTranslation,
    operation_term: &OperationTerm,
) -> FunctionTree<OperationColumnMarker> {
    match operation_term {
        OperationTerm::Primitive(primitive) => match &primitive {
            Primitive::Ground(datavalue) => FunctionTree::constant(datavalue.value()),
            Primitive::Variable(variable) => FunctionTree::reference(
                *translation
                    .get(variable)
                    .expect("Every variable must be known"),
            ),
        },
        OperationTerm::Operation(operation) => operation_to_function_tree(translation, operation),
    }
}

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

/// Helper function to translate a [Operation] into a [FunctionTree].
pub(crate) fn operation_to_function_tree(
    translation: &VariableTranslation,
    operation: &Operation,
) -> FunctionTree<OperationColumnMarker> {
    let mut sub = operation
        .subterms()
        .iter()
        .map(|term| operation_term_to_function_tree(translation, term))
        .collect::<Vec<_>>();

    match operation.operation_kind() {
        OperationKind::Equal => binary!(equals, sub),
        OperationKind::Unequals => binary!(unequals, sub),
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
