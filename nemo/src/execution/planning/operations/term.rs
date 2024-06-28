//! This module contains a helper function to translate [Term] into [FunctionTree]

use nemo_physical::{function::tree::FunctionTree, tabular::operations::OperationColumnMarker};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{
        BinaryOperation, NaryOperation, PrimitiveTerm, Term, TernaryOperation, UnaryOperation,
    },
};

/// Helper function to translate a [Term] into a [FunctionTree]
pub(super) fn term_to_function_tree(
    translation: &VariableTranslation,
    term: &Term,
) -> FunctionTree<OperationColumnMarker> {
    match term {
        Term::Primitive(primitive) => match primitive {
            PrimitiveTerm::GroundTerm(datavalue) => FunctionTree::constant(datavalue.clone()),
            PrimitiveTerm::Variable(variable) => FunctionTree::reference(
                *translation
                    .get(variable)
                    .expect("Every variable must be known"),
            ),
        },
        Term::Binary {
            operation,
            lhs,
            rhs,
        } => {
            let left = term_to_function_tree(translation, lhs);
            let right = term_to_function_tree(translation, rhs);

            match operation {
                BinaryOperation::NumericAddition => FunctionTree::numeric_addition(left, right),
                BinaryOperation::NumericSubtraction => {
                    FunctionTree::numeric_subtraction(left, right)
                }
                BinaryOperation::NumericMultiplication => {
                    FunctionTree::numeric_multiplication(left, right)
                }
                BinaryOperation::NumericDivision => FunctionTree::numeric_division(left, right),
                BinaryOperation::NumericPower => FunctionTree::numeric_power(left, right),
                BinaryOperation::NumericRemainder => FunctionTree::numeric_remainder(left, right),
                BinaryOperation::NumericLogarithm => FunctionTree::numeric_logarithm(left, right),
                BinaryOperation::StringCompare => FunctionTree::string_compare(left, right),
                BinaryOperation::StringContains => FunctionTree::string_contains(left, right),
                BinaryOperation::StringRegex => FunctionTree::string_regex(left, right),
                BinaryOperation::StringSubstring => FunctionTree::string_subtstring(left, right),
                BinaryOperation::Equal => FunctionTree::equals(left, right),
                BinaryOperation::Unequals => FunctionTree::unequals(left, right),
                BinaryOperation::NumericGreaterthan => {
                    FunctionTree::numeric_greaterthan(left, right)
                }
                BinaryOperation::NumericGreaterthaneq => {
                    FunctionTree::numeric_greaterthaneq(left, right)
                }
                BinaryOperation::NumericLessthan => FunctionTree::numeric_lessthan(left, right),
                BinaryOperation::NumericLessthaneq => FunctionTree::numeric_lessthaneq(left, right),
                BinaryOperation::StringBefore => FunctionTree::string_before(left, right),
                BinaryOperation::StringAfter => FunctionTree::string_after(left, right),
                BinaryOperation::StringStarts => FunctionTree::string_starts(left, right),
                BinaryOperation::StringEnds => FunctionTree::string_ends(left, right),
            }
        }
        Term::Unary(operation, subterm) => {
            let sub = term_to_function_tree(translation, subterm);

            match operation {
                UnaryOperation::BooleanNegation => FunctionTree::boolean_negation(sub),
                UnaryOperation::CanonicalString => FunctionTree::canonical_string(sub),
                UnaryOperation::NumericAbsolute => FunctionTree::numeric_absolute(sub),
                UnaryOperation::NumericCosine => FunctionTree::numeric_cosine(sub),
                UnaryOperation::NumericNegation => FunctionTree::numeric_negation(sub),
                UnaryOperation::NumericSine => FunctionTree::numeric_sine(sub),
                UnaryOperation::NumericSquareroot => FunctionTree::numeric_squareroot(sub),
                UnaryOperation::NumericTangent => FunctionTree::numeric_tangent(sub),
                UnaryOperation::StringLength => FunctionTree::string_length(sub),
                UnaryOperation::StringReverse => FunctionTree::string_reverse(sub),
                UnaryOperation::StringLowercase => FunctionTree::string_lowercase(sub),
                UnaryOperation::StringUppercase => FunctionTree::string_uppercase(sub),
                UnaryOperation::NumericCeil => FunctionTree::numeric_ceil(sub),
                UnaryOperation::NumericFloor => FunctionTree::numeric_floor(sub),
                UnaryOperation::NumericRound => FunctionTree::numeric_round(sub),
                UnaryOperation::CheckIsInteger => FunctionTree::check_is_integer(sub),
                UnaryOperation::CheckIsFloat => FunctionTree::check_is_float(sub),
                UnaryOperation::CheckIsDouble => FunctionTree::check_is_double(sub),
                UnaryOperation::CheckIsIri => FunctionTree::check_is_iri(sub),
                UnaryOperation::CheckIsNumeric => FunctionTree::check_is_numeric(sub),
                UnaryOperation::CheckIsNull => FunctionTree::check_is_null(sub),
                UnaryOperation::CheckIsString => FunctionTree::check_is_string(sub),
                UnaryOperation::Datatype => FunctionTree::datatype(sub),
                UnaryOperation::LanguageTag => FunctionTree::languagetag(sub),
                UnaryOperation::LexicalValue => FunctionTree::lexical_value(sub),
                UnaryOperation::CastToInteger => FunctionTree::casting_to_integer64(sub),
                UnaryOperation::CastToDouble => FunctionTree::casting_to_double(sub),
                UnaryOperation::CastToFloat => FunctionTree::casting_to_float(sub),
            }
        }
        Term::Aggregation(_) => unimplemented!("Aggregates are not implement yet"),
        Term::Function(name, _) => unimplemented!(
            "Function symbols are not supported yet. {} is not recognized as a builtin function.",
            name
        ),
        Term::Ternary {
            operation,
            first,
            second,
            third,
        } => {
            let first = term_to_function_tree(translation, first);
            let second = term_to_function_tree(translation, second);
            let third = term_to_function_tree(translation, third);

            match operation {
                TernaryOperation::StringSubstringLength => {
                    FunctionTree::string_subtstring_length(first, second, third)
                }
            }
        }
        Term::Nary {
            operation,
            parameters,
        } => {
            let parameters = parameters
                .iter()
                .map(|term| term_to_function_tree(translation, term))
                .collect::<Vec<_>>();

            match operation {
                NaryOperation::StringConcatenation => {
                    FunctionTree::string_concatenation(parameters)
                }
                NaryOperation::BooleanConjunction => FunctionTree::boolean_conjunction(parameters),
                NaryOperation::BooleanDisjunction => FunctionTree::boolean_disjunction(parameters),
                NaryOperation::BitAnd => FunctionTree::bit_and(parameters),
                NaryOperation::BitOr => FunctionTree::bit_or(parameters),
                NaryOperation::BitXor => FunctionTree::bit_xor(parameters),
                NaryOperation::NumericSum => FunctionTree::numeric_sum(parameters),
                NaryOperation::NumericProduct => FunctionTree::numeric_product(parameters),
                NaryOperation::NumericMinimum => FunctionTree::numeric_minimum(parameters),
                NaryOperation::NumericMaximum => FunctionTree::numeric_maximum(parameters),
                NaryOperation::NumericLukasiewicz => FunctionTree::numeric_lukasiewicz(parameters),
            }
        }
    }
}
