//! The comparisons: term identity, value equality, the value order and the extrema.
//!
//! Value equality and the value order are [value_eq](ValueRef::value_eq)
//! and [value_cmp](ValueRef::value_cmp), which follow SPARQL's
//! [operator mapping](https://www.w3.org/TR/sparql12-query/#OperatorMapping).
//! Term identity is a different relation, the [PartialEq] of [ValueRef].

use std::cmp::Ordering;

use crate::{
    function::StackValue,
    value::{EvaluationError, NumberRef, ValueDomain, ValueRef},
};

/// Returns whether two values are the same term,
/// SPARQL's [`sameTerm`](https://www.w3.org/TR/sparql12-query/#func-sameTerm).
///
/// This is the identity of joins and of duplicate elimination.
/// Values of different domains are distinct, so no operands make an error.
pub(crate) fn same(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left == right).into()
}

/// Returns whether two values are equal, SPARQL's `=` operator,
/// as [value_eq](ValueRef::value_eq) decides.
pub(crate) fn equal(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    left.value_eq(right).into()
}

/// Returns the order of two values as `-1`, `0` or `1`.
///
/// An undefined order, as for NaN, is an [Incomparable](EvaluationError::Incomparable) error.
/// This is not a SPARQL operator,
/// but the three-way form of the ordering predicates such as [less].
pub(crate) fn signum(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    match left.value_cmp(right) {
        Ok(Some(ordering)) => ValueRef::Number(NumberRef::Decimal((ordering as i64).into())).into(),
        Ok(None) => EvaluationError::Incomparable.into(),
        Err(error) => error.into(),
    }
}

/// Returns the greatest of numeric operands, as [extremum] describes.
pub(crate) fn maximum(values: &[StackValue<'_>]) -> StackValue<'static> {
    extremum(values, Ordering::Greater)
}

/// Returns the least of numeric operands, as [extremum] describes.
pub(crate) fn minimum(values: &[StackValue<'_>]) -> StackValue<'static> {
    extremum(values, Ordering::Less)
}

/// Returns the extremum of numeric operands in the direction `prefer`,
/// XPath's [`fn:max`](https://www.w3.org/TR/xpath-functions-31/#func-max)
/// and [`fn:min`](https://www.w3.org/TR/xpath-functions-31/#func-min) on numbers.
///
/// The operands are promoted to the smallest numeric type that holds all of them,
/// and the result is the extreme one, or NaN if one of them is NaN.
/// If an operand is an error or not a number, the first such operand decides the result,
/// its error or a [TypeMismatch](EvaluationError::TypeMismatch).
/// No operands at all are a [TypeMismatch](EvaluationError::TypeMismatch) too.
///
/// If equal values are distinct terms, such as the two zeros, the term order decides.
/// XPath leaves this choice to the implementation,
/// and the term order makes the result independent of the order of the operands.
fn extremum(values: &[StackValue<'_>], prefer: Ordering) -> StackValue<'static> {
    // The numeric domains are declared in promotion order, from integers to doubles,
    // so the smallest type that holds every operand is the greatest of their domains.
    let mut common = ValueDomain::Integer;

    for value in values {
        match value.as_result() {
            Ok(ValueRef::Number(number)) => common = common.max(number.domain()),
            Ok(_) => return EvaluationError::TypeMismatch.into(),
            Err(error) => return error.into(),
        }
    }

    let mut best: Option<NumberRef<'_>> = None;

    for value in values {
        let Ok(ValueRef::Number(number)) = value.as_result() else {
            unreachable!("the first pass checked every operand")
        };
        let number = number.promote_to(common);

        // NaN is the extremum in either direction.
        if number.is_nan() {
            return ValueRef::Number(number).to_owned().into();
        }

        best = Some(match best {
            None => number,
            Some(current) => {
                // Equal values that are distinct terms, such as the two zeros,
                // are ordered by the term order.
                let ordering = number
                    .value_cmp(current)
                    .map(|ordering| ordering.then_with(|| number.cmp(&current)));

                if ordering == Some(prefer) {
                    number
                } else {
                    current
                }
            }
        });
    }

    match best {
        Some(number) => ValueRef::Number(number).to_owned().into(),
        None => EvaluationError::TypeMismatch.into(),
    }
}

/// Applies an ordering predicate of SPARQL's
/// [operator mapping](https://www.w3.org/TR/sparql12-query/#OperatorMapping),
/// such as `<`, to the order of two values.
///
/// An undefined order, as for a NaN operand, makes every predicate false,
/// as in IEEE 754 and
/// [`op:numeric-less-than`](https://www.w3.org/TR/xpath-functions-31/#func-numeric-less-than).
fn ordering_predicate(
    left: ValueRef<'_>,
    right: ValueRef<'_>,
    predicate: impl FnOnce(Ordering) -> bool,
) -> StackValue<'static> {
    match left.value_cmp(right) {
        Ok(Some(ordering)) => predicate(ordering).into(),
        Ok(None) => false.into(),
        Err(error) => error.into(),
    }
}

/// Returns whether the first value is smaller.
pub(crate) fn less(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    ordering_predicate(left, right, Ordering::is_lt)
}

/// Returns whether the first value is smaller or equal.
pub(crate) fn less_or_equal(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    ordering_predicate(left, right, Ordering::is_le)
}

/// Returns whether the first value is greater.
pub(crate) fn greater(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    ordering_predicate(left, right, Ordering::is_gt)
}

/// Returns whether the first value is greater or equal.
pub(crate) fn greater_or_equal(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    ordering_predicate(left, right, Ordering::is_ge)
}
