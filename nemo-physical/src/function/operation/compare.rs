//! The comparisons: term identity, value equality, the value order and its extrema.
//!
//! Value equality and order are [ValueRef]'s own,
//! [value_eq](ValueRef::value_eq) and [value_cmp](ValueRef::value_cmp),
//! with the semantics of SPARQL's
//! [operator mapping](https://www.w3.org/TR/sparql12-query/#OperatorMapping);
//! this module wraps them for the stack and adds the predicates and extrema over them.
//! Term identity is a different relation, the [PartialEq] of [ValueRef] itself.

use std::cmp::Ordering;

use crate::{
    function::StackValue,
    value::{EvaluationError, NumberRef, ValueDomain, ValueRef},
};

/// Returns whether two values are the same term,
/// the identity relation of joins and duplicate elimination,
/// SPARQL's [`sameTerm`](https://www.w3.org/TR/sparql12-query/#func-sameTerm).
///
/// Values of different domains are simply distinct, so no input is an error.
pub(crate) fn same(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left == right).into()
}

/// Returns whether two values are equal, SPARQL's `=` operator:
/// its typed rows of the operator mapping, and
/// [`sameValue`](https://www.w3.org/TR/sparql12-query/#func-sameValue) beyond them;
/// see [ValueRef::value_eq].
pub(crate) fn equal(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    left.value_eq(right).into()
}

/// Returns the order of two values as `-1`, `0`, or `1`;
/// an undefined order is an error.
///
/// Not a SPARQL operator; the three-way form of the ordering predicates below.
pub(crate) fn signum(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    match left.value_cmp(right) {
        Ok(Some(ordering)) => ValueRef::Number(NumberRef::Decimal((ordering as i64).into())).into(),
        Ok(None) => EvaluationError::Incomparable.into(),
        Err(error) => error.into(),
    }
}

/// Returns the greatest of numeric operands; see [extremum].
pub(crate) fn maximum(values: &[StackValue<'_>]) -> StackValue<'static> {
    extremum(values, Ordering::Greater)
}

/// Returns the least of numeric operands; see [extremum].
pub(crate) fn minimum(values: &[StackValue<'_>]) -> StackValue<'static> {
    extremum(values, Ordering::Less)
}

/// Returns the extremum of numeric operands in the given direction,
/// XPath's [`fn:max`](https://www.w3.org/TR/xpath-functions-31/#func-max)
/// and [`fn:min`](https://www.w3.org/TR/xpath-functions-31/#func-min) over numbers:
/// the operands are promoted to the smallest type holding all of them,
/// and the result is the extreme one,
/// or NaN if one is among them.
///
/// An error propagates, and a non-number is an error.
///
/// If equal values are distinct terms, such as the two zeros, the term order decides.
/// XPath leaves this to the implementation,
/// and the term order keeps the result independent of the order of the operands.
fn extremum(values: &[StackValue<'_>], prefer: Ordering) -> StackValue<'static> {
    // The numeric domains are declared in the order of the ladder,
    // so the smallest type holding every operand is the greatest of their domains.
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

        // The NaN is the extremum in either direction.
        if number.is_nan() {
            return ValueRef::Number(number).to_owned().into();
        }

        best = Some(match best {
            None => number,
            Some(current) => {
                // Equal values that are distinct terms, the two zeros, fall to the term order.
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

/// Applies an ordering predicate, the `<` family of SPARQL's
/// [operator mapping](https://www.w3.org/TR/sparql12-query/#OperatorMapping).
///
/// An undefined order, at a NaN operand, makes every predicate false,
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
