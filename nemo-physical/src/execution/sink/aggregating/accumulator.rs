//! The accumulators of an aggregation: the running state of one aggregate within one group.

#[cfg(doc)]
use std::ops::Add;

use crate::{
    dictionary::Dictionary,
    execution::AggregateKind,
    storage::StorageValue,
    value::{
        DataValue, DecimalRef, Double, EvaluationError, Float, MapValue, NumberRef, TupleValue,
        ValueRef,
        number::decimal::{self, workspace},
    },
};

/// The running state of one aggregate within one group.
#[derive(Debug)]
pub(super) enum Accumulator {
    /// The number of rows so far.
    Count(u64),
    /// The running sum.
    Sum(SumState),
    /// The extreme value so far.
    Extremum {
        /// Whether the largest value wins, rather than the smallest.
        maximum: bool,
        /// The extreme value so far; `None` before the first row.
        best: Option<DataValue>,
    },
    /// The running sum and the number of rows so far.
    Average(SumState, u64),
    /// The values so far.
    Tuple(Vec<DataValue>),
    /// The key–value pairs so far.
    Map(Vec<(DataValue, DataValue)>),
}

impl Accumulator {
    /// Creates the empty accumulator of an aggregation function.
    pub(super) fn new(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Accumulator::Count(0),
            AggregateKind::Sum => Accumulator::Sum(SumState::Empty),
            AggregateKind::Min => Accumulator::Extremum {
                maximum: false,
                best: None,
            },
            AggregateKind::Max => Accumulator::Extremum {
                maximum: true,
                best: None,
            },
            AggregateKind::Average => Accumulator::Average(SumState::Empty, 0),
            AggregateKind::Tuple => Accumulator::Tuple(Vec::new()),
            AggregateKind::Map => Accumulator::Map(Vec::new()),
        }
    }

    /// Folds one row's value in, with its pair for a map;
    /// a count reads no value.
    pub(super) fn update(
        &mut self,
        value: Option<StorageValue>,
        pair: Option<StorageValue>,
        dictionary: &Dictionary,
    ) {
        // Counting never looks at the value; everything else decodes it.
        if let Accumulator::Count(count) = self {
            *count += 1;
            return;
        }

        let decode = |value: StorageValue| {
            dictionary
                .decode(value)
                .expect("trie values decode against their dictionary")
        };
        let value = decode(value.expect("every aggregate but a count reads a value"));

        match self {
            Accumulator::Count(_) => unreachable!("counting returned above"),
            Accumulator::Sum(sum) => sum.add(value),
            Accumulator::Extremum { maximum, best } => update_extremum(best, *maximum, value),
            Accumulator::Average(sum, count) => {
                sum.add(value);
                *count += 1;
            }
            Accumulator::Tuple(values) => values.push(value.to_owned()),
            Accumulator::Map(pairs) => {
                let pair = pair.expect("map aggregates carry a value layer");
                pairs.push((value.to_owned(), decode(pair).to_owned()));
            }
        }
    }

    /// Returns the result of the aggregate.
    pub(super) fn finish(self) -> Result<DataValue, EvaluationError> {
        match self {
            Accumulator::Count(count) => Ok(DataValue::decimal(i128::from(count), 0)
                .expect("a count is within the length limit")),
            Accumulator::Sum(sum) => sum.finish(),
            Accumulator::Extremum { best, .. } => {
                Ok(best.expect("groups contain at least one row"))
            }
            Accumulator::Average(sum, count) => {
                let sum = sum.finish()?;
                let count = i64::try_from(count).expect("group sizes fit i64");
                sum.as_ref() / ValueRef::Number(NumberRef::Decimal(count.into()))
            }
            Accumulator::Tuple(mut values) => {
                // Usually a no-op: the values arrive sorted
                // where their layer directly follows the group key.
                values.sort();
                Ok(DataValue::tuple(TupleValue::new(values)))
            }
            Accumulator::Map(mut pairs) => {
                // Identical pairs collapse; a key with two different values
                // describes no map, the error a map literal raises.
                pairs.sort();
                pairs.dedup();
                MapValue::new(pairs)
                    .map(DataValue::map)
                    .ok_or(EvaluationError::Malformed)
            }
        }
    }
}

/// Folds a value into a running extremum.
///
/// Values compare by value if they are comparable,
/// and by their order as terms otherwise or if they are equal.
/// A NaN makes the extremum NaN, as in XPath's `fn:min` and `fn:max`.
fn update_extremum(best: &mut Option<DataValue>, maximum: bool, value: ValueRef<'_>) {
    let Some(current) = best else {
        *best = Some(value.to_owned());
        return;
    };

    let is_nan = |value| matches!(value, ValueRef::Number(number) if number.is_nan());
    if is_nan(value) {
        *current = value.to_owned();
        return;
    }
    if is_nan(current.as_ref()) {
        return;
    }

    // Equal values that are distinct terms, such as `1` and `1.0d`, fall to the term order too,
    // which keeps the result independent of the order the rows arrive in.
    let ordering = match value.value_cmp(current.as_ref()) {
        Ok(Some(ordering)) => ordering.then_with(|| value.cmp(&current.as_ref())),
        _ => value.cmp(&current.as_ref()),
    };
    let improves = if maximum {
        ordering.is_gt()
    } else {
        ordering.is_lt()
    };

    if improves {
        *current = value.to_owned();
    }
}

/// The running state of a sum:
/// a fold of [`+`](ValueRef::add) over numbers and durations, as XPath's `fn:sum` takes them.
///
/// A step whose sum and value share one primitive kind folds in place;
/// every other step is the `+` of values, so the sum cannot drift from it.
#[derive(Debug)]
pub(super) enum SumState {
    /// No value yet: the sum of nothing is the integer zero.
    Empty,
    /// An exact sum of integers.
    Int(i128),
    /// An exact sum of decimals, which takes any compact exact number in place.
    Fixed(workspace::Fixed),
    /// A sum of floats.
    Float(Float),
    /// A sum of doubles.
    Double(Double),
    /// An exact sum beyond the compact range, which takes any exact number in place.
    Big(workspace::Big),
    /// Any other sum.
    Value(DataValue),
    /// The error that poisoned the sum.
    Error(EvaluationError),
}

impl SumState {
    /// Folds one value in.
    fn add(&mut self, value: ValueRef<'_>) {
        match (&mut *self, value) {
            (SumState::Error(_), _) => return,
            (
                SumState::Int(sum),
                ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Int(value)))),
            ) => {
                if let Some(total) = sum.checked_add(i128::from(value)) {
                    *sum = total;
                    return;
                }
            }
            (SumState::Float(sum), ValueRef::Number(NumberRef::Float(value))) => {
                *sum = *sum + value;
                return;
            }
            (SumState::Double(sum), ValueRef::Number(NumberRef::Double(value))) => {
                *sum = *sum + value;
                return;
            }
            // `+` sums exact numbers in the same workspaces, to the same bound;
            // where the `i128` workspace runs out, `+` below continues in the big one.
            (SumState::Fixed(sum), ValueRef::Number(NumberRef::Decimal(number))) => {
                if let Some(value) = workspace::Fixed::of(number)
                    && let Some((left, right, exponent)) = sum.align(value)
                    && let Some(total) = left.checked_add(right)
                {
                    *sum = workspace::Fixed::new(total, exponent);
                    return;
                }
            }
            (SumState::Big(sum), ValueRef::Number(NumberRef::Decimal(number))) => {
                *self = sum
                    .add(&workspace::Big::from_decimal(number))
                    .map_or(SumState::Error(EvaluationError::Overflow), SumState::Big);
                return;
            }
            _ => {}
        }

        *self = match std::mem::replace(self, SumState::Empty) {
            SumState::Empty if Self::summable(value) => Self::of(value.to_owned()),
            state if Self::summable(value) => match state.into_value().as_ref() + value {
                Ok(sum) => Self::of(sum),
                Err(error) => SumState::Error(error),
            },
            _ => SumState::Error(EvaluationError::TypeMismatch),
        };
    }

    /// Returns whether a value takes part in a sum:
    /// `+` also moves a point in time by a duration, which a sum does not.
    fn summable(value: ValueRef<'_>) -> bool {
        matches!(value, ValueRef::Number(_) | ValueRef::Duration(_))
    }

    /// Returns the state holding a sum, in place if its kind allows.
    fn of(sum: DataValue) -> Self {
        match sum.as_ref() {
            ValueRef::Number(NumberRef::Decimal(number)) => match number.0 {
                decimal::ReprRef::Int(sum) => SumState::Int(i128::from(sum)),
                decimal::ReprRef::Compact(_) => {
                    SumState::Fixed(workspace::Fixed::of(number).expect("a compact number"))
                }
                decimal::ReprRef::Wide(sum) => SumState::Big(workspace::Big::from_wide(sum)),
            },
            ValueRef::Number(NumberRef::Float(sum)) => SumState::Float(sum),
            ValueRef::Number(NumberRef::Double(sum)) => SumState::Double(sum),
            _ => SumState::Value(sum),
        }
    }

    /// Returns the sum of a state that holds one.
    fn into_value(self) -> DataValue {
        match self {
            SumState::Int(sum) => {
                DataValue::decimal(sum, 0).expect("an i128 is within the length limit")
            }
            SumState::Fixed(sum) => DataValue::decimal(sum.mantissa(), sum.exponent())
                .expect("an i128 mantissa at a compact exponent is within the length limit"),
            SumState::Float(sum) => DataValue::float(sum),
            SumState::Double(sum) => DataValue::double(sum),
            SumState::Big(sum) => DataValue::from(sum),
            SumState::Value(sum) => sum,
            SumState::Empty | SumState::Error(_) => unreachable!("the state holds a sum"),
        }
    }

    /// Returns the final sum.
    fn finish(self) -> Result<DataValue, EvaluationError> {
        match self {
            SumState::Empty => Ok(DataValue::integer(0)),
            SumState::Error(error) => Err(error),
            state => Ok(state.into_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the sum of the values, folded in order.
    fn sum_of(values: &[ValueRef<'_>]) -> Result<DataValue, EvaluationError> {
        let mut state = SumState::Empty;
        for &value in values {
            state.add(value);
        }
        state.finish()
    }

    #[test]
    fn durations_sum_within_a_kind() {
        let duration = |lexical| {
            DataValue::typed_literal("http://www.w3.org/2001/XMLSchema#duration", lexical)
                .expect(lexical)
        };
        let (day, hours, month) = (duration("P1D"), duration("PT12H"), duration("P1M"));

        assert_eq!(
            sum_of(&[day.as_ref(), hours.as_ref()]),
            Ok(duration("P1DT12H"))
        );
        assert_eq!(
            sum_of(&[month.as_ref(), month.as_ref()]),
            Ok(duration("P2M"))
        );
        assert_eq!(
            sum_of(&[day.as_ref(), month.as_ref()]),
            Err(EvaluationError::TypeMismatch)
        );
        // Durations and numbers do not sum together, whichever arrives first,
        // even where the numbers sum to zero.
        let zero = DataValue::integer(0);
        assert_eq!(
            sum_of(&[zero.as_ref(), day.as_ref()]),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            sum_of(&[day.as_ref(), zero.as_ref()]),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(sum_of(&[]), Ok(DataValue::integer(0)));

        // The average divides the sum by the count.
        let mut sum = SumState::Empty;
        sum.add(day.as_ref());
        sum.add(duration("P2D").as_ref());
        assert_eq!(
            Accumulator::Average(sum, 2).finish(),
            Ok(duration("P1DT12H"))
        );
    }

    #[test]
    fn extrema_of_equal_values_do_not_depend_on_arrival() {
        /// Returns the extremum of the values, folded in order.
        fn extremum(values: &[&DataValue], maximum: bool) -> DataValue {
            let mut best = None;
            for value in values {
                update_extremum(&mut best, maximum, value.as_ref());
            }
            best.expect("a nonempty sequence")
        }

        let (zero, negative_zero) = (DataValue::double(0.0), DataValue::double(-0.0));
        let (one, one_double) = (DataValue::integer(1), DataValue::double(1.0));
        for maximum in [false, true] {
            for (left, right) in [(&zero, &negative_zero), (&one, &one_double)] {
                assert_eq!(
                    extremum(&[left, right], maximum),
                    extremum(&[right, left], maximum),
                    "{left} and {right}"
                );
            }
        }
        // The term order decides, as for `MIN` and `MAX`.
        assert_eq!(extremum(&[&zero, &negative_zero], false), negative_zero);
        assert_eq!(extremum(&[&negative_zero, &zero], true), zero);
    }

    #[test]
    fn sums_fold_the_addition() {
        /// Returns the left fold of `+` over the values.
        fn fold(values: &[ValueRef<'_>]) -> Result<DataValue, EvaluationError> {
            let (&first, rest) = values.split_first().expect("a nonempty sequence");
            rest.iter()
                .try_fold(first.to_owned(), |sum, &value| sum.as_ref() + value)
        }

        let number = |lexical| DataValue::decimal_from_lexical(lexical).expect(lexical);
        let (int, float, double) = (DataValue::integer, DataValue::float, DataValue::double);
        let cases: Vec<(Vec<DataValue>, Result<DataValue, EvaluationError>)> = vec![
            // Integers leave the 64-bit range exactly, and come back.
            (vec![int(i64::MAX), int(1), int(-2)], Ok(int(i64::MAX - 1))),
            // Exact sums land in their home: two decimals may sum to an integer.
            (vec![number("1.5"), number("1.5"), int(1)], Ok(int(4))),
            (
                vec![number("0.25"), int(2), number("-0.125"), number("0.5")],
                Ok(number("2.625")),
            ),
            (
                vec![
                    number("0.1"),
                    DataValue::decimal(1, -40).expect("bounded"),
                    int(1),
                ],
                Ok(number("1.1000000000000000000000000000000000000001")),
            ),
            // Whichever side arrives first, a floating-point type wins the promotion.
            (
                vec![number("123456789012345678901"), double(0.5)],
                Ok(double(1.2345678901234568e20)),
            ),
            (
                vec![float(0.5), number("123456789012345678901")],
                Ok(float(1.234_567_9e20)),
            ),
            // A sum past the compact range folds exactly, and may come back.
            (
                vec![
                    int(i64::MAX),
                    number("123456789012345678901"),
                    number("0.5"),
                    number("-123456789012345678901"),
                ],
                Ok(number("9223372036854775807.5")),
            ),
            (
                vec![number("123456789012345678901"), int(1), float(0.5)],
                Ok(float(1.234_567_9e20)),
            ),
            (vec![number("0.3"), double(0.0)], Ok(double(0.3))),
            (vec![float(1.0), double(1.0), int(1)], Ok(double(3.0))),
            // An exact sum rounds to a float once, not through the double:
            // each lies just above the midpoint between two floats.
            (
                vec![float(0.0), int((1 << 53) + (1 << 29) + 1)],
                Ok(float(2f32.powi(53) + 2f32.powi(30))),
            ),
            (
                vec![number("1.00000005960464477539062500000001"), float(0.0)],
                Ok(float(1.000_000_1)),
            ),
            (vec![double(f64::NAN), int(1)], Ok(double(f64::NAN))),
        ];

        for (values, expected) in cases {
            let values: Vec<ValueRef<'_>> = values.iter().map(DataValue::as_ref).collect();
            assert_eq!(sum_of(&values), expected, "{values:?}");
            assert_eq!(fold(&values), expected, "{values:?}");
        }
    }
}
