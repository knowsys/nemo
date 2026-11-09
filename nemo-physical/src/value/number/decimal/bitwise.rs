//! The bitwise operations on integers.
//!
//! An integer is treated as its two's complement of unbounded width,
//! so a negative integer has infinitely many leading one bits.
//! The operations are therefore exact on every integer, as the arithmetic is,
//! and agree with their 64-bit counterparts wherever those do not overflow.
//! A shift left is a multiplication by a power of two,
//! and a shift right is a division by a power of two, rounded down.
//! An unsigned shift has no meaning at unbounded width, so there is none.

use num_bigint::{BigInt, Sign};

use crate::value::{
    EvaluationError,
    number::decimal::{Decimal, DecimalRef, arithmetic::big, workspace},
};

/// The bitwise connectives.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Bitwise {
    /// Bitwise and.
    And,
    /// Bitwise or.
    Or,
    /// Bitwise exclusive or.
    Xor,
}

impl DecimalRef<'_> {
    /// Returns the integer shifted left by a number of places, `self * 2^places`.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch)
    /// unless the value and the places are integers,
    /// [Malformed](EvaluationError::Malformed) for negative places,
    /// and [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    pub(crate) fn shift_left(self, places: DecimalRef<'_>) -> Result<Decimal, EvaluationError> {
        let places = shift_places(places)?;
        if let Some(value) = self.to_i64()
            && places < 64
        {
            // A 64-bit integer shifted by fewer than 64 places fits into 127 bits.
            let shifted = i128::from(value) << places;
            return Ok(Decimal::new(shifted, 0).expect("127 bits are within the length limit"));
        }

        let value = integer(self)?;
        if value.sign() == Sign::NoSign {
            return Ok(Decimal::from(0));
        }
        // A result that cannot be within the limit is refused before it is computed.
        let bits = value.bits().saturating_add(places);
        if !workspace::Big::admits_integer_bits(bits) {
            return Err(EvaluationError::Overflow);
        }
        let places = usize::try_from(places).expect("an admitted shift fits the address space");
        from_integer(value << places)
    }

    /// Returns the integer shifted right by a number of places, `⌊self / 2^places⌋`.
    ///
    /// The bits shifted out are dropped, and copies of the sign bit fill in.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch)
    /// unless the value and the places are integers,
    /// and [Malformed](EvaluationError::Malformed) for negative places.
    pub(crate) fn shift_right(self, places: DecimalRef<'_>) -> Result<Decimal, EvaluationError> {
        let places = shift_places(places)?;
        if let Some(value) = self.to_i64() {
            // After 63 places or more, only copies of the sign bit remain.
            return Ok(Decimal::from(value >> places.min(63)));
        }

        let value = integer(self)?;
        if places >= value.bits() {
            let sign = if value.sign() == Sign::Minus { -1 } else { 0 };
            return Ok(Decimal::from(sign));
        }
        let places = usize::try_from(places).expect("a shift within the bits fits");
        from_integer(value >> places)
    }

    /// Returns the result of a bitwise connective on two integers.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) unless both values are integers,
    /// and [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    pub(crate) fn bitwise(
        self,
        other: DecimalRef<'_>,
        connective: Bitwise,
    ) -> Result<Decimal, EvaluationError> {
        if let (Some(left), Some(right)) = (self.to_i64(), other.to_i64()) {
            return Ok(Decimal::from(match connective {
                Bitwise::And => left & right,
                Bitwise::Or => left | right,
                Bitwise::Xor => left ^ right,
            }));
        }

        let (left, right) = (integer(self)?, integer(other)?);
        from_integer(match connective {
            Bitwise::And => left & right,
            Bitwise::Or => left | right,
            Bitwise::Xor => left ^ right,
        })
    }
}

/// Returns the number of places of a shift as a `u64`, saturated at `u64::MAX`.
///
/// The saturation is safe, since a shift that far clears or overflows any value.
/// Fails with [TypeMismatch](EvaluationError::TypeMismatch) for a number that is not an
/// integer, and with [Malformed](EvaluationError::Malformed) for a negative integer.
fn shift_places(places: DecimalRef<'_>) -> Result<u64, EvaluationError> {
    if !places.is_integral() {
        return Err(EvaluationError::TypeMismatch);
    }
    if places.is_negative() {
        return Err(EvaluationError::Malformed);
    }
    Ok(places
        .to_i64()
        .map_or(u64::MAX, |places| places.cast_unsigned()))
}

/// Returns an integer as a big integer,
/// or [TypeMismatch](EvaluationError::TypeMismatch) for any other number.
fn integer(value: DecimalRef<'_>) -> Result<BigInt, EvaluationError> {
    if !value.is_integral() {
        return Err(EvaluationError::TypeMismatch);
    }
    Ok(big(value).to_bigint().expect("an integer has no fraction"))
}

/// Returns a big integer as a value,
/// or [Overflow](EvaluationError::Overflow) beyond the length limit of numbers.
fn from_integer(value: BigInt) -> Result<Decimal, EvaluationError> {
    workspace::Big::new(value, 0)
        .map(Decimal::from)
        .ok_or(EvaluationError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::number,
        value::{DataValue, NumberRef, ValueRef},
    };

    /// Returns the value of a 64-bit integer.
    fn int(value: i64) -> DataValue {
        DataValue::integer(value)
    }

    /// Applies an operation to the exact numbers of two values,
    /// and fails with a type mismatch if either value is not an exact number.
    fn apply(
        operation: impl Fn(DecimalRef<'_>, DecimalRef<'_>) -> Result<Decimal, EvaluationError>,
        left: &DataValue,
        right: &DataValue,
    ) -> Result<DataValue, EvaluationError> {
        match (left.as_ref(), right.as_ref()) {
            (
                ValueRef::Number(NumberRef::Decimal(left)),
                ValueRef::Number(NumberRef::Decimal(right)),
            ) => operation(left, right).map(DataValue::from),
            _ => Err(EvaluationError::TypeMismatch),
        }
    }

    /// A shift left multiplies exactly by a power of two, and fails for a result beyond the
    /// length limit, for negative places and for an operand that is not an integer.
    #[test]
    fn shifts_left_are_exact() {
        let left = |value: &DataValue, places: &DataValue| {
            apply(|value, places| value.shift_left(places), value, places)
        };
        assert_eq!(left(&int(1), &int(62)), Ok(int(1 << 62)));
        assert_eq!(left(&int(1), &int(63)), Ok(number("9223372036854775808")));
        assert_eq!(left(&int(-1), &int(63)), Ok(int(i64::MIN)));
        assert_eq!(left(&int(3), &int(64)), Ok(number("55340232221128654848")));
        assert_eq!(
            left(&number("18446744073709551616"), &int(1)),
            Ok(number("36893488147419103232"))
        );
        assert_eq!(left(&int(0), &number("100000000000000000000")), Ok(int(0)));
        // A result beyond the length limit is refused before it is computed.
        assert_eq!(
            left(&int(1), &int(10_000_000)),
            Err(EvaluationError::Overflow)
        );
        assert_eq!(
            left(&int(1), &number("100000000000000000000")),
            Err(EvaluationError::Overflow)
        );
        // Only integers can be shifted, and not by a negative number of places.
        assert_eq!(left(&int(1), &int(-1)), Err(EvaluationError::Malformed));
        assert_eq!(
            left(&number("1.5"), &int(1)),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            left(&DataValue::double(2.0), &int(1)),
            Err(EvaluationError::TypeMismatch)
        );
    }

    /// A shift right divides by a power of two and rounds down.
    #[test]
    fn shifts_right_round_down() {
        let right = |value: &DataValue, places: &DataValue| {
            apply(|value, places| value.shift_right(places), value, places)
        };
        assert_eq!(right(&int(-5), &int(1)), Ok(int(-3)));
        assert_eq!(right(&int(5), &int(1)), Ok(int(2)));
        assert_eq!(right(&int(-1), &int(64)), Ok(int(-1)));
        assert_eq!(right(&int(i64::MAX), &int(100)), Ok(int(0)));
        assert_eq!(
            right(&number("-36893488147419103233"), &int(1)),
            Ok(number("-18446744073709551617"))
        );
        assert_eq!(right(&number("36893488147419103232"), &int(64)), Ok(int(2)));
        assert_eq!(
            right(
                &number("-36893488147419103232"),
                &number("100000000000000000000")
            ),
            Ok(int(-1))
        );
    }

    /// The connectives treat integers as two's complements of unbounded width.
    #[test]
    fn connectives_read_twos_complement() {
        let and = |left: &DataValue, right: &DataValue| {
            apply(|l, r| l.bitwise(r, Bitwise::And), left, right)
        };
        let or = |left: &DataValue, right: &DataValue| {
            apply(|l, r| l.bitwise(r, Bitwise::Or), left, right)
        };
        let xor = |left: &DataValue, right: &DataValue| {
            apply(|l, r| l.bitwise(r, Bitwise::Xor), left, right)
        };
        assert_eq!(and(&int(12), &int(10)), Ok(int(8)));
        // A negative integer has one bits all the way up.
        assert_eq!(
            and(&int(-1), &number("18446744073709551616")),
            Ok(number("18446744073709551616"))
        );
        assert_eq!(
            and(&int(-8), &number("18446744073709551615")),
            Ok(number("18446744073709551608"))
        );
        assert_eq!(
            or(&number("-18446744073709551616"), &int(1)),
            Ok(number("-18446744073709551615"))
        );
        assert_eq!(
            xor(
                &number("18446744073709551616"),
                &number("18446744073709551617")
            ),
            Ok(int(1))
        );
        assert_eq!(
            and(&number("1.5"), &int(1)),
            Err(EvaluationError::TypeMismatch)
        );
    }
}
