//! Exact decimal arithmetic of unbounded precision.

use std::{
    cmp::Ordering,
    f64::consts::{LOG2_10, LOG10_2},
    fmt,
};

use num_bigint::{BigInt, BigUint, Sign};
use num_integer::Integer;

use crate::value::number::{
    decimal::{Compact, DecimalRef, ReprRef, WideRef, workspace::Fixed},
    lexical::Lexical,
};

/// An exact decimal number `mantissa * 10^exponent` of unbounded precision.
///
/// This is the workspace of the arithmetic over numbers with a wide operand,
/// and of compact computations that outgrow the [`i128` workspace](Fixed).
/// It is normalized like the value types:
/// the mantissa of a nonzero value is never divisible by ten,
/// and zero is uniquely `(0, 0)`.
///
/// Every value is bounded by [LENGTH_LIMIT](Self::LENGTH_LIMIT).
/// Construction fails beyond the limit,
/// and an operation refuses a result that cannot be within it before computing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Big {
    /// The mantissa; never divisible by ten unless the value is zero.
    mantissa: BigInt,
    /// The base-10 exponent; zero if the value is zero.
    exponent: i64,
    /// The number of decimal digits of the mantissa's magnitude; one for zero.
    digits: u64,
}

impl Big {
    /// The largest canonical lexical length a number accepts,
    /// in the workspace and in the wide value types alike.
    ///
    /// This is the safety net for arithmetic whose results grow without bound.
    pub(crate) const LENGTH_LIMIT: u64 = 1_000_000;

    /// The number of significant digits and of fraction digits a quotient keeps at least:
    /// the implementation-defined precision XPath permits,
    /// shared with the compact division.
    const QUOTIENT_DIGITS: i64 = 18;

    /// Creates the value `mantissa * 10^exponent`, normalizing the representation.
    ///
    /// Returns `None` beyond the length limit.
    pub(crate) fn new(mantissa: BigInt, exponent: i64) -> Option<Self> {
        if mantissa.sign() == Sign::NoSign {
            return Some(Self::zero());
        }

        // A mantissa that cannot be within the limit is refused before it is rendered.
        let trailing_zeros = mantissa.trailing_zeros().unwrap_or(0);
        if Self::length_at_least(mantissa.bits(), trailing_zeros, exponent) > Self::LENGTH_LIMIT {
            return None;
        }

        // Most results have no trailing zeros, and those keep their mantissa.
        // Otherwise the zeros are counted on the decimal digits;
        // dividing a long mantissa by ten repeatedly would be quadratic.
        let (mantissa, digits, zeros) = if (mantissa.magnitude() % 10u32) != BigUint::ZERO {
            let digits = Self::digit_count(mantissa.magnitude());
            (mantissa, digits, 0)
        } else {
            let digits = mantissa.magnitude().to_string();
            let trimmed = digits.trim_end_matches('0');
            let magnitude = BigUint::parse_bytes(trimmed.as_bytes(), 10)
                .expect("the digits of a mantissa parse");
            (
                BigInt::from_biguint(mantissa.sign(), magnitude),
                trimmed.len() as u64,
                digits.len() - trimmed.len(),
            )
        };
        let exponent = exponent.checked_add(i64::try_from(zeros).ok()?)?;

        (Self::canonical_length(digits, exponent) <= Self::LENGTH_LIMIT).then_some(Self {
            mantissa,
            exponent,
            digits,
        })
    }

    /// Creates the value zero.
    fn zero() -> Self {
        Self {
            mantissa: BigInt::ZERO,
            exponent: 0,
            digits: 1,
        }
    }

    /// Creates the value `digits * 10^exponent` of significant digits
    /// without leading or trailing zeros,
    /// as the [lexical parser](crate::value::number::lexical::Parsed::parse) delivers them.
    ///
    /// Returns `None` beyond the length limit.
    pub(crate) fn from_digits(negative: bool, digits: &str, exponent: i64) -> Option<Self> {
        if Self::canonical_length(digits.len() as u64, exponent) > Self::LENGTH_LIMIT {
            return None;
        }

        let magnitude = BigUint::parse_bytes(digits.as_bytes(), 10)?;
        let sign = if negative { Sign::Minus } else { Sign::Plus };

        Some(Self {
            mantissa: BigInt::from_biguint(sign, magnitude),
            exponent,
            digits: digits.len() as u64,
        })
    }

    /// Creates the value of an exact number, which is bounded already.
    pub(crate) fn from_decimal(value: DecimalRef<'_>) -> Self {
        match value.0 {
            ReprRef::Wide(value) => Self::from_wide(value),
            _ => Fixed::of(value)
                .and_then(Self::from_fixed)
                .expect("a compact number is within the length limit"),
        }
    }

    /// Creates the value of a wide number, which is canonical and bounded already.
    pub(crate) fn from_wide(value: WideRef<'_>) -> Self {
        let magnitude = value.mantissa_magnitude();
        let sign = if value.is_negative() {
            Sign::Minus
        } else {
            Sign::Plus
        };

        Self {
            digits: Self::digit_count(&magnitude),
            mantissa: BigInt::from_biguint(sign, magnitude),
            exponent: value.exponent(),
        }
    }

    /// Creates the exact value of a finite double, the binary fraction it denotes:
    /// `0.1d` is `0.1000000000000000055511151231257827021181583404541015625`.
    ///
    /// # Panics
    ///
    /// Panics for an infinity or the NaN, which have no exact value.
    pub(crate) fn from_finite_double(value: f64) -> Self {
        assert!(value.is_finite(), "only a finite double has an exact value");

        // The value is `mantissa * 2^exponent`,
        // and a negative power of two is a power of five over the same power of ten.
        let bits = value.to_bits();
        let biased = i64::try_from((bits >> 52) & 0x7ff).expect("eleven bits fit");
        let fraction = bits & ((1 << 52) - 1);
        let (magnitude, exponent) = if biased == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1 << 52), biased - 1075)
        };
        let mut mantissa = BigInt::from(magnitude);
        if value.is_sign_negative() {
            mantissa = -mantissa;
        }

        let shift = u32::try_from(exponent.unsigned_abs()).expect("a double's exponent fits");
        let exact = if exponent >= 0 {
            Self::new(mantissa << shift, 0)
        } else {
            Self::new(mantissa * BigInt::from(5u32).pow(shift), exponent)
        };
        exact.expect("a double's exact value is within the length limit")
    }

    /// Creates the value of an `i128` workspace value, or `None` beyond the length limit.
    pub(crate) fn from_fixed(value: Fixed) -> Option<Self> {
        Self::from_parts(value.mantissa(), value.exponent())
    }

    /// Creates the value `mantissa * 10^exponent` of an `i128` workspace pair.
    pub(crate) fn from_parts(mantissa: i128, exponent: i64) -> Option<Self> {
        Self::new(BigInt::from(mantissa), exponent)
    }

    /// Returns the normalized mantissa.
    pub(crate) fn mantissa(&self) -> &BigInt {
        &self.mantissa
    }

    /// Returns the normalized base-10 exponent.
    pub(crate) fn exponent(&self) -> i64 {
        self.exponent
    }

    /// Returns whether this value is zero.
    pub(crate) fn is_zero(&self) -> bool {
        self.mantissa.sign() == Sign::NoSign
    }

    /// Returns whether this value denotes an integer.
    pub(crate) fn is_integral(&self) -> bool {
        self.exponent >= 0
    }

    /// Returns the value as an integer, or `None` for a value with a fraction.
    pub(crate) fn to_bigint(&self) -> Option<BigInt> {
        if !self.is_integral() {
            return None;
        }

        let shift = u32::try_from(self.exponent).expect("the exponent of a bounded value fits");
        Some(&self.mantissa * BigInt::from(10u32).pow(shift))
    }

    /// Returns the value as a 64-bit integer, or `None` for a fraction and beyond the 64-bit range.
    pub(crate) fn to_i64(&self) -> Option<i64> {
        // An integer of at most nineteen digits is computed within an `i128`;
        // a longer one is beyond the range.
        if !self.is_integral() || self.digits.saturating_add(self.exponent.unsigned_abs()) > 19 {
            return None;
        }

        let mantissa = i128::try_from(&self.mantissa).ok()?;
        let factor = 10i128.pow(u32::try_from(self.exponent).ok()?);

        i64::try_from(mantissa * factor).ok()
    }

    /// Returns the value as a compact decimal, or `None` for an integer and beyond the compact range.
    pub(crate) fn to_compact(&self) -> Option<Compact> {
        let mantissa = i64::try_from(&self.mantissa).ok()?;
        let exponent = i32::try_from(self.exponent).ok()?;
        Compact::new(mantissa, exponent)
    }

    /// Returns the number of decimal digits of a magnitude; one for zero.
    ///
    /// The bit length gives the count up to one,
    /// and a comparison against a power of ten decides it.
    pub(crate) fn digit_count(magnitude: &BigUint) -> u64 {
        let bits = magnitude.bits();

        if bits == 0 {
            return 1;
        }

        let mut count = Self::digits_at_least(bits).max(1);
        let mut power = BigUint::from(10u32)
            .pow(u32::try_from(count).expect("digit counts of bounded values fit"));

        while *magnitude >= power {
            count += 1;
            power *= 10u32;
        }

        while count > 1 && *magnitude < &power / 10u32 {
            count -= 1;
            power /= 10u32;
        }

        count
    }

    /// Returns the length of the canonical lexical form
    /// for a mantissa of the given digit count at the given exponent;
    /// the sign is not counted.
    pub(crate) fn canonical_length(digits: u64, exponent: i64) -> u64 {
        if exponent >= 0 {
            digits.saturating_add(exponent.unsigned_abs())
        } else {
            // `0.` and the leading zeros of a small fraction.
            digits.max(exponent.unsigned_abs() + 1).saturating_add(1)
        }
    }

    /// Returns a lower bound on the canonical length of the normalized value
    /// whose unnormalized mantissa has at least `bits` bits
    /// and at most `trailing_zeros` decimal trailing zeros, at the given exponent.
    ///
    /// This is what an operation knows of its result before computing it.
    ///
    /// Normalization folds the trailing zeros into the exponent:
    /// an integer keeps its length,
    /// a fraction loses one character per zero up to its fraction digits,
    /// and its point if it becomes an integer.
    /// Zeros beyond the fraction digits stay as digits of the integer.
    fn length_at_least(bits: u64, trailing_zeros: u64, exponent: i64) -> u64 {
        let length = Self::canonical_length(Self::digits_at_least(bits), exponent);
        if exponent >= 0 {
            length
        } else {
            let folded = trailing_zeros.min(exponent.unsigned_abs());
            length.saturating_sub(folded.saturating_add(1))
        }
    }

    /// Returns whether an integer of the given bit length can be within the length limit,
    /// which an operation growing an integer asks before it computes the result.
    pub(crate) fn admits_integer_bits(bits: u64) -> bool {
        Self::length_at_least(bits, 0, 0) <= Self::LENGTH_LIMIT
    }

    /// Returns a lower bound on the decimal digits of a magnitude of at least the given bit length.
    ///
    /// `2^(bits - 1)` has more than `(bits - 1) * log10(2)` digits,
    /// and the estimate rounded down stays below the count through its floating-point error.
    fn digits_at_least(bits: u64) -> u64 {
        // `as` saturates; a bound past `u64` is past any limit.
        #[allow(clippy::cast_possible_truncation)]
        let digits = (bits.saturating_sub(1) as f64 * LOG10_2) as u64;
        digits
    }

    /// Returns a lower bound on the bit length of `10^exponent`,
    /// by the same estimate as [digits_at_least](Self::digits_at_least).
    fn pow10_bits_at_least(exponent: u64) -> u64 {
        // `as` saturates; a bound past `u64` is past any limit.
        #[allow(clippy::cast_possible_truncation)]
        let bits = (exponent as f64 * LOG2_10) as u64;
        bits
    }

    /// Returns the binary trailing zeros of the mantissa, a bound on its decimal ones;
    /// zero for zero.
    fn trailing_zeros(&self) -> u64 {
        self.mantissa.trailing_zeros().unwrap_or(0)
    }

    /// Returns the mantissas of two values aligned to their smaller exponent.
    fn aligned(&self, other: &Self) -> (BigInt, BigInt, i64) {
        let exponent = self.exponent.min(other.exponent);
        let scale = |value: &Self| {
            let shift = u32::try_from(value.exponent - exponent)
                .expect("the exponent difference of two bounded values fits");
            &value.mantissa * BigInt::from(10u32).pow(shift)
        };
        (scale(self), scale(other), exponent)
    }

    /// Returns the sum of two values.
    pub(crate) fn add(&self, other: &Self) -> Option<Self> {
        let (left, right, exponent) = self.aligned(other);
        Self::new(left + right, exponent)
    }

    /// Returns the difference of two values.
    pub(crate) fn sub(&self, other: &Self) -> Option<Self> {
        let (left, right, exponent) = self.aligned(other);
        Self::new(left - right, exponent)
    }

    /// Returns the product of two values.
    pub(crate) fn mul(&self, other: &Self) -> Option<Self> {
        if self.is_zero() || other.is_zero() {
            return Some(Self::zero());
        }

        let exponent = self.exponent.checked_add(other.exponent)?;
        // A product that cannot be within the limit is refused before it is computed:
        // the factors' bits add, less one, and their trailing zeros add.
        let bits = self.mantissa.bits() + other.mantissa.bits() - 1;
        let trailing_zeros = self.trailing_zeros() + other.trailing_zeros();
        if Self::length_at_least(bits, trailing_zeros, exponent) > Self::LENGTH_LIMIT {
            return None;
        }

        Self::new(&self.mantissa * &other.mantissa, exponent)
    }

    /// Returns the quotient of two values:
    /// exact if it terminates within the scale of [quotient_scale](Self::quotient_scale),
    /// otherwise rounded half to even at that scale.
    ///
    /// Returns `None` for a zero divisor and beyond the length limit.
    pub(crate) fn div(&self, other: &Self) -> Option<Self> {
        if other.is_zero() {
            return None;
        }
        if self.is_zero() {
            return Some(Self::zero());
        }

        let scale = Self::quotient_scale(
            i64::try_from(self.digits).ok()?,
            self.exponent,
            i64::try_from(other.digits).ok()?,
            other.exponent,
        )?;
        let exponent = self
            .exponent
            .checked_sub(other.exponent)?
            .checked_sub(scale)?;

        // A quotient that cannot be within the limit is refused before the numerator is scaled:
        // scaling adds the bits of the power of ten, less one,
        // and dividing takes away at most the divisor's;
        // the quotient's digits, and so its trailing zeros,
        // stay within the scaled numerator's less the divisor's, plus the carry of rounding.
        let scale_magnitude = u64::try_from(scale).ok()?;
        let bits = (self.mantissa.bits() + Self::pow10_bits_at_least(scale_magnitude) - 1)
            .saturating_sub(other.mantissa.bits());
        let trailing_zeros = self.digits + scale_magnitude - other.digits + 1;
        if Self::length_at_least(bits, trailing_zeros, exponent) > Self::LENGTH_LIMIT {
            return None;
        }

        let scaled =
            self.mantissa.magnitude() * BigUint::from(10u32).pow(u32::try_from(scale).ok()?);
        let divisor = other.mantissa.magnitude();
        let (mut quotient, remainder) = scaled.div_rem(divisor);
        if rounds_away(remainder.cmp(&(divisor - &remainder)), quotient.is_odd()) {
            quotient += 1u32;
        }

        let sign = if self.mantissa.sign() == other.mantissa.sign() {
            Sign::Plus
        } else {
            Sign::Minus
        };
        Self::new(BigInt::from_biguint(sign, quotient), exponent)
    }

    /// Returns the power of ten a numerator is scaled by before a rounding division,
    /// given the significant-digit counts and exponents of both operands:
    /// enough for [QUOTIENT_DIGITS](Self::QUOTIENT_DIGITS) significant digits,
    /// and enough for as many fraction digits, or as many as either operand has.
    ///
    /// The integer part is never short.
    pub(crate) fn quotient_scale(
        numerator_digits: i64,
        numerator_exponent: i64,
        denominator_digits: i64,
        denominator_exponent: i64,
    ) -> Option<i64> {
        let significant = Self::QUOTIENT_DIGITS
            .checked_add(denominator_digits)?
            .checked_sub(numerator_digits)?;

        // The quotient's fraction digits are the scale less the exponent difference.
        let fraction = Self::QUOTIENT_DIGITS
            .max(numerator_exponent.checked_neg()?)
            .max(denominator_exponent.checked_neg()?);
        let scaled_fraction = numerator_exponent
            .checked_sub(denominator_exponent)?
            .checked_add(fraction)?;

        Some(significant.max(scaled_fraction).max(0))
    }

    /// Returns the remainder of integer division, with the dividend's sign;
    /// both values must be integral.
    ///
    /// Returns `None` for a zero divisor.
    pub(crate) fn rem(&self, other: &Self) -> Option<Self> {
        debug_assert!(self.is_integral() && other.is_integral());
        if other.is_zero() {
            return None;
        }
        let (left, right, exponent) = self.aligned(other);
        Self::new(left % right, exponent)
    }

    /// Returns the value raised to a non-negative integer power.
    pub(crate) fn pow(&self, power: u32) -> Option<Self> {
        let exponent = self.exponent.checked_mul(i64::from(power))?;
        // A power that cannot be within the limit is refused before it is computed:
        // the mantissa's bits, less one, and its trailing zeros multiply by the power.
        let bits = self
            .mantissa
            .bits()
            .saturating_sub(1)
            .saturating_mul(u64::from(power))
            .saturating_add(1);
        let trailing_zeros = self.trailing_zeros().saturating_mul(u64::from(power));
        if Self::length_at_least(bits, trailing_zeros, exponent) > Self::LENGTH_LIMIT {
            return None;
        }

        Self::new(self.mantissa.pow(power), exponent)
    }

    /// Returns the negated value.
    pub(crate) fn neg(&self) -> Self {
        Self {
            mantissa: -&self.mantissa,
            exponent: self.exponent,
            digits: self.digits,
        }
    }

    /// Returns the absolute value.
    pub(crate) fn abs(&self) -> Self {
        Self {
            mantissa: BigInt::from_biguint(Sign::Plus, self.mantissa.magnitude().clone()),
            exponent: self.exponent,
            digits: self.digits,
        }
    }

    /// Returns the value rounded to the nearest integer, halves toward positive infinity.
    pub(crate) fn round(&self) -> Self {
        let negative = self.mantissa.sign() == Sign::Minus;
        self.to_integral(|remainder, divisor| {
            let doubled = remainder * 2u32;
            if negative {
                &doubled > divisor
            } else {
                &doubled >= divisor
            }
        })
    }

    /// Returns the smallest integer not below the value.
    pub(crate) fn ceil(&self) -> Self {
        let positive = self.mantissa.sign() == Sign::Plus;
        self.to_integral(|remainder, _| positive && remainder.sign() != Sign::NoSign)
    }

    /// Returns the largest integer not above the value.
    pub(crate) fn floor(&self) -> Self {
        let negative = self.mantissa.sign() == Sign::Minus;
        self.to_integral(|remainder, _| negative && remainder.sign() != Sign::NoSign)
    }

    /// Truncates the fraction toward zero,
    /// then moves one away from zero if the given rule says so,
    /// given the dropped fraction as a magnitude and the divisor it is measured against.
    fn to_integral(&self, away: impl FnOnce(&BigInt, &BigInt) -> bool) -> Self {
        if self.is_integral() {
            return self.clone();
        }

        let divisor = BigInt::from(10u32).pow(
            u32::try_from(self.exponent.unsigned_abs())
                .expect("the exponent of a bounded value fits"),
        );
        let magnitude = BigInt::from_biguint(Sign::Plus, self.mantissa.magnitude().clone());
        let mut quotient = &magnitude / &divisor;
        let remainder = &magnitude % &divisor;

        if away(&remainder, &divisor) {
            quotient += 1u32;
        }
        if self.mantissa.sign() == Sign::Minus {
            quotient = -quotient;
        }

        Self::new(quotient, 0).expect("an integral part is shorter than its value")
    }
}

impl Ord for Big {
    fn cmp(&self, other: &Self) -> Ordering {
        // Separate by sign first, then by the position of the leading digit;
        // only values with the same position are aligned,
        // which scales by at most their difference in digits.
        let left_sign = self.mantissa.sign();
        let right_sign = other.mantissa.sign();

        if left_sign != right_sign {
            return sign_rank(left_sign).cmp(&sign_rank(right_sign));
        }
        if left_sign == Sign::NoSign {
            return Ordering::Equal;
        }

        let position = |value: &Self| value.digits.cast_signed().saturating_add(value.exponent);
        let by_position = position(self).cmp(&position(other));

        let by_magnitude = if by_position == Ordering::Equal {
            let (left, right, _) = self.abs().aligned(&other.abs());
            left.cmp(&right)
        } else {
            by_position
        };

        if left_sign == Sign::Minus {
            by_magnitude.reverse()
        } else {
            by_magnitude
        }
    }
}

impl PartialOrd for Big {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Returns whether a truncated quotient moves one away from zero under half-to-even rounding,
/// given the order of the dropped remainder against the rest of the divisor
/// and whether the quotient is odd.
pub(crate) fn rounds_away(remainder_against_rest: Ordering, odd: bool) -> bool {
    match remainder_against_rest {
        Ordering::Greater => true,
        Ordering::Equal => odd,
        Ordering::Less => false,
    }
}

/// Returns the order of the signs: negative, zero, positive.
fn sign_rank(sign: Sign) -> u8 {
    match sign {
        Sign::Minus => 0,
        Sign::NoSign => 1,
        Sign::Plus => 2,
    }
}

impl fmt::Display for Big {
    /// Writes the canonical lexical form:
    /// plain digits for an integer, no trailing zeros otherwise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.mantissa.sign() == Sign::Minus {
            f.write_str("-")?;
        }
        Lexical::write_canonical(f, &self.mantissa.magnitude().to_string(), self.exponent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::{DataValue, NumberRef, ValueRef};

    /// Parses the workspace value of a number lexical.
    fn big(lexical: &str) -> Big {
        let value = DataValue::decimal_from_lexical(lexical).expect("well-formed");
        match value.as_ref() {
            ValueRef::Number(NumberRef::Decimal(value)) => Big::from_decimal(value),
            other => panic!("{other:?} is not a number"),
        }
    }

    #[test]
    fn construction_normalizes() {
        assert_eq!(
            big("123456789012345678901.500").to_string(),
            "123456789012345678901.5"
        );
        assert_eq!(big("-0.000").to_string(), "0");
        assert_eq!(
            big("1000000000000000000000000").to_string(),
            "1000000000000000000000000"
        );
        assert_eq!(
            Big::new(BigInt::from(1500), -2).expect("bounded"),
            Big::from_parts(15, 0).expect("bounded")
        );
        assert_eq!(big("0.5").exponent(), -1);
        assert_eq!(big("5000000000000000000000000").exponent(), 24);
    }

    #[test]
    fn compact_probes_follow_the_ranges() {
        assert_eq!(big("42").to_i64(), Some(42));
        assert_eq!(big("-9223372036854775808").to_i64(), Some(i64::MIN));
        assert_eq!(big("9223372036854775808").to_i64(), None);
        assert_eq!(big("1000000000000000000000").to_i64(), None);
        assert_eq!(big("1.5").to_i64(), None);
        assert_eq!(big("1.5").to_compact(), Compact::new(15, -1));
        assert_eq!(big("42").to_compact(), None);
        assert_eq!(big("1.2345678901234567").to_compact(), None);
        assert_eq!(
            big("0.000000000000000000000000000001").to_compact(),
            Compact::new(1, -30)
        );
    }

    #[test]
    fn digit_counts_are_exact() {
        for digits in [1, 2, 3, 9, 10, 15, 16, 19, 20, 21, 38, 39, 40, 100, 1000] {
            let one = BigUint::from(10u32).pow(digits - 1);
            let nine = &one * 10u32 - 1u32;
            assert_eq!(Big::digit_count(&one), u64::from(digits), "10^{digits}");
            assert_eq!(Big::digit_count(&nine), u64::from(digits), "{digits} nines");
        }
        assert_eq!(Big::digit_count(&BigUint::ZERO), 1);
        assert_eq!(Big::digit_count(&BigUint::from(u64::MAX)), 20);
    }

    #[test]
    fn arithmetic_is_exact() {
        let a = big("123456789012345678901");
        let b = big("0.000000000000000000001");
        assert_eq!(
            a.add(&b).expect("bounded").to_string(),
            "123456789012345678901.000000000000000000001"
        );
        assert_eq!(a.sub(&a).expect("bounded").to_string(), "0");
        assert_eq!(
            a.mul(&a).expect("bounded").to_string(),
            "15241578753238836750437433565526596567801"
        );
        assert_eq!(a.mul(&big("0")).expect("bounded").to_string(), "0");
        assert_eq!(
            big("0.2").mul(&big("0.5")).expect("bounded").to_string(),
            "0.1"
        );
        assert_eq!(
            big("1").div(&big("3")).expect("bounded").to_string(),
            "0.333333333333333333"
        );
        assert_eq!(
            big("-2").div(&big("3")).expect("bounded").to_string(),
            "-0.666666666666666667"
        );
        // The integer part is never short, and the fraction has eighteen digits.
        assert_eq!(
            big("1000000000000000000000000000000")
                .div(&big("3"))
                .expect("bounded")
                .to_string(),
            "333333333333333333333333333333.333333333333333333"
        );
        // Below the fraction digits, the significant digits carry the precision.
        assert_eq!(
            big("0.000000000000000001")
                .div(&big("3"))
                .expect("bounded")
                .to_string(),
            "0.000000000000000000333333333333333333"
        );
        // An operand's fraction digits are kept.
        assert_eq!(
            big("1")
                .div(&big("0.00000000000000000000000000000000000000000000000003"))
                .expect("bounded")
                .to_string(),
            "33333333333333333333333333333333333333333333333333.33333333333333333333333333333333333333333333333333"
        );
        // Halves round to even.
        assert_eq!(
            big("1").div(&big("67108864")).expect("bounded").to_string(),
            "0.0000000149011611938476562"
        );
        assert_eq!(
            big("3").div(&big("67108864")).expect("bounded").to_string(),
            "0.0000000447034835815429688"
        );
        assert_eq!(
            a.div(&big("100")).expect("bounded").to_string(),
            "1234567890123456789.01"
        );
        assert_eq!(
            big("0.5").div(&big("0.25")).expect("bounded").to_string(),
            "2"
        );
        assert!(a.div(&big("0")).is_none());
        assert_eq!(big("-7").rem(&big("3")).expect("bounded").to_string(), "-1");
        assert_eq!(big("2").pow(200).expect("bounded").to_string().len(), 61);
        assert_eq!(big("0.5").pow(3).expect("bounded").to_string(), "0.125");
        assert_eq!(
            big("-123456789012345678901").abs().to_string(),
            "123456789012345678901"
        );
    }

    #[test]
    fn rounding_matches_the_compact_conventions() {
        assert_eq!(
            big("123456789012345678901.5").round().to_string(),
            "123456789012345678902"
        );
        assert_eq!(
            big("-123456789012345678901.5").round().to_string(),
            "-123456789012345678901"
        );
        assert_eq!(
            big("-123456789012345678901.6").round().to_string(),
            "-123456789012345678902"
        );
        assert_eq!(
            big("123456789012345678901.4").round().to_string(),
            "123456789012345678901"
        );
        assert_eq!(
            big("123456789012345678901.1").ceil().to_string(),
            "123456789012345678902"
        );
        assert_eq!(
            big("-123456789012345678901.1").ceil().to_string(),
            "-123456789012345678901"
        );
        assert_eq!(
            big("123456789012345678901.9").floor().to_string(),
            "123456789012345678901"
        );
        assert_eq!(
            big("-123456789012345678901.1").floor().to_string(),
            "-123456789012345678902"
        );
        assert_eq!(big("0.25").round().to_string(), "0");
    }

    #[test]
    fn results_are_bounded() {
        let large = big("10").pow(100_000).expect("within the limit");
        assert_eq!(large.to_string().len(), 100_001);
        assert!(large.pow(20).is_none());
        assert!(large.pow(10).is_none());
        assert!(large.mul(&large.pow(8).expect("bounded")).is_some());
        assert!(Big::from_digits(false, &format!("1{}", "0".repeat(999_999)), 1).is_none());
    }

    #[test]
    fn results_within_the_limit_compute() {
        // The limit bounds the result's own length,
        // not an estimate from the factor's digits.
        let ten = big("10").pow(600_000).expect("within the limit");
        assert_eq!(ten.to_string().len(), 600_001);
        assert!(big("10").pow(1_000_000).is_none());
        let two = big("2").pow(1_500_000).expect("within the limit");
        assert_eq!(two.digits, 451_545);
    }

    #[test]
    fn results_past_the_limit_are_refused_before_they_are_computed() {
        // Each refused result would take far longer to compute than this test does,
        // and far more memory than a test should.
        assert!(big("3").pow(u32::MAX).is_none());
        // A mantissa of 1.7 million bits, more than half of what the limit allows.
        let half = Big::new((BigInt::from(1) << 1_700_000u32) + 1, 0).expect("within the limit");
        assert!(half.mul(&half).is_none());
        // The quotient's mantissa would be ten million bits long.
        let large = big("10").pow(999_999).expect("within the limit");
        let tiny = big(&format!("0.{}1", "0".repeat(999_997)));
        assert!(large.div(&tiny).is_none());
        assert!(Big::new(BigInt::from(1) << 4_000_000u32, 0).is_none());
    }

    #[test]
    fn zeros_beyond_the_fraction_stay_in_the_bound() {
        // A mantissa of three million digits at the exponent -1000000:
        // at most the million fraction digits fold away,
        // and two million characters remain.
        let bits = Big::pow10_bits_at_least(3_000_000);
        assert!(Big::length_at_least(bits, 3_000_000, -1_000_000) > Big::LENGTH_LIMIT);
    }

    #[test]
    fn order_is_numeric() {
        let ordered = [
            "-123456789012345678901",
            "-20",
            "-1.5",
            "-0.000000000000000000000000000001",
            "0",
            "0.000000000000000000000000000001",
            "0.5",
            "0.55",
            "1.5",
            "9",
            "10",
            "123456789012345678901.5",
            "1234567890123456789015",
        ]
        .map(big);
        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[0]), Ordering::Equal);
        }
    }
}
