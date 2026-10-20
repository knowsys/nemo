//! The `i128` workspace of exact arithmetic.

use std::cmp::Ordering;

use crate::value::number::{
    arithmetic::Rounding,
    decimal::{
        DecimalRef, ReprRef,
        workspace::{Big, big::rounds_away},
    },
};

/// An exact number `mantissa * 10^exponent` in the `i128` workspace.
///
/// This is the workspace of the arithmetic over compact operands,
/// integers and compact decimals alike;
/// every operation that would outgrow it says so with `None`,
/// and the caller continues in the [big workspace](Big).
/// A workspace value is not normalized unless [normalized](Self::normalized) was applied.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fixed {
    /// The mantissa.
    mantissa: i128,
    /// The base-10 exponent.
    exponent: i64,
}

impl Fixed {
    /// Creates the value `mantissa * 10^exponent`.
    pub(crate) fn new(mantissa: i128, exponent: i64) -> Self {
        Self { mantissa, exponent }
    }

    /// Creates the workspace form of an exact number of the compact ranges;
    /// `None` for a wide one.
    pub(crate) fn of(value: DecimalRef<'_>) -> Option<Self> {
        match value.0 {
            ReprRef::Int(value) => Some(Self::new(i128::from(value), 0)),
            ReprRef::Compact(value) => Some(Self::new(
                i128::from(value.mantissa()),
                i64::from(value.exponent()),
            )),
            ReprRef::Wide(_) => None,
        }
    }

    /// Returns the mantissa.
    pub(crate) fn mantissa(self) -> i128 {
        self.mantissa
    }

    /// Returns the base-10 exponent.
    pub(crate) fn exponent(self) -> i64 {
        self.exponent
    }

    /// Returns the value with the mantissa's trailing zeros folded into the exponent;
    /// zero is uniquely `(0, 0)`.
    pub(crate) fn normalized(self) -> Self {
        if self.mantissa == 0 {
            return Self::new(0, 0);
        }

        let mut value = self;

        while value.mantissa % 10 == 0 {
            value.mantissa /= 10;
            value.exponent += 1;
        }

        value
    }

    /// Returns the number of decimal digits of the mantissa's magnitude; one for zero.
    pub(crate) fn digits(self) -> i64 {
        i64::from(
            self.mantissa
                .unsigned_abs()
                .checked_ilog10()
                .map_or(1, |log| log + 1),
        )
    }

    /// Returns the mantissas of two values aligned to their smaller exponent, and that exponent,
    /// or `None` if an aligned mantissa outgrows the workspace.
    pub(crate) fn align(self, other: Self) -> Option<(i128, i128, i64)> {
        let exponent = self.exponent.min(other.exponent);
        let scale = |value: Self| {
            let shift = u32::try_from(value.exponent - exponent).ok()?;
            10i128
                .checked_pow(shift)
                .and_then(|factor| value.mantissa.checked_mul(factor))
        };

        Some((scale(self)?, scale(other)?, exponent))
    }

    /// Compares two values of the compact ranges exactly.
    ///
    /// Both mantissas have at most nineteen digits,
    /// so values at the same position of the leading digit align within the workspace.
    pub(crate) fn cmp(self, other: Self) -> Ordering {
        let left_sign = self.mantissa.signum();
        let right_sign = other.mantissa.signum();

        if left_sign != right_sign {
            return left_sign.cmp(&right_sign);
        }
        if left_sign == 0 {
            return Ordering::Equal;
        }

        let position = |value: Self| value.digits() + value.exponent;
        let by_position = position(self).cmp(&position(other));
        let by_magnitude = if by_position == Ordering::Equal {
            let (left, right, _) = self
                .align(other)
                .expect("equal positions align within the workspace");
            left.abs().cmp(&right.abs())
        } else {
            by_position
        };

        if left_sign < 0 {
            by_magnitude.reverse()
        } else {
            by_magnitude
        }
    }

    /// Returns the value taken to an integer by the given rounding,
    /// or `None` if the fraction's power of ten outgrows the workspace.
    ///
    /// The mantissa splits into quotient and remainder at that power,
    /// and the rounding decides whether the quotient moves one away from zero.
    pub(crate) fn to_integral(self, rounding: Rounding) -> Option<i128> {
        if self.exponent >= 0 {
            let factor = 10i128.checked_pow(u32::try_from(self.exponent).ok()?)?;
            return self.mantissa.checked_mul(factor);
        }

        let divisor = 10i128.checked_pow(self.exponent.unsigned_abs().try_into().ok()?)?;
        let negative = self.mantissa < 0;
        let magnitude = self.mantissa.abs();
        let (quotient, remainder) = (magnitude / divisor, magnitude % divisor);

        let away = match rounding {
            // The divisor is a positive power of ten, hence even;
            // a half goes up, which is away from zero only for a positive value.
            Rounding::Nearest if negative => remainder > divisor / 2,
            Rounding::Nearest => remainder >= divisor / 2,
            Rounding::Up => !negative && remainder != 0,
            Rounding::Down => negative && remainder != 0,
        };
        let integral = quotient + i128::from(away);

        Some(if negative { -integral } else { integral })
    }

    /// Returns the quotient of two values, the divisor nonzero,
    /// at the scale and rounding of the [big workspace](Big::div).
    ///
    /// Returns `None` if the scaled numerator or the quotient outgrows the workspace.
    pub(crate) fn quotient(self, other: Self) -> Option<Self> {
        debug_assert!(other.mantissa != 0);
        if self.mantissa == 0 {
            return Some(Self::new(0, 0));
        }

        let scale =
            Big::quotient_scale(self.digits(), self.exponent, other.digits(), other.exponent)?;
        let scaled = 10u128
            .checked_pow(u32::try_from(scale).ok()?)
            .and_then(|factor| self.mantissa.unsigned_abs().checked_mul(factor))?;
        let divisor = other.mantissa.unsigned_abs();
        let (quotient, remainder) = (scaled / divisor, scaled % divisor);
        let rounded = quotient
            + u128::from(rounds_away(
                remainder.cmp(&(divisor - remainder)),
                quotient % 2 == 1,
            ));

        let magnitude = i128::try_from(rounded).ok()?;
        let negative = (self.mantissa < 0) != (other.mantissa < 0);
        Some(Self::new(
            if negative { -magnitude } else { magnitude },
            self.exponent - other.exponent - scale,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_folds_trailing_zeros() {
        let normalized = |mantissa, exponent| {
            let value = Fixed::new(mantissa, exponent).normalized();
            (value.mantissa(), value.exponent())
        };

        assert_eq!(normalized(1500, -3), (15, -1));
        assert_eq!(normalized(-1500, -2), (-15, 0));
        assert_eq!(normalized(0, 5), (0, 0));
        assert_eq!(normalized(7, 0), (7, 0));
    }

    #[test]
    fn alignment_bails_out_of_the_workspace() {
        let (left, right, exponent) = Fixed::new(15, -1).align(Fixed::new(2, 0)).expect("aligns");

        assert_eq!((left, right, exponent), (15, 20, -1));
        assert_eq!(Fixed::new(1, 0).align(Fixed::new(1, -40)), None);
    }

    #[test]
    fn comparison_is_exact() {
        let cmp = |l: (i128, i64), r: (i128, i64)| Fixed::new(l.0, l.1).cmp(Fixed::new(r.0, r.1));

        assert_eq!(cmp((15, -1), (2, 0)), Ordering::Less);
        assert_eq!(
            cmp((20_000_000_000_000_001, -16), (2, 0)),
            Ordering::Greater
        );
        assert_eq!(cmp((-15, -1), (-2, 0)), Ordering::Greater);
        assert_eq!(cmp((0, 0), (0, 3)), Ordering::Equal);
        assert_eq!(cmp((i128::from(i64::MIN), 0), (-1, -240)), Ordering::Less);
    }

    #[test]
    fn rounding_and_division_work_in_the_workspace() {
        assert_eq!(Fixed::new(25, -1).to_integral(Rounding::Nearest), Some(3));
        assert_eq!(Fixed::new(-25, -1).to_integral(Rounding::Nearest), Some(-2));
        assert_eq!(Fixed::new(-26, -1).to_integral(Rounding::Nearest), Some(-3));
        assert_eq!(Fixed::new(-25, -1).to_integral(Rounding::Up), Some(-2));
        assert_eq!(Fixed::new(25, -1).to_integral(Rounding::Down), Some(2));
        assert_eq!(Fixed::new(5, 2).to_integral(Rounding::Down), Some(500));
        assert_eq!(Fixed::new(1, -40).to_integral(Rounding::Up), None);

        let quotient = |left: (i128, i64), right: (i128, i64)| {
            let value = Fixed::new(left.0, left.1)
                .quotient(Fixed::new(right.0, right.1))
                .expect("fits");
            (value.mantissa(), value.exponent())
        };

        assert_eq!(quotient((1, 0), (3, 0)), (333_333_333_333_333_333, -18));
        assert_eq!(quotient((2, 0), (3, 0)), (666_666_666_666_666_667, -18));
        assert_eq!(quotient((-2, 0), (3, 0)), (-666_666_666_666_666_667, -18));
        assert_eq!(quotient((7, 0), (-2, 0)), (-3_500_000_000_000_000_000, -18));
        // Halves round to even: 1 / 2^26 and 3 / 2^26 both end in ...5 past the scale.
        assert_eq!(
            quotient((1, 0), (1 << 26, 0)),
            (149_011_611_938_476_562, -25)
        );
        assert_eq!(
            quotient((3, 0), (1 << 26, 0)),
            (447_034_835_815_429_688, -25)
        );
        // The integer part is never short, and the fraction has eighteen digits.
        assert_eq!(
            quotient((10_000_000_000_000_000_000, 0), (3, 0)),
            (3_333_333_333_333_333_333_333_333_333_333_333_333, -18)
        );
        assert!(
            Fixed::new(i128::from(i64::MAX), 0)
                .quotient(Fixed::new(1, -240))
                .is_none()
        );
    }
}
