//! Decimals within the compact range that are not integral.

use std::{cmp::Ordering, fmt, ops::Range};

use crate::value::number::{decimal::workspace, lexical::Lexical};

/// An [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value
/// `mantissa * 10^exponent` that is not integral and lies within the compact range.
///
/// The *compact range* contains the values with at most
/// [SIGNIFICANT_DIGITS](Self::SIGNIFICANT_DIGITS) significant digits
/// and a magnitude of at least `10^-241`.
/// The mantissa is never divisible by ten, so a value has exactly one mantissa and exponent.
/// An integral value is never a compact decimal but an integer,
/// and a decimal beyond the compact range is a [WideRef](super::WideRef).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Compact {
    /// The mantissa, which is never zero and never divisible by ten.
    mantissa: i64,
    /// The base-10 exponent, which is always negative.
    exponent: i32,
}

impl Compact {
    /// The largest number of significant digits of a compact decimal.
    pub(crate) const SIGNIFICANT_DIGITS: u32 = 16;

    /// Creates the normalized decimal `mantissa * 10^exponent`,
    /// or `None` for an integral value or a value beyond the compact range.
    pub(crate) fn new(mantissa: i64, exponent: i32) -> Option<Self> {
        let normalized =
            workspace::Fixed::new(i128::from(mantissa), i64::from(exponent)).normalized();

        if normalized.exponent() >= 0 {
            return None;
        }

        let value = Self {
            mantissa: i64::try_from(normalized.mantissa()).expect("normalization only shortens"),
            exponent: i32::try_from(normalized.exponent()).ok()?,
        };

        if value.digits() > Self::SIGNIFICANT_DIGITS {
            return None;
        }

        let scaled =
            i64::from(value.exponent) - i64::from(Self::SIGNIFICANT_DIGITS - value.digits());
        let range = i64::from(Self::SCALED_EXPONENT_RANGE.start)
            ..i64::from(Self::SCALED_EXPONENT_RANGE.end);

        range.contains(&scaled).then_some(value)
    }

    /// Returns the normalized mantissa.
    pub(crate) fn mantissa(self) -> i64 {
        self.mantissa
    }

    /// Returns the normalized base-10 exponent.
    pub(crate) fn exponent(self) -> i32 {
        self.exponent
    }

    /// Returns the nearest double, or an infinity beyond the double range.
    pub(crate) fn to_f64(self) -> f64 {
        /// The powers of ten that a double represents exactly.
        const EXACT_POWERS: [f64; 23] = [
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
            1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
        ];
        if self.mantissa.unsigned_abs() < 1 << f64::MANTISSA_DIGITS
            && let Ok(power) = usize::try_from(self.exponent.unsigned_abs())
            && let Some(&power) = EXACT_POWERS.get(power)
        {
            return self.mantissa as f64 / power;
        }

        Lexical::nearest(
            self.mantissa < 0,
            self.mantissa.unsigned_abs(),
            i64::from(self.exponent),
        )
    }

    /// Returns the nearest float.
    pub(crate) fn to_f32(self) -> f32 {
        /// The powers of ten that a float represents exactly.
        const EXACT_POWERS: [f32; 11] = [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];
        if self.mantissa.unsigned_abs() < 1 << f32::MANTISSA_DIGITS
            && let Ok(power) = usize::try_from(self.exponent.unsigned_abs())
            && let Some(&power) = EXACT_POWERS.get(power)
        {
            return self.mantissa as f32 / power;
        }

        Lexical::nearest(
            self.mantissa < 0,
            self.mantissa.unsigned_abs(),
            i64::from(self.exponent),
        )
    }
}

impl Compact {
    /// The range of the [scaled exponent](Self::scaled_exponent),
    /// which the storage payload stores in nine bits.
    pub(crate) const SCALED_EXPONENT_RANGE: Range<i32> = -256..256;

    /// Returns the number of significant digits of the mantissa.
    pub(crate) fn digits(self) -> u32 {
        self.mantissa.unsigned_abs().ilog10() + 1
    }

    /// Returns the exponent of the value when its mantissa is written with exactly
    /// [SIGNIFICANT_DIGITS](Self::SIGNIFICANT_DIGITS) digits.
    ///
    /// The storage payload stores the value in this form.
    pub(crate) fn scaled_exponent(self) -> i32 {
        self.exponent - (Self::SIGNIFICANT_DIGITS - self.digits()).cast_signed()
    }

    /// Creates the decimal of a storage payload,
    /// whose mantissa and exponent are normalized and within the compact range.
    pub(crate) fn from_encoded(mantissa: i64, exponent: i32) -> Self {
        debug_assert!(mantissa % 10 != 0 && exponent < 0);
        Self { mantissa, exponent }
    }
}

impl Ord for Compact {
    fn cmp(&self, other: &Self) -> Ordering {
        workspace::Fixed::new(i128::from(self.mantissa), i64::from(self.exponent)).cmp(
            workspace::Fixed::new(i128::from(other.mantissa), i64::from(other.exponent)),
        )
    }
}

impl PartialOrd for Compact {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Compact {
    /// Writes the canonical `xsd:decimal` lexical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.mantissa < 0 {
            f.write_str("-")?;
        }
        Lexical::write_canonical(
            f,
            &self.mantissa.unsigned_abs().to_string(),
            i64::from(self.exponent),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::{DataValue, DecimalRef, NumberRef, ValueRef, number::decimal::ReprRef};

    /// Returns the value of a lexical form, which must denote a decimal that is not integral.
    fn decimal(lexical: &str) -> DataValue {
        let value = DataValue::decimal_from_lexical(lexical).expect("well-formed");
        assert!(
            matches!(value.as_ref(), ValueRef::Number(number) if !number.is_integral()),
            "{lexical} is not a decimal"
        );
        value
    }

    /// Returns the compact decimal of a lexical form, which must denote one.
    fn compact(lexical: &str) -> Compact {
        match decimal(lexical).as_ref() {
            ValueRef::Number(NumberRef::Decimal(DecimalRef(ReprRef::Compact(value)))) => value,
            _ => panic!("{lexical} is wide"),
        }
    }

    /// Creating a compact decimal normalizes it,
    /// and rejects integral values and values beyond the compact range.
    #[test]
    fn construction_normalizes_and_bounds() {
        assert_eq!(Compact::new(1500, -3), Compact::new(15, -1));
        assert_eq!(Compact::new(-1500, -3), Compact::new(-15, -1));

        // Integral values are integers, not decimals.
        assert_eq!(Compact::new(1500, -2), None);
        assert_eq!(Compact::new(0, 0), None);
        assert_eq!(Compact::new(42, 0), None);

        // Values beyond the compact range have too many digits or an extreme exponent.
        assert_eq!(Compact::new(12_345_678_901_234_567, -1), None);
        assert!(Compact::new(1_234_567_890_123_456, -1).is_some());
        assert_eq!(Compact::new(1, -1000), None);
        assert_eq!(Compact::new(1, i32::MIN), None);

        // The range of the scaled exponent:
        // scaling one digit to sixteen digits lowers the exponent by fifteen.
        assert!(Compact::new(1, -241).is_some());
        assert_eq!(Compact::new(1, -242), None);
        assert!(Compact::new(9_999_999_999_999_999, -256).is_some());
        assert_eq!(Compact::new(9_999_999_999_999_999, -257), None);

        let value = Compact::new(1500, -3).expect("representable");
        assert_eq!((value.mantissa(), value.exponent()), (15, -1));
    }

    /// A compact decimal converts to the nearest double.
    #[test]
    fn doubles_are_the_nearest() {
        let to_f64 = |mantissa, exponent| {
            Compact::new(mantissa, exponent)
                .expect("representable")
                .to_f64()
        };

        // Within the range of exact powers, the division rounds once, as the literal does.
        assert_eq!(to_f64(3, -1), 0.3);
        assert_eq!(to_f64(7, -1), 0.7);
        assert_eq!(to_f64(123, -2), 1.23);
        assert_eq!(to_f64(-1, -22), -1e-22);

        // Beyond that range, the value is converted through its lexical form.
        assert_eq!(to_f64(1, -23), 1e-23);
        assert_eq!(
            to_f64(1_234_567_890_123_456, -15),
            "1.234567890123456".parse().expect("float literal")
        );
        assert_eq!(to_f64(1, -241), 1e-241);

        let wide = |lexical: &str| match decimal(lexical).as_ref() {
            ValueRef::Number(value) => value.to_f64(),
            _ => unreachable!("a number"),
        };
        assert_eq!(wide("0.00000000000000000000000000000000000001"), 1e-38);
        assert_eq!(wide("1234567890123456789.5"), 1.2345678901234568e18);
    }

    /// A compact decimal converts to the nearest float.
    #[test]
    fn floats_are_the_nearest() {
        let to_f32 = |mantissa, exponent| {
            Compact::new(mantissa, exponent)
                .expect("representable")
                .to_f32()
        };

        // Within the range of exact powers, the division rounds once, as the literal does.
        assert_eq!(to_f32(3, -1), 0.3);
        assert_eq!(to_f32(123, -2), 1.23);
        assert_eq!(to_f32(-1, -10), -1e-10);

        // Beyond that range, the value is converted through its lexical form.
        assert_eq!(to_f32(1, -11), 1e-11);
        assert_eq!(to_f32(1, -241), 0.0);

        // Just above the midpoint between two floats, where the nearest double lies.
        // Rounding through that double would round the tie down to the even float.
        assert_eq!(to_f32(4_686_748_743_057_251, -15), 4.686_749);
    }

    /// Decimals are ordered by value, across the compact and wide representations.
    #[test]
    fn decimals_order_by_value() {
        let ordered = [
            decimal("-123456789012345678901.5"),
            decimal("-5.1"),
            decimal("-5.0000000000000001"),
            decimal("-0.5"),
            decimal("-0.0000000000000000000000000000000000000000000000000000000000000000000000001"),
            decimal("0.0000000000000000000000000000000000000000000000000000000000000000000000001"),
            decimal("0.5"),
            decimal("0.55"),
            decimal("4.9"),
            decimal("5.0000000000000001"),
            decimal("5.1"),
            decimal("123456789012345678901.5"),
            decimal("1234567890123456789012.5"),
        ];

        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
            assert!(window[1] > window[0]);
            assert_eq!(window[0].cmp(&window[0]), Ordering::Equal);
        }

        assert!(compact("0.5") < compact("0.55"));
        assert!(compact("-0.5") > compact("-0.55"));
    }

    /// Decimals are written in canonical form, without redundant zeros or a `+` sign.
    #[test]
    fn decimals_display_in_canonical_form() {
        for (lexical, canonical) in [
            ("1.5", "1.5"),
            ("+1.50", "1.5"),
            ("-00.0030", "-0.003"),
            (".5", "0.5"),
            ("123.45", "123.45"),
            ("123456789012345678901.500", "123456789012345678901.5"),
            (
                "-0.000000000000000000000000000000001",
                "-0.000000000000000000000000000000001",
            ),
        ] {
            assert_eq!(decimal(lexical).to_string(), canonical, "{lexical}");
        }

        assert_eq!(compact("0.0003").to_string(), "0.0003");
    }

    /// Each decimal has exactly one representation, chosen by its digits and exponent.
    #[test]
    fn the_cases_are_disjoint() {
        // Sixteen digits fit into the compact representation, but seventeen do not,
        // and a value with an exponent beyond the compact range is wide at any length.
        assert!(matches!(
            decimal("1.234567890123456").as_ref(),
            ValueRef::Number(NumberRef::Decimal(DecimalRef(ReprRef::Compact(_))))
        ));
        assert!(matches!(
            decimal("1.2345678901234567").as_ref(),
            ValueRef::Number(NumberRef::Decimal(DecimalRef(ReprRef::Wide(_))))
        ));
        assert!(matches!(
            decimal("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000015").as_ref(),
            ValueRef::Number(NumberRef::Decimal(DecimalRef(ReprRef::Wide(_))))
        ));
        assert_eq!(decimal("1.50"), decimal("1.5"));
        assert_ne!(decimal("1.5"), decimal("1.50000000000000001"));
    }
}
