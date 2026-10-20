//! Lengths of time in months and seconds.

use std::{
    cmp::Ordering,
    fmt,
    ops::{Div, Mul},
};

use num_bigint::{BigInt, Sign};
use num_integer::Integer;

use oxsdatatypes::{
    DayTimeDuration as LibraryDayTimeDuration, Duration as LibraryDuration,
    YearMonthDuration as LibraryYearMonthDuration,
};

use crate::value::{
    DataValue, EvaluationError, NumberRef, ValueDomain,
    number::decimal::workspace::Big,
    temporal::{TimezoneOffset, instant::ATTOSECONDS_PER_SECOND},
};

/// An [`xsd:duration`](https://www.w3.org/TR/xmlschema11-2/#duration) value:
/// a number of months and a number of seconds, both of one sign.
///
/// The values of
/// [`xsd:dayTimeDuration`](https://www.w3.org/TR/xmlschema11-2/#dayTimeDuration)
/// and [`xsd:yearMonthDuration`](https://www.w3.org/TR/xmlschema11-2/#yearMonthDuration)
/// are the durations without months and the durations without seconds.
/// [is_day_time](Self::is_day_time) and [is_year_month](Self::is_year_month) tell them apart,
/// and [domain](Self::domain) names the most specific of the three.
///
/// Equality, order and hashing are term identity, which is also XSD's value equality,
/// as `P1Y` and `P12M` are one value.
/// The order is by domain, then by months, then by seconds.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(Rust, packed(8))]
pub struct Duration {
    /// The months.
    months: i64,
    /// The seconds, in attoseconds.
    attoseconds: i128,
}

/// The value takes 24 bytes at 8-byte alignment.
const _: () = assert!(size_of::<Duration>() == 24 && align_of::<Duration>() == 8);

impl Duration {
    /// Creates the duration of months and attoseconds of the same sign,
    /// or `None` if the signs differ.
    pub fn new(months: i64, attoseconds: i128) -> Option<Self> {
        let agree = (months >= 0 && attoseconds >= 0) || (months <= 0 && attoseconds <= 0);
        agree.then_some(Self {
            months,
            attoseconds,
        })
    }

    /// Creates the duration of a number of months.
    pub fn from_months(months: i64) -> Self {
        Self {
            months,
            attoseconds: 0,
        }
    }

    /// Creates the duration of a number of seconds.
    pub fn from_seconds(seconds: i64) -> Self {
        Self::day_time(i128::from(seconds) * ATTOSECONDS_PER_SECOND)
    }

    /// Creates the duration of a number of attoseconds and no months.
    pub(super) fn day_time(attoseconds: i128) -> Self {
        Self {
            months: 0,
            attoseconds,
        }
    }

    /// Parses an `xsd:duration`, `xsd:dayTimeDuration` or `xsd:yearMonthDuration` lexical form,
    /// or `None` for malformed input and for a value beyond the value space.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::Duration;
    ///
    /// let value = Duration::from_lexical("-P1Y2M3DT4H5M6.5S").unwrap();
    /// assert_eq!((value.years(), value.months(), value.days()), (-1, -2, -3));
    /// assert_eq!((value.hours(), value.minutes(), value.seconds()), (-4, -5, -6));
    /// assert_eq!(value.attosecond(), -500_000_000_000_000_000);
    /// assert!(value.is_negative());
    ///
    /// assert_eq!(Duration::from_lexical("P1Y"), Duration::from_lexical("P12M"));
    /// assert!(Duration::from_lexical("PT36H").unwrap().is_day_time());
    /// ```
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        lexical
            .parse::<LibraryDuration>()
            .ok()
            .map(Self::from_library)
    }

    /// Parses an `xsd:dayTimeDuration` lexical form,
    /// or `None` for malformed input, for a lexical with years or months,
    /// and for a value beyond the value space.
    pub fn from_day_time_lexical(lexical: &str) -> Option<Self> {
        lexical
            .parse::<LibraryDayTimeDuration>()
            .ok()
            .map(|value| Self::from_library(value.into()))
    }

    /// Parses an `xsd:yearMonthDuration` lexical form,
    /// or `None` for malformed input, for a lexical with days or a time,
    /// and for a value beyond the value space.
    pub fn from_year_month_lexical(lexical: &str) -> Option<Self> {
        lexical
            .parse::<LibraryYearMonthDuration>()
            .ok()
            .map(|value| Self::from_library(value.into()))
    }

    /// Creates the duration of an `oxsdatatypes` duration.
    fn from_library(value: LibraryDuration) -> Self {
        let bytes = value.to_be_bytes();
        Self {
            months: i64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
            attoseconds: i128::from_be_bytes(bytes[8..].try_into().expect("sixteen bytes")),
        }
    }

    /// Converts the duration into the `oxsdatatypes` duration.
    pub(super) fn to_library(self) -> LibraryDuration {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.months.to_be_bytes());
        bytes[8..].copy_from_slice(&self.attoseconds.to_be_bytes());
        LibraryDuration::from_be_bytes(bytes)
    }

    /// Returns the whole years of the months, `fn:years-from-duration`.
    pub fn years(self) -> i64 {
        self.months / 12
    }

    /// Returns the months beyond the whole years, `fn:months-from-duration`.
    pub fn months(self) -> i64 {
        self.months % 12
    }

    /// Returns the whole days of the seconds, `fn:days-from-duration`.
    pub fn days(self) -> i64 {
        i64::try_from(self.whole_seconds() / 86_400).expect("fewer days than seconds")
    }

    /// Returns the hours beyond the whole days, `fn:hours-from-duration`.
    pub fn hours(self) -> i64 {
        i64::try_from(self.whole_seconds() % 86_400 / 3_600).expect("below 24")
    }

    /// Returns the minutes beyond the whole hours, `fn:minutes-from-duration`.
    pub fn minutes(self) -> i64 {
        i64::try_from(self.whole_seconds() % 3_600 / 60).expect("below 60")
    }

    /// Returns the whole seconds beyond the whole minutes.
    /// With [attosecond](Self::attosecond), they give `fn:seconds-from-duration`.
    pub fn seconds(self) -> i64 {
        i64::try_from(self.whole_seconds() % 60).expect("below 60")
    }

    /// Returns the fraction of the second in attoseconds, with the sign of the duration.
    pub fn attosecond(self) -> i64 {
        i64::try_from(self.attoseconds % ATTOSECONDS_PER_SECOND).expect("below 10^18")
    }

    /// Returns the seconds, truncated toward zero.
    fn whole_seconds(self) -> i128 {
        self.attoseconds / ATTOSECONDS_PER_SECOND
    }

    /// Returns the months.
    pub fn total_months(self) -> i64 {
        self.months
    }

    /// Returns the seconds, in attoseconds.
    pub fn total_attoseconds(self) -> i128 {
        self.attoseconds
    }

    /// Returns whether the duration runs backwards.
    pub fn is_negative(self) -> bool {
        self.months < 0 || self.attoseconds < 0
    }

    /// Returns whether the duration has no months, so that it is an `xsd:dayTimeDuration` value.
    pub fn is_day_time(self) -> bool {
        self.months == 0
    }

    /// Returns whether the duration has no seconds, so that it is an `xsd:yearMonthDuration` value.
    pub fn is_year_month(self) -> bool {
        self.attoseconds == 0
    }

    /// Returns the domain of the value, the most specific of the three duration types:
    /// a duration without months is a dayTimeDuration, the zero duration included,
    /// one without seconds a yearMonthDuration, and one with both a duration.
    pub fn domain(self) -> ValueDomain {
        if self.is_day_time() {
            ValueDomain::DayTimeDuration
        } else if self.is_year_month() {
            ValueDomain::YearMonthDuration
        } else {
            ValueDomain::Duration
        }
    }

    /// Returns whether two durations share a kind:
    /// both without months, or both without seconds,
    /// the zero duration being of either kind.
    ///
    /// XPath orders, adds and divides durations only within one kind.
    pub fn shares_kind(self, other: Self) -> bool {
        (self.is_day_time() && other.is_day_time())
            || (self.is_year_month() && other.is_year_month())
    }

    /// Returns the sum of two durations,
    /// `op:add-yearMonthDurations` and `op:add-dayTimeDurations`,
    /// or `None` if it overflows or its months and seconds would differ in sign.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Self::new(
            self.months.checked_add(other.months)?,
            self.attoseconds.checked_add(other.attoseconds)?,
        )
    }

    /// Returns the difference of two durations,
    /// `op:subtract-yearMonthDurations` and `op:subtract-dayTimeDurations`,
    /// or `None` if it overflows or its months and seconds would differ in sign.
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Self::new(
            self.months.checked_sub(other.months)?,
            self.attoseconds.checked_sub(other.attoseconds)?,
        )
    }

    /// Returns the duration of the same length running the other way,
    /// or `None` if it overflows.
    pub fn checked_neg(self) -> Option<Self> {
        Self::new(self.months.checked_neg()?, self.attoseconds.checked_neg()?)
    }

    /// Returns the order of two durations in XSD's value space,
    /// or `None` if it depends on the calendar,
    /// as it does for `P1M` and `P30D`.
    ///
    /// Durations of one kind compare by their months or seconds alone;
    /// the others by XSD's reference dates, which can overflow for huge durations.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        if self.shares_kind(other) {
            let (months, attoseconds) = (self.months, self.attoseconds);
            return Some((months, attoseconds).cmp(&(other.months, other.attoseconds)));
        }
        self.to_library().partial_cmp(&other.to_library())
    }
}

/// The hooks of the compact representations.
impl Duration {
    /// The attoseconds of a millisecond.
    const ATTOSECONDS_PER_MILLISECOND: i128 = 10i128.pow(15);

    /// Creates the duration of months and milliseconds of the same sign.
    pub(crate) fn from_millis(months: i64, millis: i64) -> Self {
        Self::new(
            months,
            i128::from(millis) * Self::ATTOSECONDS_PER_MILLISECOND,
        )
        .expect("the compact fields share the sign of the duration they came from")
    }

    /// Returns the seconds in milliseconds,
    /// or `None` if they are finer than a millisecond or beyond the 64-bit range.
    pub(crate) fn millis(self) -> Option<i64> {
        let attoseconds = self.attoseconds;
        (attoseconds % Self::ATTOSECONDS_PER_MILLISECOND == 0)
            .then(|| i64::try_from(attoseconds / Self::ATTOSECONDS_PER_MILLISECOND).ok())
            .flatten()
    }
}

impl Ord for Duration {
    fn cmp(&self, other: &Self) -> Ordering {
        let (months, attoseconds) = (self.months, self.attoseconds);
        let (other_months, other_attoseconds) = (other.months, other.attoseconds);
        self.domain()
            .cmp(&other.domain())
            .then(months.cmp(&other_months))
            .then(attoseconds.cmp(&other_attoseconds))
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The arithmetic with numbers.
impl Duration {
    /// Returns the duration with its one component mapped:
    /// the seconds of a dayTimeDuration or the months of a yearMonthDuration.
    ///
    /// Fails with [TypeMismatch](EvaluationError::TypeMismatch)
    /// for a duration with both months and seconds,
    /// and with [Overflow](EvaluationError::Overflow) for a result beyond the value space.
    fn map_kind(self, map: impl FnOnce(BigInt) -> BigInt) -> Result<Self, EvaluationError> {
        let overflow = |_| EvaluationError::Overflow;
        match self.domain() {
            ValueDomain::DayTimeDuration => {
                let attoseconds = i128::try_from(map(BigInt::from(self.attoseconds)));
                Ok(Self::day_time(attoseconds.map_err(overflow)?))
            }
            ValueDomain::YearMonthDuration => {
                let months = i64::try_from(map(BigInt::from(self.months)));
                Ok(Self::from_months(months.map_err(overflow)?))
            }
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl Mul<NumberRef<'_>> for Duration {
    type Output = Result<Duration, EvaluationError>;

    /// Returns the duration multiplied by a number,
    /// `op:multiply-yearMonthDuration` and `op:multiply-dayTimeDuration`.
    ///
    /// The product is exact, then rounded to the month or to the attosecond,
    /// halves toward positive infinity, as `fn:round` rounds.
    /// A float or double factor counts as the shortest decimal that reads back as it.
    ///
    /// Fails with [TypeMismatch](EvaluationError::TypeMismatch)
    /// for a duration with both months and seconds,
    /// with [Malformed](EvaluationError::Malformed) for a NaN factor,
    /// and with [Overflow](EvaluationError::Overflow)
    /// for an infinite factor or a product beyond the value space.
    fn mul(self, factor: NumberRef<'_>) -> Self::Output {
        let (mantissa, exponent) = exact(factor)?;
        self.map_kind(|value| scaled_product(value, &mantissa, exponent))
    }
}

impl Div<NumberRef<'_>> for Duration {
    type Output = Result<Duration, EvaluationError>;

    /// Returns the duration divided by a number,
    /// `op:divide-yearMonthDuration` and `op:divide-dayTimeDuration`.
    ///
    /// The quotient is exact, then rounded as the [product](Self::mul) is;
    /// an infinite divisor gives the zero duration.
    /// Fails as the product does,
    /// and with [DivisionByZero](EvaluationError::DivisionByZero) for a zero divisor.
    fn div(self, divisor: NumberRef<'_>) -> Self::Output {
        if is_infinite(divisor) {
            return self.map_kind(|_| BigInt::ZERO);
        }
        let (mantissa, exponent) = exact(divisor)?;
        if mantissa.sign() == Sign::NoSign {
            return Err(EvaluationError::DivisionByZero);
        }
        self.map_kind(|value| scaled_quotient(value, &mantissa, exponent))
    }
}

impl Div for Duration {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the ratio of two durations of one kind, a decimal,
    /// `op:divide-yearMonthDuration-by-yearMonthDuration`
    /// and `op:divide-dayTimeDuration-by-dayTimeDuration`.
    ///
    /// The ratio divides as exact numbers do (see [NumberRef]).
    /// Fails with [TypeMismatch](EvaluationError::TypeMismatch)
    /// for durations that [share no kind](Self::shares_kind),
    /// and with [DivisionByZero](EvaluationError::DivisionByZero) for a zero divisor.
    fn div(self, other: Self) -> Self::Output {
        if !self.shares_kind(other) {
            return Err(EvaluationError::TypeMismatch);
        }
        let (numerator, denominator) = if self.is_year_month() && other.is_year_month() {
            (i128::from(self.months), i128::from(other.months))
        } else {
            (self.attoseconds, other.attoseconds)
        };

        let number =
            |value| DataValue::decimal(value, 0).expect("an i128 is within the length limit");
        &number(numerator) / &number(denominator)
    }
}

impl From<TimezoneOffset> for Duration {
    /// Converts the offset into the duration east of UTC.
    fn from(offset: TimezoneOffset) -> Self {
        Self::from_seconds(i64::from(offset.minutes()) * 60)
    }
}

impl fmt::Debug for Duration {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Duration {
    /// Writes the canonical `xsd:duration` lexical form; `PT0S` for the zero duration.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_library())
    }
}

/// Returns the exact value of a number as `mantissa * 10^exponent`.
///
/// A float or double counts as the shortest decimal that reads back as it.
/// Fails with [Malformed](EvaluationError::Malformed) for a NaN
/// and with [Overflow](EvaluationError::Overflow) for an infinity.
fn exact(number: NumberRef<'_>) -> Result<(BigInt, i64), EvaluationError> {
    if number.is_nan() {
        return Err(EvaluationError::Malformed);
    }
    if is_infinite(number) {
        return Err(EvaluationError::Overflow);
    }

    let scientific = match number {
        NumberRef::Float(value) => format!("{:e}", value.get()),
        NumberRef::Double(value) => format!("{:e}", value.get()),
        NumberRef::Decimal(exact) => {
            let value = Big::from_decimal(exact);
            return Ok((value.mantissa().clone(), value.exponent()));
        }
    };

    // The shortest digits in scientific notation: `-1.25e3` is `-125 * 10^1`.
    let (significand, exponent) = scientific
        .split_once('e')
        .expect("the notation has an exponent");
    let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    let mantissa = format!("{whole}{fraction}")
        .parse()
        .expect("the significand is digits");
    let exponent = exponent.parse::<i64>().expect("the exponent is an integer");
    let fraction = i64::try_from(fraction.len()).expect("a float has few digits");

    Ok((mantissa, exponent - fraction))
}

/// Returns whether a number is a float or double infinity.
fn is_infinite(number: NumberRef<'_>) -> bool {
    match number {
        NumberRef::Float(value) => value.get().is_infinite(),
        NumberRef::Double(value) => value.get().is_infinite(),
        _ => false,
    }
}

/// Returns an integer multiplied by `mantissa * 10^exponent`,
/// rounded to an integer as [rounded] rounds.
fn scaled_product(value: BigInt, mantissa: &BigInt, exponent: i64) -> BigInt {
    let product = value * mantissa;
    match u64::try_from(exponent) {
        Ok(exponent) => product * power_of_ten(exponent),
        Err(_) => rounded(product, power_of_ten(exponent.unsigned_abs())),
    }
}

/// Returns an integer divided by a nonzero `mantissa * 10^exponent`,
/// rounded to an integer as [rounded] rounds.
fn scaled_quotient(value: BigInt, mantissa: &BigInt, exponent: i64) -> BigInt {
    match u64::try_from(exponent) {
        Ok(exponent) => rounded(value, mantissa * power_of_ten(exponent)),
        Err(_) => rounded(
            value * power_of_ten(exponent.unsigned_abs()),
            mantissa.clone(),
        ),
    }
}

/// Returns ten to a power.
fn power_of_ten(exponent: u64) -> BigInt {
    BigInt::from(10u32).pow(u32::try_from(exponent).expect("the exponent of a bounded number fits"))
}

/// Returns the quotient of two integers rounded to an integer,
/// halves toward positive infinity, as `fn:round` rounds.
fn rounded(numerator: BigInt, denominator: BigInt) -> BigInt {
    let (numerator, denominator) = if denominator.sign() == Sign::Minus {
        (-numerator, -denominator)
    } else {
        (numerator, denominator)
    };

    // The floor of `n / d + 1/2`.
    (numerator * 2u32 + &denominator).div_floor(&(denominator * 2u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses the duration of a lexical; panics on malformed input.
    fn duration(lexical: &str) -> Duration {
        Duration::from_lexical(lexical).expect(lexical)
    }

    #[test]
    fn lexicals_parse_and_print_canonically() {
        for lexical in [
            "P1Y",
            "P1M",
            "P1Y2M",
            "P1D",
            "PT1H",
            "PT1M",
            "PT1S",
            "PT0.5S",
            "PT0.000000000000000001S",
            "P1Y2M3DT4H5M6.5S",
            "-P1Y2M3DT4H5M6.5S",
            "-PT23H59M59S",
            "PT0S",
        ] {
            assert_eq!(duration(lexical).to_string(), lexical);
            assert_eq!(format!("{:?}", duration(lexical)), lexical);
        }
        assert_eq!(duration("P1Y"), duration("P12M"));
        assert_eq!(duration("PT24H"), duration("P1D"));
        assert_eq!(duration("PT36H").to_string(), "P1DT12H");
        assert_eq!(duration("P0D").to_string(), "PT0S");
        assert_eq!(duration("-P0M").to_string(), "PT0S");
        assert_eq!(duration("PT1.500S").to_string(), "PT1.5S");
        assert_eq!(duration("P1Y").total_months(), 12);
        assert_eq!(
            duration("PT1.5S").total_attoseconds(),
            3 * ATTOSECONDS_PER_SECOND / 2
        );
    }

    #[test]
    fn malformed_lexicals_are_refused() {
        for lexical in [
            "", "P", "PT", "P1.5Y", "P1.5D", "P1Y-1D", "1D", "P1S", "PT1D", " P1D",
        ] {
            assert_eq!(Duration::from_lexical(lexical), None, "{lexical}");
        }
        assert_eq!(Duration::from_lexical("P9223372036854775807Y"), None);
    }

    #[test]
    fn components_agree_in_sign() {
        assert_eq!(Duration::new(1, -1), None);
        assert_eq!(Duration::new(-1, 1), None);
        assert_eq!(
            Duration::new(0, -1),
            Some(duration("-PT0.000000000000000001S"))
        );
        assert_eq!(Duration::new(-1, 0), Some(duration("-P1M")));
        assert_eq!(Duration::new(0, 0), Some(duration("PT0S")));
        assert_eq!(Duration::from_months(14), duration("P1Y2M"));
        assert_eq!(Duration::from_seconds(-90_061), duration("-P1DT1H1M1S"));
    }

    #[test]
    fn accessors_read_the_components() {
        let value = duration("-P1Y2M3DT4H5M6.5S");
        assert_eq!((value.years(), value.months(), value.days()), (-1, -2, -3));
        assert_eq!(
            (value.hours(), value.minutes(), value.seconds()),
            (-4, -5, -6)
        );
        assert_eq!(i128::from(value.attosecond()), -ATTOSECONDS_PER_SECOND / 2);
        assert_eq!(
            (value.total_months(), value.total_attoseconds()),
            (
                -14,
                -(3 * 86_400 + 4 * 3_600 + 5 * 60 + 6) * ATTOSECONDS_PER_SECOND
                    - ATTOSECONDS_PER_SECOND / 2
            )
        );
        let value = duration("P36DT25H61M61.25S");
        assert_eq!((value.years(), value.months(), value.days()), (0, 0, 37));
        assert_eq!((value.hours(), value.minutes(), value.seconds()), (2, 2, 1));
        assert_eq!(i128::from(value.attosecond()), ATTOSECONDS_PER_SECOND / 4);
    }

    #[test]
    fn predicates_classify_durations() {
        let zero = duration("PT0S");
        assert!(!zero.is_negative() && zero.is_day_time() && zero.is_year_month());
        let day_time = duration("-P1DT2H");
        assert!(day_time.is_negative() && day_time.is_day_time() && !day_time.is_year_month());
        let year_month = duration("P1Y");
        assert!(
            !year_month.is_negative() && !year_month.is_day_time() && year_month.is_year_month()
        );
        let mixed = duration("P1MT1S");
        assert!(!mixed.is_negative() && !mixed.is_day_time() && !mixed.is_year_month());
    }

    #[test]
    fn arithmetic_follows_xpath() {
        assert_eq!(
            duration("P2Y11M").checked_add(duration("P3Y3M")),
            Some(duration("P6Y2M"))
        );
        assert_eq!(
            duration("P2DT12H5M").checked_add(duration("P5DT12H")),
            Some(duration("P8DT5M"))
        );
        assert_eq!(
            duration("P2Y11M").checked_sub(duration("P3Y3M")),
            Some(duration("-P4M"))
        );
        assert_eq!(
            duration("P2DT12H").checked_sub(duration("P1DT10H30M")),
            Some(duration("P1DT1H30M"))
        );
        assert_eq!(duration("P1Y").checked_neg(), Some(duration("-P1Y")));
        assert_eq!(duration("P1YT1S").checked_sub(duration("PT2S")), None);
        assert_eq!(duration("P1YT1S").checked_add(duration("-P2Y")), None);
        assert_eq!(
            Duration::from_months(i64::MAX).checked_add(duration("P1M")),
            None
        );
        assert_eq!(Duration::from_months(i64::MIN).checked_neg(), None);
    }

    #[test]
    fn value_order_is_partial() {
        assert_eq!(duration("P1M").value_cmp(duration("P30D")), None);
        assert_eq!(
            duration("P1M").value_cmp(duration("P32D")),
            Some(Ordering::Less)
        );
        assert_eq!(
            duration("P1M").value_cmp(duration("P27D")),
            Some(Ordering::Greater)
        );
        assert_eq!(
            duration("P1Y").value_cmp(duration("P12M")),
            Some(Ordering::Equal)
        );
        assert_eq!(
            duration("-P1D").value_cmp(duration("PT0S")),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn durations_of_one_kind_are_totally_ordered() {
        // Negative month counts and huge durations used to come out incomparable.
        let ordered = [
            "-P9M",
            "-P7M",
            "-P3M",
            "-P2M",
            "-P1M",
            "PT0S",
            "P1M",
            "P6000000000000Y",
        ];
        for (index, left) in ordered.iter().enumerate() {
            for (other, right) in ordered.iter().enumerate() {
                assert_eq!(
                    duration(left).value_cmp(duration(right)),
                    Some(index.cmp(&other)),
                    "{left} against {right}"
                );
            }
        }
        let huge = duration("PT170141183460000000000S");
        assert_eq!(huge.value_cmp(huge), Some(Ordering::Equal));
        assert_eq!(duration("PT0S").value_cmp(huge), Some(Ordering::Less));
    }

    #[test]
    fn offsets_are_durations() {
        let offset = |minutes: i16| TimezoneOffset::new(minutes).expect("in range");
        assert_eq!(Duration::from(offset(330)), duration("PT5H30M"));
        assert_eq!(Duration::from(offset(0)), duration("PT0S"));
        assert_eq!(Duration::from(offset(-840)), duration("-PT14H"));
    }

    #[test]
    fn the_oxsdatatypes_layout_is_pinned() {
        // `oxsdatatypes` serializes eight bytes of months and sixteen of attoseconds, big-endian.
        let value = duration("-P1Y2M3DT4H5M6.5S");
        let bytes = value.to_library().to_be_bytes();
        assert_eq!(bytes[..8], (-14i64).to_be_bytes());
        assert_eq!(bytes[8..], value.total_attoseconds().to_be_bytes());
        assert_eq!(Duration::from_library(value.to_library()), value);
    }
}
