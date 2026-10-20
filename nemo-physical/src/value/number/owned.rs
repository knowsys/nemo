//! Owned numbers with canonicalizing construction.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};

use crate::value::{
    ValueDomain,
    number::{Decimal, Double, Float, NumberRef, lexical::Lexical},
};

/// An owned number of any of the four numeric domains:
/// integer, decimal, float and double.
/// Its borrowed view is [NumberRef].
///
/// The domains are distinct: `1`, `1.0f` and `1.0d` are three values,
/// while `1.0` is the integer `1`.
/// XSD's value comparison, under which the three are equal,
/// is [value_eq](NumberRef::value_eq) and [value_cmp](NumberRef::value_cmp).
///
/// The arithmetic promotes the operands along the ladder
/// integer, decimal, float, double to the smallest type holding both.
#[derive(Clone)]
pub enum Number {
    /// An exact number, an `xsd:decimal` value;
    /// the integers are its integral values.
    Decimal(Decimal),
    /// A 32-bit float, an `xsd:float` value.
    Float(Float),
    /// A 64-bit float, an `xsd:double` value.
    Double(Double),
}

/// Compile-time check of the size target for owned numbers.
const _: () = assert!(size_of::<Number>() <= 24);

impl Number {
    /// Creates a number from a number literal of Nemo term syntax,
    /// or `None` for any other lexical
    /// and for a value whose canonical form exceeds the length limit of numbers.
    ///
    /// The literal is an optional sign, then either a radix form,
    /// `0b`, `0o` or `0x` and digits of that base within the 128-bit range,
    /// or digits with an optional fraction and exponent
    /// and an optional `f`/`F` (float) or `d`/`D` (double) suffix.
    /// Without a suffix, a literal with an exponent is a double
    /// and one without is exact, read like [Decimal::from_lexical].
    pub fn from_term_syntax(lexical: &str) -> Option<Self> {
        let negative = lexical.starts_with('-');
        let unsigned = lexical.strip_prefix(['+', '-']).unwrap_or(lexical);

        // Radix forms first, as hexadecimal digits include `d`, `e` and `f`.
        let radix = match unsigned.get(..2) {
            Some("0b") => Some(2),
            Some("0o") => Some(8),
            Some("0x") => Some(16),
            _ => None,
        };
        if let Some(radix) = radix {
            let digits = &unsigned[2..];
            if digits.is_empty() || !digits.chars().all(|digit| digit.is_digit(radix)) {
                return None;
            }
            let magnitude = i128::from_str_radix(digits, radix).ok()?;
            return Decimal::new(if negative { -magnitude } else { magnitude }, 0)
                .map(Self::Decimal);
        }

        // The standard parsers read the validated form, sign included.
        let (body, suffix) = match unsigned.as_bytes().last() {
            Some(&suffix @ (b'f' | b'F' | b'd' | b'D')) => (
                &lexical[..lexical.len() - 1],
                Some(suffix.to_ascii_lowercase()),
            ),
            _ => (lexical, None),
        };
        if !Lexical::is_term_form(body.strip_prefix(['+', '-']).unwrap_or(body)) {
            return None;
        }

        match suffix {
            Some(b'f') => body
                .parse()
                .ok()
                .map(|value| Self::Float(Float::new(value))),
            Some(_) => body
                .parse()
                .ok()
                .map(|value| Self::Double(Double::new(value))),
            None if body.contains(['e', 'E']) => body
                .parse()
                .ok()
                .map(|value| Self::Double(Double::new(value))),
            None => Decimal::from_lexical(body).map(Self::Decimal),
        }
    }

    /// Returns the domain of the value.
    pub fn domain(&self) -> ValueDomain {
        self.as_ref().domain()
    }

    /// Returns the borrowed view of this value, the input to builtin evaluation.
    pub fn as_ref(&self) -> NumberRef<'_> {
        match self {
            Number::Decimal(value) => NumberRef::Decimal(value.as_ref()),
            Number::Float(value) => NumberRef::Float(*value),
            Number::Double(value) => NumberRef::Double(*value),
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Number {}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Number {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Debug for Number {
    /// Writes the value in its canonical form,
    /// a float with the `f` and a double with the `d` suffix of the term syntax,
    /// so that the domain shows.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl fmt::Display for Number {
    /// Writes the canonical lexical form of the value's datatype.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_syntax_picks_the_domain() {
        let number = |lexical| Number::from_term_syntax(lexical).expect("well-formed");

        assert_eq!(number("42").domain(), ValueDomain::Integer);
        assert_eq!(number("2500.0").domain(), ValueDomain::Integer);
        assert_eq!(number("-0x1F"), Number::Decimal(Decimal::from(-31)));
        assert_eq!(number("1.5").domain(), ValueDomain::Decimal);
        assert_eq!(number("2.5e3").domain(), ValueDomain::Double);
        assert_eq!(number("2.5f").domain(), ValueDomain::Float);
        assert_eq!(number("9D").domain(), ValueDomain::Double);
        assert_eq!(
            number("99999999999999999999").domain(),
            ValueDomain::Integer
        );
        assert_eq!(Number::from_term_syntax("5."), None);
        assert_eq!(Number::from_term_syntax("0x"), None);
    }

    #[test]
    fn views_round_trip() {
        let values = [
            Number::Decimal(Decimal::from(-42)),
            Number::Decimal(Decimal::new(15, -1).expect("bounded")),
            Number::Decimal(Decimal::new(5, 30).expect("bounded")),
            Number::Float(Float::new(1.5)),
            Number::Double(Double::new(f64::NAN)),
        ];

        for value in values {
            assert_eq!(value.as_ref().to_owned(), value);
            assert_eq!(value.domain(), value.as_ref().domain());
        }
    }
}
