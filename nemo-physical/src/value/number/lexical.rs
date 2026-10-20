//! The lexical forms of numbers.

use std::{fmt, num::ParseFloatError, str::FromStr};

use crate::value::number::decimal::workspace;

/// An `xsd:decimal` lexical form parsed into its significant digits.
///
/// The digits carry no leading or trailing zeros,
/// and the exponent places the last of them;
/// the value is `digits * 10^exponent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Parsed {
    /// Zero.
    Zero,
    /// Significant digits that fit a `u64`, held as a magnitude.
    Small {
        /// Whether the value is negative.
        negative: bool,
        /// The significant digits as a number.
        magnitude: u64,
        /// The base-10 exponent of the last significant digit.
        exponent: i64,
    },
    /// More significant digits than a `u64` holds.
    Big {
        /// Whether the value is negative.
        negative: bool,
        /// The significant digits.
        digits: String,
        /// The base-10 exponent of the last significant digit.
        exponent: i64,
    },
}

impl Parsed {
    /// Parses an `xsd:decimal` lexical form,
    /// an optional sign, digits, an optional fraction,
    /// which includes every `xsd:integer` lexical form.
    ///
    /// The digits are scanned into a `u64` in one pass;
    /// only a value whose significant digits outgrow it allocates.
    ///
    /// Returns `None` on malformed input,
    /// and for a value whose canonical form exceeds the length limit.
    pub(crate) fn parse(lexical: &str) -> Option<Self> {
        let (negative, rest) = match lexical.as_bytes() {
            [b'+', ..] => (false, &lexical[1..]),
            [b'-', ..] => (true, &lexical[1..]),
            _ => (false, lexical),
        };

        let (integer_part, fraction_part) = match rest.split_once('.') {
            Some((integer, fraction)) => (integer, fraction),
            None => (rest, ""),
        };

        if integer_part.is_empty() && fraction_part.is_empty() {
            return None;
        }

        if !integer_part.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction_part.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }

        // The significant digits are the digits without leading and trailing zeros;
        // the exponent tracks the position of the last one.
        let digits = integer_part.bytes().chain(fraction_part.bytes());
        let total = integer_part.len() + fraction_part.len();
        let Some(first) = digits.clone().position(|byte| byte != b'0') else {
            return Some(Parsed::Zero);
        };
        let last = total
            - 1
            - digits
                .clone()
                .rev()
                .position(|byte| byte != b'0')
                .expect("a nonzero digit exists");
        let exponent =
            i64::try_from(total - 1 - last).ok()? - i64::try_from(fraction_part.len()).ok()?;
        let significant = digits.skip(first).take(last + 1 - first);
        let count = last + 1 - first;

        if workspace::Big::canonical_length(count as u64, exponent) > workspace::Big::LENGTH_LIMIT {
            return None;
        }

        if count <= 20 {
            let magnitude = significant.clone().try_fold(0u64, |magnitude, byte| {
                magnitude
                    .checked_mul(10)?
                    .checked_add(u64::from(byte - b'0'))
            });

            if let Some(magnitude) = magnitude {
                return Some(Parsed::Small {
                    negative,
                    magnitude,
                    exponent,
                });
            }
        }

        Some(Parsed::Big {
            negative,
            digits: significant.map(char::from).collect(),
            exponent,
        })
    }
}

/// The lexical forms of numbers:
/// writing them, reading them as floating-point numbers, and recognizing their forms.
pub(crate) struct Lexical;

impl Lexical {
    /// Writes the number `digits * 10^exponent` in canonical decimal notation:
    /// plain digits for an integer,
    /// `digits.digits` without trailing zeros otherwise,
    /// and no leading zeros beyond the single one of `0.x`.
    ///
    /// `digits` are significant digits without sign and without leading or trailing zeros;
    /// a lone `"0"` denotes zero and comes with exponent 0.
    pub(crate) fn write_canonical(
        target: &mut impl fmt::Write,
        digits: &str,
        exponent: i64,
    ) -> fmt::Result {
        if exponent >= 0 {
            target.write_str(digits)?;
            for _ in 0..exponent {
                target.write_char('0')?;
            }
        } else {
            let fraction_length = usize::try_from(-exponent)
                .expect("a negative i64 exponent negates into usize range");

            if digits.len() > fraction_length {
                let (integer, fraction) = digits.split_at(digits.len() - fraction_length);

                target.write_str(integer)?;
                target.write_char('.')?;
                target.write_str(fraction)?;
            } else {
                target.write_str("0.")?;

                for _ in 0..fraction_length - digits.len() {
                    target.write_char('0')?;
                }

                target.write_str(digits)?;
            }
        }

        Ok(())
    }

    /// Returns the floating-point number nearest to the exact decimal `magnitude * 10^exponent`,
    /// negated if `negative` says so,
    /// infinite beyond the range of the type.
    pub(crate) fn nearest<F>(negative: bool, magnitude: impl fmt::Display, exponent: i64) -> F
    where
        F: FromStr<Err = ParseFloatError>,
    {
        let sign = if negative { "-" } else { "" };
        format!("{sign}{magnitude}e{exponent}")
            .parse()
            .expect("a mantissa with an exponent is a float literal")
    }

    /// Returns whether a lexical has the XSD form of a finite float or double:
    /// `[+-]? (digits [. digits*] | . digits) ([eE] [+-]? digits)?`.
    pub(crate) fn is_float_form(lexical: &str) -> bool {
        let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
        let unsigned = lexical.strip_prefix(['+', '-']).unwrap_or(lexical);
        let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, Some(exponent)),
            None => (unsigned, None),
        };
        let (integer, fraction) = match mantissa.split_once('.') {
            Some((integer, fraction)) => (integer, fraction),
            None => (mantissa, ""),
        };

        (!integer.is_empty() || !fraction.is_empty())
            && digits(integer)
            && digits(fraction)
            && exponent.is_none_or(|exponent| {
                let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
                !exponent.is_empty() && digits(exponent)
            })
    }

    /// Returns whether a lexical has the form of an unsigned decimal number literal of Nemo term syntax,
    /// without its type suffix: `digits [. digits] ([eE] [+-]? digits)?`.
    ///
    /// Unlike the XSD forms, digits are required on both sides of the point.
    pub(crate) fn is_term_form(lexical: &str) -> bool {
        let digits =
            |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
        let (mantissa, exponent) = match lexical.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, Some(exponent)),
            None => (lexical, None),
        };
        let (integer, fraction) = match mantissa.split_once('.') {
            Some((integer, fraction)) => (integer, Some(fraction)),
            None => (mantissa, None),
        };

        digits(integer)
            && fraction.is_none_or(digits)
            && exponent.is_none_or(|exponent| {
                digits(exponent.strip_prefix(['+', '-']).unwrap_or(exponent))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scans_small_values_without_allocating() {
        assert_eq!(
            Parsed::parse("1.50"),
            Some(Parsed::Small {
                negative: false,
                magnitude: 15,
                exponent: -1
            })
        );
        assert_eq!(
            Parsed::parse("-00123000"),
            Some(Parsed::Small {
                negative: true,
                magnitude: 123,
                exponent: 3
            })
        );
        assert_eq!(Parsed::parse("0"), Some(Parsed::Zero));
        assert_eq!(Parsed::parse("-0.00"), Some(Parsed::Zero));
        assert_eq!(Parsed::parse("42."), Parsed::parse("42"));
        assert_eq!(
            Parsed::parse("18446744073709551615"),
            Some(Parsed::Small {
                negative: false,
                magnitude: u64::MAX,
                exponent: 0
            })
        );
    }

    #[test]
    fn parse_allocates_past_a_u64() {
        assert_eq!(
            Parsed::parse("18446744073709551616"),
            Some(Parsed::Big {
                negative: false,
                digits: String::from("18446744073709551616"),
                exponent: 0
            })
        );
        assert_eq!(
            Parsed::parse("-123456789012345678901.500"),
            Some(Parsed::Big {
                negative: true,
                digits: String::from("1234567890123456789015"),
                exponent: -1
            })
        );
    }

    #[test]
    fn parse_rejects_malformed_and_unbounded() {
        for lexical in ["", ".", "+", "-", "1.5.5", "1e5", " 1", "1 ", "--1", "0x1"] {
            assert!(
                Parsed::parse(lexical).is_none(),
                "{lexical:?} must be rejected"
            );
        }
        assert!(Parsed::parse(&format!("1{}", "0".repeat(1_000_000))).is_none());
        assert!(Parsed::parse(&format!("1{}", "0".repeat(999_999))).is_some());
        assert!(Parsed::parse(&format!("0.{}1", "0".repeat(999_999))).is_none());
    }

    #[test]
    fn canonical_writing_places_the_point() {
        let written = |digits: &str, exponent: i64| {
            let mut text = String::new();
            Lexical::write_canonical(&mut text, digits, exponent)
                .expect("writing to a string succeeds");
            text
        };
        assert_eq!(written("15", -1), "1.5");
        assert_eq!(written("15", 2), "1500");
        assert_eq!(written("15", -3), "0.015");
        assert_eq!(written("0", 0), "0");
    }

    #[test]
    fn float_forms_are_recognized() {
        for lexical in ["1", "-1.5", ".5", "5.", "1e5", "-1.5E-3", "+2"] {
            assert!(Lexical::is_float_form(lexical), "{lexical:?}");
        }

        for lexical in ["", ".", "1e", "e5", "0x1", "INF", "NaN", " 1", "1.5.5"] {
            assert!(!Lexical::is_float_form(lexical), "{lexical:?}");
        }
    }

    #[test]
    fn term_forms_need_digits_around_the_point() {
        for lexical in ["1", "007", "1.50", "1e5", "1.5E-3", "2e+0"] {
            assert!(Lexical::is_term_form(lexical), "{lexical:?}");
        }

        for lexical in [
            "", ".5", "5.", "1e", "e5", "-1", "+1", "1.5.5", "1e5e5", "0x1", "1f",
        ] {
            assert!(!Lexical::is_term_form(lexical), "{lexical:?}");
        }
    }
}
