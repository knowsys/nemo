use nom::{
    branch::alt,
    character::complete::{one_of, satisfy},
    combinator::recognize,
};

/// Parsers defined in RFC 5234
use super::types::IntermediateResult;

use macros::traced;

#[traced("parser::rfc5234")]
pub(super) fn alpha(input: &str) -> IntermediateResult<&str> {
    recognize(satisfy(|c| c.is_ascii_alphabetic()))(input)
}

#[traced("parser::rfc5234")]
pub(super) fn digit(input: &str) -> IntermediateResult<&str> {
    recognize(satisfy(|c| c.is_ascii_digit()))(input)
}

#[traced("parser::rfc5234")]
pub(super) fn hexdig(input: &str) -> IntermediateResult<&str> {
    alt((digit, recognize(one_of("ABCDEF"))))(input)
}
