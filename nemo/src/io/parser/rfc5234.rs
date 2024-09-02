use nom::{
    branch::alt,
    character::complete::{one_of, satisfy},
    combinator::recognize,
};

/// Parsers defined in RFC 5234
use super::types::{IntermediateResult, Span};

use macros::traced;

#[traced("parser::rfc5234")]
pub(super) fn alpha<'a>(input: Span<'a>) -> IntermediateResult<'a, Span<'a>> {
    recognize(satisfy(|c| c.is_ascii_alphabetic()))(input)
}

#[traced("parser::rfc5234")]
pub(super) fn digit<'a>(input: Span<'a>) -> IntermediateResult<'a, Span<'a>> {
    recognize(satisfy(|c| c.is_ascii_digit()))(input)
}

#[traced("parser::rfc5234")]
pub(super) fn hexdig<'a>(input: Span<'a>) -> IntermediateResult<'a, Span<'a>> {
    alt((digit, recognize(one_of("ABCDEF"))))(input)
}
