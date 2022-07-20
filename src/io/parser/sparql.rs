//! Parsers for productions from the SPARQL 1.1 grammar.
use macros::traced;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::satisfy,
    combinator::{opt, recognize},
    multi::{many0, many1, separated_list0},
    sequence::{delimited, pair, terminated, tuple},
};

use super::{iri, rfc5234::digit, types::IntermediateResult};

/// Parse an IRI reference, i.e., an IRI (relative or absolute)
/// wrapped in angle brackets. Roughly equivalent to the
/// [IRIREF](https://www.w3.org/TR/sparql11-query/#rIRIREF)
/// production of the SPARQL 1.1 grammar, but uses the full [RFC
/// 3987](https://www.ietf.org/rfc/rfc3987.txt) grammar to verify
/// the actual IRI.
#[traced("parser::sparql")]
pub fn iriref(input: &str) -> IntermediateResult<&str> {
    delimited(tag("<"), iri::iri_reference, tag(">"))(input)
}

#[traced("parser::sparql")]
pub fn iri(input: &str) -> IntermediateResult<&str> {
    alt((iriref, prefixed_name))(input)
}

#[traced("parser::sparql")]
pub fn pname_ns(input: &str) -> IntermediateResult<&str> {
    let (rest, prefix) = terminated(opt(pn_prefix), tag(":"))(input)?;

    Ok((rest, prefix.unwrap_or_default()))
}

#[traced("parser::sparql")]
pub fn pn_chars_base(input: &str) -> IntermediateResult<&str> {
    recognize(satisfy(|c| {
        [
            0x41_u32..=0x5A,
            0x61..=0x7A,
            0x00C0..=0x0D6,
            0x0D8..=0x0F6,
            0x00F8..=0x2FF,
            0x0370..=0x037D,
            0x037F..=0x1FFF,
            0x200C..=0x200D,
            0x2070..=0x218F,
            0x2C00..=0x2FEF,
            0x3001..=0xD7FF,
            0xF900..=0xFDCF,
            0xFDF0..=0xFFFD,
            0x10000..=0xEFFFF,
        ]
        .iter()
        .any(|range| range.contains(&c.into()))
    }))(input)
}

#[traced("parser::sparql")]
pub fn pn_chars_u(input: &str) -> IntermediateResult<&str> {
    alt((pn_chars_base, tag("_")))(input)
}

#[traced("parser::sparql")]
pub fn pn_chars(input: &str) -> IntermediateResult<&str> {
    alt((
        pn_chars_u,
        tag("-"),
        digit,
        tag("\u{00B7}"),
        recognize(satisfy(|c| {
            [0x0300u32..=0x036F, 0x203F..=0x2040]
                .iter()
                .any(|range| range.contains(&c.into()))
        })),
    ))(input)
}

#[traced("parser::sparql")]
pub fn pn_prefix(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        pn_chars_base,
        separated_list0(many1(tag(".")), many0(pn_chars)),
    )))(input)
}

#[traced("parser::sparql")]
pub fn pname_local(input: &str) -> IntermediateResult<&str> {
    todo!()
}

#[traced("parser::sparql")]
pub fn pname_ln(input: &str) -> IntermediateResult<&str> {
    recognize(pair(pname_ns, pname_ln))(input)
}

#[traced("parser::sparql")]
pub fn prefixed_name(input: &str) -> IntermediateResult<&str> {
    alt((pname_ln, pname_ns))(input)
}
