//! Parsers for productions from the SPARQL 1.1 grammar.
use std::fmt::Display;

use nom::{
    branch::alt,
    character::complete::{one_of, satisfy},
    combinator::{map, opt, recognize},
    multi::{many0, many1, separated_list0},
    sequence::{delimited, pair, preceded, terminated, tuple},
};

use super::{
    iri,
    old::map_error,
    old::token,
    old::ParseError,
    rfc5234::digit,
    turtle::hex,
    types::{IntermediateResult, Span},
};

use macros::traced;

#[derive(Debug)]
#[allow(clippy::enum_variant_names)] // `PrefixedName` comes from the SPARQL grammar
pub enum Name<'a> {
    IriReference(&'a str),
    PrefixedName { prefix: &'a str, local: &'a str },
    BlankNode(&'a str),
}

impl Display for Name<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Name::IriReference(iri) => write!(f, "{iri}"),
            Name::PrefixedName { prefix, local } => write!(f, "{prefix}:{local}"),
            Name::BlankNode(label) => write!(f, "_:{label}"),
        }
    }
}

/// Parse an IRI reference, i.e., an IRI (relative or absolute)
/// wrapped in angle brackets. Roughly equivalent to the
/// [IRIREF](https://www.w3.org/TR/sparql11-query/#rIRIREF)
/// production of the SPARQL 1.1 grammar, but uses the full [RFC
/// 3987](https://www.ietf.org/rfc/rfc3987.txt) grammar to verify
/// the actual IRI.
#[traced("parser::sparql")]
pub fn iriref(input: Span) -> IntermediateResult<Span> {
    map_error(
        delimited(token("<"), iri::iri_reference, token(">")),
        || ParseError::ExpectedIriref,
    )(input)
}

#[traced("parser::sparql")]
pub fn iri(input: Span) -> IntermediateResult<Name> {
    alt((map(iriref, |name| Name::IriReference(&name)), prefixed_name))(input)
}

#[traced("parser::sparql")]
pub fn pname_ns(input: Span) -> IntermediateResult<Span> {
    let (rest, prefix) = map_error(terminated(opt(pn_prefix), token(":")), || {
        ParseError::ExpectedPnameNs
    })(input)?;

    Ok((rest, prefix.unwrap_or("".into())))
}

#[traced("parser::sparql")]
pub fn pn_chars_base(input: Span) -> IntermediateResult<Span> {
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
pub fn pn_chars_u(input: Span) -> IntermediateResult<Span> {
    alt((pn_chars_base, token("_")))(input)
}

#[traced("parser::sparql")]
pub fn pn_chars(input: Span) -> IntermediateResult<Span> {
    alt((
        pn_chars_u,
        token("-"),
        digit,
        token("\u{00B7}"),
        recognize(satisfy(|c| {
            [0x0300u32..=0x036F, 0x203F..=0x2040]
                .iter()
                .any(|range| range.contains(&c.into()))
        })),
    ))(input)
}

#[traced("parser::sparql")]
pub fn pn_prefix(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((
        pn_chars_base,
        separated_list0(many1(token(".")), many0(pn_chars)),
    )))(input)
}

#[traced("parser::sparql")]
pub fn percent(input: Span) -> IntermediateResult<Span> {
    recognize(tuple((token("%"), hex, hex)))(input)
}

#[traced("parser::sparql")]
pub fn pn_local_esc(input: Span) -> IntermediateResult<Span> {
    recognize(preceded(token(r"\"), one_of(r#"_~.-!$&'()*+,;=/?#@%"#)))(input)
}

#[traced("parser::sparql")]
pub fn plx(input: Span) -> IntermediateResult<Span> {
    alt((percent, pn_local_esc))(input)
}

#[traced("parser::sparql")]
pub fn pn_local(input: Span) -> IntermediateResult<Span> {
    recognize(pair(
        alt((pn_chars_u, token(":"), digit, plx)),
        opt(separated_list0(
            many1(token(".")),
            many0(alt((pn_chars, token(":"), plx))),
        )),
    ))(input)
}

#[traced("parser::sparql")]
pub fn pname_ln(input: Span) -> IntermediateResult<Name> {
    map(pair(pname_ns, pn_local), |(prefix, local)| {
        Name::PrefixedName {
            prefix: &prefix,
            local: &local,
        }
    })(input)
}

#[traced("parser::sparql")]
pub fn prefixed_name(input: Span) -> IntermediateResult<Name> {
    map_error(
        alt((
            pname_ln,
            map(pname_ns, |prefix| Name::PrefixedName {
                prefix: &prefix,
                local: "",
            }),
        )),
        || ParseError::ExpectedPrefixedName,
    )(input)
}

#[traced("parser::sparql")]
pub fn blank_node_label(input: Span) -> IntermediateResult<Name> {
    map_error(
        preceded(
            token("_:"),
            map(
                recognize(pair(
                    alt((pn_chars_u, digit)),
                    opt(separated_list0(many1(token(".")), many0(pn_chars))),
                )),
                |name| Name::BlankNode(&name),
            ),
        ),
        || ParseError::ExpectedBlankNodeLabel,
    )(input)
}
