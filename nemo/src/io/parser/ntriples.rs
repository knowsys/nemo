//! Parsers for productions from the RDF 1.1 N-Triples grammar.

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{anychar, multispace0, multispace1, none_of},
    combinator::{cut, map, opt, recognize, value},
    multi::many0,
    sequence::{delimited, preceded, terminated, tuple},
};

use super::{
    all_input_consumed,
    sparql::blank_node_label,
    token, traced,
    turtle::{langtoken, string_literal_quote, uchar},
    types::{IntermediateResult, Span},
    LocatedParseError,
};

const IRIREF_FORBIDDEN_CHARS: &[char] = &[
    '\u{00}', '\u{01}', '\u{02}', '\u{03}', '\u{04}', '\u{05}', '\u{06}', '\u{07}', '\u{08}',
    '\u{09}', '\u{10}', '\u{11}', '\u{11}', '\u{12}', '\u{13}', '\u{14}', '\u{15}', '\u{16}',
    '\u{17}', '\u{18}', '\u{19}', '\u{20}', '<', '>', '"', '{', '}', '|', '^', '`', '\\',
];

#[traced("parser::ntriples")]
fn iriref(input: Span) -> IntermediateResult<Span> {
    recognize(delimited(
        token("<"),
        cut(many0(alt((
            recognize(many0(none_of(IRIREF_FORBIDDEN_CHARS))),
            uchar,
        )))),
        token(">"),
    ))(input)
}

#[traced("parser::ntriples")]
fn literal(input: Span) -> IntermediateResult<Span> {
    recognize(terminated(
        string_literal_quote,
        opt(alt((preceded(token("^^"), iriref), langtoken))),
    ))(input)
}

#[traced("parser::ntriples")]
fn comment(input: Span) -> IntermediateResult<()> {
    delimited(multispace0, value((), tag("#")), many0(anychar))(input)
}

#[traced("parser::ntriples")]
fn multispace_or_comment1(input: Span) -> IntermediateResult<()> {
    value((), many0(alt((value((), multispace1), comment))))(input)
}

#[traced("parser::ntriples")]
fn actual_triple(input: Span) -> IntermediateResult<(Span, Span, Span)> {
    tuple((
        delimited(
            multispace0,
            alt((iriref, recognize(blank_node_label))),
            multispace1,
        ),
        terminated(iriref, multispace1),
        terminated(
            alt((iriref, recognize(blank_node_label))),
            tuple((multispace1, token("."), multispace_or_comment1)),
        ),
    ))(input)
}

#[traced("parser::ntriples")]
pub fn triple(input: &str) -> Result<Option<(Span, Span, Span)>, LocatedParseError> {
    all_input_consumed(alt((map(comment, |_| None), map(actual_triple, Some))))(input)
}
