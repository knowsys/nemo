//! Parsers for productions from the RDF 1.1 Turtle grammar.
use std::num::ParseIntError;

use macros::traced;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, digit0, digit1, none_of, one_of},
    combinator::{map, map_res, opt, recognize},
    multi::{count, many0},
    sequence::{delimited, pair, preceded, tuple},
};

use crate::{logical::model::NumericLiteral, physical::datatypes::Double};

use super::{
    sparql::{iri, Name},
    types::IntermediateResult,
};

/// Characters requiring escape sequences in single-line string literals.
const REQUIRES_ESCAPE: &str = "\u{22}\u{5C}\u{0A}\u{0D}";

/// Valid hexadecimal digits.
const HEXDIGIT: &str = "0123456789ABCDEFabcdef";

#[traced("parser::turtle")]
pub fn string(input: &str) -> IntermediateResult<&str> {
    alt((
        string_literal_quote,
        string_literal_single_quote,
        string_literal_long_single_quote,
        string_literal_long_quote,
    ))(input)
}

#[traced("parser::turtle")]
pub fn string_literal_quote(input: &str) -> IntermediateResult<&str> {
    delimited(
        tag(r#"""#),
        recognize(many0(alt((
            recognize(none_of(REQUIRES_ESCAPE)),
            echar,
            uchar,
        )))),
        tag(r#"""#),
    )(input)
}

#[traced("parser::turtle")]
pub fn string_literal_single_quote(input: &str) -> IntermediateResult<&str> {
    delimited(
        tag("'"),
        recognize(many0(alt((
            recognize(none_of(REQUIRES_ESCAPE)),
            echar,
            uchar,
        )))),
        tag("'"),
    )(input)
}

#[traced("parser::turtle")]
pub fn string_literal_long_single_quote(input: &str) -> IntermediateResult<&str> {
    delimited(
        tag("'''"),
        recognize(many0(alt((recognize(none_of(r#"'\"#)), echar, uchar)))),
        tag("'''"),
    )(input)
}

#[traced("parser::turtle")]
pub fn string_literal_long_quote(input: &str) -> IntermediateResult<&str> {
    delimited(
        tag(r#"""""#),
        recognize(many0(alt((recognize(none_of(r#""\"#)), echar, uchar)))),
        tag(r#"""""#),
    )(input)
}

#[traced("parser::turtle")]
pub fn hex(input: &str) -> IntermediateResult<&str> {
    recognize(one_of(HEXDIGIT))(input)
}

#[traced("parser::turtle")]
pub fn uchar(input: &str) -> IntermediateResult<&str> {
    recognize(alt((
        preceded(tag(r#"\u"#), count(hex, 4)),
        preceded(tag(r#"\U"#), count(hex, 8)),
    )))(input)
}

#[traced("parser::turtle")]
pub fn echar(input: &str) -> IntermediateResult<&str> {
    recognize(preceded(tag(r#"\"#), one_of(r#"tbnrf"'\"#)))(input)
}

#[traced("parser::turtle")]
pub fn sign(input: &str) -> IntermediateResult<&str> {
    recognize(one_of("+-"))(input)
}

#[traced("parser::turtle")]
pub fn integer(input: &str) -> IntermediateResult<NumericLiteral> {
    map_res(recognize(preceded(opt(sign), digit1)), |value| {
        value.parse().map(NumericLiteral::Integer)
    })(input)
}

#[traced("parser::turtle")]
pub fn decimal(input: &str) -> IntermediateResult<NumericLiteral> {
    map_res(
        pair(
            recognize(preceded(opt(sign), digit0)),
            preceded(tag("."), digit1),
        ),
        |(whole, fraction)| {
            Ok::<_, ParseIntError>(NumericLiteral::Decimal(whole.parse()?, fraction.parse()?))
        },
    )(input)
}

#[traced("parser::turtle")]
pub fn exponent(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((one_of("eE"), opt(sign), digit1)))(input)
}

#[traced("parser::turtle")]
pub fn double(input: &str) -> IntermediateResult<NumericLiteral> {
    map_res(
        map_res(
            recognize(preceded(
                opt(sign),
                alt((
                    recognize(tuple((digit1, tag("."), digit0, exponent))),
                    recognize(tuple((tag("."), digit1, exponent))),
                    recognize(pair(digit1, exponent)),
                )),
            )),
            |value| value.parse().map(Double::new),
        ),
        |number| number.map(NumericLiteral::Double),
    )(input)
}

#[traced("parser::turtle")]
pub fn numeric_literal(input: &str) -> IntermediateResult<NumericLiteral> {
    alt((double, decimal, integer))(input)
}

#[derive(Debug)]
pub(super) enum RdfLiteral<'a> {
    LanguageString { value: &'a str, tag: &'a str },
    DatatypeValue { value: &'a str, datatype: Name<'a> },
}

#[traced("parser::turtle")]
pub(super) fn rdf_literal(input: &str) -> IntermediateResult<RdfLiteral<'_>> {
    let (remainder, value) = string(input)?;
    alt((
        map(langtag, |tag| RdfLiteral::LanguageString { value, tag }),
        map(preceded(tag("^^"), iri), |datatype| {
            RdfLiteral::DatatypeValue { value, datatype }
        }),
    ))(remainder)
}

#[traced("parser::turtle")]
pub fn langtag(input: &str) -> IntermediateResult<&str> {
    recognize(tuple((
        tag("@"),
        alpha1,
        many0(preceded(tag("-"), alphanumeric1)),
    )))(input)
}

#[allow(dead_code)]
#[traced("parser::turtle")]
pub fn boolean_literal(input: &str) -> IntermediateResult<bool> {
    alt((map(tag("true"), |_| true), map(tag("false"), |_| false)))(input)
}
