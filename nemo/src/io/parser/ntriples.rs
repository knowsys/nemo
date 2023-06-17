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
    '\u{09}', '\u{0A}', '\u{0B}', '\u{0C}', '\u{0D}', '\u{0E}', '\u{0F}', '\u{10}', '\u{11}',
    '\u{12}', '\u{13}', '\u{14}', '\u{15}', '\u{16}', '\u{17}', '\u{18}', '\u{19}', '\u{1A}',
    '\u{1B}', '\u{1C}', '\u{1D}', '\u{1E}', '\u{1F}', '\u{20}', '<', '>', '"', '{', '}', '|', '^',
    '`', '\\',
];

#[traced("parser::ntriples")]
fn iriref(input: Span) -> IntermediateResult<Span> {
    recognize(delimited(
        token("<"),
        cut(many0(alt((
            recognize(none_of(IRIREF_FORBIDDEN_CHARS)),
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
fn multispace_or_comment0(input: Span) -> IntermediateResult<()> {
    value((), many0(alt((value((), multispace1), comment))))(input)
}

#[traced("parser::ntriples")]
fn triple(input: Span) -> IntermediateResult<(Span, Span, Span)> {
    tuple((
        delimited(
            multispace0,
            // TODO: PN_CHARS_U (used `in blank_node_label`) differs
            // between N-Triples and SPARQL grammars, make sure they
            // accept the same languages
            alt((iriref, recognize(blank_node_label))),
            multispace1,
        ),
        terminated(iriref, multispace1),
        terminated(
            alt((iriref, recognize(blank_node_label), literal)),
            delimited(multispace1, token("."), multispace_or_comment0),
        ),
    ))(input)
}

#[traced("parser::ntriples")]
pub fn triple_or_comment(input: &str) -> Result<Option<(Span, Span, Span)>, LocatedParseError> {
    all_input_consumed(alt((map(comment, |_| None), map(triple, Some))))(input)
}

#[cfg(test)]
mod test {
    use super::*;

    use test_log::test;

    #[test]
    fn irirefs() {
        assert!(all_input_consumed(iriref)("<http://one.example/subject1>").is_ok());
    }

    #[test]
    fn literals() {
        assert!(all_input_consumed(literal)(r#""object1""#).is_ok());
        assert!(all_input_consumed(literal)(
            r#""object1"^^<http://www.w3.org/2001/XMLSchema#string>"#
        )
        .is_ok());
    }

    #[test]
    fn bnodes() {
        assert!(all_input_consumed(recognize(blank_node_label))("_:subject1").is_ok());
    }

    #[test]
    fn triples() {
        assert!(all_input_consumed(triple)(
            r#"_:subject1 <http://an.example/predicate1> "object1" ."#
        )
        .is_ok());
    }
}
