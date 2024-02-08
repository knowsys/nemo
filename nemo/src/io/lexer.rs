//! Lexical tokenization of rulewerk-style rules.

use nom::{
    branch::alt,
    bytes::complete::{is_a, is_not, tag},
    character::complete::multispace0,
    combinator::{peek, recognize},
    error::ParseError,
    multi::many0,
    sequence::{delimited, tuple},
    IResult, Parser,
};
use nom_locate::LocatedSpan;

type Span<'a> = LocatedSpan<&'a str>;

/// All the tokens the input gets parsed into.
#[derive(Debug, PartialEq)]
enum Token<'a> {
    // Directives
    Base(Span<'a>),
    Prefix(Span<'a>),
    Import(Span<'a>),
    Export(Span<'a>),
    // Syntactic symbols
    QuestionMark(Span<'a>),
    BracketOpen(Span<'a>),
    BracketClose(Span<'a>),
    SquaredBracketOpen(Span<'a>),
    SquaredBracketClose(Span<'a>),
    CurlyBracketOpen(Span<'a>),
    CurlyBracketClose(Span<'a>),
    Dot(Span<'a>),
    Comma(Span<'a>),
    Colon(Span<'a>),
    ImplicationArrow(Span<'a>),
    Greater(Span<'a>),
    Equal(Span<'a>),
    Less(Span<'a>),
    Not(Span<'a>),
    DoubleCaret(Span<'a>),
    Hash(Span<'a>),
    Underscore(Span<'a>),
    AtSign(Span<'a>),
    // Names or values
    Identifier(Span<'a>),
    IRI(Span<'a>),
    Integer(Span<'a>),
    Float(Span<'a>),
    String(Span<'a>),
    // miscellaneous
    Comment(Span<'a>),
    Illegal(Span<'a>),
    EOF(Span<'a>),
}

// FIXME: Figure out when erros occur
fn tokenize<'a>(input: Span<'a>) -> Vec<Token<'a>> {
    let (rest, vec) = many0(ignore_ws(alt((comment, base, prefix, import, export))))(input)
        .expect("An error occured");
    vec
}

fn ignore_ws<'a, F, O, E: ParseError<Span<'a>>>(
    inner: F,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, O, E>
where
    F: Parser<Span<'a>, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

fn comment<'a>(input: Span<'a>) -> IResult<Span<'a>, Token<'a>> {
    recognize(tuple((
        tag("%"),
        is_not("\n\r"),
        alt((tag("\n\r"), tag("\n"))),
    )))(input)
    .map(|(rest, span)| (rest, Token::Comment(span)))
}

/// Recognize the `@base` directive
fn base<'a>(input: Span<'a>) -> IResult<Span<'a>, Token<'a>> {
    tag("@base")(input).map(|(rest, span)| (rest, Token::Base(span)))
}

fn prefix<'a>(input: Span<'a>) -> IResult<Span<'a>, Token<'a>> {
    tag("@prefix")(input).map(|(rest, span)| (rest, Token::Prefix(span)))
}

fn import<'a>(input: Span<'a>) -> IResult<Span<'a>, Token<'a>> {
    tag("@import")(input).map(|(rest, span)| (rest, Token::Import(span)))
}

fn export<'a>(input: Span<'a>) -> IResult<Span<'a>, Token<'a>> {
    tag("@export")(input).map(|(rest, span)| (rest, Token::Export(span)))
}

#[cfg(test)]
mod test {
    use nom::multi::many0;

    use super::{Span, Token};
    // is `tag` the right denomination?
    #[test]
    fn base_tag() {
        assert_eq!(
            super::base(Span::new("@base")).unwrap().1,
            Token::Base(unsafe { Span::new_from_raw_offset(0, 1, "@base", ()) })
        );
    }

    // is `tag` the right denomination?
    #[test]
    fn prefix_tag() {
        assert_eq!(
            super::prefix(Span::new("@prefix")).unwrap().1,
            Token::Prefix(unsafe { Span::new_from_raw_offset(0, 1, "@prefix", ()) })
        );
    }

    // is `tag` the right denomination?
    #[test]
    fn import_tag() {
        assert_eq!(
            super::import(Span::new("@import")).unwrap().1,
            Token::Import(unsafe { Span::new_from_raw_offset(0, 1, "@import", ()) })
        );
    }

    // is `tag` the right denomination?
    #[test]
    fn export_tag() {
        assert_eq!(
            super::export(Span::new("@export")).unwrap().1,
            Token::Export(unsafe { Span::new_from_raw_offset(0, 1, "@export", ()) })
        );
    }

    #[test]
    fn comment() {
        assert_eq!(
            super::comment(Span::new(
                "% Some meaningful comment with some other %'s in it\n"
            ))
            .unwrap()
            .1,
            Token::Comment(unsafe {
                Span::new_from_raw_offset(
                    0,
                    1,
                    "% Some meaningful comment with some other %'s in it\n",
                    (),
                )
            })
        );
        assert_eq!(
            super::comment(Span::new(
                "% Some meaningful comment with some other %'s in it\n\r"
            ))
            .unwrap()
            .1,
            Token::Comment(unsafe {
                Span::new_from_raw_offset(
                    0,
                    1,
                    "% Some meaningful comment with some other %'s in it\n\r",
                    (),
                )
            })
        );
        assert_eq!(
            super::comment(Span::new(
                "% Some meaningful comment\n%that is more than one line long\n"
            ))
            .unwrap()
            .1,
            Token::Comment(unsafe {
                Span::new_from_raw_offset(0, 1, "% Some meaningful comment\n", ())
            })
        );
        assert_eq!(
            many0(super::comment)(Span::new(
                "% Some meaningful comment\n%that is more than one line long\n"
            ))
            .unwrap()
            .1,
            vec![
                Token::Comment(unsafe {
                    Span::new_from_raw_offset(0, 1, "% Some meaningful comment\n", ())
                }),
                Token::Comment(unsafe {
                    Span::new_from_raw_offset(26, 2, "%that is more than one line long\n", ())
                })
            ]
        );
    }
}
