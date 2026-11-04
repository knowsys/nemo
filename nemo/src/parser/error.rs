//! This module defines the error type that is returned when the parser is unsuccessful.

use std::{cmp::Ordering, fmt::Display};

use nom::{
    Parser,
    branch::alt,
    bytes::complete::{take_until, take_while},
    character::complete::line_ending,
    combinator::map,
    sequence::{preceded, terminated},
};
use smallvec::SmallVec;

use crate::error::rich::RichError;

use crate::syntax::rule;

use super::{
    ParserInput, ParserResult,
    ast::{
        statement::{Statement, StatementKind},
        token::{Token, TokenKind},
    },
    context::ParserContext,
    span::{CharacterPosition, Span},
};

/// Capacity of [ContextStack] before it spills onto the heap
const CONTEXT_STACK_CAPACITY: usize = 12;

/// Stack of [ParserContext]s recorded for a failing parser, innermost first
type ContextStack = SmallVec<[ParserContext; CONTEXT_STACK_CAPACITY]>;

/// Stack of errors that occurred while parsing a nemo program
#[derive(Debug, Clone)]
pub struct ParserErrors<'a> {
    /// Furthest position reached
    position: Span<'a>,
    /// Contexts recorded at [Self::position]
    context: ContextStack,
}

impl<'a> ParserErrors<'a> {
    /// Create an error at the given position.
    pub(crate) fn at(position: Span<'a>) -> Self {
        Self {
            position,
            context: ContextStack::new(),
        }
    }

    /// Record an enclosing [ParserContext].
    pub(crate) fn push_context(&mut self, context: ParserContext) {
        self.context.push(context);
    }

    /// Keep whichever of the two errors got further.
    fn merge(&mut self, other: Self) {
        match other
            .position
            .location_offset()
            .cmp(&self.position.location_offset())
        {
            Ordering::Less => {}
            Ordering::Greater => *self = other,
            // Neither alternative explains the failure on its own, so only
            // the contexts they agree on still describe it.
            Ordering::Equal => {
                let agreed = self
                    .context
                    .iter()
                    .zip(&other.context)
                    .take_while(|(one, other)| one == other)
                    .count();

                self.context.truncate(agreed);
            }
        }
    }

    /// Returns the position to report this error at.
    fn reported_position(&self) -> CharacterPosition {
        let expects_token = matches!(self.context.first(), Some(ParserContext::Token { .. }));

        let before = self.position.text_before();
        let text = if expects_token {
            before.trim_end()
        } else {
            before
        };

        let skipped_lines = before[text.len()..].matches('\n').count();
        let line_start = text.rfind('\n').map_or(0, |index| index + 1);

        CharacterPosition {
            offset: text.len(),
            line: self
                .position
                .location_line()
                .saturating_sub(skipped_lines as u32),
            column: bytecount::num_chars(&text.as_bytes()[line_start..]) as u32 + 1,
        }
    }

    /// Returns a suggestion for the mistake that most likely caused this error.
    fn hint(&self) -> Option<String> {
        let expected = *self.context.first()?;
        let rest = self.position.fragment();

        if expected == ParserContext::token(TokenKind::Dot) {
            for arrow in ["<-", "=>", ":=", "<=", "->"] {
                if rest.starts_with(arrow) {
                    return Some(format!(
                        "rules are written `head {} body`, not `head {arrow} body`",
                        rule::ARROW
                    ));
                }
            }

            return Some(String::from("every statement must end with `.`"));
        }

        None
    }

    /// Convert into the [ParserError]s shown to the user.
    pub(crate) fn parser_errors(&self) -> Vec<ParserError> {
        vec![ParserError {
            position: self.reported_position(),
            context: self.context.to_vec(),
            hint: self.hint(),
        }]
    }
}

impl<'a> nom::error::ParseError<ParserInput<'a>> for ParserErrors<'a> {
    fn from_error_kind(input: ParserInput<'a>, _kind: nom::error::ErrorKind) -> Self {
        Self::at(input.span)
    }

    /// nom only calls this from `alt`, which passes the error through unchanged.
    fn append(_input: ParserInput<'a>, _kind: nom::error::ErrorKind, other: Self) -> Self {
        other
    }

    fn from_char(input: ParserInput<'a>, _character: char) -> Self {
        Self::at(input.span)
    }

    fn or(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }
}

/// Error while parsing a nemo program
#[derive(Clone, Debug)]
pub struct ParserError {
    /// Position where the error occurred
    pub position: CharacterPosition,
    /// Parsing stack, innermost first
    pub context: Vec<ParserContext>,
    /// Suggestion for how to fix the error
    pub hint: Option<String>,
}

impl ParserError {
    /// The construct that was being parsed when the error occurred.
    fn enclosing(&self) -> Option<ParserContext> {
        self.context
            .iter()
            .skip(1)
            .find(|context| context.names_construct())
            .copied()
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.context.first() {
            Some(ParserContext::Token { kind }) => write!(f, "expected `{}`", kind.name())?,
            Some(context) => write!(f, "expected {}", context.name())?,
            None => write!(f, "unexpected input")?,
        }

        if let Some(enclosing) = self.enclosing() {
            write!(f, " in {}", enclosing.name())?;
        }

        Ok(())
    }
}

impl RichError for ParserError {
    fn is_warning(&self) -> bool {
        false
    }

    fn message(&self) -> String {
        self.to_string()
    }

    fn code(&self) -> usize {
        1
    }

    fn note(&self) -> Option<String> {
        self.hint.clone()
    }
}

/// Keep whichever of two failures got further.
///
/// Parsers that try an alternative by hand, rather than through `alt`, need this
/// to report what `alt` would have.
pub(crate) fn deepest<'a>(
    one: nom::Err<ParserErrors<'a>>,
    other: nom::Err<ParserErrors<'a>>,
) -> nom::Err<ParserErrors<'a>> {
    match (one, other) {
        (nom::Err::Error(one), nom::Err::Error(other)) => {
            nom::Err::Error(nom::error::ParseError::or(one, other))
        }
        (one, _) => one,
    }
}

/// Skip a statement, returning an error token.
pub(crate) fn skip_statement(input: ParserInput<'_>) -> ParserResult<'_, Token<'_>> {
    let input_span = input.span;

    let until_double_newline = map(
        alt((
            preceded(take_until("\n\n"), Token::double_newline),
            preceded(take_until("\r\n\r\n"), Token::double_newline),
            preceded(take_until("\r\r"), Token::double_newline),
        )),
        move |token| Token::error(Span::enclose(&input_span, &token.span())),
    );
    // TODO: Should there additional whitespace be allowed in-between the dot and the newline?
    let until_dot_newline = map(
        alt((
            preceded(take_until(".\n"), terminated(Token::dot, line_ending)),
            preceded(take_until(".\r\n"), terminated(Token::dot, line_ending)),
            preceded(take_until(".\r"), terminated(Token::dot, line_ending)),
        )),
        move |token| Token::error(Span::enclose(&input_span, &token.span())),
    );
    let until_eof = map(take_while(|_| true), move |_| Token::error(input_span));

    alt((until_dot_newline, until_double_newline, until_eof))(input)
}

pub(crate) fn recover<'a>(
    mut parser: impl Parser<ParserInput<'a>, Statement<'a>, ParserErrors<'a>>,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Statement<'a>> {
    move |input: ParserInput<'a>| match parser.parse(input.clone()) {
        Ok((rest, statement)) => Ok((rest, statement)),
        Err(err) if input.span.fragment().is_empty() => Err(err),
        Err(nom::Err::Error(_)) | Err(nom::Err::Failure(_)) => {
            let (rest_input, token) = skip_statement(input).expect("this parser cannot fail");
            Ok((
                rest_input,
                Statement {
                    span: token.span(),
                    comment: None,
                    kind: StatementKind::Error(token),
                    attributes: Default::default(),
                },
            ))
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn translate_error_tree<'a>(error: &nom::Err<ParserErrors<'a>>) -> Vec<ParserError> {
    match error {
        nom::Err::Incomplete(_) => vec![],
        nom::Err::Error(err) | nom::Err::Failure(err) => err.parser_errors(),
    }
}

pub(crate) fn report_error<'a>(
    mut parser: impl Parser<ParserInput<'a>, Statement<'a>, ParserErrors<'a>>,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Statement<'a>> {
    move |input| match parser.parse(input.clone()) {
        Ok(result) => Ok(result),
        Err(e) => {
            if input.span.fragment().is_empty() {
                return Err(e);
            };
            match &e {
                nom::Err::Incomplete(_) => (),
                nom::Err::Error(err) | nom::Err::Failure(err) => {
                    for error in err.parser_errors() {
                        input.state.report_error(error);
                    }
                }
            };
            Err(e)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        error::rich::RichError,
        parser::{Parser, ParserState, error::skip_statement, input::ParserInput},
    };

    /// Parse `program` and return the errors it reports.
    fn reported(program: &str) -> Vec<(u32, u32, String, Option<String>)> {
        match Parser::initialize(program).parse() {
            Ok(_) => Vec::new(),
            Err((_, report)) => report
                .errors()
                .iter()
                .map(|error| {
                    (
                        error.position.line,
                        error.position.column,
                        error.message(),
                        error.note(),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn reported_errors() {
        // A missing `.` is reported at the end of the statement that lacks it
        assert_eq!(
            reported("a(1).\nb(?x) :- a(?x)\nc(?y) :- b(?y).\n"),
            vec![(
                2,
                15,
                String::from("expected `.`"),
                Some(String::from("every statement must end with `.`"))
            )]
        );

        assert_eq!(
            reported("p(1).\nq(?x) <- p(?x).\n"),
            vec![(
                2,
                6,
                String::from("expected `.`"),
                Some(String::from(
                    "rules are written `head :- body`, not `head <- body`"
                ))
            )]
        );

        for program in ["r(1, .\n", "a(1,\n"] {
            assert_eq!(
                reported(program),
                vec![(1, 4, String::from("expected `)` in atom"), None)]
            );
        }

        assert_eq!(
            reported("@declare oops .\ns(1).\n"),
            vec![(
                1,
                14,
                String::from("expected `(` in declare directive"),
                None
            )]
        );

        for program in ["a(1),.\n", "a(1), b(2).\n"] {
            assert_eq!(
                reported(program),
                vec![(
                    1,
                    5,
                    String::from("expected `.`"),
                    Some(String::from("every statement must end with `.`"))
                )]
            );
        }

        assert_eq!(
            reported("!!!\np(1).\n"),
            vec![(1, 2, String::from("expected letter or digit in name"), None)]
        );

        assert_eq!(
            reported(":- b(?x).\n"),
            vec![(1, 1, String::from("expected fact, rule or directive"), None)]
        );

        for program in ["a :- ?x.\n", "a :- 1 + 2.\n"] {
            assert_eq!(
                reported(program),
                vec![(1, 3, String::from("expected expression"), None)]
            );
        }
    }

    #[test]
    fn skip_to_statement_end() {
        let test = vec![
            (
                "some text ending in newline",
                "some text ending in newline".to_string(),
            ),
            ("some text.\n More text", "some text.".to_string()),
            ("some text\n\n More text", "some text\n\n".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = skip_statement(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.to_string());
        }
    }
}
