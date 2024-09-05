//! This module defines the error type that is returned when the parser is unsuccessful.

use nom::{
    branch::alt,
    bytes::complete::{take_until, take_while},
    character::complete::line_ending,
    combinator::map,
    sequence::{preceded, terminated},
    Parser,
};
use nom_supreme::error::{GenericErrorTree, StackContext};

use super::{
    ast::{
        statement::{Statement, StatementKind},
        token::Token,
    },
    context::ParserContext,
    span::CharacterPosition,
    ParserInput, ParserResult,
};

/// Error tree used by nom parser
pub type ParserErrorTree<'a> = GenericErrorTree<
    ParserInput<'a>,
    &'static str,
    ParserContext,
    Box<dyn std::error::Error + Send + Sync + 'static>,
>;

/// Error while parsing a nemo program
#[derive(Debug)]
pub(crate) struct ParserError {
    /// Position where the error occurred
    pub(crate) position: CharacterPosition,
    /// Parsing stack
    pub(crate) context: Vec<ParserContext>,
}

/// Skip a statement, returning an error token.
pub(crate) fn skip_statement<'a>(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
    let input_span = input.span;

    let until_double_newline = map(
        alt((
            preceded(take_until("\n\n"), Token::double_newline),
            preceded(take_until("\r\n\r\n"), Token::double_newline),
            preceded(take_until("\r\r"), Token::double_newline),
        )),
        move |token| Token::error(input_span.enclose(&input_span, &token.span())),
    );
    // TODO: Should there additional whitespace be allowed in-between the dot and the newline?
    let until_dot_newline = map(
        alt((
            preceded(take_until(".\n"), terminated(Token::dot, line_ending)),
            preceded(take_until(".\r\n"), terminated(Token::dot, line_ending)),
            preceded(take_until(".\r"), terminated(Token::dot, line_ending)),
        )),
        move |token| Token::error(input_span.enclose(&input_span, &token.span())),
    );
    let until_eof = map(take_while(|_| true), move |_| Token::error(input_span));

    alt((until_dot_newline, until_double_newline, until_eof))(input)
}

pub(crate) fn recover<'a>(
    mut parser: impl Parser<ParserInput<'a>, Statement<'a>, ParserErrorTree<'a>>,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Statement<'a>> {
    move |input: ParserInput<'a>| match parser.parse(input.clone()) {
        Ok((rest, statement)) => Ok((rest, statement)),
        Err(err) if input.span.0.is_empty() => Err(err),
        Err(nom::Err::Error(_)) | Err(nom::Err::Failure(_)) => {
            let (rest_input, token) = skip_statement(input).expect("this parser cannot fail");
            Ok((
                rest_input,
                Statement {
                    span: token.span(),
                    comment: None,
                    kind: StatementKind::Error(token),
                },
            ))
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn report_error<'a>(
    mut parser: impl Parser<ParserInput<'a>, Statement<'a>, ParserErrorTree<'a>>,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Statement<'a>> {
    move |input| match parser.parse(input.clone()) {
        Ok(result) => Ok(result),
        Err(e) => {
            if input.span.0.is_empty() {
                return Err(e);
            };
            match &e {
                nom::Err::Incomplete(_) => (),
                nom::Err::Error(err) | nom::Err::Failure(err) => {
                    let (_deepest_pos, errors) = get_deepest_errors(err);
                    for error in errors {
                        input.state.report_error(error);
                    }
                }
            };
            Err(e)
        }
    }
}

/// Function to translate an [ParserErrorTree] returned by the nom parser
/// into a [ParserError] that can be displayed to the user.
pub(crate) fn transform_error_tree<'a, Output>(
    mut parser: impl Parser<ParserInput<'a>, Output, ParserErrorTree<'a>>,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Output> {
    move |input| match parser.parse(input.clone()) {
        Ok(result) => Ok(result),
        Err(e) => {
            if input.span.0.is_empty() {
                return Err(e);
            };
            match &e {
                nom::Err::Incomplete(_) => (),
                nom::Err::Error(err) | nom::Err::Failure(err) => {
                    let (_deepest_pos, errors) = get_deepest_errors(err);
                    for error in errors {
                        input.state.report_error(error);
                    }
                }
            };
            Err(e)
        }
    }
}

fn context_strs(
    contexts: &Vec<(ParserInput<'_>, StackContext<ParserContext>)>,
) -> Vec<ParserContext> {
    contexts
        .iter()
        .map(|(_, c)| match c {
            StackContext::Kind(_) => todo!(),
            StackContext::Context(c) => *c,
        })
        .collect()
}

fn get_deepest_errors<'a, 's>(e: &'a ParserErrorTree<'a>) -> (CharacterPosition, Vec<ParserError>) {
    match e {
        ParserErrorTree::Base { location, .. } => {
            let span = location.span.0;
            let err_pos = CharacterPosition {
                offset: span.location_offset(),
                line: span.location_line(),
                column: span.get_utf8_column() as u32,
            };
            (
                err_pos,
                vec![ParserError {
                    position: err_pos,
                    context: Vec::new(),
                }],
            )
        }
        ParserErrorTree::Stack { base, contexts } => {
            // let mut err_pos = Position::default();
            match &**base {
                ParserErrorTree::Base { location, .. } => {
                    let span = location.span.0;
                    let err_pos = CharacterPosition {
                        offset: span.location_offset(),
                        line: span.location_line(),
                        column: span.get_utf8_column() as u32,
                    };
                    let mut msg = String::from("");
                    for (_, context) in contexts {
                        match context {
                            StackContext::Kind(_) => todo!(),
                            StackContext::Context(c) => match c {
                                ParserContext::Token { kind: t } => {
                                    msg.push_str(&t.name());
                                }
                                _ => (),
                            },
                        }
                    }
                    (
                        err_pos,
                        vec![ParserError {
                            position: err_pos,
                            context: context_strs(contexts),
                        }],
                    )
                }
                ParserErrorTree::Stack { base, contexts } => {
                    let (pos, mut deepest_errors) = get_deepest_errors(base);
                    let contexts = context_strs(contexts);
                    for error in &mut deepest_errors {
                        error.context.append(&mut contexts.clone());
                    }
                    (pos, deepest_errors)
                }
                ParserErrorTree::Alt(_error_tree) => {
                    let (pos, mut deepest_errors) = get_deepest_errors(base);
                    let contexts = context_strs(contexts);
                    for error in &mut deepest_errors {
                        error.context.append(&mut contexts.clone());
                    }
                    (pos, deepest_errors)
                }
            }
        }
        ParserErrorTree::Alt(vec) => {
            let mut return_vec: Vec<ParserError> = Vec::new();
            let mut deepest_pos = CharacterPosition::default();
            for error in vec {
                let (pos, mut deepest_errors) = get_deepest_errors(error);
                if pos > deepest_pos {
                    deepest_pos = pos;
                    return_vec.clear();
                    return_vec.append(&mut deepest_errors);
                } else if pos == deepest_pos {
                    return_vec.append(&mut deepest_errors);
                }
            }
            (deepest_pos, return_vec)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::parser::{error::skip_statement, input::ParserInput, ParserState};

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
