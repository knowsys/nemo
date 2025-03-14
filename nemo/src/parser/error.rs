//! This module defines the error type that is returned when the parser is unsuccessful.

use std::fmt::Display;

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
    span::{CharacterPosition, Span},
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
#[derive(Clone, Debug)]
pub struct ParserError {
    /// Position where the error occurred
    pub position: CharacterPosition,
    /// Parsing stack
    pub context: Vec<ParserContext>,
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: We only use the first context to generate an error message
        f.write_fmt(format_args!("expected `{}`", self.context[0].name()))
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
    mut parser: impl Parser<ParserInput<'a>, Statement<'a>, ParserErrorTree<'a>>,
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

pub(crate) fn report_error<'a>(
    mut parser: impl Parser<ParserInput<'a>, Statement<'a>, ParserErrorTree<'a>>,
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
                    // dbg!(&err);
                    // let error = _get_deepest_error(&err);
                    // dbg!(&error);
                    // let error = match err {
                    //     GenericErrorTree::Base { location, .. } => ParserError {
                    //         position: CharacterPosition {
                    //             offset: location.span.location_offset(),
                    //             line: location.span.location_line(),
                    //             column: location.span.get_utf8_column() as u32,
                    //         },
                    //         context: vec![],
                    //     },
                    //     GenericErrorTree::Stack {
                    //         base: _base,
                    //         contexts,
                    //     } => ParserError {
                    //         position: CharacterPosition {
                    //             offset: contexts[0].0.span.location_offset(),
                    //             line: contexts[0].0.span.location_line(),
                    //             column: contexts[0].0.span.get_utf8_column() as u32,
                    //         },
                    //         context: match contexts[0].1 {
                    //             StackContext::Kind(_) => todo!(),
                    //             StackContext::Context(ctx) => {
                    //                 vec![ctx]
                    //             }
                    //         },
                    //     },
                    //     GenericErrorTree::Alt(_vec) => {
                    //         todo!()
                    //     }
                    // };
                    for error in _get_deepest_error(err) {
                        input.state.report_error(error);
                    }
                    // let (_deepest_pos, errors) = get_deepest_errors(err);
                    // for error in errors {
                    //     input.state.report_error(error);
                    // }
                }
            };
            Err(e)
        }
    }
}

/// Function to translate an [ParserErrorTree] returned by the nom parser
/// into a [ParserError] that can be displayed to the user.
pub(crate) fn _transform_error_tree<'a, Output>(
    mut parser: impl Parser<ParserInput<'a>, Output, ParserErrorTree<'a>>,
) -> impl FnMut(ParserInput<'a>) -> ParserResult<'a, Output> {
    move |input| match parser.parse(input.clone()) {
        Ok(result) => Ok(result),
        Err(e) => {
            if input.span.fragment().is_empty() {
                return Err(e);
            };
            match &e {
                nom::Err::Incomplete(_) => (),
                nom::Err::Error(err) | nom::Err::Failure(err) => {
                    let error = _get_deepest_error(err);
                    dbg!(error);
                    todo!()
                    // let (_deepest_pos, errors) = _get_deepest_errors(err);
                    // for error in errors {
                    //     input.state.report_error(error);
                    // }
                }
            };
            Err(e)
        }
    }
}

fn _context_strs(
    contexts: &[(ParserInput<'_>, StackContext<ParserContext>)],
) -> Vec<ParserContext> {
    contexts
        .iter()
        .map(|(_, c)| match c {
            StackContext::Kind(_) => todo!(),
            StackContext::Context(c) => *c,
        })
        .collect()
}

fn _get_deepest_error<'a>(e: &'a ParserErrorTree<'a>) -> Vec<ParserError> {
    match e {
        ParserErrorTree::Base { location, .. } => {
            let span = location.span;
            vec![ParserError {
                position: CharacterPosition {
                    offset: span.location_offset(),
                    line: span.location_line(),
                    column: span.get_utf8_column() as u32,
                },
                context: vec![],
            }]
        }
        ParserErrorTree::Stack { base, contexts } => {
            let mut errors = _get_deepest_error(base);
            if errors.len() == 1 {
                let error = errors.get_mut(0).expect(
                    "get deepest error called on base should return a vec with one element",
                );
                let mut context = vec![];
                context.append(&mut error.context);
                context.append(
                    &mut contexts
                        .iter()
                        .map(|(_, stack_context)| match stack_context {
                            StackContext::Kind(_) => todo!("unclear when NomErrorKind will occur"),
                            StackContext::Context(cxt) => *cxt,
                        })
                        .collect(),
                );
                vec![ParserError {
                    position: error.position,
                    context,
                }]
            } else {
                vec![ParserError {
                    position: errors[0].position,
                    context: contexts
                        .iter()
                        .map(|(_, stack_context)| match stack_context {
                            StackContext::Kind(_) => todo!("unclear when NomErrorKind will occur"),
                            StackContext::Context(cxt) => *cxt,
                        })
                        .collect(),
                }]
            }
        }
        ParserErrorTree::Alt(vec) => {
            let mut farthest_pos = CharacterPosition::default();
            let mut farthest_errors = Vec::new();
            for error in vec {
                let errors = _get_deepest_error(error);
                for inner_error in errors {
                    match inner_error.position.cmp(&farthest_pos) {
                        std::cmp::Ordering::Equal => farthest_errors.push(inner_error),
                        std::cmp::Ordering::Greater => {
                            farthest_pos = inner_error.position;
                            farthest_errors.clear();
                            farthest_errors.push(inner_error);
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
            }
            farthest_errors
            // ParserError {
            //     position: farthest_pos,
            //     context: vec![],
            // }
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
