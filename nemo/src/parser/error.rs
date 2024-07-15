//! This module defines the error type that is returned when the parser is unsuccessful.

use nom::Parser;
use nom_supreme::error::{GenericErrorTree, StackContext};

use super::{context::ParserContext, span::CharacterPosition, ParserInput, ParserResult};

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

// fn recover<'a, E>(
//     mut parser: impl Parser<Input<'a, 's>, Statement<'a>, E>,
//     error_msg: impl ToString,
//     context: Context,
//     _errors: ParserState<'s>,
// ) -> impl FnMut(Input<'a, 's>) -> IResult<Input<'a, 's>, Statement<'a>, E> {
//     move |input: Input<'a, 's>| match parser.parse(input) {
//         Ok(result) => Ok(result),
//         Err(err) if input.input.is_empty() => Err(err),
//         Err(nom::Err::Error(_)) | Err(nom::Err::Failure(_)) => {
//             let _err = Error {
//                 pos: Position {
//                     offset: input.input.location_offset(),
//                     line: input.input.location_line(),
//                     column: input.input.get_utf8_column() as u32,
//                 },
//                 msg: error_msg.to_string(),
//                 context: vec![context],
//             };
//             // errors.report_error(err);
//             let (rest_input, span) = skip_to_statement_end::<ErrorTree<Input<'_, '_>>>(input);
//             Ok((rest_input, Statement::Error(span)))
//         }
//         Err(err) => Err(err),
//     }
// }

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
