//! This module implements utility functions for parsing program components.

use std::fmt::{Display, Pointer};

use crate::rule_model::error::TranslationError;

#[derive(Debug)]
pub enum ComponentParseError {
    /// Parse Error
    ParseError,
    /// Translation Error
    TranslationError(TranslationError),
}

impl Display for ComponentParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentParseError::ParseError => f.write_str("error while parsing string"),
            ComponentParseError::TranslationError(error) => error.fmt(f),
        }
    }
}

#[macro_export]
macro_rules! parse_component {
    ($string:expr, $parser:expr, $builder:expr) => {
        'parse: {
            use nom::InputLength;

            let input = $crate::parser::input::ParserInput::new(
                $string,
                $crate::parser::ParserState::default(),
            );
            let ast = match $parser(input) {
                Ok((input, ast)) => {
                    if input.input_len() == 0 {
                        ast
                    } else {
                        break 'parse Err(
                            $crate::rule_model::components::parse::ComponentParseError::ParseError,
                        );
                    }
                }
                Err(_) => {
                    break 'parse Err(
                        $crate::rule_model::components::parse::ComponentParseError::ParseError,
                    )
                }
            };

            let mut translation = ASTProgramTranslation::initialize($string, String::default());

            match $builder(&mut translation, &ast) {
                Ok(component) => Ok(component),
                Err(error) => Err(
                    $crate::rule_model::components::parse::ComponentParseError::TranslationError(
                        error,
                    ),
                ),
            }
        }
    };
}
