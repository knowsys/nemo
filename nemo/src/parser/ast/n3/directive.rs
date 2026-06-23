//! This module defines [N3Directive].

use crate::parser::ast::{ProgramAST, directive::Directive};

/// A wrapper restricting [Directive] to those that may occur in an [N3Graph].
#[derive(Debug)]
pub struct N3Directive<'a>(Directive<'a>);

impl<'a> N3Directive<'a> {
    /// Wrap a [Directive], if it is one of the forms allowed in Notation3.
    pub fn new<'b: 'a>(directive: Directive<'b>) -> Option<Self> {
        match directive {
            Directive::Base(_) | Directive::Prefix(_) => Some(Self(directive)),
            _ => None,
        }
    }
}

impl<'a> ProgramAST<'a> for N3Directive<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![&self.0]
    }

    fn span(&self) -> crate::parser::span::Span<'a> {
        self.0.span()
    }

    fn parse(input: crate::parser::input::ParserInput<'a>) -> crate::parser::ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let (rest, directive) = Directive::parse(input)?;
        let result = Self::new(directive).ok_or_else(|| todo!())?;

        Ok((rest, result))
    }

    fn context(&self) -> crate::parser::context::ParserContext {
        self.0.context()
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use std::assert_matches;
    use test_log::test;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, n3::directive::N3Directive},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_prefix() {
        let graph = r#"@prefix : <http://example.com/>"#;

        let parser_input = ParserInput::new(graph, ParserState::default());
        let result = all_consuming(N3Directive::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_base() {
        let graph = r#"@base <http://example.com/>"#;

        let parser_input = ParserInput::new(graph, ParserState::default());
        let result = all_consuming(N3Directive::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
