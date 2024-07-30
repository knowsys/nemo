//! This module defines [Map].

use nom::{
    combinator::opt,
    sequence::{delimited, pair, terminated},
};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC,
        sequence::{key_value::KeyValuePair, Sequence},
        tag::structure::StructureTag,
        token::Token,
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// A possibly tagged sequence of [Expression]s.
#[derive(Debug)]
pub struct Map<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Tag of this map, if it exists
    tag: Option<StructureTag<'a>>,
    /// List of key-value pairs
    key_value: Sequence<'a, KeyValuePair<'a>>,
}

impl<'a> Map<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn key_value(&self) -> impl Iterator<Item = &KeyValuePair> {
        self.key_value.iter()
    }

    /// Return the tag of this Map.
    pub fn tag(&self) -> Option<&StructureTag<'a>> {
        self.tag.as_ref()
    }
}

const CONTEXT: ParserContext = ParserContext::Map;

impl<'a> ProgramAST<'a> for Map<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result: Vec<&dyn ProgramAST> = vec![];

        if let Some(tag) = &self.tag {
            result.push(tag)
        }

        for pair in &self.key_value {
            result.push(pair);
        }
        // result.push(&key_value);

        result
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            pair(
                opt(terminated(StructureTag::parse, opt(WSoC::parse))),
                delimited(
                    pair(Token::map_open, WSoC::parse),
                    Sequence::<KeyValuePair>::parse,
                    pair(WSoC::parse, Token::map_close),
                ),
            ),
        )(input)
        .map(|(rest, (tag, key_value))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    tag,
                    key_value,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::complex::map::Map, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_map() {
        let test = vec![
            ("{?x: 7}", (None, 1)),
            ("abc { ?x: 7 }", (Some("abc".to_string()), 1)),
            (
                "abc { ?x: 7, ?y: 12, ?z: 13 }",
                (Some("abc".to_string()), 3),
            ),
            (
                "abc { ?x : 7 ,  ?y    :   13  , ?z       : 15   }",
                (Some("abc".to_string()), 3),
            ),
            ("{a:1, b: POW(1, 2)}", (None, 2)),
            ("{a:b, c:d,}", (None, 2)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Map::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (
                    result.1.tag().as_ref().map(|tag| tag.to_string()),
                    result.1.key_value().count()
                )
            );
        }
    }
}
