//! This module defines [N3BnodePropertyList] and [N3IriPropertyList].

use nom::{
    bytes::complete::tag,
    multi::separated_list1,
    sequence::{delimited, pair, preceded, tuple},
};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        n3::{comment::WSoC, triples::verb::N3Verb},
        tag::structure::StructureTag,
        token::Token,
    },
    context::{Notation3Context, ParserContext, context},
    input::ParserInput,
    span::Span,
};

use super::N3Expression;

/// A Notation3 Bnode property list
#[derive(Clone, Debug)]
pub struct N3BnodePropertyList<'a> {
    span: Span<'a>,

    pub(crate) pairs: Vec<(N3Verb<'a>, N3Expression<'a>)>,
}

const BNODE_CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::BnodePropertyList);

impl<'a> ProgramAST<'a> for N3BnodePropertyList<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for (verb, object) in &self.pairs {
            result.push(verb);
            result.push(object);
        }

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
            BNODE_CONTEXT,
            delimited(
                tuple((WSoC::parse, tag("["), WSoC::parse)),
                separated_list1(
                    delimited(WSoC::parse, Token::semicolon, WSoC::parse),
                    pair(
                        N3Verb::parse,
                        preceded(
                            WSoC::parse,
                            context(
                                ParserContext::notation3(Notation3Context::ObjectList),
                                separated_list1(
                                    delimited(WSoC::parse, Token::seq_sep, WSoC::parse),
                                    context(
                                        ParserContext::notation3(Notation3Context::Object),
                                        N3Expression::parse,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                tuple((WSoC::parse, tag("]"), WSoC::parse)),
            ),
        )(input)
        .map(|(rest, entries)| {
            let rest_span = rest.span;
            let mut pairs = Vec::new();

            for (verb, objects) in entries {
                for object in objects {
                    pairs.push((verb.clone(), object));
                }
            }

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    pairs,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        BNODE_CONTEXT
    }
}

/// A Notation3 Iri property list
#[derive(Clone, Debug)]
pub struct N3IriPropertyList<'a> {
    span: Span<'a>,

    pub(crate) id: StructureTag<'a>,
    pub(crate) pairs: Vec<(N3Verb<'a>, N3Expression<'a>)>,
}

const IRI_CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::IriPropertyList);

impl<'a> ProgramAST<'a> for N3IriPropertyList<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for (verb, object) in &self.pairs {
            result.push(verb);
            result.push(object);
        }

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
            IRI_CONTEXT,
            delimited(
                tuple((WSoC::parse, tag("["), WSoC::parse)),
                pair(
                    delimited(
                        delimited(WSoC::parse, tag("id"), WSoC::parse),
                        context(
                            ParserContext::notation3(Notation3Context::Iri),
                            StructureTag::parse_hyphenated,
                        ),
                        WSoC::parse,
                    ),
                    separated_list1(
                        delimited(WSoC::parse, Token::semicolon, WSoC::parse),
                        pair(
                            N3Verb::parse,
                            preceded(
                                WSoC::parse,
                                context(
                                    ParserContext::notation3(Notation3Context::ObjectList),
                                    separated_list1(
                                        delimited(WSoC::parse, Token::seq_sep, WSoC::parse),
                                        context(
                                            ParserContext::notation3(Notation3Context::Object),
                                            N3Expression::parse,
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                tuple((WSoC::parse, tag("]"), WSoC::parse)),
            ),
        )(input)
        .map(|(rest, (id, entries))| {
            let rest_span = rest.span;
            let mut pairs = Vec::new();

            for (verb, objects) in entries {
                for object in objects {
                    pairs.push((verb.clone(), object));
                }
            }

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    id,
                    pairs,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        IRI_CONTEXT
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches;

    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{
            ProgramAST,
            n3::expression::propertylist::{N3BnodePropertyList, N3IriPropertyList},
        },
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_bnode_property_list() {
        let list = r#"[ :d :e ]"#;

        let parser_input = ParserInput::new(list, ParserState::default());
        let result = all_consuming(N3BnodePropertyList::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_iri_property_list() {
        let list = r#"[ id :c :d :e ]"#;

        let parser_input = ParserInput::new(list, ParserState::default());
        let result = all_consuming(N3IriPropertyList::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
