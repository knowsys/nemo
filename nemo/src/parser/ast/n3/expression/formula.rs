//! This module defines [N3Formula].

use nom::{
    bytes::complete::tag,
    combinator::opt,
    multi::separated_list0,
    sequence::{delimited, pair},
};

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST,
            n3::{
                comment::WSoC,
                statement::{N3Statement, N3StatementKind},
            },
            token::Token,
        },
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{tag::Tag, term::Term},
        translation::ASTProgramTranslation,
    },
};

use super::TranslationFor;

#[derive(Clone, Debug)]
/// A Notation3 formula.
pub struct N3Formula<'a> {
    span: Span<'a>,

    content: Vec<N3Statement<'a>>,
}

impl<'a> N3Formula<'a> {
    /// An iterator over the statements in this formula.
    pub fn iter(&'a self) -> impl Iterator<Item = &'a N3Statement<'a>> + use<'a> {
        self.content.iter()
    }

    /// The statements in this formula.
    pub fn statements(&'a self) -> Vec<N3Statement<'a>> {
        self.content.clone()
    }

    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> Vec<(Tag, Vec<Term>)> {
        match target {
            TranslationFor::Fact => {
                unimplemented!("cannot translate a formula into terms for a fact")
            }
            TranslationFor::Body | TranslationFor::Head => {
                let mut terms = Vec::new();

                for statement in &self.statements() {
                    match statement.kind() {
                        N3StatementKind::Triples(triples) => {
                            for triple in triples.iter() {
                                terms.append(&mut triple.to_terms(translation, target));
                            }
                        }
                        N3StatementKind::Directive(directive) => {
                            log::warn!("ignoring directive {directive:?} in formula; not supported")
                        }
                        N3StatementKind::Error(token) => {
                            log::warn!("ignoring erroneous statement: {token:?}")
                        }
                    }
                }

                terms
            }
        }
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Formula);

impl<'a> ProgramAST<'a> for N3Formula<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for child in &self.content {
            result.push(child);
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
            CONTEXT,
            delimited(
                delimited(WSoC::parse, tag("{"), WSoC::parse),
                separated_list0(
                    delimited(WSoC::parse, Token::dot, WSoC::parse),
                    N3Statement::parse,
                ),
                pair(
                    opt(pair(WSoC::parse, Token::dot)),
                    delimited(WSoC::parse, tag("}"), WSoC::parse),
                ),
            ),
        )(input)
        .map(|(rest, content)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    content,
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
    use std::assert_matches;

    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, n3::expression::formula::N3Formula},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_paths() {
        let formula = r#"{ ?x a :Person }"#;

        let parser_input = ParserInput::new(formula, ParserState::default());
        let result = all_consuming(N3Formula::parse)(parser_input);

        assert_matches!(result, Ok(_));
        let formula = r#"{ ?x a :Person .}"#;

        let parser_input = ParserInput::new(formula, ParserState::default());
        let result = all_consuming(N3Formula::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
