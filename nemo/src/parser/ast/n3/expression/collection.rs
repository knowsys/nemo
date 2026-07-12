//! This module defines [N3Collection].

use nom::{
    multi::separated_list0,
    sequence::{delimited, tuple},
};

use crate::{
    parser::{
        ParserResult,
        ast::{ProgramAST, n3::comment::WSoC, token::Token},
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{
            tag::Tag,
            term::{Term, primitive::Primitive},
        },
        translation::ASTProgramTranslation,
    },
    syntax::n3::translation::COLLECTION_PREDICATE_BASE,
};

use super::{N3Expression, TranslationFor};

/// A Notation3 collection.
#[derive(Clone, Debug)]
pub struct N3Collection<'a> {
    span: Span<'a>,

    members: Vec<N3Expression<'a>>,
}

impl<'a> N3Collection<'a> {
    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> (Primitive, Vec<(Tag, Vec<Term>)>) {
        let arity = self.members.len();
        let tag = Tag::new(format!("{COLLECTION_PREDICATE_BASE}_{arity}"));
        let term = translation.fresh_bnode_or_variable(target);
        let mut terms = Vec::new();
        let mut members = vec![Term::Primitive(term.clone())];

        for member in &self.members {
            let (member, mut member_terms) = member.to_terms(translation, target);
            members.push(member);
            terms.append(&mut member_terms);
        }

        terms.push((tag, members));

        (term, terms)
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Collection);

impl<'a> ProgramAST<'a> for N3Collection<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        self.members
            .iter()
            .flat_map(|item| item.children())
            .collect()
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
                tuple((WSoC::parse, Token::tuple_open, WSoC::parse)),
                separated_list0(WSoC::parse_required, N3Expression::parse),
                tuple((WSoC::parse, Token::tuple_close, WSoC::parse)),
            ),
        )(input)
        .map(|(rest, members)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    members,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
