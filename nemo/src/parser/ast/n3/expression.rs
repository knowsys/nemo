//! This module defines [N3Expression].

use path::N3Path;

use crate::{
    parser::{
        ParserResult,
        ast::ProgramAST,
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{tag::Tag, term::Term},
        translation::ASTProgramTranslation,
    },
};

pub mod collection;
pub mod formula;
pub mod literal;
pub mod path;
pub mod propertylist;
pub mod variable;

/// What to translate for
#[derive(Clone, Copy, Debug)]
pub enum TranslationFor {
    /// We are translating into (part of) a fact.
    Fact,
    /// We are translating into (part of) a rule body.
    Body,
    /// We are translating into (part of) a rule head.
    Head,
}

/// A Notation3 expression
#[derive(Clone, Debug)]
pub struct N3Expression<'a> {
    span: Span<'a>,

    pub(crate) path: N3Path<'a>,
}

impl<'a> N3Expression<'a> {
    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> (Term, Vec<(Tag, Vec<Term>)>) {
        self.path.kind().to_terms(translation, target)
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Expression);

impl<'a> ProgramAST<'a> for N3Expression<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![&self.path]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, N3Path::parse)(input).map(|(rest, path)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    path,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
