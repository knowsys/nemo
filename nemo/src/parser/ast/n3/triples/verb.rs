//! This module defines [N3Verb].

use delegate::delegate;
use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::map,
    sequence::{delimited, preceded},
};

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST,
            n3::{
                comment::WSoC,
                expression::{N3Expression, TranslationFor},
            },
        },
        context::{Notation3Context, ParserContext},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{tag::Tag, term::Term},
        translation::ASTProgramTranslation,
    },
    syntax::n3::{
        iri::{OWL_SAME_AS, RDF_TYPE},
        translation::TRIPLES_PREDICATE,
    },
};

/// The different kinds of verbs in Notation3.
#[derive(Clone, Debug)]
pub enum N3VerbKind<'a> {
    /// A (forward) predicate expression
    Predicate(N3Expression<'a>),
    /// A backward predicate expression
    Inverse(N3Expression<'a>),
    /// the `a` shorthand
    A,
    /// A `has` expression
    Has(N3Expression<'a>),
    /// An `is … of` expression,
    IsOf(N3Expression<'a>),
    /// the `=` shorthand
    Equal,
    /// A backward implication
    ImpliedBy,
    /// A (forward) implication
    Implies,
}

impl N3VerbKind<'_> {
    /// Returns true if the verb corresponds to a forward implication.
    pub fn is_forward_implication(&self) -> bool {
        matches!(self, Self::Implies)
    }

    /// Returns true if the verb corresponds to a backward implication.
    pub fn is_backward_implication(&self) -> bool {
        matches!(self, Self::ImpliedBy)
    }

    /// Returns true if the verb corresponds to an implication.
    pub fn is_implication(&self) -> bool {
        matches!(self, Self::ImpliedBy | Self::Implies)
    }
}

/// A Notation3 Verb.
#[derive(Clone, Debug)]
pub struct N3Verb<'a> {
    span: Span<'a>,
    pub(crate) kind: N3VerbKind<'a>,
}

impl N3Verb<'_> {
    delegate! {
        to self.kind {
            /// Returns true if the verb corresponds to a forward implication.
            pub fn is_forward_implication(&self) -> bool;

            /// Returns true if the verb corresponds to a backward implication.
            pub fn is_backward_implication(&self) -> bool;

            /// Returns true if the verb corresponds to an implication.
            pub fn is_implication(&self) -> bool;
        }
    }

    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
        subject: Term,
        object: Term,
    ) -> Vec<(Tag, Vec<Term>)> {
        let mut result = Vec::new();
        let tag = Tag::new(TRIPLES_PREDICATE.to_string());

        match &self.kind {
            N3VerbKind::Predicate(expression) | N3VerbKind::Has(expression) => {
                let (predicate, mut facts) = expression.to_terms(translation, target);
                result.append(&mut facts);
                result.push((tag, vec![subject, predicate, object]));
            }
            N3VerbKind::Inverse(expression) | N3VerbKind::IsOf(expression) => {
                let (predicate, mut facts) = expression.to_terms(translation, target);
                result.append(&mut facts);
                result.push((tag, vec![object, predicate, subject]));
            }
            N3VerbKind::A => {
                result.push((tag, vec![subject, Term::constant(RDF_TYPE), object]));
            }
            N3VerbKind::Equal => {
                result.push((tag, vec![subject, Term::constant(OWL_SAME_AS), object]));
            }
            N3VerbKind::ImpliedBy | N3VerbKind::Implies => (),
        };

        result
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Verb);

impl<'a> ProgramAST<'a> for N3Verb<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match &self.kind {
            N3VerbKind::Predicate(_) | N3VerbKind::Inverse(_) => vec![],
            N3VerbKind::Has(n3_expression) | N3VerbKind::IsOf(n3_expression) => {
                n3_expression.children()
            }
            N3VerbKind::A | N3VerbKind::Equal | N3VerbKind::ImpliedBy | N3VerbKind::Implies => {
                vec![]
            }
        }
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        alt((
            map(delimited(WSoC::parse, tag("a"), WSoC::parse), |_| {
                N3VerbKind::A
            }),
            map(delimited(WSoC::parse, tag("<="), WSoC::parse), |_| {
                N3VerbKind::ImpliedBy
            }),
            map(delimited(WSoC::parse, tag("=>"), WSoC::parse), |_| {
                N3VerbKind::Implies
            }),
            map(delimited(WSoC::parse, tag("="), WSoC::parse), |_| {
                N3VerbKind::Equal
            }),
            map(
                preceded(
                    delimited(WSoC::parse, tag("has"), WSoC::parse),
                    preceded(WSoC::parse, N3Expression::parse),
                ),
                |expression| N3VerbKind::Has(expression),
            ),
            map(
                delimited(
                    delimited(WSoC::parse, tag("is"), WSoC::parse),
                    delimited(WSoC::parse, N3Expression::parse, WSoC::parse),
                    delimited(WSoC::parse, tag("of"), WSoC::parse),
                ),
                |expression| N3VerbKind::IsOf(expression),
            ),
            map(
                preceded(
                    delimited(WSoC::parse, tag("<-"), WSoC::parse),
                    preceded(WSoC::parse, N3Expression::parse),
                ),
                |expression| N3VerbKind::Inverse(expression),
            ),
            map(N3Expression::parse, |expression| {
                N3VerbKind::Predicate(expression)
            }),
        ))(input)
        .map(|(rest, verb)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind: verb,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
