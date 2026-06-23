//! This module defines [N3Verb].

use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::map,
    sequence::{delimited, preceded},
};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        n3::{
            comment::WSoC,
            expression::{N3Expression, path::N3PathDirection},
        },
        tag::structure::StructureTagKind,
    },
    context::{Notation3Context, ParserContext},
    input::ParserInput,
    span::Span,
};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
const LOG_IMPLIES: &str = "http://www.w3.org/2000/10/swap/log#implies";
const LOG_IMPLIED_BY: &str = "http://www.w3.org/2000/10/swap/log#impliedBy";

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

impl<'a> From<N3Expression<'a>> for N3VerbKind<'a> {
    fn from(value: N3Expression<'a>) -> Self {
        if let Some(constant) = value.try_to_constant() {
            match constant.tag().kind() {
                StructureTagKind::Iri(iri) if iri.content() == RDF_TYPE => N3VerbKind::A,
                StructureTagKind::Iri(iri) if iri.content() == OWL_SAME_AS => N3VerbKind::Equal,
                StructureTagKind::Iri(iri) if iri.content() == LOG_IMPLIED_BY => {
                    N3VerbKind::ImpliedBy
                }
                StructureTagKind::Iri(iri) if iri.content() == LOG_IMPLIES => N3VerbKind::Implies,
                _ => N3VerbKind::Predicate(N3Expression::forward(value)),
            }
        } else {
            if value
                .path
                .items
                .first()
                .expect("path should not be empty")
                .0
                == N3PathDirection::Forward
            {
                N3VerbKind::Predicate(N3Expression::forward(value))
            } else {
                N3VerbKind::Inverse(N3Expression::backward(value))
            }
        }
    }
}

/// A Notation3 Verb.
#[derive(Clone, Debug)]
pub struct N3Verb<'a> {
    span: Span<'a>,
    kind: N3VerbKind<'a>,
}

impl<'a> From<N3Verb<'a>> for N3Expression<'a> {
    fn from(value: N3Verb<'a>) -> Self {
        match value.kind {
            N3VerbKind::Predicate(n3_expression) | N3VerbKind::Has(n3_expression) => {
                N3Expression::forward(n3_expression)
            }
            N3VerbKind::Inverse(n3_expression) | N3VerbKind::IsOf(n3_expression) => {
                N3Expression::backward(n3_expression)
            }
            N3VerbKind::A => N3Expression::from_span_and_iri(value.span, RDF_TYPE),
            N3VerbKind::Equal => N3Expression::from_span_and_iri(value.span, OWL_SAME_AS),
            N3VerbKind::ImpliedBy => N3Expression::from_span_and_iri(value.span, LOG_IMPLIED_BY),
            N3VerbKind::Implies => N3Expression::from_span_and_iri(value.span, LOG_IMPLIES),
        }
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
