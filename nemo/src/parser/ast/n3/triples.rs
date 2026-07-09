//! This module defines [N3Triples].

use std::fmt::Debug;

use nom::{
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, tuple},
};
use verb::{N3Verb, N3VerbKind};

use crate::{
    parser::{
        ParserResult,
        ast::{
            self, ProgramAST,
            expression::{self},
            guard::Guard,
            n3::{comment::WSoC, expression::N3Expression},
            sequence::simple::ExpressionSequenceSimple,
            statement::{Statement, StatementKind},
            tag::structure::StructureTag,
            token::{Token, TokenKind},
        },
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{atom::Atom, fact::Fact, literal::Literal, rule::Rule, tag::Tag, term::Term},
        programs::ProgramWrite,
        translation::ASTProgramTranslation,
    },
};

use super::expression::BnodeTarget;

pub mod verb;

const TRIPLES_PREDICATE_NAME: &str = "_TRIPLES";
const TRIPLES_PREDICATE_SPAN: Span<'static> = Span::new(TRIPLES_PREDICATE_NAME);
const TRIPLES_PREDICATE: StructureTag = StructureTag::from_span_and_token(
    TRIPLES_PREDICATE_SPAN,
    Token::from_span_and_kind(TRIPLES_PREDICATE_SPAN, TokenKind::Name),
);

/// The Body of a Notation3 rule.
#[derive(Clone, Copy, Debug)]
pub struct N3Body<'a>(pub &'a N3Expression<'a>);

/// The Head of a Notation3 rule.
#[derive(Clone, Copy, Debug)]
pub struct N3Head<'a>(pub &'a N3Expression<'a>);

/// A Triple of Notation3.
#[derive(Clone, Debug)]
pub struct N3Triple<'a> {
    subject: N3Expression<'a>,
    predicate: N3Expression<'a>,
    object: N3Expression<'a>,

    span: Span<'a>,
}

impl<'a> N3Triple<'a> {
    /// Returns true if this triple corresponds to a rule
    pub fn is_rule(&self) -> bool {
        N3VerbKind::from(self.predicate.clone()).is_implication()
    }

    /// Returns true if this triple corresponds to a forward rule
    pub fn is_forward_rule(&self) -> bool {
        N3VerbKind::from(self.predicate.clone()).is_forward_implication()
    }

    /// Returns true if this triple corresponds to a backward rule
    pub fn is_backward_rule(&self) -> bool {
        N3VerbKind::from(self.predicate.clone()).is_backward_implication()
    }

    pub(crate) fn add_to_program<Writer: Debug + ProgramWrite>(
        &self,
        program: &mut Writer,
        translation: &mut ASTProgramTranslation,
    ) {
        let predicate = Tag::new("_TRIPLES".to_string());

        if !self.is_rule() {
            let fact = Fact::new(
                predicate,
                [
                    Term::Primitive(
                        self.subject
                            .to_primitive(translation, BnodeTarget::Constant)
                            .expect("is a valid primitive"),
                    ),
                    Term::Primitive(
                        self.predicate
                            .to_primitive(translation, BnodeTarget::Constant)
                            .expect("is a valid primitive"),
                    ),
                    Term::Primitive(
                        self.object
                            .to_primitive(translation, BnodeTarget::Constant)
                            .expect("is a valid primitive"),
                    ),
                ],
            );
            program.add_fact(fact);
        } else {
            let (body, head) = if self.is_forward_rule() {
                (self.subject.clone(), self.object.clone())
            } else {
                (self.object.clone(), self.subject.clone())
            };

            let mut rule_body = body
                .try_into_formula()
                .expect("body is a valid formula")
                .iter()
                .flat_map(|statement| {
                    statement
                        .clone()
                        .try_into_triples()
                        .expect("formula contains only triples")
                        .iter()
                        .map(|triple| {
                            Literal::Positive(Atom::new(
                                predicate.clone(),
                                [
                                    Term::Primitive(
                                        triple
                                            .subject
                                            .to_primitive(translation, BnodeTarget::Universal)
                                            .expect("is a valid primitive"),
                                    ),
                                    Term::Primitive(
                                        triple
                                            .predicate
                                            .to_primitive(translation, BnodeTarget::Universal)
                                            .expect("is a valid primitive"),
                                    ),
                                    Term::Primitive(
                                        triple
                                            .object
                                            .to_primitive(translation, BnodeTarget::Universal)
                                            .expect("is a valid primitive"),
                                    ),
                                ],
                            ))
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            if rule_body.is_empty() {
                rule_body.push(Literal::Positive(Atom::new(
                    Tag::new("_dummy".to_string()),
                    [Term::anonymous_variable()],
                )));
            }

            let rule_head = head
                .try_into_formula()
                .expect("head is a valid formula")
                .iter()
                .flat_map(|statement| {
                    statement
                        .clone()
                        .try_into_triples()
                        .expect("formula contains only triples")
                        .iter()
                        .map(|triple| {
                            Atom::new(
                                predicate.clone(),
                                [
                                    Term::Primitive(
                                        triple
                                            .subject
                                            .to_primitive(translation, BnodeTarget::Existential)
                                            .expect("is a valid primitive"),
                                    ),
                                    Term::Primitive(
                                        triple
                                            .predicate
                                            .to_primitive(translation, BnodeTarget::Existential)
                                            .expect("is a valid primitive"),
                                    ),
                                    Term::Primitive(
                                        triple
                                            .object
                                            .to_primitive(translation, BnodeTarget::Existential)
                                            .expect("is a valid primitive"),
                                    ),
                                ],
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            program.add_rule(Rule::new(rule_head, rule_body));
        }
    }

    /// Returns the rule body and rule head if this triple corresponds to a rule.
    pub fn try_into_rule(self) -> Option<ast::rule::Rule<'a>> {
        if !self.is_rule() {
            return None;
        }

        let (body, head) = if self.is_forward_rule() {
            (self.subject, self.object)
        } else {
            (self.object, self.subject)
        };

        let rule_body = body
            .try_into_formula()
            .expect("body is a valid formula")
            .try_into_guards();

        let rule_head = head
            .try_into_formula()
            .expect("head is a valid formula")
            .try_into_guards();

        Some(ast::rule::Rule::from_span_body_and_head(
            self.span, rule_body, rule_head,
        ))
    }

    /// Convert this triple into a [Statement].
    pub fn try_into_statement(self) -> Option<Statement<'a>> {
        let span = self.span.clone();
        if self.is_rule() {
            let Some(rule) = self.try_into_rule() else {
                unreachable!()
            };

            Some(Statement {
                kind: StatementKind::Rule(rule),
                attributes: Vec::new(),
                comment: None,
                span,
            })
        } else {
            let atom = expression::complex::atom::Atom::from(self);

            Some(Statement {
                kind: StatementKind::Fact(Guard::from_atom(atom)),
                attributes: Vec::new(),
                comment: None,
                span,
            })
        }
    }
}

impl<'a> From<N3Triple<'a>> for expression::complex::atom::Atom<'a> {
    fn from(value: N3Triple<'a>) -> Self {
        let subject = value
            .subject
            .try_to_expression()
            .expect("is a valid expression");
        let predicate = value
            .predicate
            .try_to_expression()
            .expect("is a valid expression");
        let object = value
            .object
            .try_to_expression()
            .expect("is a valid expression");

        Self {
            span: value.span.clone(),
            tag: TRIPLES_PREDICATE.clone(),
            expressions: ExpressionSequenceSimple::from_span_and_expressions(
                value.span,
                vec![subject, predicate, object],
            ),
        }
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Triple);

impl<'a> ProgramAST<'a> for N3Triple<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![&self.subject, &self.predicate, &self.object]
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
            tuple((
                delimited(WSoC::parse, N3Expression::parse, WSoC::parse),
                delimited(WSoC::parse, N3Expression::parse, WSoC::parse),
                delimited(WSoC::parse, N3Expression::parse, WSoC::parse),
            )),
        )(input)
        .map(|(rest, (subject, predicate, object))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    subject,
                    predicate,
                    object,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

/// An expression consisting of, potentially, several triples.
#[derive(Clone, Debug)]
pub struct N3Triples<'a> {
    span: Span<'a>,
    triples: Vec<N3Triple<'a>>,
}

impl<'a> N3Triples<'a> {
    /// An iterator over the triples.
    pub fn iter(&self) -> impl Iterator<Item = &N3Triple<'a>> {
        self.triples.iter()
    }

    /// The contained triples
    pub fn triples(&self) -> Vec<N3Triple<'a>> {
        self.triples.clone()
    }

    /// Convert these triples into [Statement]s.
    pub fn into_statements(self) -> Vec<Statement<'a>> {
        self.triples
            .into_iter()
            .flat_map(|triple| triple.try_into_statement())
            .collect()
    }
}

const TRIPLES_CONTEXT: ParserContext = ParserContext::Notation3 {
    kind: Notation3Context::Triples,
};

impl<'a> ProgramAST<'a> for N3Triples<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        self.triples
            .iter()
            .flat_map(|triple| triple.children())
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
            TRIPLES_CONTEXT,
            separated_list1(
                delimited(WSoC::parse, Token::dot, WSoC::parse),
                tuple((
                    context(
                        ParserContext::notation3(Notation3Context::Subject),
                        N3Expression::parse,
                    ),
                    preceded(
                        WSoC::parse,
                        separated_list0(
                            delimited(WSoC::parse, Token::semicolon, WSoC::parse),
                            context(
                                ParserContext::notation3(Notation3Context::PredicateObjectList),
                                tuple((
                                    N3Verb::parse,
                                    preceded(
                                        WSoC::parse,
                                        context(
                                            ParserContext::notation3(Notation3Context::ObjectList),
                                            separated_list1(
                                                delimited(WSoC::parse, Token::seq_sep, WSoC::parse),
                                                context(
                                                    ParserContext::notation3(
                                                        Notation3Context::Object,
                                                    ),
                                                    N3Expression::parse,
                                                ),
                                            ),
                                        ),
                                    ),
                                )),
                            ),
                        ),
                    ),
                )),
            ),
        )(input)
        .map(|(rest, expressions)| {
            let rest_span = rest.span;
            let mut triples = Vec::new();

            for (subject, object_predicate_list) in expressions {
                if object_predicate_list.is_empty() {
                    log::debug!("subject: {subject:?} rest: {rest:?}");
                    unimplemented!("subject-only triples not yet implemented") // TODO(MX): implement `[ :p :o ].`
                }

                for (verb, objects) in object_predicate_list {
                    let predicate = N3Expression::from(verb);

                    for object in objects {
                        triples.push(N3Triple {
                            subject: subject.clone(),
                            predicate: predicate.clone(),
                            object,
                            span: input_span.until_rest(&rest_span),
                        });
                    }
                }
            }

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    triples,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        TRIPLES_CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use std::assert_matches;
    use test_log::test;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, n3::triples::N3Triples},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_triples() {
        // from the N3 Test suite
        let triple = r#":doerthe a :Person"#;

        let parser_input = ParserInput::new(triple, ParserState::default());
        let result = all_consuming(N3Triples::parse)(parser_input);

        assert_matches!(result, Ok(_));
        assert_eq!(result.unwrap().1.triples.len(), 1);

        let triples = r#":doerthe a :Person, :human ; :foo :bar"#;

        let parser_input = ParserInput::new(triples, ParserState::default());
        let result = all_consuming(N3Triples::parse)(parser_input);

        assert_matches!(result, Ok(_));
        assert_eq!(result.unwrap().1.triples.len(), 3);

        let rule = r#"{} => { :foo :bar :quux }"#;

        let parser_input = ParserInput::new(rule, ParserState::default());
        let result = all_consuming(N3Triples::parse)(parser_input);

        assert_matches!(result, Ok(_));
        assert_eq!(result.unwrap().1.triples.len(), 1);
    }
}
