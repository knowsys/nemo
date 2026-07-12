//! This module defines [N3Triples].

use std::{assert_matches, fmt::Debug};

use nom::{
    combinator::opt,
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, tuple},
};
use verb::N3Verb;

use crate::{
    parser::ast::{
        ProgramAST,
        n3::{
            comment::WSoC,
            expression::{N3Expression, TranslationFor},
        },
        token::Token,
    },
    rule_model::components::{
        tag::Tag,
        term::{Term, primitive::Primitive},
    },
    syntax::n3::translation::DUMMY_PREDICATE,
};
use crate::{
    parser::{
        ParserResult,
        context::{Notation3Context, ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::{
        components::{atom::Atom, fact::Fact, literal::Literal, rule::Rule},
        programs::ProgramWrite,
        translation::ASTProgramTranslation,
    },
};

pub mod verb;

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
    predicate_object: Option<(N3Verb<'a>, N3Expression<'a>)>,

    span: Span<'a>,
}

impl<'a> N3Triple<'a> {
    /// Returns true if this triple corresponds to a rule
    pub fn is_rule(&self) -> bool {
        self.predicate_object
            .as_ref()
            .map(|(verb, _)| verb.is_implication())
            .unwrap_or_default()
    }

    /// Returns true if this triple corresponds to a forward rule
    pub fn is_forward_rule(&self) -> bool {
        self.predicate_object
            .as_ref()
            .map(|(verb, _)| verb.is_forward_implication())
            .unwrap_or_default()
    }

    /// Returns true if this triple corresponds to a backward rule
    pub fn is_backward_rule(&self) -> bool {
        self.predicate_object
            .as_ref()
            .map(|(verb, _)| verb.is_backward_implication())
            .unwrap_or_default()
    }

    pub(crate) fn to_facts(&self, translation: &mut ASTProgramTranslation) -> Vec<Fact> {
        self.to_terms(translation, TranslationFor::Fact)
            .into_iter()
            .map(|(tag, terms)| Fact::new(tag, terms))
            .collect()
    }

    pub(crate) fn to_terms(
        &self,
        translation: &mut ASTProgramTranslation,
        target: TranslationFor,
    ) -> Vec<(Tag, Vec<Term>)> {
        assert!(!self.is_rule(), "nested rules are not supported");
        let (subject, mut result) = self.subject.to_terms(translation, target);

        match &self.predicate_object {
            None if result.is_empty() => log::warn!(
                "ignoring simple subject in subject-only triple: {:?}",
                self.subject
            ),
            None => (),
            Some((verb, object)) => {
                let (object, mut object_facts) = object.to_terms(translation, target);
                result.append(&mut object_facts);
                result.append(&mut verb.to_terms(translation, target, subject, object));
            }
        };

        result
    }

    pub(crate) fn to_body_literals(&self, translation: &mut ASTProgramTranslation) -> Vec<Literal> {
        assert!(self.is_rule());
        assert_matches!(self.predicate_object, Some(_));

        let formula = if self.is_forward_rule() {
            &self.subject
        } else {
            &self.predicate_object.as_ref().expect("is present").1
        };

        let (_, mut literals) = formula.to_terms(translation, TranslationFor::Body);

        if literals.is_empty() {
            literals.push((
                Tag::new(DUMMY_PREDICATE.to_string()),
                vec![Term::Primitive(Primitive::anonymous_variable())],
            ));
        }

        literals
            .into_iter()
            .map(|(tag, terms)| Literal::Positive(Atom::new(tag, terms)))
            .collect()
    }

    pub(crate) fn to_head_atoms(&self, translation: &mut ASTProgramTranslation) -> Vec<Atom> {
        assert!(self.is_rule());
        assert_matches!(self.predicate_object, Some(_));

        let formula = if self.is_backward_rule() {
            &self.subject
        } else {
            &self.predicate_object.as_ref().expect("is present").1
        };

        let (_, literals) = formula.to_terms(translation, TranslationFor::Head);

        literals
            .into_iter()
            .map(|(tag, terms)| Atom::new(tag, terms))
            .collect()
    }

    pub(crate) fn add_to_program<Writer: Debug + ProgramWrite>(
        &self,
        program: &mut Writer,
        translation: &mut ASTProgramTranslation,
    ) {
        if !self.is_rule() {
            for fact in self.to_facts(translation) {
                log::debug!("adding fact: {fact}");
                program.add_fact(fact);
            }
        } else {
            let body = self.to_body_literals(translation);
            let head = self.to_head_atoms(translation);
            let rule = Rule::new(head, body);
            log::debug!("adding rule: {rule}");
            program.add_rule(rule);
        }
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Triple);

impl<'a> ProgramAST<'a> for N3Triple<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        result.push(&self.subject);

        if let Some((verb, object)) = &self.predicate_object {
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
            CONTEXT,
            tuple((
                delimited(WSoC::parse, N3Expression::parse, WSoC::parse),
                delimited(WSoC::parse, N3Verb::parse, WSoC::parse),
                delimited(WSoC::parse, N3Expression::parse, WSoC::parse),
            )),
        )(input)
        .map(|(rest, (subject, verb, object))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    subject,
                    predicate_object: Some((verb, object)),
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
                                opt(tuple((
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
                                ))),
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
                    let span = input_span.until_rest(&rest_span);

                    triples.push(N3Triple {
                        subject: subject.clone(),
                        predicate_object: None,
                        span,
                    });
                }

                for predicate_object in object_predicate_list {
                    if let Some((verb, objects)) = predicate_object {
                        for object in objects {
                            triples.push(N3Triple {
                                subject: subject.clone(),
                                predicate_object: Some((verb.clone(), object)),
                                span: input_span.until_rest(&rest_span),
                            });
                        }
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
