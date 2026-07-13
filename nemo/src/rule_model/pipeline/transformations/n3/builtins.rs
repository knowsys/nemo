//! This module defines [TransformationN3Builtins].

use std::collections::HashSet;

use crate::{
    rule_model::{
        components::{
            atom::Atom,
            literal::Literal,
            rule::Rule,
            statement::Statement,
            tag::Tag,
            term::{
                Term,
                operation::{Operation, operation_kind::OperationKind},
                primitive::Primitive,
            },
        },
        error::ValidationReport,
        pipeline::transformations::n3::tag_from_term,
        programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    },
    syntax::n3::{
        iri::builtins::{MATH_NOT_LESS_THAN, MATH_SUM},
        translation::{COLLECTION_PREDICATE_BASE, DUMMY_PREDICATE, TRIPLES_PREDICATE},
    },
};

use super::ProgramTransformation;

/// Builtins transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationN3Builtins {
    variable_index: usize,
}

impl TransformationN3Builtins {
    /// Create a new [TransformationN3Builtins].
    pub fn new() -> Self {
        Self { variable_index: 0 }
    }

    fn fresh_variable(&mut self) -> Term {
        let index = self.variable_index;
        self.variable_index += 1;

        Term::Primitive(Primitive::universal_variable(&format!("_variable_{index}")))
    }

    fn collection_members(&mut self, collection: &Atom) -> (Vec<Term>, Vec<Literal>) {
        let mut terms = Vec::new();
        let mut literals = Vec::new();

        if collection
            .predicate()
            .name()
            .starts_with(COLLECTION_PREDICATE_BASE)
        {
            let collection_tag = collection.predicate();
            let (_, arity) = collection_tag
                .name()
                .rsplit_once("_")
                .expect("contains an underscore");
            let arity = arity.parse::<usize>().expect("is a valid arity");
            let members = collection.terms().collect::<Vec<_>>();

            for idx in 1..=arity {
                let variable = self.fresh_variable();
                terms.push(variable.clone());
                literals.push(Literal::Operation(Operation::new(
                    OperationKind::Equal,
                    vec![variable, members[idx].clone()],
                )));
            }
        } else {
            log::warn!("trying to obtain members of non-collection {collection}");
        }

        (terms, literals)
    }

    fn translate_builtin(
        &mut self,
        positive: Vec<&Atom>,
        atom: &Atom,
    ) -> (Vec<Literal>, HashSet<Atom>) {
        let mut literals = Vec::new();
        let mut drop = HashSet::new();
        let terms = atom.terms().collect::<Vec<_>>();
        assert_eq!(terms.len(), 3);

        let subject = terms[0].clone();
        let object = terms[2].clone();

        if let Some(predicate) = tag_from_term(terms[1]) {
            match predicate.name() {
                MATH_SUM => {
                    let collection = positive
                        .iter()
                        .find(|atom| {
                            atom.predicate()
                                .name()
                                .starts_with(COLLECTION_PREDICATE_BASE)
                                && *atom.terms().next().expect("is not empty") == subject
                        })
                        .cloned()
                        .cloned()
                        .expect("collection is bound");
                    drop.insert(collection.clone());

                    let (members, mut member_literals) = self.collection_members(&collection);
                    literals.append(&mut member_literals);
                    literals.push(Literal::Operation(Operation::new(
                        OperationKind::Equal,
                        vec![
                            object,
                            Term::Operation(Operation::new(OperationKind::NumericSum, members)),
                        ],
                    )));
                }
                MATH_NOT_LESS_THAN => literals.push(Literal::Operation(Operation::new(
                    OperationKind::NumericGreaterthaneq,
                    vec![subject, object],
                ))),
                _ => literals.push(Literal::Positive(atom.clone())),
            }
        } else {
            literals.push(Literal::Positive(atom.clone()));
        }

        (literals, drop)
    }
}

impl ProgramTransformation for TransformationN3Builtins {
    fn apply(mut self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    let head = rule.head().clone();
                    let mut body = Vec::new();
                    let mut drop = HashSet::new();

                    for literal in rule.body() {
                        match literal {
                            Literal::Positive(atom)
                                if atom.predicate().name() == TRIPLES_PREDICATE =>
                            {
                                let (mut literals, drops) =
                                    self.translate_builtin(rule.body_positive().collect(), atom);
                                body.append(&mut literals);
                                drop.extend(drops.into_iter());
                            }
                            Literal::Positive(_) => body.push(literal.clone()),
                            Literal::Negative(_) => body.push(literal.clone()),
                            Literal::Operation(_) => body.push(literal.clone()),
                        }
                    }

                    let mut body = body
                        .into_iter()
                        .filter(|literal| match literal {
                            Literal::Positive(atom) => !drop.contains(atom),
                            Literal::Negative(_) | Literal::Operation(_) => true,
                        })
                        .collect::<Vec<_>>();

                    if body
                        .iter()
                        .filter(|literal| matches!(literal, Literal::Positive(_)))
                        .next()
                        .is_none()
                    {
                        body.push(Literal::Positive(Atom::new(
                            Tag::new(DUMMY_PREDICATE.to_string()),
                            vec![Term::Primitive(Primitive::anonymous_variable())],
                        )));
                    }

                    commit.add_rule(Rule::new(head, body));
                }
                _ => commit.keep(statement),
            }
        }

        commit.submit()
    }
}
