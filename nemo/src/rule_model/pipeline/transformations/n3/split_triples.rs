//! This module defines [TransformationN3SplitTriples].

use std::collections::HashSet;

use crate::{
    rule_model::{
        components::{
            atom::Atom,
            fact::Fact,
            literal::Literal,
            rule::Rule,
            statement::Statement,
            tag::Tag,
            term::{Term, primitive::Primitive},
        },
        error::ValidationReport,
        programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    },
    syntax::n3::translation::{IMPORTS_PREDICATE, TRIPLES_PREDICATE},
};

use super::{ProgramTransformation, tag_from_term};

/// SplitTriples transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationN3SplitTriples {
    have_extra_triples: bool,
}

impl TransformationN3SplitTriples {
    /// Create a new [TransformationN3SplitTriples].
    pub fn new(have_extra_triples: bool) -> Self {
        Self { have_extra_triples }
    }
}

impl ProgramTransformation for TransformationN3SplitTriples {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();
        let mut is_splittable = true;
        let mut need_to_split = false;
        let mut predicates = HashSet::new();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if rule.body_negative().next().is_some() {
                        need_to_split = true;
                    }

                    if rule.body_atoms().any(is_not_splittable)
                        || rule.head().iter().any(is_not_splittable)
                    {
                        is_splittable = false;
                    }
                }
                _ => (),
            }
        }

        if need_to_split && !is_splittable {
            log::warn!(
                "rules combine negation and variables in the predicate position, program will likely not be stratified"
            );
        }

        for statement in program.statements() {
            if is_splittable {
                match statement {
                    Statement::Rule(rule) => commit.add_rule(split_rule(&mut predicates, rule)),
                    Statement::Fact(fact) => commit.add_fact(split_fact(&mut predicates, fact)),
                    _ => commit.keep(statement),
                }
            } else {
                commit.keep(statement)
            }
        }

        if !is_splittable && self.have_extra_triples {
            let subject = Term::Primitive(Primitive::universal_variable("subject"));
            let predicate = Term::Primitive(Primitive::universal_variable("predicate"));
            let object = Term::Primitive(Primitive::universal_variable("object"));

            commit.add_rule(Rule::new(
                vec![Atom::new(
                    Tag::new(TRIPLES_PREDICATE.to_string()),
                    vec![subject.clone(), predicate.clone(), object.clone()],
                )],
                vec![Literal::Positive(Atom::new(
                    Tag::new(IMPORTS_PREDICATE.to_string()),
                    vec![subject.clone(), predicate.clone(), object.clone()],
                ))],
            ));
        }

        for predicate in predicates {
            let tag = Tag::new(TRIPLES_PREDICATE.to_string());
            let subject = Term::Primitive(Primitive::universal_variable("subject"));
            let object = Term::Primitive(Primitive::universal_variable("object"));

            commit.add_rule(Rule::new(
                vec![Atom::new(
                    tag.clone(),
                    vec![
                        subject.clone(),
                        Term::Primitive(Primitive::constant(&predicate.name())),
                        object.clone(),
                    ],
                )],
                vec![Literal::Positive(Atom::new(
                    predicate.clone(),
                    vec![subject.clone(), object.clone()],
                ))],
            ));

            if self.have_extra_triples {
                let tag = Tag::new(IMPORTS_PREDICATE.to_string());
                commit.add_rule(Rule::new(
                    vec![Atom::new(
                        predicate.clone(),
                        vec![subject.clone(), object.clone()],
                    )],
                    vec![Literal::Positive(Atom::new(
                        tag.clone(),
                        vec![
                            subject.clone(),
                            Term::Primitive(Primitive::constant(&predicate.name())),
                            object.clone(),
                        ],
                    ))],
                ));
            }
        }

        commit.submit()
    }
}

fn is_not_splittable(atom: &Atom) -> bool {
    atom.predicate().name() == TRIPLES_PREDICATE && tag_from_term(&atom[1]).is_none()
}

fn split_fact(predicates: &mut HashSet<Tag>, fact: &Fact) -> Fact {
    if fact.predicate().name() != TRIPLES_PREDICATE {
        return fact.clone();
    }

    let subject = fact[0].clone();
    let predicate = tag_from_term(&fact[1]).expect("is an IRI");
    let object = fact[2].clone();
    predicates.insert(predicate.clone());

    Fact::new(predicate, vec![subject, object])
}

fn split_atom(predicates: &mut HashSet<Tag>, atom: &Atom) -> Atom {
    if atom.predicate().name() != TRIPLES_PREDICATE {
        return atom.clone();
    }

    let subject = atom[0].clone();
    let predicate = tag_from_term(&atom[1]).expect("is an IRI");
    let object = atom[2].clone();
    predicates.insert(predicate.clone());

    Atom::new(predicate, vec![subject, object])
}

fn split_rule(predicates: &mut HashSet<Tag>, rule: &Rule) -> Rule {
    let mut body = rule
        .body_positive()
        .map(|atom| Literal::Positive(split_atom(predicates, atom)))
        .collect::<Vec<_>>();
    body.extend(
        rule.body_negative()
            .map(|atom| Literal::Negative(split_atom(predicates, atom))),
    );
    let head = rule
        .head()
        .iter()
        .map(|atom| split_atom(predicates, atom))
        .collect();

    Rule::new(head, body)
}
