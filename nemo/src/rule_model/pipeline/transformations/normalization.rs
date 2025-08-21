//! This module defines [Normalization].

use crate::rule_model::{
    components::{atom::Atom, literal::Literal, rule::Rule, statement::Statement, term::{operation::{operation_kind::OperationKind, Operation}, primitive::Primitive, Term}},
    error::ValidationReport,
    programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Normalizes program by resolving duplicated variables in same atom, using NumericLessThan(Eq)
/// over NumericGreaterThan(Eq)
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationNormalization {}

impl TransformationNormalization {
    fn normalize(rule: &Rule) -> Option<Rule> {
        let mut body: Vec<Literal> = Vec::new();
        let mut head: Vec<Atom> = Vec::new();
        let mut helper: Vec<Literal> = Vec::new();
        let mut count = 0;

        for literal in rule.body() {
            let mut terms: Vec<Term> = Vec::new();

            for term in literal.terms() {
                if (matches!(literal, Literal::Positive(_) | Literal::Negative(_)) && term.is_ground()) || terms.contains(term) {
                    let var: Term = Self::get_fresh_variable(count);
                    count += 1;
                    terms.push(var.clone());
                    helper.push(Literal::Operation(Operation::new(OperationKind::Equal, vec![var, term.clone()])));
                } else {
                    terms.push(term.clone());
                }
            }

            match literal {
                Literal::Positive(atom) => {
                    body.push(Literal::Positive(Atom::new(atom.predicate(), terms)));
                },
                Literal::Negative(atom) => {
                    body.push(Literal::Negative(Atom::new(atom.predicate(), terms)));
                },
                Literal::Operation(op) => {
                    match op.operation_kind() {
                        OperationKind::NumericGreaterthan => {
                            body.push(Literal::Operation(Operation::new(
                                OperationKind::NumericLessthan, vec![terms[1].clone(), terms[0].clone()])));
                        },
                        OperationKind::NumericGreaterthaneq => {
                            body.push(Literal::Operation(Operation::new(
                                OperationKind::NumericLessthaneq, vec![terms[1].clone(), terms[0].clone()])));
                        },
                        kind => {
                            body.push(Literal::Operation(Operation::new(kind, terms)));
                        }
                    }
                }
            }
        }

        for atom in rule.head() {
            let mut terms: Vec<Term> = Vec::new();

            for term in atom.terms() {
                if term.is_ground() || terms.contains(term) {
                    let var: Term = Self::get_fresh_variable(count);
                    count += 1;
                    terms.push(var.clone());
                    helper.push(Literal::Operation(Operation::new(OperationKind::Equal, vec![var, term.clone()])));
                } else {
                    terms.push(term.clone());
                }
            }

            head.push(Atom::new(atom.predicate(), terms));
        }

        if helper.is_empty() {
            None
        } else {
            body.extend(helper);
            Some(Rule::new(head, body))
        }
    }

    fn get_fresh_variable(idx: usize) -> Term {
        Term::Primitive(Primitive::universal_variable(format!("_{idx}").as_str()))
    }
}

impl ProgramTransformation for TransformationNormalization {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if let Some(mod_rule) = Self::normalize(rule) {
                        commit.add_rule(mod_rule);
                    } else {
                        commit.keep(rule);
                    }
                }
                Statement::Fact(fact) => {
                    commit.keep(fact);
                }
                Statement::Import(import) => {
                    commit.keep(import);
                }
                Statement::Export(_) | Statement::Output(_) | Statement::Parameter(_) => {
                    commit.keep(statement)
                }
            }
        }

        commit.submit()
    }
}
