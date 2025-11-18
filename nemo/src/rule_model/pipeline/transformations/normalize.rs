//! This module defines [TransformationNormalize].

use crate::rule_model::{
    components::{
        atom::Atom,
        literal::Literal,
        rule::Rule,
        term::{
            Term,
            operation::{Operation, operation_kind::OperationKind},
            primitive::{
                Primitive,
                variable::{Variable, universal::UniversalVariable},
            },
        },
    },
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// Normalization transformation.
///
/// This transformation normalizes the program for the benefit of
/// later transformations. Currently, this only ensures that all terms
/// in positive body atoms are variables.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransformationNormalize {
    next_fresh_variable: usize,
}

impl TransformationNormalize {
    fn fresh_variable(&mut self) -> Variable {
        let index = self.next_fresh_variable;
        self.next_fresh_variable += 1;
        Variable::Universal(UniversalVariable::new(&format!("_{index}")))
    }

    fn normalize_rule(&mut self, rule: &Rule) -> Option<Rule> {
        let mut modified = false;
        let mut new_body = Vec::new();

        for literal in rule.body() {
            match literal {
                Literal::Positive(atom) => {
                    let mut terms = Vec::new();
                    let mut new_equalities = Vec::new();

                    for term in atom.terms() {
                        match term {
                            Term::Primitive(primitive) => match primitive {
                                Primitive::Variable(_) => terms.push(term.clone()),
                                Primitive::Ground(ground_term) => {
                                    let variable = self.fresh_variable();
                                    terms.push(Term::Primitive(Primitive::Variable(
                                        variable.clone(),
                                    )));
                                    new_equalities.push((
                                        variable,
                                        Term::Primitive(Primitive::Ground(ground_term.clone())),
                                    ));
                                }
                            },
                            Term::Aggregate(aggregate) => {
                                let variable = self.fresh_variable();
                                terms.push(Term::Primitive(Primitive::Variable(variable.clone())));
                                new_equalities.push((variable, Term::Aggregate(aggregate.clone())));
                            }
                            Term::FunctionTerm(function_term) => {
                                let variable = self.fresh_variable();
                                terms.push(Term::Primitive(Primitive::Variable(variable.clone())));
                                new_equalities
                                    .push((variable, Term::FunctionTerm(function_term.clone())));
                            }
                            Term::Map(map) => {
                                let variable = self.fresh_variable();
                                terms.push(Term::Primitive(Primitive::Variable(variable.clone())));
                                new_equalities.push((variable, Term::Map(map.clone())));
                            }
                            Term::Operation(operation) => {
                                let variable = self.fresh_variable();
                                terms.push(Term::Primitive(Primitive::Variable(variable.clone())));
                                new_equalities.push((variable, Term::Operation(operation.clone())));
                            }
                            Term::Tuple(tuple) => {
                                let variable = self.fresh_variable();
                                terms.push(Term::Primitive(Primitive::Variable(variable.clone())));
                                new_equalities.push((variable, Term::Tuple(tuple.clone())));
                            }
                        }
                    }

                    if new_equalities.is_empty() {
                        new_body.push(literal.clone())
                    } else {
                        modified = true;

                        for (variable, term) in new_equalities {
                            new_body.push(Literal::Operation(Operation::new(
                                OperationKind::Equal,
                                vec![Term::Primitive(Primitive::Variable(variable)), term],
                            )));
                        }

                        new_body.push(Literal::Positive(Atom::new(
                            atom.predicate(),
                            terms.clone(),
                        )))
                    }
                }
                Literal::Negative(_) | Literal::Operation(_) => new_body.push(literal.clone()),
            }
        }

        modified.then(|| Rule::new(rule.head().to_vec(), new_body))
    }
}

impl ProgramTransformation for TransformationNormalize {
    fn apply(mut self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            match statement {
                crate::rule_model::components::statement::Statement::Rule(rule) => {
                    if let Some(new_rule) = self.normalize_rule(rule) {
                        log::debug!("normalizing {rule} to {new_rule}");
                        commit.add_rule(new_rule)
                    } else {
                        commit.keep(rule)
                    }
                }
                _ => commit.keep(statement),
            }
        }

        commit.submit()
    }
}
