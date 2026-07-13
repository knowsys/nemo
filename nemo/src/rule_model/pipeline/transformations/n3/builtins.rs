//! This module defines [TransformationN3Builtins].

use crate::{
    rule_model::{
        components::{
            atom::Atom,
            literal::Literal,
            rule::Rule,
            statement::Statement,
            term::operation::{Operation, operation_kind::OperationKind},
        },
        error::ValidationReport,
        pipeline::transformations::n3::tag_from_term,
        programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    },
    syntax::n3::{iri::builtins::MATH_NOT_LESS_THAN, translation::TRIPLES_PREDICATE},
};

use super::ProgramTransformation;

/// Builtins transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationN3Builtins {}

impl TransformationN3Builtins {
    /// Create a new [TransformationN3Builtins].
    pub fn new() -> Self {
        Self {}
    }
}

impl ProgramTransformation for TransformationN3Builtins {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    let head = rule.head().clone();
                    let mut body = Vec::new();

                    for literal in rule.body() {
                        match literal {
                            Literal::Positive(atom)
                                if atom.predicate().name() == TRIPLES_PREDICATE =>
                            {
                                body.append(&mut translate_builtin(atom))
                            }
                            Literal::Positive(_) => body.push(literal.clone()),
                            Literal::Negative(_) => body.push(literal.clone()),
                            Literal::Operation(_) => body.push(literal.clone()),
                        }
                    }

                    commit.add_rule(Rule::new(head, body));
                }
                _ => commit.keep(statement),
            }
        }

        commit.submit()
    }
}

fn translate_builtin(atom: &Atom) -> Vec<Literal> {
    let mut literals = Vec::new();
    let terms = atom.terms().collect::<Vec<_>>();
    assert_eq!(terms.len(), 3);

    let subject = terms[0].clone();
    let object = terms[2].clone();

    if let Some(predicate) = tag_from_term(terms[1]) {
        match predicate.name() {
            MATH_NOT_LESS_THAN => literals.push(Literal::Operation(Operation::new(
                OperationKind::NumericGreaterthaneq,
                vec![subject, object],
            ))),
            _ => literals.push(Literal::Positive(atom.clone())),
        }
    } else {
        literals.push(Literal::Positive(atom.clone()));
    }

    literals
}
