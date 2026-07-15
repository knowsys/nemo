//! This module defines [TransformationN3FixupNegation].

use std::{collections::HashSet, iter::chain};

use crate::{
    rule_model::{
        components::{
            IterableVariables,
            atom::Atom,
            literal::Literal,
            rule::Rule,
            statement::Statement,
            tag::Tag,
            term::{Term, primitive::Primitive},
        },
        error::ValidationReport,
        programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    },
    syntax::n3::translation::NEGATION_PREDICATE,
};

use super::ProgramTransformation;

/// FixupNegation transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationN3FixupNegation {
    predicate_index: usize,
}

impl TransformationN3FixupNegation {
    /// Create a new [TransformationN3FixupNegation].
    pub fn new() -> Self {
        Self { predicate_index: 0 }
    }

    fn fresh_predicate(&mut self) -> Tag {
        let index = self.predicate_index;
        self.predicate_index += 1;

        Tag::new(format!("{NEGATION_PREDICATE}_{index}"))
    }
}

impl ProgramTransformation for TransformationN3FixupNegation {
    fn apply(mut self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    let head = rule.head().clone();
                    let mut body = chain(
                        rule.body_positive().cloned().map(Literal::Positive),
                        rule.body_operations().cloned().map(Literal::Operation),
                    )
                    .collect::<Vec<_>>();
                    let negative_body = rule.body_negative().cloned().collect::<Vec<_>>();

                    if negative_body.is_empty() {
                        commit.keep(rule);
                    } else {
                        let variables = negative_body
                            .iter()
                            .flat_map(|atom| atom.variables().cloned())
                            .collect::<HashSet<_>>();
                        let helper_atom = Atom::new(
                            self.fresh_predicate(),
                            variables
                                .into_iter()
                                .map(|variable| Term::Primitive(Primitive::Variable(variable)))
                                .collect::<Vec<_>>(),
                        );
                        body.push(Literal::Negative(helper_atom.clone()));
                        let helper_rule = Rule::new(
                            vec![helper_atom],
                            negative_body.into_iter().map(Literal::Positive).collect(),
                        );

                        commit.add_rule(helper_rule);
                        commit.add_rule(Rule::new(head, body));
                    }
                }
                _ => commit.keep(statement),
            }
        }

        commit.submit()
    }
}
