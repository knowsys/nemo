//! This module implements [TransformationEmpty].

use crate::rule_model::{
    components::{
        atom::Atom, fact::Fact, literal::Literal, statement::Statement, tag::Tag, term::Term,
    },
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Adds an additional atom to rules with an empty positive body.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransformationEmpty {}

impl TransformationEmpty {
    /// Create a new [TransformationEmpty].
    pub fn new() -> Self {
        Self {}
    }

    /// Predicate used in placeholder atom
    fn empty_predicate() -> Tag {
        Tag::new(String::from("_EMPTY"))
    }

    /// Fact used so that placeholder atom has matches
    fn empty_fact() -> Fact {
        let empty_term = Term::constant("_empty");
        Fact::new(Self::empty_predicate(), vec![empty_term])
    }

    /// Placeholder atom used in rules with empty positive body
    fn empty_atom() -> Atom {
        let empty_variable = Term::universal_variable("empty");
        Atom::new(Self::empty_predicate(), vec![empty_variable])
    }
}

impl ProgramTransformation for TransformationEmpty {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let mut empty_body = false;

        for statement in program.statements() {
            if let Statement::Rule(rule) = statement {
                if rule.body_positive().next().is_none() {
                    let mut new_rule = rule.clone();

                    new_rule
                        .body_mut()
                        .push(Literal::Positive(Self::empty_atom()));

                    commit.add_rule(new_rule);
                    empty_body = true;
                } else {
                    commit.keep(statement);
                }
            } else {
                commit.keep(statement);
            }
        }

        if empty_body {
            commit.add_fact(Self::empty_fact());
        }

        commit.submit()
    }
}
