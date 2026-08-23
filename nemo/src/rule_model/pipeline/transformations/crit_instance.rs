//! This module defines [TransformationCriticalInstance].

use std::collections::HashSet;

use itertools::Itertools;

use super::ProgramTransformation;
use crate::rule_model::{
    components::{
        IterablePrimitives, fact::Fact, literal::Literal, rule::Rule, statement::Statement,
        tag::Tag, term::Term,
    },
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

/// Program transformation that replaces a program's facts with its critical instance.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationCriticalInstance;

fn predicates_and_arities(rule: &Rule) -> impl Iterator<Item = (&Tag, usize)> {
    rule.body()
        .iter()
        .filter_map(|literal| match literal {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom),
            Literal::Operation(_) => None,
        })
        .chain(rule.head())
        .map(|atom| (atom.predicate_ref(), atom.len()))
}

/// Return predicate and arity pairs for all atoms in the provided rules.
pub fn preds_and_lens_of_rules<'a>(rules: &[&'a Rule]) -> impl Iterator<Item = (&'a Tag, usize)> {
    rules.iter().flat_map(|rule| predicates_and_arities(rule))
}

/// Return every fact for the given predicate and arity that can be formed
/// from the provided constants.
pub fn facts_for_predicate_and_constants(
    predicate: &Tag,
    arity: usize,
    constants: &[Term],
) -> HashSet<Fact> {
    (0..arity)
        .map(|_| constants.iter().cloned())
        .multi_cartesian_product()
        .map(|terms| Fact::new(predicate.clone(), terms))
        .collect()
}

fn critical_instance(rules: &[&Rule]) -> impl Iterator<Item = Fact> {
    let mut constants = rules
        .iter()
        .flat_map(|rule| rule.primitive_terms())
        .filter(|primitive| primitive.is_ground())
        .cloned()
        .map(Term::from)
        .collect::<Vec<_>>();
    constants.push(Term::from("__STAR__"));

    let predicates_and_arities = preds_and_lens_of_rules(rules).collect::<HashSet<_>>();

    predicates_and_arities
        .into_iter()
        .flat_map(move |(predicate, arity)| {
            facts_for_predicate_and_constants(predicate, arity, &constants)
        })
}

impl ProgramTransformation for TransformationCriticalInstance {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let rules = program
            .statements()
            .filter_map(|statement| {
                if let Statement::Rule(rule) = statement {
                    commit.keep(statement);
                    Some(rule)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for fact in critical_instance(&rules) {
            commit.add_fact(fact);
        }

        commit.submit()
    }
}
