use super::ProgramTransformation;
use crate::rule_model::components::IterablePrimitives;
use crate::rule_model::components::{
    fact::Fact, literal::Literal, rule::Rule, statement::Statement, tag::Tag, term::Term,
};
use crate::rule_model::error::ValidationReport;
use crate::rule_model::programs::{ProgramRead, ProgramWrite, handle::ProgramHandle};

use itertools::Itertools;
use std::collections::HashSet;

#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationCriticalInstance {}

fn preds_and_lens_of_rule(rule: &Rule) -> impl Iterator<Item = (&Tag, usize)> {
    rule.body()
        .iter()
        .filter_map(|literal| match literal {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom),
            _ => None,
        })
        .chain(rule.head().iter())
        .map(|atom| (atom.predicate_ref(), atom.len()))
}

pub fn preds_and_lens_of_rules<'a>(rules: &[&'a Rule]) -> impl Iterator<Item = (&'a Tag, usize)> {
    rules.iter().flat_map(|rule| preds_and_lens_of_rule(rule))
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
    let mut constants: Vec<Term> = rules
        .iter()
        .flat_map(|rule| rule.primitive_terms())
        .filter(|primitive| primitive.is_ground())
        .cloned()
        .map(Term::from)
        .collect();
    constants.push(Term::from("__STAR__"));

    // let predicates_and_lens: HashSet<(Tag, usize)> = rules
    //     .iter()
    //     .flat_map(|rule| preds_and_lens_of_rule(rule))
    //     .collect();
    let preds_and_lens: HashSet<(&Tag, usize)> = preds_and_lens_of_rules(rules).collect();

    preds_and_lens
        .into_iter()
        .flat_map(move |(predicate, arity)| {
            facts_for_predicate_and_constants(predicate, arity, &constants)
        })
}

impl ProgramTransformation for TransformationCriticalInstance {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let rules: Vec<&Rule> = program
            .statements()
            .filter_map(|stmt| {
                if let Statement::Rule(rule) = stmt {
                    commit.keep(stmt);
                    return Some(rule);
                }
                None
            })
            .collect();

        critical_instance(&rules).for_each(|fact| {
            commit.add_fact(fact);
        });

        commit.submit()
    }
}
