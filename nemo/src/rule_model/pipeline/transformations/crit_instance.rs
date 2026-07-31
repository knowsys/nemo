use super::ProgramTransformation;
use crate::rule_model::components::{
    fact::Fact, rule::Rule, statement::Statement, tag::Tag, term::Term,
};
use crate::rule_model::error::ValidationReport;
use crate::rule_model::programs::handle::ProgramHandle;
use crate::rule_model::programs::{ProgramRead, ProgramWrite};

use std::collections::HashSet;

#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationCriticalInstance {}

fn critical_instance(rules: &[&Rule]) -> HashSet<Fact> {
    let star_term: Term = Term::from("__STAR__");
    let predicates_and_lens: HashSet<(&Tag, usize)> = rules
        .iter()
        .flat_map(|rule| rule.predicates_ref_and_lens())
        .collect();
    predicates_and_lens
        .into_iter()
        .map(|(pred, len)| {
            let terms: Vec<Term> = vec![star_term.clone(); len];
            Fact::from((pred, terms))
        })
        .collect()
}

impl ProgramTransformation for TransformationCriticalInstance {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let mut rules: Vec<&Rule> = Vec::new();

        for stmt in program.statements() {
            if let Statement::Rule(rule) = stmt {
                rules.push(rule);
                commit.keep(stmt)
            }
        }

        let crit_inst: HashSet<Fact> = critical_instance(&rules);
        crit_inst.into_iter().for_each(|fact| {
            commit.add_fact(fact);
        });

        commit.submit()
    }
}
