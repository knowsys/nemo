//! Functionality for the msa check

use nemo::execution::DefaultExecutionEngine;
use nemo::execution::ExecutionEngine;
use nemo::io::{import_manager::ImportManager, resource_providers::ResourceProviders};
use nemo::rule_model::components::{fact::Fact, rule::Rule, tag::Tag, term::Term};
use nemo::rule_model::error::ValidationReport;
use nemo::rule_model::pipeline::{
    ProgramPipeline, commit::ProgramCommit, transformations::msa::TransformationMSA,
};
use nemo::rule_model::programs::{ProgramWrite, handle::ProgramHandle, program::Program};

use std::collections::HashSet;

fn critical_instance(rules: &[Rule]) -> HashSet<Fact> {
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

pub async fn msa_execution_engine_from_rules(rules: &[Rule]) -> DefaultExecutionEngine {
    let mut commit: ProgramCommit =
        ProgramCommit::empty(ProgramPipeline::new(), ValidationReport::default());
    rules.iter().for_each(|rule| {
        commit.add_rule(rule.clone());
    });
    let crit_inst: HashSet<Fact> = critical_instance(rules);
    crit_inst.into_iter().for_each(|fact| {
        commit.add_fact(fact);
    });
    let mut prog_hand: ProgramHandle = commit.submit().expect("no errors possible");
    prog_hand = prog_hand
        .transform(TransformationMSA::default())
        .expect("no errors possible");
    let prog: Program = prog_hand.materialize();
    // println!("{:#?}", prog.all_rules());
    let import_manager: ImportManager = ImportManager::new(ResourceProviders::empty());
    ExecutionEngine::initialize(prog, import_manager)
        .await
        .expect("no errors possible")
}
