//! Functionality for the msa check

use nemo::error::Error;
use nemo::execution::DefaultExecutionEngine;
use nemo::execution::ExecutionEngine;
use nemo::io::{import_manager::ImportManager, resource_providers::ResourceProviders};
use nemo::rule_model::components::rule::Rule;
use nemo::rule_model::error::ValidationReport;
use nemo::rule_model::pipeline::{
    commit::ProgramCommit, transformations::msa::TransformationMSA, ProgramPipeline,
};
use nemo::rule_model::programs::{handle::ProgramHandle, program::Program, ProgramWrite};

fn msa_execution_engine_from_rules(rules: Vec<Rule>) -> DefaultExecutionEngine {
    let mut commit: ProgramCommit =
        ProgramCommit::empty(ProgramPipeline::new(), ValidationReport::default());
    rules.into_iter().for_each(|rule| {
        commit.add_rule(rule);
    });
    let mut prog_hand: ProgramHandle = commit.submit().expect("no errors possible");
    prog_hand = prog_hand
        .transform(TransformationMSA::default())
        .expect("no errors possible");
    let prog: Program = prog_hand.materialize();
    let import_manager: ImportManager = ImportManager::new(ResourceProviders::empty());
    ExecutionEngine::initialize(prog, import_manager).expect("no errors possible")
}
