//! Functionality for the msa check

use nemo::execution::DefaultExecutionEngine;
use nemo::execution::ExecutionEngine;
use nemo::io::{import_manager::ImportManager, resource_providers::ResourceProviders};
use nemo::rule_model::pipeline::transformations::{
    crit_instance::TransformationCriticalInstance, msa::TransformationMSA,
};
use nemo::rule_model::programs::{handle::ProgramHandle, program::Program};

pub async fn msa_execution_engine_from_handle(mut handle: ProgramHandle) -> DefaultExecutionEngine {
    handle = handle
        .transform(TransformationCriticalInstance::default())
        .expect("TransformationCriticalInstance Error")
        .transform(TransformationMSA::default())
        .expect("TransformationMSA Error");
    let prog: Program = handle.materialize();
    let import_manager: ImportManager = ImportManager::new(ResourceProviders::empty());
    ExecutionEngine::initialize(prog, import_manager)
        .await
        .expect("ExecutionEngine initialization failed")
}
