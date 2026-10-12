//! Functionality for the msa check
use crate::transformations::{
    crit_instance::TransformationCriticalInstance, msa::TransformationMSA,
};
use nemo::execution::DefaultExecutionEngine;
use nemo::execution::ExecutionEngine;
use nemo::io::{import_manager::ImportManager, resource_providers::ResourceProviders};
use nemo::rule_model::programs::handle::ProgramHandle;

pub async fn msa_execution_engine_from_handle(mut handle: ProgramHandle) -> DefaultExecutionEngine {
    handle = handle
        .transform(TransformationCriticalInstance)
        .expect("TransformationCriticalInstance Error")
        .transform(TransformationMSA::default())
        .expect("TransformationMSA Error");

    handle = ProgramHandle::from(handle.materialize());

    let import_manager: ImportManager = ImportManager::new(ResourceProviders::empty());
    ExecutionEngine::initialize(handle, import_manager)
        .await
        .expect("ExecutionEngine initialization failed")
}
